"""Clustering regression test.

Compares Scala/Spark transformation clustering output against Rust clustering
output for partition equivalence, testing both full and incremental paths.

Test flow:
1. Ingest all blocks [0, end_block] to raw Cassandra + Delta Lake
2. Run Scala full transformation on all blocks → reference clusters
3. Run Rust clustering on blocks [0, initial_end_block] → initial clusters
4. Run Rust incremental clustering on blocks [initial_end_block+1, end_block]
5. Compare Rust final clusters vs Scala reference → must be partition-equivalent

Requires:
- Docker (for MinIO, Cassandra testcontainers, and Scala transformation container)
- Node URLs configured in .graphsense.yaml
- ``CLUSTERING_CURRENCIES`` env var (default: btc only)
- ``gs_clustering`` Rust module installed (build with: make build-rust)
- Scala transformation repo as sibling (or ``CLUSTERING_SCALA_IMAGE`` env var)
"""

import pytest

from tests.clustering.config import ClusteringConfig
from tests.clustering.ingest_runner import (
    _create_transformed_keyspace,
    read_scala_clusters,
    run_exchange_rates_ingest,
    run_ingest_cassandra_raw,
    run_ingest_delta_only,
    run_rust_clustering,
    run_rust_clustering_incremental,
    run_scala_transformation,
)

pytestmark = pytest.mark.clustering


def compare_cluster_partitions(
    scala_clusters: dict[int, set[int]],
    rust_mapping: dict[int, int],
) -> tuple[bool, list[str]]:
    """Compare two clusterings for partition equivalence.

    The comparison works by building a canonical representative for each address
    (the minimum address_id in its cluster). Two partitions are equivalent iff
    every address maps to the same canonical representative in both systems.
    """
    # Build Scala: address_id -> canonical representative
    scala_repr: dict[int, int] = {}
    for _cluster_id, members in scala_clusters.items():
        representative = min(members)
        for addr_id in members:
            scala_repr[addr_id] = representative

    # Build Rust: address_id -> canonical representative
    rust_clusters: dict[int, set[int]] = {}
    for addr_id, cluster_id in rust_mapping.items():
        rust_clusters.setdefault(cluster_id, set()).add(addr_id)

    rust_repr: dict[int, int] = {}
    for _cluster_id, members in rust_clusters.items():
        representative = min(members)
        for addr_id in members:
            rust_repr[addr_id] = representative

    # Compare over the union of all addresses
    all_addrs = set(scala_repr.keys()) | set(rust_repr.keys())
    mismatches = []
    for addr_id in sorted(all_addrs):
        s = scala_repr.get(addr_id)
        r = rust_repr.get(addr_id)
        if s != r:
            mismatches.append(f"addr {addr_id}: scala_rep={s}, rust_rep={r}")

    return len(mismatches) == 0, mismatches


def _cluster_stats(clusters: dict[int, set[int]]) -> str:
    total_addrs = sum(len(m) for m in clusters.values())
    non_singleton = sum(1 for m in clusters.values() if len(m) > 1)
    return f"{len(clusters)} clusters, {total_addrs} addresses, {non_singleton} non-singleton"


class TestClustering:
    """Scala/Spark clustering must produce the same partition as Rust clustering."""

    def test_scala_vs_rust_incremental_clustering(
        self,
        clustering_config: ClusteringConfig,
        minio_config: dict[str, str],
        cassandra_coords: tuple[str, int],
        current_venv,
        scala_transformation_image: str,
    ):
        pytest.importorskip(
            "gs_clustering",
            reason="gs_clustering not installed (build with: make build-rust)",
        )

        currency = clustering_config.currency
        range_id = clustering_config.range_id
        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        # Unique keyspace names
        ks_raw = f"clust_{currency}_{range_id}_raw"
        ks_transformed = f"clust_{currency}_{range_id}_transformed"
        delta_path = f"s3://{bucket}/{currency}/{range_id}"

        initial_end = clustering_config.initial_end_block
        final_end = clustering_config.end_block

        print(f"\n{'=' * 68}")
        print(f"CLUSTERING: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:          "
            f"{clustering_config.start_block:,}-{final_end:,} "
            f"({clustering_config.num_blocks} blocks)"
        )
        print(f"  initial (full):  0-{initial_end:,}")
        print(f"  incremental:     {initial_end + 1:,}-{final_end:,}")
        if clustering_config.range_note:
            print(f"  note:            {clustering_config.range_note}")

        # ------------------------------------------------------------------
        # Step 1: Ingest all blocks to Delta Lake
        # ------------------------------------------------------------------
        print("  [1/7] delta-only ingest ...", end=" ", flush=True)
        run_ingest_delta_only(
            venv_dir=current_venv,
            config=clustering_config,
            delta_directory=delta_path,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 2: Ingest all blocks to Cassandra raw
        # ------------------------------------------------------------------
        print("  [2/7] raw Cassandra ingest ...", end=" ", flush=True)
        run_ingest_cassandra_raw(
            venv_dir=current_venv,
            config=clustering_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_raw,
        )
        print("done")

        # Ingest real exchange rates (Scala transform drops blocks without rates)
        print("  [2b/7] exchange rates ingest ...", end=" ", flush=True)
        run_exchange_rates_ingest(
            venv_dir=current_venv,
            config=clustering_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_raw,
        )
        print("done")

        # Create transformed keyspace schema for Scala to write to
        _create_transformed_keyspace(cass_host, cass_port, ks_transformed)

        # ------------------------------------------------------------------
        # Step 3: Run Scala full transformation on ALL blocks → reference
        # ------------------------------------------------------------------
        print("  [3/7] Scala full transformation ...", end=" ", flush=True)
        run_scala_transformation(
            image_name=scala_transformation_image,
            config=clustering_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            raw_keyspace=ks_raw,
            transformed_keyspace=ks_transformed,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 4: Read Scala reference clusters
        # ------------------------------------------------------------------
        print("  [4/7] reading Scala clusters ...", end=" ", flush=True)
        scala_clusters = read_scala_clusters(cass_host, cass_port, ks_transformed)
        print(f"done ({_cluster_stats(scala_clusters)})")

        # ------------------------------------------------------------------
        # Step 5: Rust full clustering on blocks [0, initial_end_block]
        # ------------------------------------------------------------------
        print(
            f"  [5/7] Rust clustering blocks 0-{initial_end:,} ...",
            end=" ", flush=True,
        )
        initial_mapping = run_rust_clustering(
            cass_host, cass_port, ks_raw, ks_transformed,
            max_block_id=initial_end,
        )
        initial_clusters: dict[int, set[int]] = {}
        for aid, cid in initial_mapping.items():
            initial_clusters.setdefault(cid, set()).add(aid)
        print(f"done ({_cluster_stats(initial_clusters)})")

        # ------------------------------------------------------------------
        # Step 6: Rust incremental clustering adding [initial_end+1, final_end]
        # ------------------------------------------------------------------
        print(
            f"  [6/7] Rust incremental blocks {initial_end + 1:,}-{final_end:,} ...",
            end=" ", flush=True,
        )
        final_mapping = run_rust_clustering_incremental(
            cass_host, cass_port, ks_raw, ks_transformed,
            existing_mapping=initial_mapping,
            min_block_id=initial_end + 1,
            max_block_id=final_end,
        )
        final_clusters: dict[int, set[int]] = {}
        for aid, cid in final_mapping.items():
            final_clusters.setdefault(cid, set()).add(aid)
        print(f"done ({_cluster_stats(final_clusters)})")

        # ------------------------------------------------------------------
        # Step 7: Compare partitions
        # ------------------------------------------------------------------
        print("\n  Partition comparison:")
        is_equivalent, mismatches = compare_cluster_partitions(
            scala_clusters, final_mapping
        )

        if is_equivalent:
            print(f"  result:          PASS (partitions equivalent)")
            print(f"{'=' * 68}")
        else:
            print(f"  result:          FAIL ({len(mismatches)} mismatches)")
            for m in mismatches[:20]:
                print(f"    {m}")
            if len(mismatches) > 20:
                print(f"    ... and {len(mismatches) - 20} more")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] Clustering partition mismatch:\n"
                + "\n".join(f"  - {m}" for m in mismatches[:50])
            )
