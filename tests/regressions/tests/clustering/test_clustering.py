"""Clustering regression test.

Compares Scala/Spark transformation clustering output against Rust clustering
output for partition equivalence.

The Scala and Rust clusterings assign different cluster IDs. The test compares
cluster MEMBERSHIPS: for every address, the set of addresses in its cluster
must be identical in both systems.

Requires:
- Docker (for MinIO, Cassandra testcontainers, and Scala transformation container)
- Node URLs configured in .graphsense.yaml
- ``CLUSTERING_CURRENCIES`` env var (default: btc only)
- ``CLUSTERING_SCALA_IMAGE`` env var (Docker image for full Scala/Spark transformation)
- ``gs_clustering`` Rust module installed (build with: make build-rust)
"""

import pytest

from tests.clustering.config import ClusteringConfig
from tests.clustering.ingest_runner import (
    _create_transformed_keyspace,
    _seed_exchange_rates,
    read_scala_clusters,
    run_ingest_cassandra_raw,
    run_ingest_delta_only,
    run_rust_clustering,
    run_scala_transformation,
)

pytestmark = pytest.mark.clustering


def compare_cluster_partitions(
    scala_clusters: dict[int, set[int]],
    rust_mapping: dict[int, int],
) -> tuple[bool, list[str]]:
    """Compare two clusterings for partition equivalence.

    Args:
        scala_clusters: cluster_id -> set of address_ids (from Scala)
        rust_mapping: address_id -> cluster_id (from Rust)

    Returns:
        (is_equivalent, list of mismatch descriptions)

    The comparison works by building a canonical representative for each address
    (the minimum address_id in its cluster). Two partitions are equivalent iff
    every address maps to the same canonical representative in both systems.
    """
    # Build Scala mapping: address_id -> canonical representative (min addr in cluster)
    scala_repr: dict[int, int] = {}
    for _cluster_id, members in scala_clusters.items():
        representative = min(members)
        for addr_id in members:
            scala_repr[addr_id] = representative

    # Build Rust mapping: address_id -> canonical representative
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


class TestClustering:
    """Scala/Spark clustering must produce the same partition as Rust clustering."""

    def test_scala_vs_rust_clustering(
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

        print(f"\n{'=' * 68}")
        print(f"CLUSTERING: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:          "
            f"{clustering_config.start_block:,}-"
            f"{clustering_config.end_block:,} "
            f"({clustering_config.num_blocks} blocks)"
        )
        if clustering_config.range_note:
            print(f"  note:            {clustering_config.range_note}")

        # ------------------------------------------------------------------
        # Step 1: Ingest raw data to Delta Lake
        # ------------------------------------------------------------------
        print("  [1/5] delta-only ingest ...", end=" ", flush=True)
        run_ingest_delta_only(
            venv_dir=current_venv,
            config=clustering_config,
            delta_directory=delta_path,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 2: Ingest raw data to Cassandra (for Rust to read tx inputs)
        # ------------------------------------------------------------------
        print("  [2/5] raw Cassandra ingest ...", end=" ", flush=True)
        run_ingest_cassandra_raw(
            venv_dir=current_venv,
            config=clustering_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_raw,
        )
        print("done")

        # Seed dummy exchange rates and create transformed keyspace
        # (Scala transform requires both to exist before it runs)
        _seed_exchange_rates(cass_host, cass_port, ks_raw)
        _create_transformed_keyspace(cass_host, cass_port, ks_transformed)

        # ------------------------------------------------------------------
        # Step 3: Run Scala transformation -> transformed keyspace with clusters
        # ------------------------------------------------------------------
        print("  [3/5] Scala transformation ...", end=" ", flush=True)
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
        # Step 4: Read Scala clusters from cluster_addresses
        # ------------------------------------------------------------------
        print("  [4/5] reading Scala clusters ...", end=" ", flush=True)
        scala_clusters = read_scala_clusters(
            cass_host, cass_port, ks_transformed
        )
        scala_total_addrs = sum(len(m) for m in scala_clusters.values())
        scala_non_singleton = sum(
            1 for m in scala_clusters.values() if len(m) > 1
        )
        print(
            f"done ({len(scala_clusters)} clusters, "
            f"{scala_total_addrs} addresses, "
            f"{scala_non_singleton} non-singleton)"
        )

        # ------------------------------------------------------------------
        # Step 5: Run Rust clustering using Scala's address IDs + raw tx data
        # ------------------------------------------------------------------
        print("  [5/5] Rust clustering ...", end=" ", flush=True)
        rust_mapping = run_rust_clustering(
            cass_host, cass_port, ks_raw, ks_transformed
        )
        rust_clusters_by_id: dict[int, set[int]] = {}
        for aid, cid in rust_mapping.items():
            rust_clusters_by_id.setdefault(cid, set()).add(aid)
        rust_non_singleton = sum(
            1 for m in rust_clusters_by_id.values() if len(m) > 1
        )
        print(
            f"done ({len(rust_clusters_by_id)} clusters, "
            f"{len(rust_mapping)} addresses, "
            f"{rust_non_singleton} non-singleton)"
        )

        # ------------------------------------------------------------------
        # Step 6: Compare partitions
        # ------------------------------------------------------------------
        print("\n  Partition comparison:")
        is_equivalent, mismatches = compare_cluster_partitions(
            scala_clusters, rust_mapping
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
