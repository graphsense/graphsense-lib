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

import time

import pytest

from tests.clustering.config import ClusteringConfig
from tests.clustering.ingest_runner import (
    _create_transformed_keyspace,
    read_scala_clusters,
    run_exchange_rates_ingest,
    run_incremental_clustering_via_production,
    run_ingest_cassandra_raw,
    run_ingest_delta_only,
    run_rust_clustering,
    run_scala_transformation,
    write_fresh_clustering_to_cassandra,
)

pytestmark = pytest.mark.clustering


def compare_cluster_partitions(
    scala_clusters: dict[int, set[int]],
    rust_mapping: dict[int, int],
) -> tuple[bool, list[str]]:
    """Compare two clusterings for exact partition equivalence.

    The comparison canonicalizes each side as a sorted list of sorted
    address-id tuples — one tuple per cluster, keyed by the cluster's minimum
    address id — and compares the two canonical forms directly. Cluster ids
    are allowed to differ; every cluster's exact membership must match.

    Checks, in order:
      1. Both sides cover exactly the same set of address ids.
      2. Both sides produce the same number of clusters.
      3. Every cluster in Scala has an identical counterpart in Rust.
    """
    # Build rust clusters: cluster_id -> set[addr_id]
    rust_clusters: dict[int, set[int]] = {}
    for addr_id, cluster_id in rust_mapping.items():
        rust_clusters.setdefault(cluster_id, set()).add(addr_id)

    mismatches: list[str] = []

    # 1. Address-set equality
    scala_addrs = {a for m in scala_clusters.values() for a in m}
    rust_addrs = set(rust_mapping.keys())

    only_scala = scala_addrs - rust_addrs
    only_rust = rust_addrs - scala_addrs
    if only_scala:
        sample = sorted(only_scala)[:10]
        mismatches.append(
            f"{len(only_scala)} addresses in Scala missing from Rust "
            f"(first {len(sample)}: {sample})"
        )
    if only_rust:
        sample = sorted(only_rust)[:10]
        mismatches.append(
            f"{len(only_rust)} addresses in Rust missing from Scala "
            f"(first {len(sample)}: {sample})"
        )

    # 2. Canonical form: sorted tuple per cluster, then order clusters by
    #    their minimum address id.  min(cluster) is unique across clusters
    #    since each address belongs to exactly one cluster, so this is a
    #    total order.
    def canonical(
        clusters: dict[int, set[int]],
    ) -> list[tuple[int, ...]]:
        return sorted(
            (tuple(sorted(members)) for members in clusters.values()),
            key=lambda t: t[0],
        )

    scala_canon = canonical(scala_clusters)
    rust_canon = canonical(rust_clusters)

    if len(scala_canon) != len(rust_canon):
        mismatches.append(
            f"cluster count differs: scala={len(scala_canon)}, "
            f"rust={len(rust_canon)}"
        )

    # 3. Cluster-by-cluster comparison on the canonical order.  Key each
    #    cluster by its minimum address id so we can pair Scala and Rust
    #    clusters even when cluster counts differ.
    scala_by_min = {c[0]: c for c in scala_canon}
    rust_by_min = {c[0]: c for c in rust_canon}

    cluster_diffs = 0
    for min_addr in sorted(set(scala_by_min) | set(rust_by_min)):
        s = scala_by_min.get(min_addr)
        r = rust_by_min.get(min_addr)
        if s == r:
            continue
        cluster_diffs += 1
        if cluster_diffs <= 20:
            if s is None:
                mismatches.append(
                    f"cluster min={min_addr}: only in rust, members={r}"
                )
            elif r is None:
                mismatches.append(
                    f"cluster min={min_addr}: only in scala, members={s}"
                )
            else:
                s_only = sorted(set(s) - set(r))
                r_only = sorted(set(r) - set(s))
                mismatches.append(
                    f"cluster min={min_addr}: "
                    f"scala-only={s_only}, rust-only={r_only}"
                )
    if cluster_diffs > 20:
        mismatches.append(
            f"... and {cluster_diffs - 20} more cluster mismatches"
        )

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
        batch_count = clustering_config.incremental_batch_count

        # Split [initial_end+1, final_end] into N roughly-equal batches.
        # The last batch absorbs any remainder so it always reaches final_end.
        incremental_total = final_end - initial_end
        chunk = incremental_total // batch_count
        batches: list[tuple[int, int]] = []
        b_start = initial_end + 1
        for i in range(batch_count):
            b_end = final_end if i == batch_count - 1 else b_start + chunk - 1
            batches.append((b_start, b_end))
            b_start = b_end + 1

        print(f"\n{'=' * 68}")
        print(f"CLUSTERING: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:          "
            f"{clustering_config.start_block:,}-{final_end:,} "
            f"({clustering_config.num_blocks} blocks)"
        )
        print(f"  initial (full):  0-{initial_end:,}")
        print(f"  incremental:     {initial_end + 1:,}-{final_end:,} "
              f"in {batch_count} batches")
        for i, (bs, be) in enumerate(batches, 1):
            print(f"    batch {i}/{batch_count}: {bs:,}-{be:,}")
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
        assert len(scala_clusters) > 0, (
            "Scala produced no clusters — transformation may have failed silently"
        )

        # ------------------------------------------------------------------
        # Step 5: Rust full clustering on blocks [0, initial_end_block]
        # ------------------------------------------------------------------
        print(
            f"  [5/7] Rust clustering blocks 0-{initial_end:,} ...",
            end=" ", flush=True,
        )
        t0 = time.perf_counter()
        initial_mapping = run_rust_clustering(
            cass_host, cass_port, ks_raw, ks_transformed,
            max_block_id=initial_end,
        )
        full_secs = time.perf_counter() - t0
        full_blocks = initial_end + 1
        initial_clusters: dict[int, set[int]] = {}
        for aid, cid in initial_mapping.items():
            initial_clusters.setdefault(cid, set()).add(aid)
        print(
            f"done in {full_secs:.2f}s "
            f"({full_blocks / full_secs:,.0f} blocks/s, "
            f"{len(initial_mapping) / full_secs:,.0f} addrs/s) "
            f"({_cluster_stats(initial_clusters)})"
        )
        assert len(initial_mapping) > 0, (
            "Rust full clustering produced no mappings"
        )

        # Verify that the partial result (blocks 0-initial_end) does NOT match
        # Scala (all blocks).  If it already matches, the incremental step
        # would be a no-op and the test would not exercise it.
        is_initial_equiv, _ = compare_cluster_partitions(
            scala_clusters, initial_mapping
        )
        assert not is_initial_equiv, (
            f"Rust full-only (0-{initial_end}) already matches Scala "
            f"(0-{final_end}); incremental step would not be exercised"
        )

        # Seed the fresh_* tables in Cassandra so the production
        # run_incremental_clustering can read existing state from them.
        write_fresh_clustering_to_cassandra(
            cass_host, cass_port, ks_transformed, initial_mapping,
        )

        # ------------------------------------------------------------------
        # Step 6: Rust incremental clustering across multiple batches.
        #         Uses the PRODUCTION UpdateStrategyUtxo.run_fresh_clustering
        #         code path: point reads of block_transactions, transaction,
        #         and address_ids_by_address_prefix for blocks in the range,
        #         then run_incremental_clustering over the affected clusters,
        #         and apply_changes back to fresh_address_cluster /
        #         fresh_cluster_addresses.  Each batch reads the *accumulated*
        #         Cassandra state from previous batches.
        # ------------------------------------------------------------------
        current_mapping = initial_mapping
        batch_timings: list[tuple[int, float]] = []  # (num_blocks, secs)
        for i, (b_start, b_end) in enumerate(batches, 1):
            print(
                f"  [6.{i}/{batch_count}] Rust incremental "
                f"blocks {b_start:,}-{b_end:,} ...",
                end=" ", flush=True,
            )
            t0 = time.perf_counter()
            current_mapping = run_incremental_clustering_via_production(
                cass_host, cass_port, ks_raw, ks_transformed, currency,
                initial_mapping=current_mapping,
                min_block_id=b_start,
                max_block_id=b_end,
                current_venv=current_venv,
            )
            batch_secs = time.perf_counter() - t0
            batch_blocks = b_end - b_start + 1
            batch_timings.append((batch_blocks, batch_secs))
            batch_clusters: dict[int, set[int]] = {}
            for aid, cid in current_mapping.items():
                batch_clusters.setdefault(cid, set()).add(aid)
            print(
                f"done in {batch_secs:.2f}s "
                f"({batch_blocks / batch_secs:,.0f} blocks/s) "
                f"({_cluster_stats(batch_clusters)})"
            )

        total_inc_blocks = sum(b for b, _ in batch_timings)
        total_inc_secs = sum(s for _, s in batch_timings)
        print(
            f"  incremental total: {total_inc_blocks:,} blocks in "
            f"{total_inc_secs:.2f}s "
            f"({total_inc_blocks / total_inc_secs:,.0f} blocks/s avg, "
            f"{total_inc_secs / batch_count:.2f}s/batch incl. subprocess "
            f"+ Cassandra writes)"
        )

        final_mapping = current_mapping
        final_clusters: dict[int, set[int]] = {}
        for aid, cid in final_mapping.items():
            final_clusters.setdefault(cid, set()).add(aid)

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
