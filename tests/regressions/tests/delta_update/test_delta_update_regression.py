"""Delta-update regression test.

Verifies that the local graphsense-lib produces the same UTXO transformed
keyspace as a reference release (default v2.12.3), and reports per-version
delta-update wall-clock time so we can track perf regressions / improvements.

Test flow per currency/range:
1. Ingest blocks [start, end] from the configured node into Delta Lake (MinIO).
2. PySpark transformation: Delta Lake -> Cassandra raw keyspace ``ks_raw``.
3. Ingest exchange rates into ``ks_raw``.
4. Run ``delta-update update`` with the **current** venv -> ``ks_current_tx``,
   record wall time.
5. Run ``delta-update update`` with the **baseline** venv (v2.12.3 by default)
   -> ``ks_baseline_tx``, record wall time.
6. Compare the two transformed keyspaces table-by-table:
     * Tables present on both sides are content-hashed (sha256 over sorted
       row reprs).
     * The schema-evolved ``fresh_*`` clustering tables and the metadata
       ``state``/``configuration``/``summary_statistics`` tables are excluded
       from content equality (they legitimately differ across versions or
       between runs).
7. Report per-side wall time, blocks/s, and the speedup factor.

Requires:
- Docker (for MinIO, Cassandra testcontainers, and the PySpark image).
- Node URLs configured in .graphsense.yaml.
- ``DELTA_UPDATE_CURRENCIES`` env var (default: btc,ltc,bch,zec).
- ``DELTA_UPDATE_REF_VERSION`` env var (default: v2.12.3).
"""

import hashlib
import os
from collections import defaultdict

import pytest

from tests.delta_update.config import DeltaUpdateConfig
from tests.delta_update.ingest_runner import (
    run_delta_update,
    run_exchange_rates_ingest,
    run_ingest_delta_only,
    run_spark_transformation_to_raw,
)

pytestmark = pytest.mark.delta_update

# Worker processes for the CURRENT side's delta-update run. The baseline
# CLI predates the flag, so it always runs single-process; with workers > 1
# a passing comparison therefore also proves pooled == single-process output.
PARALLEL_WORKERS_CURRENT = int(os.environ.get("GS_REGRESSION_PARALLEL_WORKERS", "1"))


# Tables whose content legitimately differs across runs/versions.
#   state                 -- bootstrap-marker timestamp
#   configuration         -- target keyspace name baked in
#   summary_statistics    -- includes per-run timestamp
#   fresh_address_cluster, fresh_cluster_addresses
#                         -- only exist on current schema (clustering tables);
#                            populated by the Rust clustering path, not by
#                            delta-update update
METADATA_TABLES = {
    "state",
    "configuration",
    "summary_statistics",
    "fresh_address_cluster",
    "fresh_cluster_addresses",
    # delta_updater_history records per-batch run timestamps and durations --
    # legitimately differs across runs and across versions.
    "delta_updater_history",
}


def _table_content_hash(session, keyspace: str, table: str) -> tuple[int, str]:
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    count = len(rows)
    sorted_rows = sorted(str(sorted(row._asdict().items())) for row in rows)
    h = hashlib.sha256()
    for row_str in sorted_rows:
        h.update(row_str.encode())
    return count, h.hexdigest()


def _list_tables(session, keyspace: str) -> set[str]:
    return {
        row.table_name
        for row in session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name = %s",
            (keyspace,),
        )
    }


def _get_pk_columns(session, keyspace: str, table: str) -> list[str]:
    """Return the primary-key columns (partition + clustering) for a table,
    in correct order."""
    rows = list(
        session.execute(
            "SELECT column_name, kind, position FROM system_schema.columns "
            "WHERE keyspace_name = %s AND table_name = %s",
            (keyspace, table),
        )
    )
    pk_rows = [r for r in rows if r.kind in ("partition_key", "clustering")]
    pk_rows.sort(key=lambda r: (0 if r.kind == "partition_key" else 1, r.position))
    return [r.column_name for r in pk_rows]


def _diff_table_columns(
    session, ks_cur: str, ks_base: str, table: str
) -> tuple[int, dict[str, int], list[tuple[tuple, list]]]:
    """Diff a Cassandra table row-by-row across two keyspaces.

    Returns:
        (rows_with_diff, columns_differing_count, sample_diffs)
        - rows_with_diff: how many PKs have at least one non-matching column.
        - columns_differing_count: per-column count of differing rows.
        - sample_diffs: up to 3 (pk_tuple, [(col, cur_val, base_val)...]).
    """
    pk_cols = _get_pk_columns(session, ks_cur, table)
    cur_rows = {
        tuple(getattr(r, c) for c in pk_cols): r._asdict()
        for r in session.execute(f"SELECT * FROM {ks_cur}.{table}")  # noqa: S608
    }
    base_rows = {
        tuple(getattr(r, c) for c in pk_cols): r._asdict()
        for r in session.execute(f"SELECT * FROM {ks_base}.{table}")  # noqa: S608
    }

    rows_with_diff = 0
    col_diffs: dict[str, int] = defaultdict(int)
    sample_diffs: list[tuple[tuple, list]] = []
    for pk, cur_row in cur_rows.items():
        base_row = base_rows.get(pk)
        if base_row is None:
            continue
        diffs = [
            (k, cur_row[k], base_row[k])
            for k in cur_row
            if k not in pk_cols and cur_row[k] != base_row[k]
        ]
        if diffs:
            rows_with_diff += 1
            for col, _, _ in diffs:
                col_diffs[col] += 1
            if len(sample_diffs) < 3:
                sample_diffs.append((pk, diffs))
    return rows_with_diff, dict(col_diffs), sample_diffs


class TestDeltaUpdateRegression:
    """Local graphsense-lib must match the reference release on UTXO delta-update."""

    def test_btc_delta_update_matches_reference(
        self,
        delta_update_config: DeltaUpdateConfig,
        minio_config: dict[str, str],
        cassandra_coords: tuple[str, int],
        current_venv,
        baseline_venv,
        baseline_version: str,
        transformation_image: str,
    ):
        currency = delta_update_config.currency
        range_id = delta_update_config.range_id
        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        ks_raw = f"du_{currency}_{range_id}_raw"
        ks_current_tx = f"du_{currency}_{range_id}_curr_tx"
        ks_baseline_tx = f"du_{currency}_{range_id}_base_tx"
        delta_path = f"s3://{bucket}/{currency}/{range_id}"

        print(f"\n{'=' * 68}")
        print(f"DELTA-UPDATE REGRESSION: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:          "
            f"{delta_update_config.start_block:,}-"
            f"{delta_update_config.end_block:,} "
            f"({delta_update_config.num_blocks} blocks)"
        )
        print(f"  current venv:    {current_venv}")
        print(f"  baseline:        {baseline_version}  ({baseline_venv})")
        if delta_update_config.range_note:
            print(f"  note:            {delta_update_config.range_note}")

        # ------------------------------------------------------------------
        # Step 1: Delta-only ingest (shared between both sides)
        # ------------------------------------------------------------------
        print("  [1/5] delta-only ingest ...", end=" ", flush=True)
        run_ingest_delta_only(
            venv_dir=current_venv,
            config=delta_update_config,
            delta_directory=delta_path,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 2: PySpark transformation Delta Lake -> Cassandra raw
        # ------------------------------------------------------------------
        print("  [2/5] PySpark Delta -> Cassandra raw ...",
              end=" ", flush=True)
        run_spark_transformation_to_raw(
            image_name=transformation_image,
            config=delta_update_config,
            delta_directory=delta_path,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_raw,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 3: Exchange rates (shared)
        # ------------------------------------------------------------------
        print("  [3/5] exchange rates ingest ...", end=" ", flush=True)
        run_exchange_rates_ingest(
            venv_dir=current_venv,
            config=delta_update_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_raw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 4: delta-update update (current)
        # ------------------------------------------------------------------
        print(f"  [4/5] delta-update update (current, "
              f"workers={PARALLEL_WORKERS_CURRENT}) ...",
              end=" ", flush=True)
        current_secs, current_timings = run_delta_update(
            venv_dir=current_venv,
            config=delta_update_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            raw_keyspace=ks_raw,
            transformed_keyspace=ks_current_tx,
            delta_directory=delta_path,
            **minio_kw,
            label="delta-update[current]",
            parallel_workers=PARALLEL_WORKERS_CURRENT,
        )
        current_bps = delta_update_config.num_blocks / current_secs
        print(
            f"done in {current_secs:.2f}s "
            f"({current_bps:,.1f} blocks/s)"
        )

        # ------------------------------------------------------------------
        # Step 5: delta-update update (baseline)
        # ------------------------------------------------------------------
        print(f"  [5/5] delta-update update (baseline {baseline_version}) ...",
              end=" ", flush=True)
        baseline_secs, baseline_timings = run_delta_update(
            venv_dir=baseline_venv,
            config=delta_update_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            raw_keyspace=ks_raw,
            transformed_keyspace=ks_baseline_tx,
            delta_directory=delta_path,
            **minio_kw,
            label=f"delta-update[{baseline_version}]",
        )
        baseline_bps = delta_update_config.num_blocks / baseline_secs
        print(
            f"done in {baseline_secs:.2f}s "
            f"({baseline_bps:,.1f} blocks/s)"
        )

        # ------------------------------------------------------------------
        # Compare transformed keyspaces
        # ------------------------------------------------------------------
        print("\n  Transformed keyspace comparison:")

        from cassandra.cluster import Cluster

        with Cluster([cass_host], port=cass_port) as cluster:
            session = cluster.connect()

            current_tables = _list_tables(session, ks_current_tx)
            baseline_tables = _list_tables(session, ks_baseline_tx)
            shared_tables = sorted(current_tables & baseline_tables)
            current_only = sorted(current_tables - baseline_tables)
            baseline_only = sorted(baseline_tables - current_tables)

            if current_only:
                print(f"    info: tables only in current schema: "
                      f"{current_only}")
            if baseline_only:
                print(f"    info: tables only in baseline schema: "
                      f"{baseline_only}")

            mismatches = []
            for table_name in shared_tables:
                cur_count, cur_hash = _table_content_hash(
                    session, ks_current_tx, table_name
                )
                base_count, base_hash = _table_content_hash(
                    session, ks_baseline_tx, table_name
                )

                if table_name in METADATA_TABLES:
                    print(
                        f"    {table_name:30s} "
                        f"current={cur_count:>6,}  baseline={base_count:>6,}  META"
                    )
                    continue

                match = cur_hash == base_hash
                status = "MATCH" if match else "MISMATCH"
                print(
                    f"    {table_name:30s} "
                    f"current={cur_count:>6,}  baseline={base_count:>6,}  "
                    f"{status}"
                )
                if not match:
                    mismatches.append(
                        f"{table_name}: content differs "
                        f"(current={cur_count} rows hash={cur_hash[:12]}... "
                        f"baseline={base_count} rows hash={base_hash[:12]}...)"
                    )
                    rows_diff, col_diffs, samples = _diff_table_columns(
                        session, ks_current_tx, ks_baseline_tx, table_name
                    )
                    if col_diffs:
                        col_summary = ", ".join(
                            f"{c}={n}"
                            for c, n in sorted(
                                col_diffs.items(),
                                key=lambda kv: kv[1],
                                reverse=True,
                            )
                        )
                        print(
                            f"      {rows_diff:,} rows differ; "
                            f"by column: {col_summary}"
                        )
                        for pk, diffs in samples:
                            diff_str = "; ".join(
                                f"{c}: cur={cv!r} base={bv!r}"
                                for c, cv, bv in diffs
                            )
                            print(f"      pk={pk}  {diff_str}")

        # ------------------------------------------------------------------
        # Timing summary
        # ------------------------------------------------------------------
        speedup = baseline_secs / current_secs if current_secs > 0 else 0.0
        delta_pct = (
            (baseline_secs - current_secs) / baseline_secs * 100.0
            if baseline_secs > 0 else 0.0
        )
        print("\n  Delta-update timing:")
        print(
            f"    current   {current_secs:>7.2f}s  "
            f"({current_bps:,.1f} blocks/s)"
        )
        print(
            f"    {baseline_version:9s} {baseline_secs:>7.2f}s  "
            f"({baseline_bps:,.1f} blocks/s)"
        )
        if speedup >= 1.0:
            print(
                f"    speedup:  {speedup:.2f}x  "
                f"(current is {delta_pct:+.1f}% faster than {baseline_version})"
            )
        else:
            print(
                f"    speedup:  {speedup:.2f}x  "
                f"(current is {-delta_pct:.1f}% SLOWER than {baseline_version})"
            )

        # ------------------------------------------------------------------
        # Section breakdown (LoggerScope timings under -vv)
        # ------------------------------------------------------------------
        # Show the top-N sections by total wall time across all batches.
        # Sections are LoggerScope.debug scopes inside the updater, so the
        # entries highlight exactly which read/merge/write phase dominates
        # and how that shifts between current and baseline.
        top_n = 12
        baseline_lookup = {msg: secs for msg, secs, _ in baseline_timings}
        print("\n  Section breakdown (top sections by total time):")
        if not current_timings:
            print("    (no timings captured -- did -vv DEBUG get through?)")
        else:
            print(
                f"    {'section':<55s}  "
                f"{'current':>10s}  {'baseline':>10s}  {'delta':>9s}"
            )
            for msg, cur_secs, cur_count in current_timings[:top_n]:
                base_secs = baseline_lookup.get(msg, 0.0)
                if base_secs > 0:
                    delta_str = f"{(cur_secs - base_secs):+.2f}s"
                else:
                    delta_str = "  n/a"
                print(
                    f"    {msg[:55]:<55s}  "
                    f"{cur_secs:>8.2f}s   "
                    f"{base_secs:>8.2f}s   "
                    f"{delta_str:>9s}"
                )

            # Sections that exist on baseline but not on current (e.g. the
            # outgoing-relations reads removed by the perf commit) are easy
            # to miss otherwise -- list any whose baseline time was non-trivial.
            current_msgs = {m for m, _, _ in current_timings}
            removed = [
                (msg, secs, cnt)
                for msg, secs, cnt in baseline_timings
                if msg not in current_msgs and secs >= 0.1
            ]
            if removed:
                print("\n  Sections only on baseline "
                      f"({baseline_version}, top by time):")
                for msg, secs, cnt in removed[:6]:
                    print(
                        f"    {msg[:55]:<55s}  "
                        f"{'':>10s}  {secs:>8.2f}s  ({cnt}x)"
                    )

        if mismatches:
            print("  result:          FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] delta-update regression mismatches:\n"
                + "\n".join(f"  - {m}" for m in mismatches)
            )
        else:
            print("  result:          PASS")
            print(f"{'=' * 68}")
