"""Sink catch-up regression test.

Verifies that the auto-catch-up of a diverged idempotent sink during a
mixed (delta + cassandra) append produces the same end state as a sync
ingest of the full range from the start.

Two parallel runs land at block ``end``:

A. **sync baseline**:
   1. dual-sink overwrite ``[start, end]`` → (delta_A, cass_A)

B. **catch-up scenario**:
   1. cassandra-only overwrite ``[start, mid]`` — cass_B at ``mid``
   2. delta-only overwrite ``[start, end - 1]`` — delta_B at ``end - 1``
   3. dual-sink append ``[end, end]``:
      - on entry, divergence is detected (delta=end-1, cass=mid);
      - cassandra is auto-caught-up to ``end - 1`` via a single-sink run;
      - the forward step then writes block ``end`` to both sinks.

Comparison: per-table content hashes for cassandra and delta must match
between A and B. Tables carrying ingest metadata (``state``, ``configuration``,
``summary_statistics``) are checked for row count only — they carry
timestamps that naturally differ between runs.

Requires:
- Docker (MinIO + Cassandra testcontainers)
- Node URLs configured in .graphsense.yaml
- ``SINK_CATCHUP_CURRENCIES`` env var (default: all account-chain currencies)
"""

import hashlib

import pytest

from tests.deltalake.snapshot import (
    TableSnapshot,
    _compute_content_hash,
    _sortable_type,
    capture_snapshot,
)
from tests.sink_catchup.config import SinkCatchupConfig
from tests.sink_catchup.ingest_runner import run_ingest

pytestmark = pytest.mark.sink_catchup

# Cassandra metadata tables — checked for row count only.
METADATA_TABLES = {"configuration", "summary_statistics", "state"}


def _find_differing_columns(
    a: TableSnapshot, b: TableSnapshot
) -> list[str]:
    """Return column names whose content differs between two delta snapshots."""
    if a._arrow_table is None or b._arrow_table is None:
        return []
    import pyarrow.compute as pc

    common = sorted(set(a.column_names) & set(b.column_names))
    differing = []
    for col in common:
        ca = a._arrow_table.column(col)
        cb = b._arrow_table.column(col)
        if _sortable_type(ca.type):
            ca_s = ca.take(pc.sort_indices(ca))
            cb_s = cb.take(pc.sort_indices(cb))
            if ca_s != cb_s:
                differing.append(col)
        else:
            if (
                _compute_content_hash(a._arrow_table.select([col]))
                != _compute_content_hash(b._arrow_table.select([col]))
            ):
                differing.append(col)
    return differing


def _table_content_hash(session, keyspace: str, table: str) -> tuple[int, str]:
    """Return (row_count, sha256_hex) for a Cassandra table's full content."""
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    count = len(rows)
    sorted_rows = sorted(str(sorted(row._asdict().items())) for row in rows)
    h = hashlib.sha256()
    for r in sorted_rows:
        h.update(r.encode())
    return count, h.hexdigest()


class TestSinkCatchup:
    """Auto-catch-up + 1-block forward must match a sync ingest of the same range."""

    def test_catchup_equivalence(
        self,
        catchup_config: SinkCatchupConfig,
        minio_config: dict[str, str],
        storage_options: dict[str, str],
        cassandra_coords: tuple[str, int],
        current_venv,
    ):
        currency = catchup_config.currency
        range_id = catchup_config.range_id
        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]
        start = catchup_config.start_block
        end = catchup_config.end_block
        mid = catchup_config.mid_block

        # Sanity: the chosen mid must leave a non-trivial catch-up gap and
        # leave at least one block for the post-catch-up forward step.
        assert start <= mid < end - 1, (
            f"Range {start}-{end} with mid={mid} doesn't leave room for "
            f"a catch-up gap and a forward block"
        )

        ks_a = f"catchup_{currency}_{range_id}_a"
        ks_b = f"catchup_{currency}_{range_id}_b"
        delta_a = f"s3://{bucket}/baseline/{currency}/{range_id}"
        delta_b = f"s3://{bucket}/catchup/{currency}/{range_id}"

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        print(f"\n{'=' * 68}")
        print(f"SINK CATCH-UP: {currency.upper()} [{range_id}]")
        print(f"  blocks:          {start:,}-{end:,} ({catchup_config.num_blocks} blocks)")
        print(f"  mid (cass-only): {mid:,}")
        if catchup_config.range_note:
            print(f"  note:            {catchup_config.range_note}")

        # ------------------------------------------------------------------
        # Phase A: sync baseline — dual-sink overwrite [start, end]
        # ------------------------------------------------------------------
        print("  [A]  sync baseline ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=catchup_config,
            sinks=["delta", "cassandra"],
            start_block=start,
            end_block=end,
            write_mode="overwrite",
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_a,
            delta_directory=delta_a,
            label="sync baseline",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Phase B step 1: cassandra-only overwrite [start, mid]
        # ------------------------------------------------------------------
        print("  [B1] cass-only overwrite [start, mid] ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=catchup_config,
            sinks=["cassandra"],
            start_block=start,
            end_block=mid,
            write_mode="overwrite",
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_b,
            label="cass-only overwrite",
        )
        print("done")

        # ------------------------------------------------------------------
        # Phase B step 2: delta-only overwrite [start, end-1]
        # ------------------------------------------------------------------
        print(f"  [B2] delta-only overwrite [start, end-1] ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=catchup_config,
            sinks=["delta"],
            start_block=start,
            end_block=end - 1,
            write_mode="overwrite",
            delta_directory=delta_b,
            label="delta-only overwrite",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Phase B step 3: dual-sink append [end, end] — auto-catch-up cass
        # to end-1, then forward block `end` lands on both sinks.
        # ------------------------------------------------------------------
        print(f"  [B3] dual-sink append [end, end] (triggers catch-up) ...",
              end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=catchup_config,
            sinks=["delta", "cassandra"],
            start_block=end,
            end_block=end,
            write_mode="append",
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_b,
            delta_directory=delta_b,
            label="dual-sink append (catch-up + forward)",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Compare Cassandra: ks_a vs ks_b
        # ------------------------------------------------------------------
        print("\n  Cassandra comparison:")

        from cassandra.cluster import Cluster

        cluster = Cluster([cass_host], port=cass_port)
        session = cluster.connect()

        a_tables = sorted(
            row.table_name
            for row in session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (ks_a,),
            )
        )
        b_tables = sorted(
            row.table_name
            for row in session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (ks_b,),
            )
        )

        all_cass_tables = sorted(set(a_tables) | set(b_tables))
        only_a = set(a_tables) - set(b_tables)
        only_b = set(b_tables) - set(a_tables)
        if only_a:
            print(f"    WARN: tables only in baseline: {only_a}")
        if only_b:
            print(f"    WARN: tables only in catch-up: {only_b}")

        cass_mismatches = []
        for table_name in all_cass_tables:
            in_a = table_name in a_tables
            in_b = table_name in b_tables

            if not in_a:
                cass_mismatches.append(f"{table_name}: only in catch-up")
                print(f"    {table_name:30s}  CATCHUP ONLY")
                continue
            if not in_b:
                cass_mismatches.append(f"{table_name}: only in baseline")
                print(f"    {table_name:30s}  BASELINE ONLY")
                continue

            if table_name in METADATA_TABLES:
                a_count, _ = _table_content_hash(session, ks_a, table_name)
                b_count, _ = _table_content_hash(session, ks_b, table_name)
                print(
                    f"    {table_name:30s} "
                    f"a={a_count:>6,}  b={b_count:>6,}  META"
                )
                continue

            a_count, a_hash = _table_content_hash(session, ks_a, table_name)
            b_count, b_hash = _table_content_hash(session, ks_b, table_name)
            match = a_hash == b_hash
            status = "MATCH" if match else "MISMATCH"
            print(
                f"    {table_name:30s} "
                f"a={a_count:>6,}  b={b_count:>6,}  {status}"
            )
            if not match:
                cass_mismatches.append(
                    f"{table_name}: content differs "
                    f"(a={a_count} rows hash={a_hash[:12]}... "
                    f"b={b_count} rows hash={b_hash[:12]}...)"
                )

        cluster.shutdown()

        # ------------------------------------------------------------------
        # Compare Delta: delta_a vs delta_b
        # ------------------------------------------------------------------
        print("\n  Delta Lake comparison:")
        tables = catchup_config.tables

        snap_a = capture_snapshot(
            storage_options, delta_a, tables,
            "baseline (sync)",
            block_range=(start, end),
        )
        snap_b = capture_snapshot(
            storage_options, delta_b, tables,
            "catch-up (split + append)",
            block_range=(start, end),
        )

        delta_mismatches = []
        for table_name in tables:
            ta = snap_a.tables.get(table_name)
            tb = snap_b.tables.get(table_name)
            if ta is None and tb is None:
                print(f"    {table_name:30s}  EMPTY (both)")
                continue
            if ta is None:
                delta_mismatches.append(f"{table_name}: missing in baseline")
                print(f"    {table_name:30s}  MISSING IN BASELINE")
                continue
            if tb is None:
                delta_mismatches.append(f"{table_name}: missing in catch-up")
                print(f"    {table_name:30s}  MISSING IN CATCH-UP")
                continue

            match = ta.content_hash == tb.content_hash
            status = "MATCH" if match else "MISMATCH"
            print(
                f"    {table_name:30s} "
                f"a={ta.row_count:>6,}  b={tb.row_count:>6,}  {status}"
            )
            if not match:
                cols = _find_differing_columns(ta, tb)
                col_info = f" differing_cols={cols}" if cols else ""
                if cols:
                    print(f"      differing columns: {cols}")
                delta_mismatches.append(
                    f"{table_name}: content differs "
                    f"(a={ta.row_count} hash={ta.content_hash[:12]}... "
                    f"b={tb.row_count} hash={tb.content_hash[:12]}...{col_info})"
                )

        # ------------------------------------------------------------------
        # Report
        # ------------------------------------------------------------------
        all_mismatches: list[str] = []
        if cass_mismatches:
            all_mismatches.append("Cassandra mismatches:")
            all_mismatches.extend(f"  - {m}" for m in cass_mismatches)
        if delta_mismatches:
            all_mismatches.append("Delta Lake mismatches:")
            all_mismatches.extend(f"  - {m}" for m in delta_mismatches)

        if all_mismatches:
            print(f"  result:          FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] catch-up vs sync mismatches:\n"
                + "\n".join(all_mismatches)
            )
        else:
            print(f"  result:          PASS")
            print(f"{'=' * 68}")
