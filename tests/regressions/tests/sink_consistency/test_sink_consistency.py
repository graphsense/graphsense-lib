"""Sink consistency test.

Verifies that ingesting with both cassandra and delta sinks simultaneously
produces identical output to running each sink individually.

Three ingests per currency:
1. delta-only:    ``from-node --sinks delta``       → Delta tables at path A
2. cassandra-only: ``from-node --sinks cassandra``  → Cassandra keyspace X
3. dual-sink:     ``from-node --sinks delta --sinks cassandra`` → Delta at path B + Cassandra Y

Comparisons:
- Delta:     A == B (same parquet data regardless of cassandra co-writing)
- Cassandra: X == Y (same rows regardless of delta co-writing)

Requires:
- Docker (for MinIO and Cassandra testcontainers)
- Node URLs configured in .graphsense.yaml
- ``SINK_CONSISTENCY_CURRENCIES`` env var (default: all currencies)
"""

import hashlib

import pytest

from tests.deltalake.snapshot import (
    TableSnapshot,
    _compute_content_hash,
    _sortable_type,
    capture_snapshot,
)
from tests.sink_consistency.config import SinkConsistencyConfig
from tests.sink_consistency.ingest_runner import run_ingest

pytestmark = pytest.mark.sink_consistency

# Tables where content differences are known/expected between individual
# and dual-sink modes. Should ideally remain empty.
KNOWN_DIVERGENCES: set[tuple[str, str]] = set()

# Cassandra metadata tables — checked for row count but not content hash.
METADATA_TABLES = {"configuration", "summary_statistics"}


def _find_differing_columns(
    ind_table: TableSnapshot, dual_table: TableSnapshot
) -> list[str]:
    """Return the list of column names whose content differs between two snapshots."""
    if ind_table._arrow_table is None or dual_table._arrow_table is None:
        return []
    import pyarrow.compute as pc

    common_cols = sorted(
        set(ind_table.column_names) & set(dual_table.column_names)
    )
    differing = []
    for col in common_cols:
        ind_col = ind_table._arrow_table.column(col)
        dual_col = dual_table._arrow_table.column(col)
        if _sortable_type(ind_col.type):
            ind_sorted = ind_col.take(pc.sort_indices(ind_col))
            dual_sorted = dual_col.take(pc.sort_indices(dual_col))
            if ind_sorted != dual_sorted:
                differing.append(col)
                # Show first differing value
                for i in range(min(len(ind_sorted), 3)):
                    if ind_sorted[i] != dual_sorted[i]:
                        print(
                            f"        {col}[{i}]: ind={ind_sorted[i].as_py()!r} "
                            f"dual={dual_sorted[i].as_py()!r}"
                        )
                        break
        else:
            # Non-sortable: compare per-column hash
            if (
                _compute_content_hash(ind_table._arrow_table.select([col]))
                != _compute_content_hash(dual_table._arrow_table.select([col]))
            ):
                differing.append(col)
    return differing


def _table_content_hash(
    session, keyspace: str, table: str
) -> tuple[int, str]:
    """Return (row_count, sha256_hex) for a Cassandra table."""
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    count = len(rows)
    sorted_rows = sorted(str(sorted(row._asdict().items())) for row in rows)
    h = hashlib.sha256()
    for row_str in sorted_rows:
        h.update(row_str.encode())
    return count, h.hexdigest()


class TestSinkConsistency:
    """Individual vs dual-sink ingestion must produce identical output."""

    def test_sink_consistency(
        self,
        sink_config: SinkConsistencyConfig,
        minio_config: dict[str, str],
        storage_options: dict[str, str],
        cassandra_coords: tuple[str, int],
        current_venv,
    ):
        currency = sink_config.currency
        range_id = sink_config.range_id
        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        # Unique keyspace/path names to avoid collisions
        ks_individual = f"sink_{currency}_{range_id}_ind"
        ks_dual = f"sink_{currency}_{range_id}_dual"
        delta_path_individual = f"s3://{bucket}/individual/{currency}/{range_id}"
        delta_path_dual = f"s3://{bucket}/dual/{currency}/{range_id}"

        print(f"\n{'=' * 68}")
        print(f"SINK CONSISTENCY: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:          "
            f"{sink_config.start_block:,}-{sink_config.end_block:,} "
            f"({sink_config.num_blocks} blocks)"
        )
        if sink_config.range_note:
            print(f"  note:            {sink_config.range_note}")

        # ------------------------------------------------------------------
        # 1. Delta-only ingest
        # ------------------------------------------------------------------
        print("  [1/3] delta-only ingest ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=sink_config,
            sinks=["delta"],
            delta_directory=delta_path_individual,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # 2. Cassandra-only ingest
        # ------------------------------------------------------------------
        print("  [2/3] cassandra-only ingest ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=sink_config,
            sinks=["cassandra"],
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_individual,
        )
        print("done")

        # ------------------------------------------------------------------
        # 3. Dual-sink ingest (delta + cassandra)
        # ------------------------------------------------------------------
        print("  [3/3] dual-sink ingest ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=sink_config,
            sinks=["delta", "cassandra"],
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_dual,
            delta_directory=delta_path_dual,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Compare Delta Lake tables: individual vs dual
        # ------------------------------------------------------------------
        print("\n  Delta Lake comparison:")
        tables = sink_config.tables

        snap_ind = capture_snapshot(
            storage_options, delta_path_individual, tables,
            "individual (delta-only)",
            block_range=(sink_config.start_block, sink_config.end_block),
        )
        snap_dual = capture_snapshot(
            storage_options, delta_path_dual, tables,
            "dual (delta+cassandra)",
            block_range=(sink_config.start_block, sink_config.end_block),
        )

        delta_mismatches = []
        for table_name in tables:
            ind_table = snap_ind.tables.get(table_name)
            dual_table = snap_dual.tables.get(table_name)

            if ind_table is None and dual_table is None:
                print(f"    {table_name:30s}  EMPTY (both)")
                continue
            if ind_table is None:
                delta_mismatches.append(f"{table_name}: missing in individual")
                print(f"    {table_name:30s}  MISSING IN INDIVIDUAL")
                continue
            if dual_table is None:
                delta_mismatches.append(f"{table_name}: missing in dual")
                print(f"    {table_name:30s}  MISSING IN DUAL")
                continue

            match = ind_table.content_hash == dual_table.content_hash
            row_match = ind_table.row_count == dual_table.row_count
            known = (currency, table_name) in KNOWN_DIVERGENCES

            if match:
                status = "MATCH"
            elif known:
                status = "KNOWN DIVERGENCE"
            else:
                status = "MISMATCH"

            print(
                f"    {table_name:30s} "
                f"ind={ind_table.row_count:>6,}  dual={dual_table.row_count:>6,}  "
                f"{status}"
            )

            if not match and not known:
                # Identify which columns differ
                differing_cols = _find_differing_columns(ind_table, dual_table)
                col_info = f" differing_cols={differing_cols}" if differing_cols else ""
                if differing_cols:
                    print(f"      differing columns: {differing_cols}")
                delta_mismatches.append(
                    f"{table_name}: content differs "
                    f"(ind={ind_table.row_count} rows hash={ind_table.content_hash[:12]}... "
                    f"dual={dual_table.row_count} rows hash={dual_table.content_hash[:12]}..."
                    f"{col_info})"
                )

        # ------------------------------------------------------------------
        # Compare Cassandra tables: individual vs dual
        # ------------------------------------------------------------------
        print("\n  Cassandra comparison:")

        from cassandra.cluster import Cluster

        cluster = Cluster([cass_host], port=cass_port)
        session = cluster.connect()

        ind_tables = sorted(
            row.table_name
            for row in session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (ks_individual,),
            )
        )
        dual_tables = sorted(
            row.table_name
            for row in session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (ks_dual,),
            )
        )

        all_cass_tables = sorted(set(ind_tables) | set(dual_tables))
        ind_only = set(ind_tables) - set(dual_tables)
        dual_only = set(dual_tables) - set(ind_tables)

        if ind_only:
            print(f"    WARN: tables only in individual: {ind_only}")
        if dual_only:
            print(f"    WARN: tables only in dual: {dual_only}")

        cass_mismatches = []
        for table_name in all_cass_tables:
            in_ind = table_name in ind_tables
            in_dual = table_name in dual_tables

            if not in_ind:
                print(f"    {table_name:30s}  DUAL ONLY")
                cass_mismatches.append(f"{table_name}: only in dual")
                continue
            if not in_dual:
                print(f"    {table_name:30s}  INDIVIDUAL ONLY")
                cass_mismatches.append(f"{table_name}: only in individual")
                continue

            if table_name in METADATA_TABLES:
                ind_count, _ = _table_content_hash(session, ks_individual, table_name)
                dual_count, _ = _table_content_hash(session, ks_dual, table_name)
                print(
                    f"    {table_name:30s} "
                    f"ind={ind_count:>6,}  dual={dual_count:>6,}  META"
                )
                continue

            ind_count, ind_hash = _table_content_hash(
                session, ks_individual, table_name
            )
            dual_count, dual_hash = _table_content_hash(
                session, ks_dual, table_name
            )

            match = ind_hash == dual_hash
            known = (currency, table_name) in KNOWN_DIVERGENCES

            if match:
                status = "MATCH"
            elif known:
                status = "KNOWN DIVERGENCE"
            else:
                status = "MISMATCH"

            print(
                f"    {table_name:30s} "
                f"ind={ind_count:>6,}  dual={dual_count:>6,}  "
                f"{status}"
            )

            if not match and not known:
                cass_mismatches.append(
                    f"{table_name}: content differs "
                    f"(ind={ind_count} rows hash={ind_hash[:12]}... "
                    f"dual={dual_count} rows hash={dual_hash[:12]}...)"
                )

        cluster.shutdown()

        # ------------------------------------------------------------------
        # Report results
        # ------------------------------------------------------------------
        all_mismatches = []
        if delta_mismatches:
            all_mismatches.append("Delta Lake mismatches:")
            all_mismatches.extend(f"  - {m}" for m in delta_mismatches)
        if cass_mismatches:
            all_mismatches.append("Cassandra mismatches:")
            all_mismatches.extend(f"  - {m}" for m in cass_mismatches)

        if all_mismatches:
            print(f"  result:          FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] Sink consistency failures:\n"
                + "\n".join(all_mismatches)
            )
        else:
            print(f"  result:          PASS")
            print(f"{'=' * 68}")
