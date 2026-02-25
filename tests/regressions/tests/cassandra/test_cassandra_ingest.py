"""Cassandra ingest regression test.

Ingests a small block range for each configured currency into a
testcontainer Cassandra instance using both the reference and current
versions of graphsense-lib, then compares the results row-by-row.

The reference version always uses the legacy ``ingest from-node`` command.
The current version uses the new pipeline for all chains:
- UTXO chains: ``ingest from-node --sinks cassandra``
- Account chains: ``ingest delta-lake ingest --sinks delta --sinks cassandra``

Requires:
- Docker (for Cassandra testcontainer)
- Node URLs configured in .graphsense.yaml
- ``CASSANDRA_CURRENCIES`` env var (default: all currencies)
- ``CASSANDRA_REF_VERSION`` env var (default: v25.11.18)
"""

import hashlib
from pathlib import Path

import pytest

from tests.cassandra.config import CassandraTestConfig, EXPECTED_MIN_ROWS
from tests.cassandra.ingest_runner import run_cassandra_ingest

pytestmark = pytest.mark.cassandra

# Tables where content differences are expected (e.g. bug fixes).
# Set of (currency, table_name) tuples.
KNOWN_CONTENT_DIVERGENCES: set[tuple[str, str]] = set()

# Tables that contain version-specific metadata (e.g. ingest timestamps,
# software version strings). These are checked for existence and row count
# but not for byte-identical content.
METADATA_TABLES = {"configuration", "summary_statistics"}


def _cassandra_release(session) -> str:
    """Query the Cassandra server for its release version."""
    row = session.execute("SELECT release_version FROM system.local").one()
    return row.release_version if row else "unknown"


def _table_content_hash(session, keyspace: str, table: str) -> tuple[int, str]:
    """Return (row_count, sha256_hex) for a table's full content.

    Rows are fetched, converted to sorted tuples, and hashed
    deterministically so that row ordering doesn't matter.
    """
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    count = len(rows)
    # Sort rows deterministically by converting each to a tuple of (col, val)
    sorted_rows = sorted(str(sorted(row._asdict().items())) for row in rows)
    h = hashlib.sha256()
    for row_str in sorted_rows:
        h.update(row_str.encode())
    return count, h.hexdigest()


class TestCassandraIngest:
    """Cross-version test: ingest with reference and current, then compare."""

    def test_ingest_and_verify(
        self,
        cassandra_config: CassandraTestConfig,
        cassandra_coords: tuple[str, int],
        reference_venv: Path,
        current_venv: Path,
        ref_package_versions: dict[str, str],
        current_package_versions: dict[str, str],
    ):
        currency = cassandra_config.currency
        range_id = cassandra_config.range_id
        host, port = cassandra_coords
        ref_ks = f"regtest_{currency}_{range_id}_ref"
        cur_ks = f"regtest_{currency}_{range_id}_cur"

        ref_ver = ref_package_versions.get("graphsense-lib", "?")
        cur_ver = current_package_versions.get("graphsense-lib", "?")

        # UTXO: from-node (new IngestRunner pipeline), Account: delta dual-sink
        is_utxo = cassandra_config.schema_type == "utxo"
        cur_mode = "from-node" if is_utxo else "delta"

        print(f"\n{'=' * 68}")
        print(f"CASSANDRA INGEST: {currency.upper()} [{range_id}]")
        print(f"  reference:       {ref_ver} (legacy)")
        print(f"  current:         {cur_ver} ({cur_mode})")
        print(
            f"  blocks:          "
            f"{cassandra_config.start_block:,}-{cassandra_config.end_block:,} "
            f"({cassandra_config.num_blocks} blocks)"
        )
        if cassandra_config.range_note:
            print(f"  note:            {cassandra_config.range_note}")
        print(f"  cassandra:       {host}:{port}")

        # --- Run reference ingest (always legacy) ---
        print(f"  ingesting ref    ({ref_ks}) ...", end=" ", flush=True)
        run_cassandra_ingest(
            venv_dir=reference_venv,
            config=cassandra_config,
            cassandra_host=host,
            cassandra_port=port,
            keyspace_name=ref_ks,
            mode="legacy",
        )
        print("done")

        # --- Run current ingest (delta for account, legacy for utxo) ---
        print(f"  ingesting cur    ({cur_ks}, {cur_mode}) ...", end=" ", flush=True)
        run_cassandra_ingest(
            venv_dir=current_venv,
            config=cassandra_config,
            cassandra_host=host,
            cassandra_port=port,
            keyspace_name=cur_ks,
            mode=cur_mode,
        )
        print("done")

        # --- Connect and compare ---
        from cassandra.cluster import Cluster

        cluster = Cluster([host], port=port)
        session = cluster.connect()

        cass_ver = _cassandra_release(session)
        print(f"  cassandra ver:   {cass_ver}")

        # Get tables from both keyspaces
        ref_tables = sorted(
            row.table_name
            for row in session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (ref_ks,),
            )
        )
        cur_tables = sorted(
            row.table_name
            for row in session.execute(
                "SELECT table_name FROM system_schema.tables "
                "WHERE keyspace_name = %s",
                (cur_ks,),
            )
        )

        assert len(ref_tables) > 0, f"No tables in reference keyspace {ref_ks}"
        assert len(cur_tables) > 0, f"No tables in current keyspace {cur_ks}"

        # Tables present in both
        all_tables = sorted(set(ref_tables) | set(cur_tables))
        ref_only = set(ref_tables) - set(cur_tables)
        cur_only = set(cur_tables) - set(ref_tables)

        if ref_only:
            print(f"  WARN: tables only in ref: {ref_only}")
        if cur_only:
            print(f"  WARN: tables only in cur: {cur_only}")

        # Compare each table
        print(f"  tables ({len(all_tables)}):")
        mismatches = []
        for table in all_tables:
            in_ref = table in ref_tables
            in_cur = table in cur_tables

            if not in_ref:
                print(f"    {table:30s}  NEW (cur only)")
                continue
            if not in_cur:
                print(f"    {table:30s}  REMOVED (ref only)")
                mismatches.append(
                    f"{table}: present in ref but missing in current"
                )
                continue

            ref_count, ref_hash = _table_content_hash(session, ref_ks, table)
            cur_count, cur_hash = _table_content_hash(session, cur_ks, table)

            if table in METADATA_TABLES:
                status = f"META (ref={ref_count}, cur={cur_count})"
                print(
                    f"    {table:30s} "
                    f"ref={ref_count:>6,}  cur={cur_count:>6,}  "
                    f"{status}"
                )
                continue

            match = ref_hash == cur_hash
            known = (currency, table) in KNOWN_CONTENT_DIVERGENCES
            status = "MATCH" if match else ("KNOWN DIVERGENCE" if known else "MISMATCH")

            print(
                f"    {table:30s} "
                f"ref={ref_count:>6,}  cur={cur_count:>6,}  "
                f"{status}"
            )

            if not match and not known:
                mismatches.append(
                    f"{table}: content differs "
                    f"(ref={ref_count} rows hash={ref_hash[:12]}... "
                    f"cur={cur_count} rows hash={cur_hash[:12]}...)"
                )

        # Verify minimum row counts in current
        min_rows = EXPECTED_MIN_ROWS.get(currency, {})
        for table_name, expected_min in min_rows.items():
            if table_name not in cur_tables:
                continue
            cur_count, _ = _table_content_hash(session, cur_ks, table_name)
            assert cur_count >= expected_min, (
                f"{currency}.{table_name}: got {cur_count} rows, "
                f"expected >= {expected_min}"
            )

        cluster.shutdown()

        if mismatches:
            print("  result:          FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] Cassandra content mismatches:\n"
                + "\n".join(f"  - {m}" for m in mismatches)
            )
        else:
            print("  result:          PASS")
            print(f"{'=' * 68}")
