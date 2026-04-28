"""Cassandra ingest regression test.

Ingests a small block range for each configured currency into a
testcontainer Cassandra instance using both the reference and current
versions of graphsense-lib, then compares the results row-by-row.

The reference version always uses the legacy ``ingest from-node`` command.
The current version uses ``ingest from-node --sinks cassandra`` (new
IngestRunner-based pipeline) for all chains.

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
KNOWN_CONTENT_DIVERGENCES: set[tuple[str, str]] = {
    ("trx", "fee"),  # energy_penalty_total was mapped to net_fee, now correct
}

# Columns added in the current version that the reference version lacks.
# These are excluded from the content hash comparison.
KNOWN_COLUMN_ADDITIONS: dict[tuple[str, str], set[str]] = {
    ("eth", "transaction"): {"max_fee_per_blob_gas", "blob_versioned_hashes", "access_list"},
    ("trx", "transaction"): {"max_fee_per_blob_gas", "blob_versioned_hashes", "access_list"},
    ("btc", "transaction"): {"lock_time", "version"},
    ("ltc", "transaction"): {"lock_time", "version"},
    ("bch", "transaction"): {"lock_time", "version"},
    ("zec", "transaction"): {"lock_time", "version"},
}

# UDT-internal fields added in the current version that the reference lacks.
# Keyed by (currency, table) -> {column: {udt_field, ...}}. The named UDT
# fields are stripped from each UDT instance in `column` before hashing.
KNOWN_UDT_FIELD_ADDITIONS: dict[tuple[str, str], dict[str, set[str]]] = {
    ("btc", "transaction"): {"inputs": {"sequence"}, "outputs": {"sequence"}},
    ("ltc", "transaction"): {"inputs": {"sequence"}, "outputs": {"sequence"}},
    ("bch", "transaction"): {"inputs": {"sequence"}, "outputs": {"sequence"}},
    ("zec", "transaction"): {"inputs": {"sequence"}, "outputs": {"sequence"}},
}


def _strip_udt_fields(value, fields_to_strip: set[str]):
    """Return a list-of-tuples representation of `value` with named fields
    removed from each UDT. `value` is a list of namedtuple-like UDTs.
    Falls back to the original value if it's not a list of UDTs.
    """
    if value is None:
        return value
    out = []
    for item in value:
        if hasattr(item, "_asdict"):
            d = item._asdict()
            for f in fields_to_strip:
                d.pop(f, None)
            out.append(tuple(sorted(d.items())))
        else:
            out.append(item)
    return out

# Tables that contain version-specific metadata (e.g. ingest timestamps,
# software version strings). These are checked for existence and row count
# but not for byte-identical content.
METADATA_TABLES = {"configuration", "summary_statistics"}


def _cassandra_release(session) -> str:
    """Query the Cassandra server for its release version."""
    row = session.execute("SELECT release_version FROM system.local").one()
    return row.release_version if row else "unknown"


def _normalize_row(
    row,
    exclude_cols: set[str] | None,
    udt_field_additions: dict[str, set[str]] | None,
) -> str:
    items = []
    for k, v in row._asdict().items():
        if exclude_cols and k in exclude_cols:
            continue
        if udt_field_additions and k in udt_field_additions:
            v = _strip_udt_fields(v, udt_field_additions[k])
        items.append((k, v))
    return str(sorted(items, key=lambda kv: kv[0]))


def _table_content_hash(
    session,
    keyspace: str,
    table: str,
    exclude_cols: set[str] | None = None,
    udt_field_additions: dict[str, set[str]] | None = None,
) -> tuple[int, str]:
    """Return (row_count, sha256_hex) for a table's full content.

    Rows are fetched, converted to sorted tuples, and hashed
    deterministically so that row ordering doesn't matter.
    *exclude_cols* drops top-level columns before hashing.
    *udt_field_additions* maps a column name to a set of UDT field names that
    should be stripped from each UDT instance in that column before hashing.
    """
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    count = len(rows)
    sorted_rows = sorted(
        _normalize_row(row, exclude_cols, udt_field_additions) for row in rows
    )
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

        # All chains use from-node with the new IngestRunner pipeline
        cur_mode = "from-node"

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

            # Columns added in current that ref lacks — exclude from comparison
            extra_cols = KNOWN_COLUMN_ADDITIONS.get((currency, table), set())
            udt_extras = KNOWN_UDT_FIELD_ADDITIONS.get((currency, table))

            ref_count, ref_hash = _table_content_hash(
                session, ref_ks, table,
                exclude_cols=extra_cols, udt_field_additions=udt_extras,
            )
            cur_count, cur_hash = _table_content_hash(
                session, cur_ks, table,
                exclude_cols=extra_cols, udt_field_additions=udt_extras,
            )

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
            if extra_cols:
                status += f" (excl {len(extra_cols)} new cols)"

            print(
                f"    {table:30s} "
                f"ref={ref_count:>6,}  cur={cur_count:>6,}  "
                f"{status}"
            )

            if not match and not known:
                # Debug: show first differing rows (excluding new columns)
                ref_rows = list(session.execute(
                    f"SELECT * FROM {ref_ks}.{table}"  # noqa: S608
                ))
                cur_rows = list(session.execute(
                    f"SELECT * FROM {cur_ks}.{table}"  # noqa: S608
                ))
                def _row_key(row):
                    return _normalize_row(row, extra_cols, udt_extras)

                ref_by_key = {_row_key(r): r._asdict() for r in ref_rows}
                cur_by_key = {_row_key(r): r._asdict() for r in cur_rows}
                ref_set = set(ref_by_key.keys())
                cur_set = set(cur_by_key.keys())
                only_ref = ref_set - cur_set
                only_cur = cur_set - ref_set
                if only_ref:
                    sample = list(only_ref)[:2]
                    for s in sample:
                        print(f"      REF ONLY: {ref_by_key[s]}")
                if only_cur:
                    sample = list(only_cur)[:2]
                    for s in sample:
                        print(f"      CUR ONLY: {cur_by_key[s]}")

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
