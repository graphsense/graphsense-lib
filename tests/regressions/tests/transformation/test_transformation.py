"""Transformation regression test.

Verifies that the Delta Lake → PySpark → Cassandra path produces identical
Cassandra raw keyspace data as direct Cassandra ingest.

Two paths per currency:
1. Path A (direct):  ``ingest from-node --sinks cassandra``
2. Path B (via Delta): ``ingest from-node --sinks delta`` → ``transformation run``

Requires:
- Docker (for MinIO, Cassandra testcontainers, and PySpark container)
- Node URLs configured in .graphsense.yaml
- ``TRANSFORMATION_CURRENCIES`` env var (default: btc,eth)
"""

import hashlib

import pytest

from tests.transformation.config import TransformationConfig
from tests.transformation.ingest_runner import (
    run_ingest_cassandra_direct,
    run_ingest_delta_only,
    run_transformation,
)

pytestmark = pytest.mark.transformation

# Cassandra metadata tables — checked for existence but not content hash.
METADATA_TABLES = {"configuration", "summary_statistics"}


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


class TestTransformation:
    """Delta Lake → PySpark → Cassandra must match direct Cassandra ingest."""

    def test_delta_to_cassandra_equivalence(
        self,
        transformation_config: TransformationConfig,
        minio_config: dict[str, str],
        cassandra_coords: tuple[str, int],
        current_venv,
        transformation_image: str,
    ):
        currency = transformation_config.currency
        range_id = transformation_config.range_id
        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        # Unique keyspace names
        ks_direct = f"xform_{currency}_{range_id}_direct"
        ks_delta = f"xform_{currency}_{range_id}_delta"
        delta_path = f"s3://{bucket}/{currency}/{range_id}"

        print(f"\n{'=' * 68}")
        print(f"TRANSFORMATION: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:          "
            f"{transformation_config.start_block:,}-"
            f"{transformation_config.end_block:,} "
            f"({transformation_config.num_blocks} blocks)"
        )
        if transformation_config.range_note:
            print(f"  note:            {transformation_config.range_note}")

        # ------------------------------------------------------------------
        # Step 1: Path A — direct Cassandra ingest
        # ------------------------------------------------------------------
        print("  [1/3] direct Cassandra ingest ...", end=" ", flush=True)
        run_ingest_cassandra_direct(
            venv_dir=current_venv,
            config=transformation_config,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_direct,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 2: Path B part 1 — delta-only ingest to MinIO
        # ------------------------------------------------------------------
        print("  [2/3] delta-only ingest ...", end=" ", flush=True)
        run_ingest_delta_only(
            venv_dir=current_venv,
            config=transformation_config,
            delta_directory=delta_path,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 3: Path B part 2 — PySpark transformation in Docker → Cassandra
        # ------------------------------------------------------------------
        print("  [3/3] PySpark transformation ...", end=" ", flush=True)
        run_transformation(
            image_name=transformation_image,
            config=transformation_config,
            delta_directory=delta_path,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_delta,
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Step 4: Compare all Cassandra tables
        # ------------------------------------------------------------------
        print("\n  Cassandra comparison:")

        from cassandra.cluster import Cluster

        with Cluster([cass_host], port=cass_port) as cluster:
            session = cluster.connect()

            direct_tables = sorted(
                row.table_name
                for row in session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (ks_direct,),
                )
            )
            delta_tables = sorted(
                row.table_name
                for row in session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (ks_delta,),
                )
            )

            all_tables = sorted(set(direct_tables) | set(delta_tables))
            direct_only = set(direct_tables) - set(delta_tables)
            delta_only = set(delta_tables) - set(direct_tables)

            if direct_only:
                print(f"    WARN: tables only in direct: {direct_only}")
            if delta_only:
                print(f"    WARN: tables only in delta: {delta_only}")

            mismatches = []
            for table_name in all_tables:
                in_direct = table_name in direct_tables
                in_delta = table_name in delta_tables

                if not in_direct:
                    print(f"    {table_name:30s}  DELTA ONLY")
                    mismatches.append(f"{table_name}: only in delta path")
                    continue
                if not in_delta:
                    print(f"    {table_name:30s}  DIRECT ONLY")
                    mismatches.append(f"{table_name}: only in direct path")
                    continue

                if table_name in METADATA_TABLES:
                    direct_count, _ = _table_content_hash(
                        session, ks_direct, table_name
                    )
                    delta_count, _ = _table_content_hash(
                        session, ks_delta, table_name
                    )
                    print(
                        f"    {table_name:30s} "
                        f"direct={direct_count:>6,}  delta={delta_count:>6,}  META"
                    )
                    continue

                direct_count, direct_hash = _table_content_hash(
                    session, ks_direct, table_name
                )
                delta_count, delta_hash = _table_content_hash(
                    session, ks_delta, table_name
                )

                match = direct_hash == delta_hash

                if match:
                    status = "MATCH"
                else:
                    status = "MISMATCH"

                print(
                    f"    {table_name:30s} "
                    f"direct={direct_count:>6,}  delta={delta_count:>6,}  "
                    f"{status}"
                )

                if not match:
                    mismatches.append(
                        f"{table_name}: content differs "
                        f"(direct={direct_count} rows hash={direct_hash[:12]}... "
                        f"delta={delta_count} rows hash={delta_hash[:12]}...)"
                    )

        # ------------------------------------------------------------------
        # Report results
        # ------------------------------------------------------------------
        if mismatches:
            print(f"  result:          FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] Transformation regression failures:\n"
                + "\n".join(f"  - {m}" for m in mismatches)
            )
        else:
            print(f"  result:          PASS")
            print(f"{'=' * 68}")
