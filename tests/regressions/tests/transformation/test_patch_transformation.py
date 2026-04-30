"""Patch-mode transformation regression test.

Verifies that running the PySpark transformation as a single full pass over
[start, end] produces the same Cassandra raw keyspace content as splitting the
range into two halves and running the second half with ``--patch``.

Account chains only — for UTXO chains ``--patch`` is rejected by the CLI
because the derived spend tables (``transaction_spending`` /
``transaction_spent_in``) are not window-local.

Two paths per (account-)currency:
1. Path FULL:    transformation run --start a --end b
2. Path PATCH:   transformation run --start a --end m
                 transformation run --start m+1 --end b --patch

Requires the same docker / minio / cassandra / node setup as the sibling
``test_transformation.py``.
"""

import hashlib

import pytest

from tests.transformation.config import TransformationConfig
from tests.transformation.ingest_runner import (
    run_ingest_delta_only,
    run_transformation,
)

pytestmark = pytest.mark.transformation

# Same set as test_transformation.py: state + summary tables carry per-run
# timestamps that legitimately differ across paths.
METADATA_TABLES = {"configuration", "summary_statistics", "state"}


def _table_content_hash(session, keyspace: str, table: str) -> tuple[int, str]:
    rows = list(session.execute(f"SELECT * FROM {keyspace}.{table}"))  # noqa: S608
    count = len(rows)
    sorted_rows = sorted(str(sorted(row._asdict().items())) for row in rows)
    h = hashlib.sha256()
    for row_str in sorted_rows:
        h.update(row_str.encode())
    return count, h.hexdigest()


class TestPatchTransformation:
    """Full pass and split-with-patch must produce identical raw keyspaces."""

    def test_patch_equivalence(
        self,
        transformation_config: TransformationConfig,
        minio_config: dict,
        cassandra_coords: tuple,
        current_venv,
        transformation_image: str,
    ):
        if transformation_config.schema_type not in ("account", "account_trx"):
            pytest.skip(
                f"--patch is account-chains-only "
                f"(schema_type={transformation_config.schema_type})"
            )

        currency = transformation_config.currency
        range_id = transformation_config.range_id
        start = transformation_config.start_block
        end = transformation_config.end_block
        if end - start < 1:
            pytest.skip(
                f"need at least 2 blocks to split, got [{start}, {end}]"
            )
        mid = start + (end - start) // 2  # split point; second half includes mid+1

        cass_host, cass_port = cassandra_coords
        bucket = minio_config["bucket"]
        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        ks_full = f"xform_{currency}_{range_id}_patch_full"
        ks_split = f"xform_{currency}_{range_id}_patch_split"
        delta_path = f"s3://{bucket}/{currency}/{range_id}/patch"

        print(f"\n{'=' * 68}")
        print(f"PATCH: {currency.upper()} [{range_id}]")
        print(
            f"  blocks:  full=[{start:,}, {end:,}]  "
            f"split=[{start:,}, {mid:,}] + [{mid + 1:,}, {end:,}]"
        )

        # 1. Delta-only ingest once — both transformation paths read the same data.
        print("  [1/4] delta-only ingest ...", end=" ", flush=True)
        run_ingest_delta_only(
            venv_dir=current_venv,
            config=transformation_config,
            delta_directory=delta_path,
            **minio_kw,
        )
        print("done")

        # 2. Path FULL: single-shot transformation over [start, end].
        print("  [2/4] full transformation ...", end=" ", flush=True)
        run_transformation(
            image_name=transformation_image,
            config=transformation_config,
            delta_directory=delta_path,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_full,
            **minio_kw,
        )
        print("done")

        # 3. Path PATCH part 1: first half into a fresh keyspace.
        print("  [3/4] patch — first half ...", end=" ", flush=True)
        run_transformation(
            image_name=transformation_image,
            config=transformation_config,
            delta_directory=delta_path,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_split,
            start_block=start,
            end_block=mid,
            **minio_kw,
        )
        print("done")

        # 4. Path PATCH part 2: second half into the same keyspace with --patch.
        print("  [4/4] patch — second half ...", end=" ", flush=True)
        run_transformation(
            image_name=transformation_image,
            config=transformation_config,
            delta_directory=delta_path,
            cassandra_host=cass_host,
            cassandra_port=cass_port,
            keyspace_name=ks_split,
            start_block=mid + 1,
            end_block=end,
            extra_args=["--patch"],
            **minio_kw,
        )
        print("done")

        # 5. Compare keyspaces.
        print("\n  Cassandra comparison:")
        from cassandra.cluster import Cluster

        with Cluster([cass_host], port=cass_port) as cluster:
            session = cluster.connect()

            full_tables = sorted(
                row.table_name
                for row in session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (ks_full,),
                )
            )
            split_tables = sorted(
                row.table_name
                for row in session.execute(
                    "SELECT table_name FROM system_schema.tables "
                    "WHERE keyspace_name = %s",
                    (ks_split,),
                )
            )

            all_tables = sorted(set(full_tables) | set(split_tables))
            mismatches = []
            for table_name in all_tables:
                in_full = table_name in full_tables
                in_split = table_name in split_tables

                if not in_full or not in_split:
                    only = "split" if not in_full else "full"
                    print(f"    {table_name:30s}  {only.upper()} ONLY")
                    mismatches.append(f"{table_name}: only in {only} path")
                    continue

                if table_name in METADATA_TABLES:
                    full_count, _ = _table_content_hash(
                        session, ks_full, table_name
                    )
                    split_count, _ = _table_content_hash(
                        session, ks_split, table_name
                    )
                    print(
                        f"    {table_name:30s} "
                        f"full={full_count:>6,}  split={split_count:>6,}  META"
                    )
                    continue

                full_count, full_hash = _table_content_hash(
                    session, ks_full, table_name
                )
                split_count, split_hash = _table_content_hash(
                    session, ks_split, table_name
                )
                match = full_hash == split_hash
                status = "MATCH" if match else "MISMATCH"
                print(
                    f"    {table_name:30s} "
                    f"full={full_count:>6,}  split={split_count:>6,}  {status}"
                )
                if not match:
                    mismatches.append(
                        f"{table_name}: content differs "
                        f"(full={full_count} rows hash={full_hash[:12]}... "
                        f"split={split_count} rows hash={split_hash[:12]}...)"
                    )

        if mismatches:
            print("  result:          FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] Patch-mode regression failures:\n"
                + "\n".join(f"  - {m}" for m in mismatches)
            )
        print("  result:          PASS")
        print(f"{'=' * 68}")
