"""Continuation (split-ingest) regression test.

Verifies that ingesting a block range in two sequential runs produces
identical Delta Lake output to ingesting the same range in one shot.

This catches bugs in cross-batch state such as:
- UTXO output cache not persisting between batches (input resolution fails)
- Transaction ID counters resetting on continuation
- Delta Lake append-mode producing different data than overwrite

Two ingests per currency:
1. one-shot:   [start, end] with write-mode=overwrite  → Delta at path A
2. split:      [start, split] overwrite + [split+1, end] append → Delta at path B

Comparison: A == B (identical parquet content for all tables)

Requires:
- Docker (for MinIO testcontainer)
- Node URLs configured in .graphsense.yaml
- ``CONTINUATION_CURRENCIES`` env var (default: all currencies)
"""

import pytest

from tests.deltalake.snapshot import (
    TableSnapshot,
    _compute_content_hash,
    _sortable_type,
    capture_snapshot,
)
from tests.continuation.config import ContinuationConfig
from tests.continuation.ingest_runner import run_ingest

pytestmark = pytest.mark.continuation


def _find_differing_columns(
    oneshot_table: TableSnapshot, split_table: TableSnapshot
) -> list[str]:
    """Return column names whose content differs between two snapshots."""
    if oneshot_table._arrow_table is None or split_table._arrow_table is None:
        return []
    import pyarrow.compute as pc

    common_cols = sorted(
        set(oneshot_table.column_names) & set(split_table.column_names)
    )
    differing = []
    for col in common_cols:
        a_col = oneshot_table._arrow_table.column(col)
        b_col = split_table._arrow_table.column(col)
        if _sortable_type(a_col.type):
            a_sorted = a_col.take(pc.sort_indices(a_col))
            b_sorted = b_col.take(pc.sort_indices(b_col))
            if a_sorted != b_sorted:
                differing.append(col)
                for i in range(min(len(a_sorted), 3)):
                    if a_sorted[i] != b_sorted[i]:
                        print(
                            f"        {col}[{i}]: oneshot={a_sorted[i].as_py()!r} "
                            f"split={b_sorted[i].as_py()!r}"
                        )
                        break
        else:
            if (
                _compute_content_hash(oneshot_table._arrow_table.select([col]))
                != _compute_content_hash(split_table._arrow_table.select([col]))
            ):
                differing.append(col)
    return differing


class TestContinuation:
    """One-shot ingest vs split ingest must produce identical Delta Lake output."""

    def test_continuation(
        self,
        continuation_config: ContinuationConfig,
        minio_config: dict[str, str],
        storage_options: dict[str, str],
        current_venv,
    ):
        currency = continuation_config.currency
        range_id = continuation_config.range_id
        bucket = minio_config["bucket"]

        minio_kw = dict(
            minio_endpoint=minio_config["endpoint"],
            minio_access_key=minio_config["access_key"],
            minio_secret_key=minio_config["secret_key"],
        )

        delta_path_oneshot = f"s3://{bucket}/oneshot/{currency}/{range_id}"
        delta_path_split = f"s3://{bucket}/split/{currency}/{range_id}"

        start = continuation_config.start_block
        end = continuation_config.end_block
        split = continuation_config.split_block

        print(f"\n{'=' * 68}")
        print(f"CONTINUATION: {currency.upper()} [{range_id}]")
        print(f"  blocks:     {start:,}-{end:,} ({continuation_config.num_blocks} blocks)")
        print(f"  split at:   {split:,} (first: {start}-{split}, second: {split + 1}-{end})")
        if continuation_config.range_note:
            print(f"  note:       {continuation_config.range_note}")

        # ------------------------------------------------------------------
        # 1. One-shot ingest: [start, end]
        # ------------------------------------------------------------------
        print("  [1/3] one-shot ingest ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=continuation_config,
            delta_directory=delta_path_oneshot,
            start_block=start,
            end_block=end,
            write_mode="overwrite",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # 2. Split ingest part 1: [start, split]
        # ------------------------------------------------------------------
        print("  [2/3] split ingest part 1 ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=continuation_config,
            delta_directory=delta_path_split,
            start_block=start,
            end_block=split,
            write_mode="overwrite",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # 3. Split ingest part 2: [split+1, end] (append)
        # ------------------------------------------------------------------
        print("  [3/3] split ingest part 2 ...", end=" ", flush=True)
        run_ingest(
            venv_dir=current_venv,
            config=continuation_config,
            delta_directory=delta_path_split,
            start_block=split + 1,
            end_block=end,
            write_mode="append",
            **minio_kw,
        )
        print("done")

        # ------------------------------------------------------------------
        # Compare Delta Lake tables: one-shot vs split
        # ------------------------------------------------------------------
        print("\n  Delta Lake comparison:")
        tables = continuation_config.tables

        snap_oneshot = capture_snapshot(
            storage_options, delta_path_oneshot, tables,
            "one-shot",
            block_range=(start, end),
        )
        snap_split = capture_snapshot(
            storage_options, delta_path_split, tables,
            "split",
            block_range=(start, end),
        )

        mismatches = []
        for table_name in tables:
            oneshot_table = snap_oneshot.tables.get(table_name)
            split_table = snap_split.tables.get(table_name)

            if oneshot_table is None and split_table is None:
                print(f"    {table_name:30s}  EMPTY (both)")
                continue
            if oneshot_table is None:
                mismatches.append(f"{table_name}: missing in one-shot")
                print(f"    {table_name:30s}  MISSING IN ONE-SHOT")
                continue
            if split_table is None:
                mismatches.append(f"{table_name}: missing in split")
                print(f"    {table_name:30s}  MISSING IN SPLIT")
                continue

            match = oneshot_table.content_hash == split_table.content_hash
            row_match = oneshot_table.row_count == split_table.row_count

            if match:
                status = "MATCH"
            else:
                status = "MISMATCH"

            print(
                f"    {table_name:30s} "
                f"one={oneshot_table.row_count:>6,}  split={split_table.row_count:>6,}  "
                f"{status}"
            )

            if not match:
                differing_cols = _find_differing_columns(oneshot_table, split_table)
                col_info = f" differing_cols={differing_cols}" if differing_cols else ""
                if differing_cols:
                    print(f"      differing columns: {differing_cols}")
                mismatches.append(
                    f"{table_name}: content differs "
                    f"(one={oneshot_table.row_count} rows hash={oneshot_table.content_hash[:12]}... "
                    f"split={split_table.row_count} rows hash={split_table.content_hash[:12]}..."
                    f"{col_info})"
                )

        if mismatches:
            print(f"  result:     FAIL")
            print(f"{'=' * 68}")
            pytest.fail(
                f"{currency}[{range_id}] Continuation mismatches:\n"
                + "\n".join(f"  - {m}" for m in mismatches)
            )
        else:
            print(f"  result:     PASS")
            print(f"{'=' * 68}")
