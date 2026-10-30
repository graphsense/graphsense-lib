"""Tests for DeltaTableConnector.get_table_files.

Uses a local temp Delta Lake table (no S3/MinIO required).
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from graphsenselib.utils.DeltaTableConnector import DeltaTableConnector


@pytest.fixture
def delta_table_dir():
    """Create a temp directory with a small Delta Lake table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        table_path = str(Path(tmpdir) / "eth" / "block")

        schema = pa.schema(
            [
                ("block_id", pa.int64()),
                ("partition", pa.int32()),
                ("value", pa.string()),
            ]
        )
        batch = pa.RecordBatch.from_pydict(
            {"block_id": [1, 2, 3], "partition": [0, 0, 0], "value": ["a", "b", "c"]},
            schema=schema,
        )
        table = pa.Table.from_batches([batch])
        write_deltalake(table_path, table)

        yield tmpdir


class TestGetTableFiles:
    """get_table_files must return full paths that point to real parquet files."""

    def test_returns_full_paths(self, delta_table_dir):
        base_dir = str(Path(delta_table_dir) / "eth")
        connector = DeltaTableConnector(base_directory=base_dir, s3_credentials=None)
        table_path = connector.get_table_path("block")

        files = connector.get_table_files(table_path)

        assert len(files) > 0
        for f in files:
            assert f.endswith(".parquet"), f"Expected parquet file, got: {f}"
            assert Path(f).exists(), f"File does not exist: {f}"

    def test_files_are_readable_parquet(self, delta_table_dir):
        base_dir = str(Path(delta_table_dir) / "eth")
        connector = DeltaTableConnector(base_directory=base_dir, s3_credentials=None)
        table_path = connector.get_table_path("block")

        files = connector.get_table_files(table_path)

        # Each path should be readable as a parquet file
        for f in files:
            pf = pq.read_table(f)
            assert pf.num_rows > 0


def _append_rows(table_path: str, n: int):
    schema = pa.schema(
        [("block_id", pa.int64()), ("partition", pa.int32()), ("value", pa.string())]
    )
    for i in range(n):
        write_deltalake(
            table_path,
            pa.table(
                {"block_id": [10 + i], "partition": [0], "value": ["x"]}, schema=schema
            ),
            mode="append",
        )


class TestVacuumRetention:
    """Compaction must not delete files an older snapshot still reads."""

    @pytest.mark.parametrize("full_vacuum", [False, True])
    def test_snapshot_from_before_compaction_stays_readable(
        self, delta_table_dir, full_vacuum
    ):
        from deltalake import DeltaTable

        from graphsenselib.ingest.delta.sink import optimize_table

        base_dir = str(Path(delta_table_dir) / "eth")
        table_path = str(Path(base_dir) / "block")
        _append_rows(table_path, 3)
        # A long-running reader (Spark) pinned this snapshot before compaction.
        snapshot_files = DeltaTable(table_path).file_uris()

        optimize_table(base_dir, "block", mode="both", full_vacuum=full_vacuum)

        assert DeltaTable(table_path).file_uris() != snapshot_files
        for f in snapshot_files:
            assert Path(f).exists(), f"vacuum deleted {f} under a running reader"

    def test_last_vacuum_date_is_set_when_nothing_was_deleted(self, delta_table_dir):
        from deltalake import DeltaTable

        from graphsenselib.ingest.delta.sink import optimize_table

        base_dir = str(Path(delta_table_dir) / "eth")
        optimize_table(base_dir, "block", mode="both", full_vacuum=True)

        ops = [h["operation"] for h in DeltaTable(f"{base_dir}/block").history()]
        assert "VACUUM END" not in ops  # nothing old enough to delete
        connector = DeltaTableConnector(base_directory=base_dir, s3_credentials=None)
        last = connector.get_last_completed_vacuum_date("block")
        assert last is not None
        assert abs((datetime.now() - last).total_seconds()) < 60

    def test_last_vacuum_date_falls_back_to_vacuum_end(self, delta_table_dir):
        from deltalake import DeltaTable

        base_dir = str(Path(delta_table_dir) / "eth")
        table_path = str(Path(base_dir) / "block")
        _append_rows(table_path, 2)
        DeltaTable(table_path).optimize.compact()
        # How tables were vacuumed before the marker property existed.
        DeltaTable(table_path).vacuum(
            retention_hours=0, enforce_retention_duration=False, dry_run=False
        )

        connector = DeltaTableConnector(base_directory=base_dir, s3_credentials=None)
        assert connector.get_last_completed_vacuum_date("block") is not None

    def test_last_vacuum_date_none_for_never_vacuumed_table(self, delta_table_dir):
        base_dir = str(Path(delta_table_dir) / "eth")
        connector = DeltaTableConnector(base_directory=base_dir, s3_credentials=None)
        assert connector.get_last_completed_vacuum_date("block") is None
