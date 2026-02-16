"""Tests for DeltaTableConnector.get_table_files.

Uses a local temp Delta Lake table (no S3/MinIO required).
"""

import tempfile
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
