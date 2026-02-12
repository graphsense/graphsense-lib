"""Capture Delta Lake table state for comparison."""

import hashlib
from dataclasses import dataclass, field

import pyarrow as pa
from deltalake import DeltaTable


@dataclass
class TableSnapshot:
    """Immutable snapshot of a single Delta Lake table's state."""

    table_name: str
    row_count: int
    column_names: list[str]
    schema: dict[str, str]  # column name → arrow type string
    content_hash: str  # SHA-256 of deterministically-sorted data
    file_names: list[str]  # parquet part files
    total_file_size_bytes: int
    delta_log_version: int


@dataclass
class IngestionSnapshot:
    """Snapshot of all tables produced by an ingestion run."""

    version_label: str
    tables: dict[str, TableSnapshot] = field(default_factory=dict)
    block_range: tuple[int, int] = (0, 0)


def _sortable_type(t: pa.DataType) -> bool:
    """Return True if pyarrow can sort by this type."""
    return not (pa.types.is_list(t) or pa.types.is_large_list(t)
                or pa.types.is_struct(t) or pa.types.is_map(t))


def _compute_content_hash(table: pa.Table) -> str:
    """Deterministic SHA-256 hash of an Arrow table's contents.

    Sorts rows by sortable columns for deterministic ordering, then
    serializes to IPC format for hashing.
    """
    sort_keys = [
        (col, "ascending") for col in table.column_names
        if _sortable_type(table.schema.field(col).type)
    ]
    sorted_table = table.sort_by(sort_keys) if sort_keys else table

    hasher = hashlib.sha256()

    # Hash schema
    hasher.update(str(sorted_table.schema).encode())

    # Hash data via IPC serialization (avoids pandas dependency)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, sorted_table.schema)
    for batch in sorted_table.to_batches(max_chunksize=10_000):
        writer.write_batch(batch)
    writer.close()
    hasher.update(sink.getvalue().to_pybytes())

    return hasher.hexdigest()


def capture_table_snapshot(
    table_path: str,
    table_name: str,
    storage_options: dict[str, str],
) -> TableSnapshot:
    """Read a Delta Lake table and capture its current state."""
    dt = DeltaTable(table_path, storage_options=storage_options)
    arrow_table = dt.to_pyarrow_table()

    schema = {
        f.name: str(f.type)
        for f in arrow_table.schema
    }

    files = dt.file_uris()
    # Sum file sizes from add actions metadata
    add_actions = dt.get_add_actions(flatten=True)
    size_col_idx = add_actions.column_names.index("size_bytes")
    size_col = add_actions.column(size_col_idx)
    file_sizes = sum(int(v) for v in pa.array(size_col))

    return TableSnapshot(
        table_name=table_name,
        row_count=arrow_table.num_rows,
        column_names=arrow_table.column_names,
        schema=schema,
        content_hash=_compute_content_hash(arrow_table),
        file_names=sorted(files),
        total_file_size_bytes=file_sizes,
        delta_log_version=dt.version(),
    )


def capture_snapshot(
    storage_options: dict[str, str],
    base_path: str,
    table_names: list[str],
    version_label: str,
    block_range: tuple[int, int] = (0, 0),
) -> IngestionSnapshot:
    """Capture snapshots of all requested tables under *base_path*."""
    snapshot = IngestionSnapshot(
        version_label=version_label,
        block_range=block_range,
    )

    for name in table_names:
        table_path = f"{base_path}/{name}"
        snapshot.tables[name] = capture_table_snapshot(
            table_path, name, storage_options
        )

    return snapshot
