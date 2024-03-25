from pathlib import Path
from urllib.parse import urlparse

import deltalake as dl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import HadoopFileSystem


def write_parquet(
    path: str,
    table_name: str,
    parameters: list,
    schema_table: dict,
    partition_cols: list = ["partition"],
    deltatable: bool = False,
) -> None:
    print("Writing ", table_name)
    if not parameters:
        return
    table = pa.Table.from_pylist(parameters, schema=schema_table[table_name])

    if deltatable:
        path = Path(path)
        dl.write_deltalake(
            path / table_name, table, partition_by=partition_cols, mode="append"
        )
        return

    if path.startswith("hdfs://"):
        o = urlparse(path)
        fs = HadoopFileSystem(o.hostname, o.port)

        pq.write_to_dataset(
            table,
            root_path=o.path / table_name,
            partition_cols=partition_cols,
            existing_data_behavior="app",
            filesystem=fs,
        )
    else:
        path = Path(path)
        if path.exists():
            pq.write_to_dataset(
                table,
                root_path=path / table_name,
                partition_cols=partition_cols,
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            raise Exception(f"Parquet file path not found: {path}")
