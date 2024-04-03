import logging
import time
from pathlib import Path

import deltalake as dl
import pyarrow as pa
from deltalake import DeltaTable

logger = logging.getLogger(__name__)

# todo restore previous parquet functionality?
# 1. Delta
# overwrite mode, -> append/merge mode
# minio -> docker compose

# todo restore previous parquet functionality?


def delta_table_exists(table_path):
    try:
        # Attempt to load the DeltaTable
        DeltaTable(table_path)

        # If the DeltaTable can be loaded, it exists
        print("Delta table exists.")
        return True
    except Exception as e:
        # If an exception is raised, the Delta table does not exist
        print(f"Delta table does not exist. {e}")
        return False


def write_delta(
    path: str,
    table_name: str,
    data: list,
    schema_table: dict,
    partition_cols: list = ["partition"],
    primary_keys: list = None,
    mode: str = "append",
) -> None:
    print("Writing ", table_name)
    if not data:
        return
    path = Path(path)

    table = pa.Table.from_pylist(data, schema=schema_table[table_name])
    assert len(table.column("partition").unique()) == 1

    time_ = time.time()

    if mode in ["append", "overwrite"]:
        dl.write_deltalake(
            path / table_name, table, partition_by=partition_cols, mode=mode
        )
        return

    if not delta_table_exists(path / table_name):
        dl.write_deltalake(
            path / table_name, table, partition_by=partition_cols, mode="overwrite"
        )
        return

    target = DeltaTable(path / table_name)
    # can either use overwrite with partition_filters; or try to merge
    predicate_cols = ["partition"] + primary_keys if primary_keys else ["partition"]
    predicate = " AND ".join([f"s.{col} = t.{col}" for col in predicate_cols])

    (
        target.merge(
            source=table, predicate=predicate, source_alias="s", target_alias="t"
        )  # would require a predicate (primary key) to merge
        # .when_matched_update_all() # we dont need this; can simply overwrite instead
        .when_not_matched_insert_all().execute()
    )
    logger.warning(
        f"Delta merge of length {len(data)} on {table_name} "
        f"took {time.time() - time_} seconds"
    )
