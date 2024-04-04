import logging
import time
from pathlib import Path
from typing import List, Optional

import deltalake as dl
import pyarrow as pa
from deltalake import DeltaTable

logger = logging.getLogger(__name__)

# 1. Delta
# overwrite mode, -> append/merge mode
# minio -> docker compose

# todo dependecies pip install boto3 s3fs deltalake?


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


def write_delta(*args, **kwargs):
    pass


class DeltaTableWriter:
    def __init__(
        self,
        path: str,
        table_name: str,
        schema: dict,
        partition_cols: tuple = ("partition"),
        mode: str = "append",
        primary_keys: Optional[List[str]] = None,
        s3_credentials: Optional[dict] = None,
    ) -> None:
        self.path = path
        self.table_name = table_name
        self.schema = schema
        self.partition_cols = partition_cols
        self.primary_keys = primary_keys
        self.mode = mode
        self.s3_credentials = s3_credentials
        if mode not in ["overwrite", "merge"]:
            raise ValueError(f"Invalid mode: {mode}")

        self.current_partition = -1

    def write_delta(
        self,
        data: List[dict],
    ) -> None:
        print("Writing ", self.table_name)

        if not data:
            return

        table_path = f"{self.path}/{self.table_name}"

        # create table_path if it doesnt exist
        if self.path.startswith("s3://"):
            pass
        else:
            Path(table_path).mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(data, schema=self.schema)

        time_ = time.time()

        if self.s3_credentials:
            storage_options = {
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            }
            storage_options.update(self.s3_credentials)
        else:
            storage_options = {}

        if self.mode == "overwrite":
            unique_partitions = table.column("partition").unique()

            # only 1 partition is allowed to be written at once in overwrite mode
            assert len(unique_partitions) == 1

            partition = unique_partitions[0].as_py()

            if partition == self.current_partition:
                delta_write_mode = "append"
            else:
                self.current_partition = partition
                delta_write_mode = "overwrite"

            partition_filters = [("partition", "=", str(partition))]

            dl.write_deltalake(
                table_path,
                table,
                partition_by=list(self.partition_cols),
                mode=delta_write_mode,
                partition_filters=partition_filters,
                storage_options=storage_options,
            )
            return

        elif self.mode == "merge":
            # MERGE MODE
            if not delta_table_exists(table_path):
                dl.write_deltalake(
                    table_path,
                    table,
                    partition_by=self.partition_cols,
                    mode="overwrite",
                )
                return

            target = DeltaTable(table_path)
            # can either use overwrite with partition_filters; or try to merge
            predicate_cols = (
                ["partition"] + self.primary_keys
                if self.primary_keys
                else ["partition"]
            )
            predicate = " AND ".join([f"s.{col} = t.{col}" for col in predicate_cols])

            (
                target.merge(
                    source=table,
                    predicate=predicate,
                    source_alias="s",
                    target_alias="t",
                )  # would require a predicate (primary key) to merge
                # .when_matched_update_all() # we dont need this; can
                # simply overwrite instead
                .when_not_matched_insert_all().execute()
            )
            logger.warning(
                f"Delta merge of length {len(data)} on {self.table_name} "
                f"took {time.time() - time_} seconds"
            )
            return
