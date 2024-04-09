import logging
import time
from pathlib import Path
from typing import Iterable, List, Optional

import deltalake as dl
import pyarrow as pa
from deltalake import DeltaTable

logger = logging.getLogger(__name__)

# 1. Delta
# overwrite mode, -> append/merge mode
# minio -> docker compose


def delta_table_exists(table_path, storage_options=None):
    try:
        # Attempt to load the DeltaTable
        DeltaTable(table_path, storage_options=storage_options)

        return True
    except Exception as e:
        # If an exception is raised, the Delta table does not exist
        print(f"Delta table does not exist. \n{e}")
        return False


class DeltaTableWriter:
    def __init__(
        self,
        path: str,
        table_name: str,
        schema: dict,
        partition_cols: Optional[tuple] = None,
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
        data: Iterable[dict],
    ) -> None:
        logger.debug(f"Writing table {self.table_name}")

        if not data:
            return

        table_path = f"{self.path}/{self.table_name}"

        # create table_path if it doesnt exist
        if self.path.startswith("s3://"):
            pass
        else:
            Path(table_path).mkdir(parents=True, exist_ok=True)

        fields_in_data = [list(d.keys()) for d in data]
        unique_fields = set([item for sublist in fields_in_data for item in sublist])
        table = pa.Table.from_pylist(data, schema=self.schema)

        fields_not_covered = unique_fields - set(table.column_names)
        if fields_not_covered:
            logger.warning(
                f"Fields {fields_not_covered} in table {self.table_name}"
                f" not covered by schema. "
            )

        if self.s3_credentials:
            storage_options = {
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            }
            storage_options.update(self.s3_credentials)
        else:
            storage_options = {}

        if self.mode == "overwrite":
            time_ = time.time()

            unique_partitions = table.column("partition").unique()

            # only 1 partition is allowed to be written at once in overwrite mode
            assert len(unique_partitions) == 1

            partition = unique_partitions[0].as_py()

            if partition == self.current_partition:
                delta_write_mode = "append"
            else:
                self.current_partition = partition
                delta_write_mode = "overwrite"

            if self.partition_cols:
                partition_filters = [("partition", "=", str(partition))]
                partition_by = list(self.partition_cols)
            else:
                partition_filters = None
                partition_by = None

            dl.write_deltalake(
                table_path,
                table,
                partition_by=partition_by,
                mode=delta_write_mode,
                partition_filters=partition_filters,
                storage_options=storage_options,
            )

            logger.debug(
                f"Writing {len(table)} records took " f"{time.time() - time_} seconds"
            )

            return

        elif self.mode == "merge":
            time_ = time.time()

            target = DeltaTable(table_path, storage_options=storage_options)
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
                f"Delta merge of length {len(table)} on {self.table_name} "
                f"took {time.time() - time_} seconds"
            )
            return


def read_table(path: str, table_name: str):
    if not path.startswith("s3://"):
        table_path = f"{path}/{table_name}"
        table = dl.DeltaTable(table_path)
        return table.to_pandas()
    else:
        raise NotImplementedError("Reading from s3 not implemented yet")
