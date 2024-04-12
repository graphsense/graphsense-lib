import logging
import time
from pathlib import Path
from typing import Iterable, List, Optional

import deltalake as dl
import pyarrow as pa
import pydantic
from deltalake import DeltaTable

from ..schema.resources.parquet.account_trx import ACCOUNT_TRX_SCHEMA_RAW

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
        schema: pa.Schema,
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
        table = pa.Table.from_pylist(mapping=data, schema=self.schema)  # todo test this

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

        if self.mode in ["overwrite", "append"]:
            time_ = time.time()

            unique_partitions = table.column("partition").unique()

            # only 1 partition is allowed to be written at once in overwrite mode
            assert (
                len(unique_partitions) == 1
            )  # in the the case of append we wouldnt need this

            partition = unique_partitions[0].as_py()

            if self.mode == "overwrite":
                if partition == self.current_partition:
                    delta_write_mode = "append"
                else:
                    self.current_partition = partition
                    delta_write_mode = "overwrite"
            else:
                delta_write_mode = "append"

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
                f"Writing {len(table)} records in mode {self.mode} "
                f"took "
                f"{time.time() - time_} seconds"
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


# a tableconfig contains the name, schema, partition_cols, mode and primary_keys
# additionally there is an optional field that says if the table is written only once
# we call it a "small" table
class TableWriteConfig(pydantic.BaseModel):  # todo could be a dataclass
    table_name: str
    table_schema: pa.Schema
    partition_cols: Optional[tuple] = None
    primary_keys: Optional[List[str]] = None
    small: Optional[bool] = False

    class Config:
        arbitrary_types_allowed = True


class TableContent(pydantic.BaseModel):
    table_name: str
    data: List[dict]


class DBContent(pydantic.BaseModel):
    table_contents: List[TableContent]


class DBWriteConfig(pydantic.BaseModel):
    path: str
    table_configs: List[TableWriteConfig]


TRX_DBWRITE_CONFIG = DBWriteConfig(
    path="s3://test2/trx",
    # todo make flexible? in graphsense yaml it is the whole path
    table_configs=[
        TableWriteConfig(
            table_name="block",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["block"],
            partition_cols=("partition",),
            primary_keys=["block_id"],
        ),
        TableWriteConfig(
            table_name="transaction",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["transaction"],
            partition_cols=("partition",),
            primary_keys=["block_id", "tx_hash"],
        ),
        TableWriteConfig(
            table_name="trace",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["trace"],
            partition_cols=("partition",),
            primary_keys=["block_id", "trace_index"],
        ),
        TableWriteConfig(
            table_name="log",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["log"],
            partition_cols=("partition",),
            primary_keys=["block_id", "log_index"],
        ),
        TableWriteConfig(
            table_name="trc10",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["trc10"],
            primary_keys=["contract_address"],
            small=True,
        ),
        TableWriteConfig(
            table_name="fee",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["fee"],
            partition_cols=("partition",),
            primary_keys=["tx_hash"],
        ),
    ],
)


ETH_DBWRITE_CONFIG = DBWriteConfig(
    path="s3://test2/eth",  # todo remove path from graphsense yaml? - No, have
    # to find way
    # to incorporate it. probably best in the factory method of writer. path
    # is something that can change, shouldnt be in this config here
    # todo make flexible? in graphsense yaml it is the whole path
    table_configs=[
        TableWriteConfig(
            table_name="block",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["block"],
            partition_cols=("partition",),
            primary_keys=["block_id"],
        ),
        TableWriteConfig(
            table_name="transaction",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["transaction"],
            partition_cols=("partition",),
            primary_keys=["block_id", "tx_hash"],
        ),
        TableWriteConfig(
            table_name="trace",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["trace"],
            partition_cols=("partition",),
            primary_keys=["block_id", "trace_index"],
        ),
        TableWriteConfig(
            table_name="log",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW["log"],
            partition_cols=("partition",),
            primary_keys=["block_id", "log_index"],
        ),
    ],
)


class DeltaDumpWriter:
    def __init__(
        self,
        db_write_config: DBWriteConfig,
        s3_credentials: Optional[dict] = None,
        write_mode: str = "overwrite",
    ) -> None:
        self.db_write_config = db_write_config
        self.s3_credentials = s3_credentials
        self.db_write_config = db_write_config
        self.write_mode = write_mode

    def set_write_mode(self, write_mode: str):  # todo use this in the ingest module
        self.write_mode = write_mode
        self.writers = {  # todo this should be in the init, probably requires a factory
            # method instead the lookup we now have which is probably
            # better anyway (see WRITER_MAP)
            table_config.table_name: self.create_table_writer(table_config)
            for table_config in self.db_write_config.table_configs
        }

    def set_s3_credentials(
        self, s3_credentials: dict
    ):  # todo use this in the ingest module
        self.s3_credentials = s3_credentials

    def create_table_writer(self, table_config: TableWriteConfig):
        if table_config.small:
            mode = "overwrite"
        else:
            mode = self.write_mode
        return DeltaTableWriter(
            path=self.db_write_config.path,
            table_name=table_config.table_name,
            schema=table_config.table_schema,
            partition_cols=table_config.partition_cols,
            mode=mode,
            primary_keys=table_config.primary_keys,
            s3_credentials=self.s3_credentials,
        )

    def write_table(self, table_content: TableContent):
        writer = self.writers[table_content.table_name]
        writer.write_delta(table_content.data)

    def write_db(self, db_content: DBContent):
        for table_content in db_content.table_contents:
            self.write_table(
                table_content
            )  # todo table content for trc10 (small) should be given by the ETL-Module


WRITER_MAP = {  # todo kills the connection to the ingest module
    "trx": DeltaDumpWriter(db_write_config=TRX_DBWRITE_CONFIG),
    "eth": DeltaDumpWriter(db_write_config=ETH_DBWRITE_CONFIG),
}
