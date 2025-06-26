import logging
import os
import time
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

try:
    import deltalake as dl
    import pyarrow as pa
    from deltalake import DeltaTable
    from pyarrow.lib import ArrowInvalid
except ImportError:
    _has_ingest_dependencies = False
else:
    _has_ingest_dependencies = True

import pydantic

from ...schema.resources.parquet.account import ACCOUNT_SCHEMA_RAW
from ...schema.resources.parquet.account_trx import ACCOUNT_TRX_SCHEMA_RAW
from ...schema.resources.parquet.utxo import UTXO_SCHEMA_RAW
from ..common import BlockRangeContent, Sink

logger = logging.getLogger(__name__)


def optimize_tables(
    network: str, directory: str, s3_credentials: Optional[dict] = None, mode="both"
) -> None:
    tables = [n.table_name for n in CONFIG_MAP[network].table_configs]

    for table in tables:
        optimize_table(directory, table, s3_credentials=s3_credentials, mode=mode)


def optimize_table(
    directory: str, table_name: str, s3_credentials: Optional[dict] = None, mode="both"
):
    if not _has_ingest_dependencies:
        raise ImportError(
            "Need deltalake and pyarrow installed. Please install gslib with ingest dependencies."
        )
    logger.debug(f"Optimizing table {table_name} in directory {directory}...")

    if s3_credentials and directory.startswith("s3"):
        storage_options = {
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "timeout": "300s",
        }
        storage_options.update(s3_credentials)
        table_path = f"{directory}/{table_name}"
    else:
        storage_options = {}
        table_path = os.path.join(directory, table_name)

    table = DeltaTable(table_path, storage_options=storage_options)
    MB = 1024 * 1024
    if mode in ["both", "compact"]:
        logger.debug("Compacting table...")
        # some sources say 1GB, default in the lib is 256MB, we take 512MB
        # we strive for a manageable amount of Memory consumption, so we limit
        # the concurrency
        metrics = table.optimize.compact(target_size=512 * MB, max_concurrent_tasks=15)
        logger.debug(f"Compaction metrics: {metrics}")

    if mode in ["both", "vacuum"]:
        logger.debug("Vacuuming table...")
        metrics = table.vacuum(
            retention_hours=0, enforce_retention_duration=False, dry_run=False
        )
        logger.debug(f"Files vacuumed: {len(metrics)}")
    logger.debug("Table optimized")


class DeltaTableWriter:
    def __init__(
        self,
        path: str,
        table_name: str,
        schema: "pa.Schema",
        partition_cols: Optional[tuple] = None,
        mode: str = "append",
        primary_keys: Optional[List[str]] = None,
        s3_credentials: Optional[dict] = None,
    ) -> None:
        if not _has_ingest_dependencies:
            raise ImportError(
                "Need deltalake and pyarrow installed. Please install gslib with ingest dependencies."
            )
        self.path = path
        self.table_name = table_name
        self.schema = schema
        self.partition_cols = partition_cols
        self.primary_keys = primary_keys
        self.mode = mode
        self.s3_credentials = s3_credentials
        if mode not in ["overwrite", "merge", "append"]:
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
        unique_fields = {item for sublist in fields_in_data for item in sublist}
        table = pa.Table.from_pylist(mapping=data, schema=self.schema)

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
                "AWS_EC2_METADATA_DISABLED": "true",  # right now only works as env var
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
                predicate = ""
                for col in self.partition_cols:
                    predicate += f" AND {col} = '{partition}'"

                predicate = predicate[5:]
                partition_by = list(self.partition_cols)
            else:
                predicate = None
                partition_by = None

            not_written = True
            options = {}
            max_attempts = 20
            attempts = 0
            fraction = 0.5
            while not_written:
                attempts += 1
                if attempts > max_attempts:
                    raise ValueError(
                        f"Could not write delta-file after {max_attempts} attempts."
                    )

                try:
                    dl.write_deltalake(
                        table_path,
                        table,
                        partition_by=partition_by,
                        mode=delta_write_mode,
                        # schema_mode="overwrite", # todo maybe make this dependent
                        # on delta_write mode?
                        engine="rust",
                        predicate=predicate,
                        storage_options=storage_options,
                        **options,
                    )
                    not_written = False
                except ArrowInvalid as e:
                    ste = str(e)
                    if "large_binary" in ste or "named input expected length":
                        new_row_group_size = int(len(data) * fraction)
                        if new_row_group_size < 100:
                            raise e
                        options = {
                            "max_rows_per_group": new_row_group_size,
                            "min_rows_per_group": new_row_group_size,
                        }
                        logger.warning(
                            "Could not write delta-file binary input col because "
                            "its too large (> 2GB uncompressed),"
                            f"retry with smaller row group size {new_row_group_size}."
                        )
                        fraction /= 2
                    else:
                        raise e

            logger.debug(
                f"Writing {len(table)} records in mode {self.mode} "
                f"took "
                f"{time.time() - time_} seconds"
            )

            return

        elif self.mode == "merge":
            time_ = time.time()

            target = DeltaTable(table_path, storage_options=storage_options)
            # can either use overwrite with predicate; or try to merge,
            # as of 0.19 merge is faster and doesnt read the entire partition
            # todo viability in graphsense not yet tested
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
                .when_not_matched_insert_all()
                .execute()
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


class TableWriteConfig(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    table_name: str
    table_schema: object
    partition_cols: Optional[tuple] = None
    primary_keys: Optional[List[str]] = None
    blockindep: Optional[bool] = False


class DBWriteConfig(pydantic.BaseModel):
    table_configs: List[TableWriteConfig]


TRX_DBWRITE_CONFIG = DBWriteConfig(
    table_configs=[
        TableWriteConfig(
            table_name="block",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW.get("block"),
            partition_cols=("partition",),
            primary_keys=["block_id"],
        ),
        TableWriteConfig(
            table_name="transaction",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW.get("transaction"),
            partition_cols=("partition",),
            primary_keys=["block_id", "tx_hash"],
        ),
        TableWriteConfig(
            table_name="trace",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW.get("trace"),
            partition_cols=("partition",),
            primary_keys=["block_id", "trace_index"],
        ),
        TableWriteConfig(
            table_name="log",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW.get("log"),
            partition_cols=("partition",),
            primary_keys=["block_id", "log_index"],
        ),
        TableWriteConfig(
            table_name="trc10",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW.get("trc10"),
            primary_keys=["contract_address"],
            blockindep=True,
        ),
        TableWriteConfig(
            table_name="fee",
            table_schema=ACCOUNT_TRX_SCHEMA_RAW.get("fee"),
            partition_cols=("partition",),
            primary_keys=["tx_hash"],
        ),
    ],
)


ETH_DBWRITE_CONFIG = DBWriteConfig(
    table_configs=[
        TableWriteConfig(
            table_name="block",
            table_schema=ACCOUNT_SCHEMA_RAW.get("block"),
            partition_cols=("partition",),
            primary_keys=["block_id"],
        ),
        TableWriteConfig(
            table_name="transaction",
            table_schema=ACCOUNT_SCHEMA_RAW.get("transaction"),
            partition_cols=("partition",),
            primary_keys=["block_id", "tx_hash"],
        ),
        TableWriteConfig(
            table_name="trace",
            table_schema=ACCOUNT_SCHEMA_RAW.get("trace"),
            partition_cols=("partition",),
            primary_keys=["block_id", "trace_index"],
        ),
        TableWriteConfig(
            table_name="log",
            table_schema=ACCOUNT_SCHEMA_RAW.get("log"),
            partition_cols=("partition",),
            primary_keys=["block_id", "log_index"],
        ),
    ],
)

UTXO_DBWRITE_CONFIG = DBWriteConfig(
    table_configs=[
        TableWriteConfig(
            table_name="block",
            table_schema=UTXO_SCHEMA_RAW.get("block"),
            partition_cols=("partition",),
            primary_keys=["block_id"],
        ),
        TableWriteConfig(
            table_name="transaction",
            table_schema=UTXO_SCHEMA_RAW.get("transaction"),
            partition_cols=("partition",),
            primary_keys=["block_id", "index"],
        ),
        # TableWriteConfig(
        #     table_name="transaction_spending",
        #     table_schema=UTXO_SCHEMA_RAW["transaction_spending"],
        #     partition_cols=("partition",),
        #     primary_keys=[
        #         "spending_tx_hash",
        #         "spent_tx_hash",
        #         "spending_input_index",
        #         "spent_output_index",
        #     ],
        # ),
    ]
)


class DeltaDumpWriter(Sink):
    def __init__(
        self,
        directory: str,
        db_write_config: DBWriteConfig,
        s3_credentials: Optional[dict] = None,
        write_mode: str = "overwrite",
    ) -> None:
        if not _has_ingest_dependencies:
            raise ImportError(
                "Need deltalake and pyarrow installed. Please install gslib with ingest dependencies."
            )
        self.directory = directory
        self.db_write_config = db_write_config
        self.s3_credentials = s3_credentials
        self.write_mode = write_mode

        self.writers = {
            # method instead the lookup we now have which is probably
            # better anyway (see WRITER_MAP)
            table_config.table_name: self.create_table_writer(table_config)
            for table_config in self.db_write_config.table_configs
        }

    def create_table_writer(self, table_config: TableWriteConfig):
        if table_config.blockindep:
            mode = "overwrite"
        else:
            mode = self.write_mode
        return DeltaTableWriter(
            path=self.directory,
            table_name=table_config.table_name,
            schema=table_config.table_schema,
            partition_cols=table_config.partition_cols,
            mode=mode,
            primary_keys=table_config.primary_keys,
            s3_credentials=self.s3_credentials,
        )

    def write_table(self, table_content: Tuple[str, List[dict]]):
        writer = self.writers[table_content[0]]
        writer.write_delta(table_content[1])

    def write(self, sink_content: BlockRangeContent):
        for table_content in sink_content.table_contents.items():
            self.write_table(table_content)

    def highest_block(self):
        logger.debug("Getting highest block")
        if self.s3_credentials:
            storage_options = {
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            }
            storage_options.update(self.s3_credentials)
        else:
            storage_options = {}

        dataset = DeltaTable(
            f"{self.directory}/block", storage_options=storage_options
        ).to_pyarrow_dataset()
        highest_block = pa.compute.max(
            dataset.to_table(columns=["block_id"])["block_id"]
        ).as_py()
        logger.info(f"Highest block: {highest_block}")
        return highest_block


CONFIG_MAP = {
    "trx": TRX_DBWRITE_CONFIG,
    "eth": ETH_DBWRITE_CONFIG,
    "btc": UTXO_DBWRITE_CONFIG,
    "ltc": UTXO_DBWRITE_CONFIG,
    "bch": UTXO_DBWRITE_CONFIG,
    "zec": UTXO_DBWRITE_CONFIG,
}


class DeltaDumpSinkFactory:  # todo could be a function
    @staticmethod
    def create_writer(
        network: str, s3_credentials: dict, write_mode: str, directory: str
    ) -> DeltaDumpWriter:
        db_write_config = CONFIG_MAP.get(network)
        if not db_write_config:
            raise ValueError(f"Invalid network: {network}")

        return DeltaDumpWriter(
            db_write_config=db_write_config,
            s3_credentials=s3_credentials,
            write_mode=write_mode,
            directory=directory,
        )
