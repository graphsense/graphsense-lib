import logging
import os
import time
from pathlib import Path
from typing import List, Optional

try:
    import deltalake as dl
    import pyarrow as pa
    import pyarrow.compute  # noqa: F401 — explicit import for type checkers
    from deltalake import DeltaTable
    from pyarrow.lib import ArrowInvalid  # ty: ignore[unresolved-import]
except ImportError:
    _has_ingest_dependencies = False
else:
    _has_ingest_dependencies = True

import pydantic

try:
    from deltalake import WriterProperties

    _WRITER_PROPERTIES = WriterProperties(compression="ZSTD", compression_level=5)
except ImportError:
    _WRITER_PROPERTIES = None

from ...schema.resources.parquet.account import (
    ACCOUNT_SCHEMA_RAW,
    BINARY_COL_CONVERSION_MAP_ACCOUNT,
)
from ...schema.resources.parquet.account_trx import (
    ACCOUNT_TRX_SCHEMA_RAW,
    BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX,
)
from ...schema.resources.parquet.utxo import UTXO_SCHEMA_RAW
from ..common import BlockRangeContent, Sink
from ..transform import _finalize_inplace

logger = logging.getLogger(__name__)


def delta_lake_highest_block(directory: str, s3_credentials: Optional[dict] = None):
    """Return max(block_id) from the ``block`` Delta table at ``directory``.

    Free function so callers (e.g. the transformation CLI) can pin a top-block
    snapshot without instantiating a full ``DeltaDumpWriter``. Reads via
    deltalake/pyarrow — no Spark dependency.
    """
    if s3_credentials:
        storage_options = {
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "false",
            "AWS_CONDITIONAL_PUT": "etag",
        }
        storage_options.update(s3_credentials)
    else:
        storage_options = {}

    dataset = DeltaTable(
        f"{directory}/block", storage_options=storage_options
    ).to_pyarrow_dataset()
    return pa.compute.max(  # ty: ignore[unresolved-attribute]
        dataset.to_table(columns=["block_id"])["block_id"]
    ).as_py()


def optimize_tables(
    network: str,
    directory: str,
    s3_credentials: Optional[dict] = None,
    mode="both",
    full_vacuum=False,
    last_n_partitions: Optional[int] = None,
) -> None:
    table_configs = CONFIG_MAP[network].table_configs

    for cfg in table_configs:
        # blockindep tables (e.g. trc10) have no partition column; skip the
        # partition filter for those — compact the whole table.
        per_table_last_n = None if cfg.blockindep else last_n_partitions
        optimize_table(
            directory,
            cfg.table_name,
            s3_credentials=s3_credentials,
            mode=mode,
            full_vacuum=full_vacuum,
            last_n_partitions=per_table_last_n,
        )


def _recent_partition_filter(table: "DeltaTable", last_n: int) -> Optional[List[tuple]]:
    """Build a partition_filters list restricting to the most recent `last_n`
    partitions. Returns None if the table has no active partitions or fewer
    than `last_n` partitions (compact everything in that case).
    """
    active = table._table.get_active_partitions()
    if not active:
        return None
    # Each entry is a frozenset of (col, value) pairs; values are strings.
    partitions = sorted(int(list(p)[0][1]) for p in active)
    if len(partitions) <= last_n:
        return None
    threshold = partitions[-last_n]
    return [("partition", ">=", str(threshold))]


def optimize_table(
    directory: str,
    table_name: str,
    s3_credentials: Optional[dict] = None,
    mode="both",
    full_vacuum=False,
    last_n_partitions: Optional[int] = None,
):
    if not _has_ingest_dependencies:
        raise ImportError(
            "Need deltalake and pyarrow installed. Please install gslib with ingest dependencies."
        )
    logger.debug(f"Optimizing table {table_name} in directory {directory}...")

    if s3_credentials and directory.startswith("s3"):
        storage_options = {
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "false",
            "AWS_CONDITIONAL_PUT": "etag",
            "timeout": "300s",
        }
        storage_options.update(s3_credentials)
        table_path = f"{directory}/{table_name}"
    else:
        storage_options = {}
        table_path = os.path.join(directory, table_name)

    table = DeltaTable(table_path, storage_options=storage_options)
    MB = 1024 * 1024

    partition_filters = None
    if last_n_partitions is not None:
        partition_filters = _recent_partition_filter(table, last_n_partitions)
        if partition_filters is not None:
            logger.debug(
                f"Restricting compaction of {table_name} to "
                f"partition_filters={partition_filters}"
            )

    if mode in ["both", "compact"]:
        logger.debug("Compacting table...")
        # some sources say 1GB, default in the lib is 256MB, we take 512MB
        # we strive for a manageable amount of Memory consumption, so we limit
        # the concurrency
        metrics = table.optimize.compact(
            partition_filters=partition_filters,
            target_size=512 * MB,
            max_concurrent_tasks=15,
            writer_properties=_WRITER_PROPERTIES,
        )
        logger.debug(f"Compaction metrics: {metrics}")

    if mode in ["both", "vacuum"]:
        logger.debug("Vacuuming table...")
        metrics = table.vacuum(
            retention_hours=0,
            enforce_retention_duration=False,
            dry_run=False,
            full=full_vacuum,
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
        data: List[dict],
    ) -> None:
        time_write_start = time.time()
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
            logger.debug(
                f"Fields {fields_not_covered} in table {self.table_name}"
                f" not covered by schema (ignored)."
            )

        if self.s3_credentials:
            storage_options = {
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "false",
                "AWS_CONDITIONAL_PUT": "etag",
                "AWS_EC2_METADATA_DISABLED": "true",  # right now only works as env var
            }
            storage_options.update(self.s3_credentials)
        else:
            storage_options = {}

        if self.mode in ["overwrite", "append"]:
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
            writer_properties = _WRITER_PROPERTIES
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
                        mode=delta_write_mode,  # ty: ignore[invalid-argument-type]
                        schema_mode="merge",
                        predicate=predicate,
                        storage_options=storage_options,
                        writer_properties=writer_properties,  # ty: ignore[invalid-argument-type]
                    )
                    not_written = False
                except ArrowInvalid as e:
                    ste = str(e)
                    if "large_binary" in ste or "named input expected length":
                        new_row_group_size = int(len(data) * fraction)
                        if new_row_group_size < 100:
                            raise e
                        writer_properties = WriterProperties(
                            compression="ZSTD",
                            compression_level=5,
                            max_row_group_size=new_row_group_size,
                        )
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
                f"{time.time() - time_write_start} seconds"
            )

            return

        elif self.mode == "merge":
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
                f"took {time.time() - time_write_start} seconds"
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
        # transaction_spending, transaction_spent_in, block_transactions,
        # transaction_by_tx_prefix are derived/processed data computed from
        # raw transactions for Cassandra's query patterns. They are not stored
        # in the Delta Lake which holds only raw blockchain data.
    ]
)


class DeltaDumpWriter(Sink):
    name = "delta"

    def __init__(
        self,
        directory: str,
        db_write_config: DBWriteConfig,
        network: str = "",
        s3_credentials: Optional[dict] = None,
        write_mode: str = "overwrite",
        finalize_int_cols: Optional[dict] = None,
    ) -> None:
        if not _has_ingest_dependencies:
            raise ImportError(
                "Need deltalake and pyarrow installed. Please install gslib with ingest dependencies."
            )
        self.directory = directory
        self.db_write_config = db_write_config
        self.network = network
        self.s3_credentials = s3_credentials
        self.write_mode = write_mode
        self.finalize_int_cols = finalize_int_cols or {}

        from graphsenselib.utils.locking import delta_ingest_lock_name

        self._lock_name = delta_ingest_lock_name(directory, network)

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

    def write_table(self, table_name: str, rows: List[dict]):
        writer = self.writers[table_name]
        writer.write_delta(rows)

    def lock_name(self) -> str:
        return self._lock_name

    def write(self, block_range_content: BlockRangeContent):
        for table_name, rows in block_range_content.table_contents.items():
            if not rows:
                continue
            # Skip Cassandra-only tables that have no delta writer
            if table_name not in self.writers:
                continue
            # Shallow copy to avoid mutating shared data
            rows = [dict(r) for r in rows]
            int_cols = self.finalize_int_cols.get(table_name, [])
            if int_cols:
                _finalize_inplace(rows, int_cols)
            else:
                # Still need to pop block_id_group for Delta even when no int cols
                for row in rows:
                    row.pop("block_id_group", None)
            self.write_table(table_name, rows)

    def highest_block(self):
        logger.debug("Getting highest block")
        highest_block = delta_lake_highest_block(self.directory, self.s3_credentials)
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


FINALIZE_INT_COLS_MAP = {
    "eth": BINARY_COL_CONVERSION_MAP_ACCOUNT,
    "trx": BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX,
}


class DeltaDumpSinkFactory:  # todo could be a function
    @staticmethod
    def create_writer(
        network: str, s3_credentials: Optional[dict], write_mode: str, directory: str
    ) -> DeltaDumpWriter:
        db_write_config = CONFIG_MAP.get(network)
        if not db_write_config:
            raise ValueError(f"Invalid network: {network}")

        finalize_int_cols = FINALIZE_INT_COLS_MAP.get(network, {})

        return DeltaDumpWriter(
            db_write_config=db_write_config,
            network=network,
            s3_credentials=s3_credentials,
            write_mode=write_mode,
            directory=directory,
            finalize_int_cols=finalize_int_cols,
        )
