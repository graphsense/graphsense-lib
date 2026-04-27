"""Account-model transformation: Delta Lake → Cassandra raw keyspace.

Defines `AccountTransformationBase` with all logic shared between EVM-style
chains (read/write IO, block/transaction/log transforms, configuration write,
run loop, access_list field rename), driven by class-attribute column sets.

`AccountTransformation` is the ETH binding; TRX lives in `account_trx.py`.
"""

import logging

from graphsenselib.ingest.account import (
    BLOCK_BUCKET_SIZE as ACCOUNT_BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN as ACCOUNT_TX_HASH_PREFIX_LEN,
)

logger = logging.getLogger(__name__)


def _binary_to_bigint_string_udf():
    """UDF: big-endian binary bytes → unsigned decimal string.

    Runs on executors. Driver/worker Python versions must match — set
    spark.pyspark.python in spark_config when they differ.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    @F.udf(StringType())
    def bin_to_str(b):
        if b is None:
            return None
        return str(int.from_bytes(b, "big", signed=False))

    return bin_to_str


def _write_ingest_complete_marker(spark, write_cassandra_fn):
    """Write the `ingest_complete` row to the per-keyspace `state` table.

    Spark-side equivalent of `graphsenselib.db.state.mark_ingest_complete`
    for callers that write via the cassandra-spark connector instead of an
    AnalyticsDb. MUST be the very last write of a transformation run — REST
    auto-discovery treats this row's presence as the readiness signal.
    """
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from graphsenselib.db.state import STATE_TABLE, build_ingest_complete_row

    row = build_ingest_complete_row()
    schema = StructType(
        [
            StructField("key", StringType(), False),
            StructField("value", StringType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    df = spark.createDataFrame([(row["key"], row["value"], row["updated_at"])], schema)
    write_cassandra_fn(df, STATE_TABLE)


def _convert_varint_cols(df, varint_cols):
    """Convert binary varint columns to decimal-string for Cassandra varint.

    Integer columns are left alone — the connector handles int → varint
    natively.
    """
    from pyspark.sql.types import BinaryType

    udf = _binary_to_bigint_string_udf()
    for col_name in varint_cols:
        if col_name in df.columns:
            col_type = df.schema[col_name].dataType
            if isinstance(col_type, BinaryType):
                df = df.withColumn(col_name, udf(df[col_name]))
    return df


class AccountTransformationBase:
    """Shared transformation logic for account-model chains.

    Subclasses set the class-attribute column sets and implement
    `transform_trace`. They may also add chain-specific tables (e.g. TRX
    fee/trc10) by overriding `_table_methods` and `TABLES`.
    """

    # Columns present in Delta but not in the Cassandra raw schema.
    DELTA_ONLY_COLS_BLOCK = frozenset(
        {
            "partition",
            "withdrawals",
            "excess_blob_gas",
            "withdrawals_root",
            "blob_gas_used",
            "parent_beacon_block_root",
            "uncles",
            "requests_hash",
        }
    )
    DELTA_ONLY_COLS_TX = frozenset(
        {
            "partition",
            "receipt_l1_gas_used",
            "receipt_l1_fee",
            "receipt_l1_fee_scalar",
            "receipt_l1_gas_price",
            "receipt_blob_gas_used",
            "receipt_blob_gas_price",
            "y_parity",
            "authorization_list",
        }
    )
    DELTA_ONLY_COLS_LOG = frozenset({"partition"})

    # Columns stored as binary in Delta but typed as varint in Cassandra.
    VARINT_COLS_BLOCK = frozenset()
    VARINT_COLS_TX = frozenset(
        {
            "value",
            "gas_price",
            "receipt_cumulative_gas_used",
            "receipt_gas_used",
            "r",
            "s",
        }
    )

    TABLES = ("block", "transaction", "trace", "log")

    def __init__(
        self,
        spark,
        delta_lake_path,
        raw_keyspace,
        block_bucket_size=ACCOUNT_BLOCK_BUCKET_SIZE,
        tx_hash_prefix_len=ACCOUNT_TX_HASH_PREFIX_LEN,
    ):
        self.spark = spark
        # Spark/Hadoop uses s3a:// not s3://
        self.delta_lake_path = delta_lake_path.rstrip("/").replace("s3://", "s3a://")
        self.raw_keyspace = raw_keyspace
        self.block_bucket_size = block_bucket_size
        self.tx_hash_prefix_len = tx_hash_prefix_len

    def _read_delta(self, table_name, start_block=None, end_block=None):
        path = f"{self.delta_lake_path}/{table_name}"
        df = self.spark.read.format("delta").load(path)
        if (
            start_block is not None
            and end_block is not None
            and "block_id" in df.columns
        ):
            df = df.filter(
                (df["block_id"] >= start_block) & (df["block_id"] <= end_block)
            )
        return df

    def _write_cassandra(self, df, table_name):
        (
            df.write.format("org.apache.spark.sql.cassandra")
            .options(table=table_name, keyspace=self.raw_keyspace)
            .mode("append")
            .save()
        )
        logger.info(f"Wrote to {self.raw_keyspace}.{table_name}")

    def transform_block(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("block", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in self.DELTA_ONLY_COLS_BLOCK if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_count" in df.columns:
            df = df.withColumn(
                "transaction_count", F.col("transaction_count").cast("short")
            )
        df = _convert_varint_cols(df, self.VARINT_COLS_BLOCK)
        self._write_cassandra(df, "block")

    def transform_transaction(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("transaction", start_block, end_block)
        drop_cols = [c for c in self.DELTA_ONLY_COLS_TX if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_index" in df.columns:
            df = df.withColumn(
                "transaction_index", F.col("transaction_index").cast("short")
            )
        if "v" in df.columns:
            df = df.withColumn("v", F.col("v").cast("short"))
        df = _convert_varint_cols(df, self.VARINT_COLS_TX)
        # Cassandra UDT access_list_entry names the field `storage_keys`, but
        # Delta keeps the JSON-RPC `storageKeys`. Rename at write time.
        if "access_list" in df.columns:
            df = df.withColumn(
                "access_list",
                F.transform(
                    "access_list",
                    lambda x: F.struct(
                        x["address"].alias("address"),
                        x["storageKeys"].alias("storage_keys"),
                    ),
                ),
            )
        self._write_cassandra(df, "transaction")

    def transform_log(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("log", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in self.DELTA_ONLY_COLS_LOG if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_index" in df.columns:
            df = df.withColumn(
                "transaction_index", F.col("transaction_index").cast("short")
            )
        if "log_index" in df.columns:
            df = df.withColumn("log_index", F.col("log_index").cast("int"))
        self._write_cassandra(df, "log")

    def transform_trace(self, start_block, end_block):
        raise NotImplementedError

    def write_configuration(self):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("block_bucket_size", IntegerType(), False),
                StructField("tx_prefix_length", IntegerType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [(self.raw_keyspace, self.block_bucket_size, self.tx_hash_prefix_len)],
            schema,
        )
        self._write_cassandra(df, "configuration")

    def _table_methods(self):
        return {
            "block": self.transform_block,
            "transaction": self.transform_transaction,
            "trace": self.transform_trace,
            "log": self.transform_log,
        }

    def write_ingest_complete_marker(self):
        _write_ingest_complete_marker(self.spark, self._write_cassandra)

    def run(self, start_block, end_block, tables=None):
        targets = list(tables) if tables else list(self.TABLES)
        cls_name = type(self).__name__
        logger.info(
            f"{cls_name}: {self.raw_keyspace} blocks {start_block}-{end_block}, "
            f"tables={targets}"
        )
        methods = self._table_methods()
        for table in targets:
            if table in methods:
                logger.info(f"Transforming {table}...")
                methods[table](start_block, end_block)
        logger.info("Writing configuration...")
        self.write_configuration()
        # MUST stay last — see graphsenselib.db.state.mark_ingest_complete.
        logger.info("Writing ingest_complete marker...")
        self.write_ingest_complete_marker()
        logger.info(f"{cls_name} complete.")


class AccountTransformation(AccountTransformationBase):
    """ETH binding."""

    VARINT_COLS_BLOCK = frozenset({"difficulty", "total_difficulty"})

    DELTA_ONLY_COLS_TRACE = frozenset({"partition", "creation_method"})
    VARINT_COLS_TRACE = frozenset({"value"})

    def transform_trace(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("trace", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in self.DELTA_ONLY_COLS_TRACE if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_index" in df.columns:
            df = df.withColumn(
                "transaction_index", F.col("transaction_index").cast("short")
            )
        if "status" in df.columns:
            df = df.withColumn("status", F.col("status").cast("short"))
        df = _convert_varint_cols(df, self.VARINT_COLS_TRACE)
        # Repartition to eliminate stragglers from data skew (e.g. ETH DoS blocks)
        df = df.repartitionByRange(2000, "block_id_group", "block_id")
        self._write_cassandra(df, "trace")
