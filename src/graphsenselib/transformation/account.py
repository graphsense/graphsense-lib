"""Account (ETH) transformation: Delta Lake → Cassandra raw keyspace."""

import logging

from graphsenselib.ingest.account import (
    BLOCK_BUCKET_SIZE as ACCOUNT_BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN as ACCOUNT_TX_HASH_PREFIX_LEN,
)

logger = logging.getLogger(__name__)

# Columns that exist in Delta (parquet) but NOT in the Cassandra raw schema.
# These are dropped before writing to Cassandra.
_DELTA_ONLY_COLS_BLOCK = {
    "partition",
    "withdrawals",
    "excess_blob_gas",
    "withdrawals_root",
    "blob_gas_used",
    "parent_beacon_block_root",
    "uncles",
    "requests_hash",
}

_DELTA_ONLY_COLS_TX = {
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

_DELTA_ONLY_COLS_TRACE = {"partition", "creation_method"}

_DELTA_ONLY_COLS_LOG = {"partition"}

# Columns stored as binary in Delta but typed as varint in Cassandra.
# These need to be converted from big-endian bytes to Decimal.
_VARINT_COLS_BLOCK = {"difficulty", "total_difficulty"}
_VARINT_COLS_TX = {
    "value",
    "gas_price",
    "receipt_cumulative_gas_used",
    "receipt_gas_used",
    "r",
    "s",
}
_VARINT_COLS_TRACE = {"value"}


def _binary_to_bigint_string_udf():
    """Create a UDF converting big-endian binary bytes to decimal string.

    This is a Python UDF that runs on executors. Requires matching Python
    versions between driver and workers — the driver ships its environment
    to workers via spark.archives (see create_spark_session).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    @F.udf(StringType())
    def bin_to_str(b):
        if b is None:
            return None
        return str(int.from_bytes(b, "big", signed=False))

    return bin_to_str


def _convert_varint_cols(df, varint_cols):
    """Convert binary varint columns to string for Cassandra varint.

    Only converts columns that are actually BinaryType — integer columns
    are left as-is (the connector handles int→varint natively).
    """
    from pyspark.sql.types import BinaryType

    udf = _binary_to_bigint_string_udf()
    for col_name in varint_cols:
        if col_name in df.columns:
            col_type = df.schema[col_name].dataType
            if isinstance(col_type, BinaryType):
                df = df.withColumn(col_name, udf(df[col_name]))
    return df


class AccountTransformation:
    """Reads ETH Delta tables and writes to Cassandra raw keyspace."""

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
        # Drop delta-only columns
        drop_cols = [c for c in _DELTA_ONLY_COLS_BLOCK if c in df.columns]
        df = df.drop(*drop_cols)
        # Cast transaction_count to smallint to match Cassandra schema
        if "transaction_count" in df.columns:
            df = df.withColumn(
                "transaction_count", F.col("transaction_count").cast("short")
            )
        df = _convert_varint_cols(df, _VARINT_COLS_BLOCK)
        self._write_cassandra(df, "block")

    def transform_transaction(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("transaction", start_block, end_block)
        # Drop delta-only columns
        drop_cols = [c for c in _DELTA_ONLY_COLS_TX if c in df.columns]
        df = df.drop(*drop_cols)
        # Cast types to match Cassandra schema
        if "transaction_index" in df.columns:
            df = df.withColumn(
                "transaction_index",
                F.col("transaction_index").cast("short"),
            )
        if "v" in df.columns:
            df = df.withColumn("v", F.col("v").cast("short"))
        df = _convert_varint_cols(df, _VARINT_COLS_TX)
        self._write_cassandra(df, "transaction")

    def transform_trace(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("trace", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in _DELTA_ONLY_COLS_TRACE if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_index" in df.columns:
            df = df.withColumn(
                "transaction_index",
                F.col("transaction_index").cast("short"),
            )
        if "status" in df.columns:
            df = df.withColumn("status", F.col("status").cast("short"))
        df = _convert_varint_cols(df, _VARINT_COLS_TRACE)
        self._write_cassandra(df, "trace")

    def transform_log(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("log", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in _DELTA_ONLY_COLS_LOG if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_index" in df.columns:
            df = df.withColumn(
                "transaction_index",
                F.col("transaction_index").cast("short"),
            )
        if "log_index" in df.columns:
            df = df.withColumn("log_index", F.col("log_index").cast("int"))
        self._write_cassandra(df, "log")

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

    def run(self, start_block, end_block, tables=None):
        all_tables = ["block", "transaction", "trace", "log"]
        targets = tables if tables else all_tables

        logger.info(
            f"AccountTransformation: {self.raw_keyspace} "
            f"blocks {start_block}-{end_block}, tables={targets}"
        )

        table_methods = {
            "block": self.transform_block,
            "transaction": self.transform_transaction,
            "trace": self.transform_trace,
            "log": self.transform_log,
        }

        for table in targets:
            if table in table_methods:
                logger.info(f"Transforming {table}...")
                table_methods[table](start_block, end_block)

        logger.info("Writing configuration...")
        self.write_configuration()

        # Account schema has no summary_statistics table
        logger.info("AccountTransformation complete.")
