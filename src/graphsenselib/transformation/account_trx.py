"""Account TRX (Tron) transformation: Delta Lake → Cassandra raw keyspace."""

import logging

from graphsenselib.ingest.account import (
    BLOCK_BUCKET_SIZE as ACCOUNT_BLOCK_BUCKET_SIZE,
    TX_HASH_PREFIX_LEN as ACCOUNT_TX_HASH_PREFIX_LEN,
)

from graphsenselib.transformation.account import (
    _DELTA_ONLY_COLS_LOG,
    _DELTA_ONLY_COLS_TX,
    _VARINT_COLS_TX,
    _convert_varint_cols,
)

logger = logging.getLogger(__name__)

# TRX block has same base columns as ETH but gas_limit is varint
_DELTA_ONLY_COLS_BLOCK_TRX = {
    "partition",
    "withdrawals",
    "excess_blob_gas",
    "withdrawals_root",
    "blob_gas_used",
    "parent_beacon_block_root",
    "uncles",
    "requests_hash",
}

_VARINT_COLS_BLOCK_TRX = {"difficulty", "total_difficulty", "gas_limit"}
_VARINT_COLS_TRACE_TRX = {"call_value"}

# TRX trc10 has varint columns stored as int64 in parquet (no binary conversion needed)
# but we list them for completeness
_VARINT_COLS_TRC10 = {
    "total_supply",
    "trx_num",
    "num",
    "start_time",
    "end_time",
    "public_latest_free_net_time",
}

# Columns in Delta fee table that are not in Cassandra
_DELTA_ONLY_COLS_FEE = {"partition", "block_id"}

# Columns in Delta trc10 table that are not in Cassandra
_DELTA_ONLY_COLS_TRC10 = {"partition", "public_free_asset_net_usage", "order"}

# TRX trace columns that are not in Cassandra
_DELTA_ONLY_COLS_TRACE_TRX = {"partition"}


class AccountTrxTransformation:
    """Reads TRX Delta tables and writes to Cassandra raw keyspace.

    TRX shares the block/transaction/log schema with ETH but has
    different trace columns and additional fee/trc10 tables.
    """

    def __init__(
        self,
        spark,
        delta_lake_path,
        raw_keyspace,
        block_bucket_size=ACCOUNT_BLOCK_BUCKET_SIZE,
        tx_hash_prefix_len=ACCOUNT_TX_HASH_PREFIX_LEN,
    ):
        self.spark = spark
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
        drop_cols = [c for c in _DELTA_ONLY_COLS_BLOCK_TRX if c in df.columns]
        df = df.drop(*drop_cols)
        if "transaction_count" in df.columns:
            df = df.withColumn(
                "transaction_count", F.col("transaction_count").cast("short")
            )
        df = _convert_varint_cols(df, _VARINT_COLS_BLOCK_TRX)
        self._write_cassandra(df, "block")

    def transform_transaction(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("transaction", start_block, end_block)
        drop_cols = [c for c in _DELTA_ONLY_COLS_TX if c in df.columns]
        df = df.drop(*drop_cols)
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
        drop_cols = [c for c in _DELTA_ONLY_COLS_TRACE_TRX if c in df.columns]
        df = df.drop(*drop_cols)
        if "internal_index" in df.columns:
            df = df.withColumn("internal_index", F.col("internal_index").cast("short"))
        if "call_info_index" in df.columns:
            df = df.withColumn(
                "call_info_index", F.col("call_info_index").cast("short")
            )
        df = _convert_varint_cols(df, _VARINT_COLS_TRACE_TRX)
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

    def transform_fee(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("fee", start_block, end_block)
        drop_cols = [c for c in _DELTA_ONLY_COLS_FEE if c in df.columns]
        df = df.drop(*drop_cols)
        # Derive tx_hash_prefix from tx_hash (needed for Cassandra primary key)
        if "tx_hash_prefix" not in df.columns and "tx_hash" in df.columns:
            df = df.withColumn(
                "tx_hash_prefix",
                F.substring(
                    F.lower(F.hex(F.col("tx_hash"))), 1, self.tx_hash_prefix_len
                ),
            )
        self._write_cassandra(df, "fee")

    def transform_trc10(self, start_block, end_block):
        df = self._read_delta("trc10", start_block, end_block)
        drop_cols = [c for c in _DELTA_ONLY_COLS_TRC10 if c in df.columns]
        df = df.drop(*drop_cols)
        # trc10 varint columns are int64 in parquet, connector handles int→varint
        df = _convert_varint_cols(df, _VARINT_COLS_TRC10)
        if "vote_score" in df.columns:
            from pyspark.sql import functions as F

            df = df.withColumn("vote_score", F.col("vote_score").cast("short"))
        if "precision" in df.columns:
            from pyspark.sql import functions as F

            df = df.withColumn("precision", F.col("precision").cast("short"))
        self._write_cassandra(df, "trc10")

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
        all_tables = ["block", "transaction", "trace", "log", "fee", "trc10"]
        targets = tables if tables else all_tables

        logger.info(
            f"AccountTrxTransformation: {self.raw_keyspace} "
            f"blocks {start_block}-{end_block}, tables={targets}"
        )

        table_methods = {
            "block": self.transform_block,
            "transaction": self.transform_transaction,
            "trace": self.transform_trace,
            "log": self.transform_log,
            "fee": self.transform_fee,
            "trc10": self.transform_trc10,
        }

        for table in targets:
            if table in table_methods:
                logger.info(f"Transforming {table}...")
                table_methods[table](start_block, end_block)

        logger.info("Writing configuration...")
        self.write_configuration()

        logger.info("AccountTrxTransformation complete.")
