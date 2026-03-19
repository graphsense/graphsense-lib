"""UTXO (BTC/LTC/BCH/ZEC) transformation: Delta Lake → Cassandra raw keyspace."""

import logging

from graphsenselib.ingest.utxo import (
    BLOCK_BUCKET_SIZE as UTXO_BLOCK_BUCKET_SIZE,
    TX_BUCKET_SIZE as UTXO_TX_BUCKET_SIZE,
    TX_HASH_PREFIX_LENGTH as UTXO_TX_HASH_PREFIX_LEN,
    _address_types as ADDRESS_TYPE_MAP,
)

logger = logging.getLogger(__name__)

# Types where address is set to null (matches logic in ingest/utxo.py:address_as_string)
NULL_ADDRESS_TYPES = {"null", "nulldata", "nonstandard", "witness_unknown", "shielded"}

# Columns in Delta that are not in the Cassandra raw schema for block
_DELTA_ONLY_COLS_BLOCK = {
    "partition",
    "type",
    "size",
    "stripped_size",
    "weight",
    "version",
    "merkle_root",
    "nonce",
    "bits",
    "coinbase_param",
}


class UtxoTransformation:
    """Reads UTXO Delta tables and writes to Cassandra raw keyspace.

    The delta-only ingest only writes `block` and `transaction` tables.
    All derived tables (spending, block_transactions, tx_prefix lookup)
    are computed from the base transaction data by Spark.
    """

    def __init__(
        self,
        spark,
        delta_lake_path,
        raw_keyspace,
        block_bucket_size=UTXO_BLOCK_BUCKET_SIZE,
        tx_bucket_size=UTXO_TX_BUCKET_SIZE,
        tx_hash_prefix_len=UTXO_TX_HASH_PREFIX_LEN,
    ):
        self.spark = spark
        # Spark/Hadoop uses s3a:// not s3://
        self.delta_lake_path = delta_lake_path.rstrip("/").replace("s3://", "s3a://")
        self.raw_keyspace = raw_keyspace
        self.block_bucket_size = block_bucket_size
        self.tx_bucket_size = tx_bucket_size
        self.tx_hash_prefix_len = tx_hash_prefix_len
        self._tx_df_cache = None
        self._tx_df_cache_range = None

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

    def _get_tx_df_with_ids(self, start_block, end_block):
        """Read transaction Delta table and compute tx_id.

        Results are cached for reuse across derived table generation.
        """
        if self._tx_df_cache is not None:
            assert self._tx_df_cache_range == (start_block, end_block), (
                f"tx_df cache range mismatch: cached {self._tx_df_cache_range} "
                f"vs requested ({start_block}, {end_block})"
            )
            return self._tx_df_cache

        from pyspark.sql import Window, functions as F

        path = f"{self.delta_lake_path}/transaction"
        full_df = self.spark.read.format("delta").load(path)
        full_df = full_df.filter(full_df["block_id"] <= end_block)

        # Count txs before start_block for offset
        if start_block > 0:
            before_df = self.spark.read.format("delta").load(path)
            before_df = before_df.filter(before_df["block_id"] < start_block)
            tx_offset = before_df.count()
        else:
            tx_offset = 0

        # Filter to our range and assign tx_ids
        range_df = full_df.filter(
            (full_df["block_id"] >= start_block) & (full_df["block_id"] <= end_block)
        )

        # Compute per-block tx offsets to avoid global SinglePartition exchange.
        # block_tx_counts is small (~one row per block) so its window is fast.
        block_tx_counts = range_df.groupBy("block_id").agg(
            F.count("*").alias("_tx_count")
        )
        w_block = Window.orderBy("block_id").rowsBetween(Window.unboundedPreceding, -1)
        block_tx_counts = block_tx_counts.withColumn(
            "_block_tx_offset",
            F.coalesce(F.sum("_tx_count").over(w_block), F.lit(0)).cast("long"),
        )

        # Broadcast join back (block_tx_counts is tiny) and compute tx_id
        # using a partitioned window (fully parallel across executors).
        range_df = range_df.join(
            F.broadcast(block_tx_counts.select("block_id", "_block_tx_offset")),
            "block_id",
        )
        w_within_block = Window.partitionBy("block_id").orderBy("index")
        range_df = range_df.withColumn(
            "tx_id",
            (
                F.col("_block_tx_offset")
                + F.row_number().over(w_within_block)
                - 1
                + tx_offset
            ).cast("long"),
        )
        range_df = range_df.drop("_block_tx_offset")

        range_df = range_df.withColumn(
            "tx_id_group",
            F.floor(F.col("tx_id") / self.tx_bucket_size).cast("int"),
        )
        range_df = range_df.withColumn(
            "tx_prefix",
            F.substring(F.lower(F.hex(F.col("tx_hash"))), 1, self.tx_hash_prefix_len),
        )

        range_df = range_df.cache()
        self._tx_df_cache = range_df
        self._tx_df_cache_range = (start_block, end_block)
        return range_df

    def _address_type_map_expr(self):
        """Build a Spark SQL map expression for address type string → int."""
        from pyspark.sql import functions as F

        args = []
        for k, v in ADDRESS_TYPE_MAP.items():
            args.append(F.lit(k))
            args.append(F.lit(v).cast("short"))
        return F.create_map(*args)

    def transform_block(self, start_block, end_block):
        from pyspark.sql import functions as F

        df = self._read_delta("block", start_block, end_block)
        df = df.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )
        drop_cols = [c for c in _DELTA_ONLY_COLS_BLOCK if c in df.columns]
        df = df.drop(*drop_cols)
        self._write_cassandra(df, "block")

    def transform_transaction(self, start_block, end_block):
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, BinaryType

        tx_df = self._get_tx_df_with_ids(start_block, end_block)
        type_map = self._address_type_map_expr()

        def null_type_cond(t):
            return t.isin(
                "null", "nulldata", "nonstandard", "witness_unknown", "shielded"
            )

        # Transform outputs: map type string→int, script_hex from hex→binary
        tx_df = tx_df.withColumn(
            "outputs",
            F.transform(
                F.col("outputs"),
                lambda o: F.struct(
                    F.when(null_type_cond(o["type"]), F.lit(None))
                    .otherwise(o["addresses"])
                    .alias("address"),
                    o["value"].cast("long").alias("value"),
                    F.coalesce(type_map[o["type"]], F.lit(1).cast("short")).alias(
                        "address_type"
                    ),
                    F.unhex(o["script_hex"]).alias("script_hex"),
                    F.lit(None).cast(ArrayType(BinaryType())).alias("txinwitness"),
                ),
            ),
        )

        # Transform inputs: map type string→int, script_hex, txinwitness already binary
        tx_df = tx_df.withColumn(
            "inputs",
            F.transform(
                F.col("inputs"),
                lambda i: F.struct(
                    F.when(null_type_cond(i["type"]), F.lit(None))
                    .otherwise(i["addresses"])
                    .alias("address"),
                    i["value"].cast("long").alias("value"),
                    F.coalesce(type_map[i["type"]], F.lit(1).cast("short")).alias(
                        "address_type"
                    ),
                    F.unhex(i["script_hex"]).alias("script_hex"),
                    i["txinwitness"].alias("txinwitness"),
                ),
            ),
        )

        # Select only Cassandra columns
        result = tx_df.select(
            "tx_id_group",
            F.col("tx_id").cast("long"),
            "tx_hash",
            "block_id",
            F.col("timestamp").cast("int"),
            "coinbase",
            "total_input",
            "total_output",
            "inputs",
            "outputs",
            "coinjoin",
        )

        self._write_cassandra(result, "transaction")

    def transform_spending_tables(self, start_block, end_block):
        """Derive transaction_spent_in and transaction_spending from base tx data.

        Explodes transaction inputs, filters out coinbase (no spent_transaction_hash),
        and writes both spending reference tables.
        """
        from pyspark.sql import functions as F

        tx_df = self._get_tx_df_with_ids(start_block, end_block)
        prefix_len = self.tx_hash_prefix_len

        # Explode inputs with their index
        exploded = tx_df.select(
            F.col("tx_hash").alias("spending_tx_hash"),
            F.posexplode("inputs").alias("spending_input_index", "inp"),
        )

        # Filter out coinbase inputs (no spent_transaction_hash)
        exploded = exploded.filter(F.col("inp.spent_transaction_hash").isNotNull())

        # Extract fields
        refs = exploded.select(
            F.col("spending_tx_hash"),
            F.col("spending_input_index").cast("int"),
            F.col("inp.spent_transaction_hash").alias("spent_tx_hash"),
            F.col("inp.spent_output_index").cast("int").alias("spent_output_index"),
        )

        # Add prefixes
        refs = refs.withColumn(
            "spending_tx_prefix",
            F.substring(F.lower(F.hex(F.col("spending_tx_hash"))), 1, prefix_len),
        ).withColumn(
            "spent_tx_prefix",
            F.substring(F.lower(F.hex(F.col("spent_tx_hash"))), 1, prefix_len),
        )

        # transaction_spent_in
        spent_in = refs.select(
            "spent_tx_prefix",
            "spent_tx_hash",
            "spent_output_index",
            "spending_tx_hash",
            "spending_input_index",
        )
        self._write_cassandra(spent_in, "transaction_spent_in")

        # transaction_spending
        spending = refs.select(
            "spending_tx_prefix",
            "spending_tx_hash",
            "spending_input_index",
            "spent_tx_hash",
            "spent_output_index",
        )
        self._write_cassandra(spending, "transaction_spending")

    def transform_block_transactions(self, start_block, end_block):
        """Derive block_transactions from base tx data.

        Groups transactions by block_id and collects tx_summary structs.
        """
        from pyspark.sql import functions as F

        tx_df = self._get_tx_df_with_ids(start_block, end_block)

        # Build tx_summary struct per transaction
        tx_summaries = tx_df.select(
            "block_id",
            F.struct(
                F.col("tx_id").cast("long").alias("tx_id"),
                F.size("inputs").cast("int").alias("no_inputs"),
                F.size("outputs").cast("int").alias("no_outputs"),
                F.col("total_input").cast("long").alias("total_input"),
                F.col("total_output").cast("long").alias("total_output"),
            ).alias("tx_summary"),
        )

        # Group by block_id, collect list
        block_txs = tx_summaries.groupBy("block_id").agg(
            F.collect_list("tx_summary").alias("txs")
        )

        block_txs = block_txs.withColumn(
            "block_id_group",
            F.floor(F.col("block_id") / self.block_bucket_size).cast("int"),
        )

        self._write_cassandra(block_txs, "block_transactions")

    def transform_transaction_by_tx_prefix(self, start_block, end_block):
        """Derive transaction_by_tx_prefix from base tx data."""
        from pyspark.sql import functions as F

        tx_df = self._get_tx_df_with_ids(start_block, end_block)

        result = tx_df.select(
            "tx_prefix",
            "tx_hash",
            F.col("tx_id").cast("long"),
        )

        self._write_cassandra(result, "transaction_by_tx_prefix")

    def write_configuration(self):
        from pyspark.sql import Row

        row = Row(
            id=self.raw_keyspace,
            block_bucket_size=self.block_bucket_size,
            tx_prefix_length=self.tx_hash_prefix_len,
            tx_bucket_size=self.tx_bucket_size,
        )
        df = self.spark.createDataFrame([row])
        self._write_cassandra(df, "configuration")

    def write_summary_statistics(self, start_block, end_block):
        from pyspark.sql import Row, functions as F

        block_df = self._read_delta("block", start_block, end_block)
        tx_df = self._get_tx_df_with_ids(start_block, end_block)

        no_blocks = block_df.count()
        no_txs = tx_df.count()

        max_ts_row = block_df.agg(F.max("timestamp").alias("ts")).collect()
        timestamp = max_ts_row[0]["ts"] if max_ts_row else 0

        row = Row(
            id=self.raw_keyspace,
            no_blocks=int(no_blocks),
            no_txs=int(no_txs),
            timestamp=int(timestamp) if timestamp else 0,
        )
        df = self.spark.createDataFrame([row])
        self._write_cassandra(df, "summary_statistics")

    def run(self, start_block, end_block, tables=None):
        all_tables = [
            "block",
            "transaction",
            "transaction_spent_in",
            "transaction_spending",
            "block_transactions",
            "transaction_by_tx_prefix",
        ]
        targets = tables if tables else all_tables

        logger.info(
            f"UtxoTransformation: {self.raw_keyspace} "
            f"blocks {start_block}-{end_block}, tables={targets}"
        )

        if "block" in targets:
            logger.info("Transforming block...")
            self.transform_block(start_block, end_block)

        if "transaction" in targets:
            logger.info("Transforming transaction...")
            self.transform_transaction(start_block, end_block)

        if "transaction_spent_in" in targets or "transaction_spending" in targets:
            logger.info("Transforming spending tables...")
            self.transform_spending_tables(start_block, end_block)

        if "block_transactions" in targets:
            logger.info("Transforming block_transactions...")
            self.transform_block_transactions(start_block, end_block)

        if "transaction_by_tx_prefix" in targets:
            logger.info("Transforming transaction_by_tx_prefix...")
            self.transform_transaction_by_tx_prefix(start_block, end_block)

        logger.info("Writing configuration...")
        self.write_configuration()

        logger.info("Writing summary statistics...")
        self.write_summary_statistics(start_block, end_block)

        # Unpersist cached DataFrame
        if self._tx_df_cache is not None:
            self._tx_df_cache.unpersist()
            self._tx_df_cache = None

        logger.info("UtxoTransformation complete.")
