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
        debug_write_audit=False,
    ):
        self.spark = spark
        # Spark/Hadoop uses s3a:// not s3://
        self.delta_lake_path = delta_lake_path.rstrip("/").replace("s3://", "s3a://")
        self.raw_keyspace = raw_keyspace
        self.block_bucket_size = block_bucket_size
        self.tx_bucket_size = tx_bucket_size
        self.tx_hash_prefix_len = tx_hash_prefix_len
        self.debug_write_audit = debug_write_audit
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

    def _write_cassandra(self, df, table_name, partition_key=None):
        if partition_key and partition_key in df.columns:
            df = df.repartition(partition_key)
        if self.debug_write_audit and partition_key and partition_key in df.columns:
            self._log_partition_audit(df, table_name, partition_key)
        (
            df.write.format("org.apache.spark.sql.cassandra")
            .options(table=table_name, keyspace=self.raw_keyspace)
            .mode("append")
            .save()
        )
        logger.info(f"Wrote to {self.raw_keyspace}.{table_name}")

    def _log_partition_audit(self, df, table_name, partition_key):
        """Aggregate per-Spark-partition stats and log them on the driver.

        Runs an extra Spark job (one shuffle aggregation) before the Cassandra
        write. Use to diagnose stragglers: correlate Spark UI task durations
        with row counts and partition-key skew.
        """
        from pyspark.sql import functions as F

        per_key = df.groupBy(
            F.spark_partition_id().alias("task_idx"),
            F.col(partition_key).alias("key"),
        ).agg(F.count("*").alias("rows"))

        per_task = (
            per_key.groupBy("task_idx")
            .agg(
                F.sum("rows").alias("rows"),
                F.countDistinct("key").alias("distinct_keys"),
                F.max("rows").alias("max_key_count"),
            )
            .orderBy(F.desc("rows"))
            .limit(20)
            .collect()
        )

        logger.info(
            f"AUDIT {self.raw_keyspace}.{table_name} partition_key={partition_key} "
            f"top {len(per_task)} heaviest tasks (of {df.rdd.getNumPartitions()}):"
        )
        for r in per_task:
            logger.info(
                f"AUDIT  task={r.task_idx:>5} rows={r.rows:>10} "
                f"distinct_keys={r.distinct_keys:>8} "
                f"max_rows_per_key={r.max_key_count:>8}"
            )

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

        # Compute per-block tx counts (parallel), then cumulative offsets on
        # the driver to avoid any SinglePartition window operation.
        block_rows = (
            range_df.groupBy("block_id")
            .agg(F.count("*").alias("_tx_count"))
            .orderBy("block_id")
            .collect()
        )
        cumulative = 0
        offset_rows = []
        for row in block_rows:
            offset_rows.append((row["block_id"], cumulative))
            cumulative += row["_tx_count"]

        from pyspark.sql.types import IntegerType, LongType, StructField, StructType

        offset_schema = StructType(
            [
                StructField("block_id", IntegerType(), False),
                StructField("_block_tx_offset", LongType(), False),
            ]
        )
        offset_df = self.spark.createDataFrame(offset_rows, offset_schema)

        # Broadcast join back and compute tx_id using a partitioned window
        # (fully parallel across executors).
        range_df = range_df.join(F.broadcast(offset_df), "block_id")
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

        self._write_cassandra(result, "transaction", partition_key="tx_id_group")

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
        self._write_cassandra(
            spent_in, "transaction_spent_in", partition_key="spent_tx_prefix"
        )

        # transaction_spending
        spending = refs.select(
            "spending_tx_prefix",
            "spending_tx_hash",
            "spending_input_index",
            "spent_tx_hash",
            "spent_output_index",
        )
        self._write_cassandra(
            spending, "transaction_spending", partition_key="spending_tx_prefix"
        )

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

        self._write_cassandra(
            block_txs, "block_transactions", partition_key="block_id_group"
        )

    def transform_transaction_by_tx_prefix(self, start_block, end_block):
        """Derive transaction_by_tx_prefix from base tx data."""
        from pyspark.sql import functions as F

        tx_df = self._get_tx_df_with_ids(start_block, end_block)

        result = tx_df.select(
            "tx_prefix",
            "tx_hash",
            F.col("tx_id").cast("long"),
        )

        self._write_cassandra(
            result, "transaction_by_tx_prefix", partition_key="tx_prefix"
        )

    def write_configuration(self):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("block_bucket_size", IntegerType(), False),
                StructField("tx_prefix_length", IntegerType(), False),
                StructField("tx_bucket_size", IntegerType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [
                (
                    self.raw_keyspace,
                    self.block_bucket_size,
                    self.tx_hash_prefix_len,
                    self.tx_bucket_size,
                )
            ],
            schema,
        )
        self._write_cassandra(df, "configuration")

    def write_summary_statistics(self, start_block, end_block):
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        block_df = self._read_delta("block", start_block, end_block)
        tx_df = self._get_tx_df_with_ids(start_block, end_block)

        no_blocks = block_df.count()
        no_txs = tx_df.count()

        max_ts_row = block_df.agg(F.max("timestamp").alias("ts")).collect()
        timestamp = max_ts_row[0]["ts"] if max_ts_row else 0

        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("no_blocks", LongType(), False),
                StructField("no_txs", LongType(), False),
                StructField("timestamp", IntegerType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [
                (
                    self.raw_keyspace,
                    int(no_blocks),
                    int(no_txs),
                    int(timestamp) if timestamp else 0,
                )
            ],
            schema,
        )
        self._write_cassandra(df, "summary_statistics")

    def run(self, start_block, end_block, tables=None):
        from concurrent.futures import ThreadPoolExecutor, as_completed

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

        # Derived tables all read from the cached tx DataFrame and write to
        # independent Cassandra tables, so they can run in parallel.
        parallel_tasks = []
        if "transaction_spent_in" in targets or "transaction_spending" in targets:
            parallel_tasks.append(
                (
                    "spending tables",
                    self.transform_spending_tables,
                    start_block,
                    end_block,
                )
            )
        if "block_transactions" in targets:
            parallel_tasks.append(
                (
                    "block_transactions",
                    self.transform_block_transactions,
                    start_block,
                    end_block,
                )
            )
        if "transaction_by_tx_prefix" in targets:
            parallel_tasks.append(
                (
                    "transaction_by_tx_prefix",
                    self.transform_transaction_by_tx_prefix,
                    start_block,
                    end_block,
                )
            )

        if parallel_tasks:
            logger.info(
                f"Running {len(parallel_tasks)} derived table transforms in parallel..."
            )
            with ThreadPoolExecutor(max_workers=len(parallel_tasks)) as pool:
                futures = {}
                for name, fn, sb, eb in parallel_tasks:
                    futures[pool.submit(fn, sb, eb)] = name
                for future in as_completed(futures):
                    name = futures[future]
                    future.result()
                    logger.info(f"Parallel transform complete: {name}")

        logger.info("Writing configuration...")
        self.write_configuration()

        logger.info("Writing summary statistics...")
        self.write_summary_statistics(start_block, end_block)

        # MUST stay last — see graphsenselib.db.state.mark_ingest_complete.
        logger.info("Writing ingest_complete marker...")
        self.write_ingest_complete_marker()

        # Unpersist cached DataFrame
        if self._tx_df_cache is not None:
            self._tx_df_cache.unpersist()
            self._tx_df_cache = None

        logger.info("UtxoTransformation complete.")

    def write_ingest_complete_marker(self):
        from graphsenselib.transformation.account import (
            _write_ingest_complete_marker,
        )

        _write_ingest_complete_marker(self.spark, self._write_cassandra)
