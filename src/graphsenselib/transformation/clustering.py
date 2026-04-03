"""One-off clustering: assign address IDs, run Rust clustering, write to Cassandra."""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

CHUNK_SIZE = 5_000_000


def assign_address_ids(spark: SparkSession, tx_df: DataFrame) -> DataFrame:
    """Assign sequential address IDs to all unique input addresses.

    Expects tx_df to have an 'inputs' column (list of tx_input_output UDTs)
    where each element has an 'address' field (list of strings).

    Returns DataFrame with columns: (address: str, address_id: int)
    """
    # Explode inputs to get one row per input
    input_addrs = (
        tx_df.select(F.explode("inputs").alias("inp"))
        .select(F.explode("inp.address").alias("address"))
        .filter(F.col("address").isNotNull())
        .distinct()
    )

    # Assign sequential IDs using row_number (1-based, 0 reserved for coinbase)
    w = Window.orderBy("address")
    return input_addrs.withColumn("address_id", F.row_number().over(w).cast("int"))


def extract_tx_input_address_ids(tx_df: DataFrame, addr_id_df: DataFrame) -> DataFrame:
    """Extract input address IDs grouped by transaction.

    Returns DataFrame with columns: (tx_id: long, input_address_ids: array<int>)
    """
    # Explode inputs with index
    exploded = tx_df.select(
        "tx_id",
        F.posexplode("inputs").alias("input_idx", "inp"),
    ).select(
        "tx_id",
        F.explode("inp.address").alias("address"),
    )

    # Join with address IDs
    joined = exploded.join(addr_id_df, on="address", how="inner")

    # Group by tx_id, collect address IDs
    return joined.groupBy("tx_id").agg(
        F.collect_set("address_id").alias("input_address_ids")
    )


def run_clustering_one_off(
    spark: SparkSession,
    tx_df: DataFrame,
    raw_keyspace: str,
    transformed_keyspace: str,
    addr_id_df: Optional[DataFrame] = None,
):
    """Run full clustering from scratch and write results to Cassandra.

    Args:
        spark: Active SparkSession with Cassandra connector configured
        tx_df: Transaction DataFrame with 'inputs' and 'tx_id' columns
        raw_keyspace: Raw keyspace name (for reading config)
        transformed_keyspace: Transformed keyspace name (for writing results)
        addr_id_df: Optional pre-computed address ID DataFrame.
                    If None, address IDs are assigned from the transaction inputs.
    """
    from gs_clustering import Clustering

    logger.info("Starting one-off clustering")

    # Step 1: Get or compute address IDs
    if addr_id_df is None:
        logger.info("Assigning address IDs from transaction inputs")
        addr_id_df = assign_address_ids(spark, tx_df)
        addr_id_df.cache()

    # Step 2: Extract input address IDs per transaction
    logger.info("Extracting input address IDs per transaction")
    tx_inputs_df = extract_tx_input_address_ids(tx_df, addr_id_df)

    # Step 3: Find max address ID for Clustering initialization
    max_id_row = addr_id_df.agg(F.max("address_id")).collect()[0]
    max_address_id = max_id_row[0]
    logger.info(f"Max address ID: {max_address_id}")

    # Step 4: Process via Rust (collect to driver, chunk in Python)
    c = Clustering(max_address_id=max_address_id)

    logger.info("Collecting transaction inputs to driver")
    rows = tx_inputs_df.select("input_address_ids").collect()
    all_tx_inputs = [row.input_address_ids for row in rows if row.input_address_ids]
    logger.info(f"Processing {len(all_tx_inputs)} transactions")

    for i in range(0, len(all_tx_inputs), CHUNK_SIZE):
        chunk = all_tx_inputs[i : i + CHUNK_SIZE]
        c.process_transactions(chunk)
        logger.info(
            f"Processed {min(i + CHUNK_SIZE, len(all_tx_inputs))}"
            f"/{len(all_tx_inputs)} transactions"
        )

    # Step 5: Get full mapping and write to Cassandra
    logger.info("Generating cluster mapping")
    mapping_batch = c.get_mapping()

    import pyarrow as pa

    # Convert Arrow RecordBatch to PySpark DataFrame
    mapping_pdf = pa.RecordBatch.to_pandas(mapping_batch, types_mapper=None)
    # Filter to only addresses that actually appeared (skip address_id 0)
    mapping_pdf = mapping_pdf[mapping_pdf["address_id"] > 0]

    mapping_spark_df = spark.createDataFrame(mapping_pdf)
    mapping_spark_df = mapping_spark_df.withColumn(
        "address_id", F.col("address_id").cast("int")
    ).withColumn("cluster_id", F.col("cluster_id").cast("int"))

    # Write fresh_address_cluster
    logger.info("Writing fresh_address_cluster")
    mapping_spark_df.select("address_id", "cluster_id").write.format(
        "org.apache.spark.sql.cassandra"
    ).options(table="fresh_address_cluster", keyspace=transformed_keyspace).mode(
        "append"
    ).save()

    # Write fresh_cluster_addresses (reverse mapping)
    logger.info("Writing fresh_cluster_addresses")
    mapping_spark_df.select("cluster_id", "address_id").write.format(
        "org.apache.spark.sql.cassandra"
    ).options(table="fresh_cluster_addresses", keyspace=transformed_keyspace).mode(
        "append"
    ).save()

    logger.info("One-off clustering complete")
