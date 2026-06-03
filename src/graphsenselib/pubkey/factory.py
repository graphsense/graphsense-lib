"""Factory for the per-currency pubkey-update Spark job (deferred PySpark import)."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def run_pubkey(
    env: str,
    currency: str,
    source_path: str,
    sink_path: str,
    cassandra_nodes=None,
    cassandra_username: Optional[str] = None,
    cassandra_password: Optional[str] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    local: bool = False,
    s3_credentials=None,
    spark_config=None,
    pubkey_keyspace: str = "pubkey",
    sink_type: str = "cassandra",
    skip_detect: bool = False,
) -> None:
    from graphsenselib.pubkey.job import (
        ACCOUNT_CURRENCIES,
        PubkeyUpdate,
        SINK_CASSANDRA,
        UTXO_CURRENCIES,
        VALID_SINKS,
    )
    from graphsenselib.transformation.spark import create_spark_session

    if currency not in UTXO_CURRENCIES and currency not in ACCOUNT_CURRENCIES:
        raise ValueError(f"Unsupported currency for pubkey update: {currency}")
    if sink_type not in VALID_SINKS:
        raise ValueError(f"sink_type must be one of {VALID_SINKS}, got {sink_type!r}")
    if end_block is None:
        raise ValueError(
            "end_block must be set; pin a top-block before calling run_pubkey()."
        )
    if sink_type == SINK_CASSANDRA and not cassandra_nodes:
        raise ValueError("cassandra_nodes is required when sink_type='cassandra'.")
    # Spark's Cassandra connector wants a host even if we never read/write;
    # use a harmless placeholder when running with sink_type=delta and no nodes.
    spark_cassandra_nodes = cassandra_nodes or ["localhost:9042"]

    # UTXO extraction reads wide `transaction` parquet row groups (full input
    # arrays). On local[*] every core buffers a row group into the single
    # driver heap at once, which OOMs on dense modern BTC blocks. For local
    # runs, cap parallelism and give the JVM a generous heap; bound the Arrow
    # batch shipped to the UDFs too. All overridable via the env spark_config.
    import os

    job_spark_config = {"spark.sql.execution.arrow.maxRecordsPerBatch": "512"}
    if local:
        job_spark_config.update(
            {
                "spark.master": f"local[{min(os.cpu_count() or 2, 2)}]",
                "spark.driver.memory": "8g",
                "spark.sql.parquet.columnarReaderBatchSize": "256",
            }
        )
    if spark_config:
        job_spark_config.update(spark_config)

    spark = create_spark_session(
        app_name=f"graphsense-pubkey-update-{currency}-{env}",
        local=local,
        cassandra_nodes=spark_cassandra_nodes,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=job_spark_config,
    )
    try:
        PubkeyUpdate(
            spark=spark,
            currency=currency,
            source_path=source_path,
            sink_path=sink_path,
            cassandra_keyspace=pubkey_keyspace,
            sink_type=sink_type,
        ).run(start_block=start_block, end_block=end_block, skip_detect=skip_detect)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


def run_pubkey_compact(
    env: str,
    sink_path: str,
    local: bool = False,
    s3_credentials=None,
    spark_config=None,
) -> None:
    """Deduplicate and compact the ``observed`` table at ``sink_path``.

    Independent of currency/source — it only rewrites the shared cross-chain
    ``observed`` Delta table. Schedule between ``pubkey-update`` runs.
    """
    import os

    from graphsenselib.pubkey.job import compact_observed
    from graphsenselib.transformation.spark import create_spark_session

    job_spark_config = {}
    if local:
        job_spark_config.update(
            {
                "spark.master": f"local[{min(os.cpu_count() or 2, 2)}]",
                "spark.driver.memory": "8g",
            }
        )
    if spark_config:
        job_spark_config.update(spark_config)

    spark = create_spark_session(
        app_name=f"graphsense-pubkey-compact-{env}",
        local=local,
        # No Cassandra needed; placeholder host keeps the connector config happy.
        cassandra_nodes=["localhost:9042"],
        s3_credentials=s3_credentials,
        spark_config=job_spark_config,
    )
    try:
        compact_observed(spark, sink_path)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


def run_pubkey_detect(
    env: str,
    sink_path: str,
    cassandra_nodes=None,
    cassandra_username: Optional[str] = None,
    cassandra_password: Optional[str] = None,
    pubkey_keyspace: str = "pubkey_v2",
    sink_type: str = "cassandra",
    local: bool = False,
    s3_credentials=None,
    spark_config=None,
) -> None:
    """Run cross-chain detection + materialisation once over the shared store.

    The deferred half of ``pubkey-update --skip-detect``: append every chain to
    ``observed`` with detection skipped, then call this once so the full-table
    ``groupBy`` runs a single time instead of once per chain. Currency-agnostic;
    reads only ``observed`` / ``materialised`` under ``sink_path``.
    """
    import os

    from graphsenselib.pubkey.job import (
        SINK_CASSANDRA,
        VALID_SINKS,
        detect_and_materialise_cross_chain,
    )
    from graphsenselib.transformation.spark import create_spark_session

    if sink_type not in VALID_SINKS:
        raise ValueError(f"sink_type must be one of {VALID_SINKS}, got {sink_type!r}")
    if sink_type == SINK_CASSANDRA and not cassandra_nodes:
        raise ValueError("cassandra_nodes is required when sink_type='cassandra'.")
    # Cassandra connector wants a host even when sink_type=delta never writes.
    spark_cassandra_nodes = cassandra_nodes or ["localhost:9042"]

    job_spark_config = {}
    if local:
        job_spark_config.update(
            {
                "spark.master": f"local[{min(os.cpu_count() or 2, 2)}]",
                "spark.driver.memory": "8g",
            }
        )
    if spark_config:
        job_spark_config.update(spark_config)

    spark = create_spark_session(
        app_name=f"graphsense-pubkey-detect-{env}",
        local=local,
        cassandra_nodes=spark_cassandra_nodes,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=job_spark_config,
    )
    try:
        detect_and_materialise_cross_chain(
            spark,
            sink_path,
            sink_type=sink_type,
            cassandra_keyspace=pubkey_keyspace,
        )
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


def run_pubkey_load(
    env: str,
    sink_path: str,
    cassandra_nodes=None,
    cassandra_username: Optional[str] = None,
    cassandra_password: Optional[str] = None,
    pubkey_keyspace: str = "pubkey_v2",
    local: bool = False,
    s3_credentials=None,
    spark_config=None,
) -> None:
    """Load the Delta ``pubkey_by_address`` table at ``sink_path`` into Cassandra.

    The throttled Cassandra-write half of the decoupled flow: run the heavy
    extraction with ``sink_type=delta`` first, then call this to load the
    resulting Delta table into the (isolated) Cassandra keyspace.
    """
    from graphsenselib.pubkey.job import load_pubkey_to_cassandra
    from graphsenselib.transformation.spark import create_spark_session

    if not cassandra_nodes:
        raise ValueError("cassandra_nodes is required to load into Cassandra.")

    import os

    job_spark_config = {}
    if local:
        job_spark_config.update(
            {
                "spark.master": f"local[{min(os.cpu_count() or 2, 2)}]",
                "spark.driver.memory": "8g",
            }
        )
    if spark_config:
        job_spark_config.update(spark_config)

    spark = create_spark_session(
        app_name=f"graphsense-pubkey-load-{env}",
        local=local,
        cassandra_nodes=cassandra_nodes,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=job_spark_config,
    )
    try:
        load_pubkey_to_cassandra(spark, sink_path, pubkey_keyspace)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")
