"""Factory for the per-currency pubkey-update Spark job (deferred PySpark import)."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def run_pubkey(
    env: str,
    currency: str,
    source_delta_path: str,
    pubkey_delta_path: str,
    cassandra_nodes=None,
    cassandra_username: Optional[str] = None,
    cassandra_password: Optional[str] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    local: bool = False,
    s3_credentials=None,
    spark_config=None,
    pubkey_keyspace: str = "pubkey",
    sink: str = "cassandra",
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
    if sink not in VALID_SINKS:
        raise ValueError(f"sink must be one of {VALID_SINKS}, got {sink!r}")
    if end_block is None:
        raise ValueError(
            "end_block must be set; pin a top-block before calling run_pubkey()."
        )
    if sink == SINK_CASSANDRA and not cassandra_nodes:
        raise ValueError("cassandra_nodes is required when sink='cassandra'.")
    # Spark's Cassandra connector wants a host even if we never read/write;
    # use a harmless placeholder when running with sink=delta and no nodes.
    spark_cassandra_nodes = cassandra_nodes or ["localhost:9042"]

    spark = create_spark_session(
        app_name=f"graphsense-pubkey-update-{currency}-{env}",
        local=local,
        cassandra_nodes=spark_cassandra_nodes,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=spark_config,
    )
    try:
        PubkeyUpdate(
            spark=spark,
            currency=currency,
            source_delta_path=source_delta_path,
            pubkey_delta_path=pubkey_delta_path,
            cassandra_keyspace=pubkey_keyspace,
            sink=sink,
        ).run(start_block=start_block, end_block=end_block)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")
