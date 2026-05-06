"""Factory to create and run the appropriate transformation based on currency."""

import logging

from graphsenselib.config import currency_to_schema_type

logger = logging.getLogger(__name__)


def run(
    env,
    currency,
    delta_lake_path,
    cassandra_nodes,
    cassandra_username=None,
    cassandra_password=None,
    raw_keyspace=None,
    start_block=0,
    end_block=None,
    local=False,
    tables=None,
    s3_credentials=None,
    spark_config=None,
    debug_write_audit=False,
):
    from graphsenselib.transformation.spark import create_spark_session

    schema_type = currency_to_schema_type.get(currency)

    spark = create_spark_session(
        app_name=f"graphsense-bulk-ingest-{currency}-{env}",
        local=local,
        cassandra_nodes=cassandra_nodes,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=spark_config,
    )

    if end_block is None:
        spark.stop()
        raise ValueError("end_block must be set; pin a top-block before calling run().")

    if schema_type == "account":
        from graphsenselib.transformation.account import AccountTransformation

        transformation = AccountTransformation(
            spark=spark,
            delta_lake_path=delta_lake_path,
            raw_keyspace=raw_keyspace,
        )
    elif schema_type == "account_trx":
        from graphsenselib.transformation.account_trx import AccountTrxTransformation

        transformation = AccountTrxTransformation(
            spark=spark,
            delta_lake_path=delta_lake_path,
            raw_keyspace=raw_keyspace,
        )
    elif schema_type == "utxo":
        if start_block != 0:
            logger.warning(
                f"UTXO transformation with start_block={start_block} > 0: "
                f"tx_id values will only be correct if the Delta table "
                f"contains all blocks from genesis (block 0)."
            )
        from graphsenselib.transformation.utxo import UtxoTransformation

        transformation = UtxoTransformation(
            spark=spark,
            delta_lake_path=delta_lake_path,
            raw_keyspace=raw_keyspace,
            debug_write_audit=debug_write_audit,
        )
    else:
        spark.stop()
        raise ValueError(
            f"Unsupported schema type '{schema_type}' for currency '{currency}'"
        )

    try:
        transformation.run(start_block, end_block, tables=tables)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")
