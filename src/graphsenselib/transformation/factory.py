"""Factory to create and run the appropriate transformation based on currency."""

import logging

from graphsenselib.config import currency_to_schema_type

logger = logging.getLogger(__name__)


def run(
    env,
    currency,
    delta_lake_path,
    cassandra_host,
    cassandra_username=None,
    cassandra_password=None,
    raw_keyspace=None,
    start_block=0,
    end_block=None,
    local=False,
    tables=None,
    s3_credentials=None,
):
    from graphsenselib.transformation.spark import create_spark_session

    schema_type = currency_to_schema_type.get(currency)

    spark = create_spark_session(
        app_name=f"graphsense-transformation-{currency}-{env}",
        local=local,
        cassandra_host=cassandra_host,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        raw_keyspace=raw_keyspace,
        s3_credentials=s3_credentials,
    )

    if schema_type == "account":
        from graphsenselib.transformation.account import AccountTransformation

        transformation = AccountTransformation(
            spark=spark,
            delta_lake_path=delta_lake_path,
            raw_keyspace=raw_keyspace,
        )
    elif schema_type in ("utxo",):
        from graphsenselib.transformation.utxo import UtxoTransformation

        transformation = UtxoTransformation(
            spark=spark,
            delta_lake_path=delta_lake_path,
            raw_keyspace=raw_keyspace,
        )
    else:
        spark.stop()
        raise ValueError(
            f"Unsupported schema type '{schema_type}' for currency '{currency}'"
        )

    transformation.run(start_block, end_block, tables=tables)
    spark.stop()
    logger.info("Transformation complete, SparkSession stopped.")
