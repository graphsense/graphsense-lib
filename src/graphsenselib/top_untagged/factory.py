"""Factory for the top-untagged-addresses Spark job (deferred PySpark import)."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def run_top_untagged(
    env: str,
    currency: str,
    schema_type: str,
    transformed_keyspace: str,
    tagstore_db_url: str,
    out_path: str,
    tagstore_schema: str = "public",
    out_format: str = "csv",
    limit: int = 1000,
    sort_by: str = "txs",
    min_txs: int = 0,
    fiat_index: int = 0,
    candidate_multiplier: int = 50,
    cassandra_nodes=None,
    cassandra_username: Optional[str] = None,
    cassandra_password: Optional[str] = None,
    local: bool = False,
    s3_credentials=None,
    spark_config=None,
) -> None:
    import os

    from graphsenselib.transformation.spark import create_spark_session
    from graphsenselib.top_untagged.job import TopUntaggedAddresses

    if not cassandra_nodes:
        raise ValueError(
            "cassandra_nodes is required to read the transformed keyspace."
        )

    job_spark_config = {}
    if local:
        job_spark_config.update(
            {
                "spark.master": f"local[{min(os.cpu_count() or 2, 4)}]",
                "spark.driver.memory": "8g",
            }
        )
    if spark_config:
        job_spark_config.update(spark_config)

    spark = create_spark_session(
        app_name=f"graphsense-top-untagged-{currency}-{env}",
        local=local,
        cassandra_nodes=cassandra_nodes,
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=job_spark_config,
    )
    try:
        TopUntaggedAddresses(
            spark=spark,
            currency=currency,
            schema_type=schema_type,
            transformed_keyspace=transformed_keyspace,
            tagstore_db_url=tagstore_db_url,
            tagstore_schema=tagstore_schema,
        ).run(
            out_path=out_path,
            out_format=out_format,
            limit=limit,
            sort_by=sort_by,
            min_txs=min_txs,
            fiat_index=fiat_index,
            candidate_multiplier=candidate_multiplier,
        )
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")
