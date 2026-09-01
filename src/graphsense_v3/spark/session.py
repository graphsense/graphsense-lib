"""SparkSession for the v3 backfill.

Thin wrapper over ``graphsenselib.transformation.spark.create_spark_session``:
the package set (Cassandra connector, delta-spark, hadoop-aws) and the S3 and
Cassandra wiring are already correct there, and duplicating them would be a
second thing to keep in step with the cluster's hadoop version.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_session(
    app_name: str,
    *,
    local: bool = False,
    cassandra_nodes: list[str] | None = None,
    cassandra_username: str | None = None,
    cassandra_password: str | None = None,
    s3_credentials: Any = None,
    spark_config: dict[str, str] | None = None,
    spark_packages: dict[str, str] | None = None,
) -> SparkSession:
    from graphsenselib.transformation.spark import create_spark_session

    return create_spark_session(
        app_name,
        local,
        cassandra_nodes or [],
        cassandra_username=cassandra_username,
        cassandra_password=cassandra_password,
        s3_credentials=s3_credentials,
        spark_config=spark_config,
        spark_packages=spark_packages,
    )
