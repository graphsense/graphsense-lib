"""SparkSession factory for Delta Lake → Cassandra transformation."""

import logging

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name,
    local,
    cassandra_host,
    cassandra_username=None,
    cassandra_password=None,
    raw_keyspace=None,
    s3_credentials=None,
):
    """Create and configure a SparkSession for reading Delta Lake and writing to Cassandra.

    PySpark imports are deferred to this function to avoid ImportError when
    pyspark is not installed.
    """
    from pyspark.sql import SparkSession

    SPARK_CASSANDRA_CONNECTOR = (
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
    )
    DELTA_SPARK = "io.delta:delta-spark_2.12:3.2.1"
    HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"

    builder = SparkSession.builder.appName(app_name)

    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
        )

    packages = [SPARK_CASSANDRA_CONNECTOR, DELTA_SPARK]
    if s3_credentials:
        packages.append(HADOOP_AWS)

    builder = (
        builder.config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # Cassandra connection
    host, _, port = cassandra_host.partition(":")
    builder = builder.config("spark.cassandra.connection.host", host)
    if port:
        builder = builder.config("spark.cassandra.connection.port", port)
    if cassandra_username:
        builder = builder.config("spark.cassandra.auth.username", cassandra_username)
    if cassandra_password:
        builder = builder.config("spark.cassandra.auth.password", cassandra_password)
    if raw_keyspace:
        builder = builder.config(
            "spark.cassandra.output.consistency.level", "LOCAL_QUORUM"
        )

    # S3/MinIO configuration
    if s3_credentials:
        endpoint = s3_credentials.get("AWS_ENDPOINT_URL", "")
        access_key = s3_credentials.get("AWS_ACCESS_KEY_ID", "")
        secret_key = s3_credentials.get("AWS_SECRET_ACCESS_KEY", "")
        allow_http = s3_credentials.get("AWS_ALLOW_HTTP", "false")

        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
        )
        if allow_http.lower() == "true":
            builder = builder.config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
            )

    spark = builder.getOrCreate()
    logger.info(f"SparkSession created: {app_name} (local={local})")
    return spark
