"""SparkSession factory for Delta Lake → Cassandra transformation."""

import logging
import os

logger = logging.getLogger(__name__)

# Default Maven packages. On the iknaio cluster, hadoop-aws must match the
# cluster's hadoop-common version (3.3.1). Override via spark_config
# {"spark.jars.packages": "..."} if needed.
SPARK_CASSANDRA_CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
JODA_TIME = "joda-time:joda-time:2.10.10"
DELTA_SPARK = "io.delta:delta-spark_2.12:3.2.1"
HADOOP_AWS_DEFAULT = "org.apache.hadoop:hadoop-aws:3.3.1"


def create_spark_session(
    app_name,
    local,
    cassandra_nodes,
    cassandra_username=None,
    cassandra_password=None,
    raw_keyspace=None,
    s3_credentials=None,
    spark_config=None,
):
    """Create and configure a SparkSession for reading Delta Lake and writing to Cassandra.

    PySpark imports are deferred to this function to avoid ImportError when
    pyspark is not installed.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
        )

    packages = [SPARK_CASSANDRA_CONNECTOR, JODA_TIME, DELTA_SPARK]
    if s3_credentials:
        packages.append(HADOOP_AWS_DEFAULT)

    builder = (
        builder.config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # Cassandra connection — accepts a list of "host:port" strings
    if isinstance(cassandra_nodes, str):
        cassandra_nodes = [cassandra_nodes]
    hosts = []
    port = None
    for node in cassandra_nodes:
        h, _, p = node.partition(":")
        hosts.append(h)
        if p and port is None:
            port = p
    builder = builder.config("spark.cassandra.connection.host", ",".join(hosts))
    if port:
        builder = builder.config("spark.cassandra.connection.port", port)
    if cassandra_username:
        builder = builder.config("spark.cassandra.auth.username", cassandra_username)
    if cassandra_password:
        builder = builder.config("spark.cassandra.auth.password", cassandra_password)
    # Cassandra write tuning — reduce pressure, avoid timeouts
    builder = (
        builder.config("spark.cassandra.output.concurrent.writes", "2")
        .config("spark.cassandra.output.batch.size.bytes", "4096")
        .config("spark.cassandra.connection.timeoutMS", "600000")
        .config("spark.cassandra.output.throughputMBPerSec", "1")
    )

    # Spark performance defaults
    # Arrow-optimized Python UDFs (Spark 3.5+) use Arrow serialization
    # instead of row-at-a-time, giving ~2x speedup for UDFs like varint
    # conversion. Requires Java 17 (Arrow Java 12 is incompatible with 21+).
    builder = (
        builder.config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true")
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

    # Apply user-provided Spark config overrides last (highest priority).
    # This allows overriding spark.jars.packages, spark.master, etc.
    if spark_config:
        for key, value in spark_config.items():
            builder = builder.config(key, value)

    # PySpark's SparkContext reads PYSPARK_PYTHON from os.environ (not from
    # Spark config) to determine which Python binary workers use. Mirror the
    # spark.pyspark.python config value to the env var so it actually works.
    if spark_config and "spark.pyspark.python" in spark_config:
        os.environ["PYSPARK_PYTHON"] = spark_config["spark.pyspark.python"]

    spark = builder.getOrCreate()
    logger.info(f"SparkSession created: {app_name} (local={local})")
    return spark
