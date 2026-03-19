"""SparkSession factory for Delta Lake → Cassandra transformation."""

import logging
import os
import subprocess
import sys
import tempfile

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

    # Ship the driver's Python environment to workers so Python UDFs
    # use the same version. Only needed in cluster mode (not local).
    if not local:
        archive_path = _get_python_archive()
        if archive_path:
            builder = builder.config("spark.archives", f"{archive_path}#__pyenv__")
            builder = builder.config("spark.pyspark.python", "./__pyenv__/bin/python")
            logger.info(f"Shipping Python environment to workers: {archive_path}")

    # Apply user-provided Spark config overrides last (highest priority).
    # This allows overriding spark.jars.packages, spark.master, etc.
    if spark_config:
        for key, value in spark_config.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    logger.info(f"SparkSession created: {app_name} (local={local})")
    return spark


def _get_python_archive():
    """Create a tarball of the current Python environment for shipping to workers.

    Uses venv-pack if available (fast, proper relocation), otherwise falls
    back to tarring sys.prefix directly. Returns the archive path or None
    if packing fails.
    """
    archive_dir = tempfile.gettempdir()
    archive_path = os.path.join(archive_dir, "pyspark_pyenv.tar.gz")

    if os.path.exists(archive_path):
        logger.info(f"Reusing existing Python archive: {archive_path}")
        return archive_path

    prefix = sys.prefix
    logger.info(f"Packing Python environment from {prefix}")

    # tar the environment — works without extra dependencies
    result = subprocess.run(
        ["tar", "czf", archive_path, "-C", prefix, "."],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.warning(f"Failed to pack Python environment: {result.stderr}")
        return None

    size_mb = os.path.getsize(archive_path) / 1024 / 1024
    logger.info(f"Python archive created: {archive_path} ({size_mb:.0f} MB)")
    return archive_path
