"""SparkSession factory for Delta Lake → Cassandra transformation."""

import logging
import os
import sys

logger = logging.getLogger(__name__)

# Default Maven packages, keyed by logical name. On the iknaio cluster,
# hadoop-aws must match the cluster's hadoop-common version (3.3.1).
# Override individual coordinates via the `spark_packages` config field, e.g.
# {"hadoop_aws": "org.apache.hadoop:hadoop-aws:3.3.4"}; or replace the whole
# list via spark_config {"spark.jars.packages": "..."} (applied last, wins).
# hadoop_aws is only added to the package list when s3 credentials are present.
DEFAULT_SPARK_PACKAGES = {
    "cassandra_connector": "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
    "joda_time": "joda-time:joda-time:2.10.10",
    "delta_spark": "io.delta:delta-spark_2.12:3.2.1",
    "hadoop_aws": "org.apache.hadoop:hadoop-aws:3.3.1",
}


def _apply_sidecar_settings(packages, spark_config):
    """Return (packages, spark_config) augmented for the Sidecar bulk writer.

    Adds the cassandra-analytics package and the SSTable-writer JVM flags;
    requires ``spark.local.dir`` in ``spark_config`` for the temp-dir redirect.
    """
    from graphsenselib.transformation.sidecar import (
        sidecar_packages,
        sidecar_spark_properties,
    )

    return sidecar_packages(packages), sidecar_spark_properties(spark_config or {})


def create_spark_session(
    app_name,
    local,
    cassandra_nodes,
    cassandra_username=None,
    cassandra_password=None,
    s3_credentials=None,
    spark_config=None,
    spark_packages=None,
    sidecar=False,
):
    """Create and configure a SparkSession for reading Delta Lake and writing to Cassandra.

    PySpark imports are deferred to this function to avoid ImportError when
    pyspark is not installed.
    """
    from pyspark.sql import SparkSession

    # py4j's clientserver logs every Python<->JVM command/answer at DEBUG, which
    # floods -vvv output for every Spark job (it is a PySpark-wide concern, not
    # specific to any one job). Mute it to WARNING so our own DEBUG lines remain
    # readable.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    builder = SparkSession.builder.appName(app_name)

    if local:
        # In client mode the driver JVM is already running by the time the
        # builder config is read, so `spark.driver.memory` set here is a no-op
        # (see Spark docs). The heap must be set before launch via the
        # launcher args. Set PYSPARK_SUBMIT_ARGS unless the user already did.
        driver_memory = (spark_config or {}).get("spark.driver.memory", "8g")
        if "PYSPARK_SUBMIT_ARGS" not in os.environ:
            os.environ["PYSPARK_SUBMIT_ARGS"] = (
                f"--driver-memory {driver_memory} pyspark-shell"
            )
        builder = (
            builder.master("local[*]")
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.shuffle.partitions", "8")
            # Local mode has no periodic revive timer — resource offers happen
            # only on task events. A taskset mixing PROCESS_LOCAL preferences
            # (cached blocks) with host preferences that can never match the
            # executor (Cassandra-connector scans prefer the contact point,
            # while the executor registers under the machine's LAN address)
            # can then starve forever: the last task completion lands inside
            # spark.locality.wait, launches nothing, and no further event ever
            # re-offers. Delay scheduling buys nothing on one machine anyway.
            .config("spark.locality.wait", "0")
        )

    # Merge per-package overrides from config over the defaults, then select
    # the packages to load. hadoop-aws is only needed when reading from s3.
    coords = {**DEFAULT_SPARK_PACKAGES, **(spark_packages or {})}
    packages = [
        coords["cassandra_connector"],
        coords["joda_time"],
        coords["delta_spark"],
    ]
    if s3_credentials:
        packages.append(coords["hadoop_aws"])

    # Sidecar bulk writes need the analytics package and JVM flags in force
    # at JVM launch; both must land in the builder config before getOrCreate.
    if sidecar:
        packages, spark_config = _apply_sidecar_settings(packages, spark_config)

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

    # Spark performance defaults
    # Arrow-optimized Python UDFs (Spark 3.5+) use Arrow serialization
    # instead of row-at-a-time, giving ~2x speedup for UDFs like varint
    # conversion. Requires Java 17 (Arrow Java 12 is incompatible with 21+).
    builder = (
        builder.config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "true")
    )

    # Cassandra connector instrumentation. The connector publishes per-task
    # write latency / batch / retry metrics via the Spark metrics system; this
    # is the Layer-2 input for straggler diagnosis. Override sink/dir or
    # disable via spark_config.
    metrics_dir = "/tmp/spark-metrics"
    os.makedirs(metrics_dir, exist_ok=True)
    builder = (
        builder.config("spark.cassandra.output.metrics", "true")
        .config(
            "spark.metrics.conf.*.sink.csv.class",
            "org.apache.spark.metrics.sink.CsvSink",
        )
        .config("spark.metrics.conf.*.sink.csv.period", "10")
        .config("spark.metrics.conf.*.sink.csv.unit", "seconds")
        .config("spark.metrics.conf.*.sink.csv.directory", metrics_dir)
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
    # Spark config) to pick the worker Python. If it is unset the workers fall
    # back to `python3` on PATH, which under `uv run` is NOT the venv — so they
    # can't import graphsenselib and UDFs fail with ModuleNotFoundError. Default
    # the worker interpreter to the driver's (sys.executable); an explicit
    # spark.pyspark.python or a pre-set PYSPARK_PYTHON still wins.
    if spark_config and "spark.pyspark.python" in spark_config:
        os.environ["PYSPARK_PYTHON"] = spark_config["spark.pyspark.python"]
    else:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    spark = builder.getOrCreate()
    logger.info(f"SparkSession created: {app_name} (local={local})")
    return spark
