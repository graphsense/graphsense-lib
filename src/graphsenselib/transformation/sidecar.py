"""Cassandra Sidecar bulk-write path (cassandra-analytics SSTable streaming).

Port of graphsense-ethereum-transformation's ``SidecarBulkWriter`` to PySpark.
Instead of the spark-cassandra-connector's CQL write (coordinator → commitlog →
memtable), executors generate SSTables and stream them into Cassandra through
the Sidecar, then trigger an import — the canonical bulk-load path.

Unlike the Scala job, PySpark cannot run cassandra-analytics'
``BulkSparkConf.setupSparkConf`` / ``KryoRegister.setup`` before the JVM and the
``--packages`` classpath exist, so :func:`sidecar_spark_config` reproduces their
effect as plain SparkSession-builder configs (extracted from
cassandra-analytics 0.3.0). The reader-side Kryo registrator is
Cassandra-version-specific (``KryoRegister$V40/$V41/$V50``); we name it
explicitly per :func:`_registrator_for`.
"""

import logging

logger = logging.getLogger(__name__)

# cassandra-analytics bulk-writer Spark data source.
CASSANDRA_DATA_SINK = "org.apache.cassandra.spark.sparksql.CassandraDataSink"

# Spark package providing the bulk writer; added to spark.jars.packages only
# for the sidecar writer (the default connector path does not need it).
CASSANDRA_ANALYTICS_PACKAGE = (
    "org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.3.0"
)

# KryoRegister.setup() resolves this from the Cassandra version at runtime; we
# can't, so map major.minor → the concrete (no-arg-constructable) registrator.
_KRYO_REGISTRATOR_BY_VERSION = {
    "4.0": "org.apache.cassandra.spark.KryoRegister$V40",
    "4.1": "org.apache.cassandra.spark.KryoRegister$V41",
    "5.0": "org.apache.cassandra.spark.KryoRegister$V50",
}

# Bulk-writer Kryo registrator (version-independent; public no-arg ctor).
_SBW_KRYO_REGISTRATOR = "org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator"

# Verbatim BulkSparkConf.JDK11_OPTIONS — the --add-exports/--add-opens the bulk
# writer needs for off-heap / unsafe SSTable generation on Java 11+.
_JDK11_OPTIONS = (
    "-Djdk.attach.allowAttachSelf=true "
    "--add-exports java.base/jdk.internal.misc=ALL-UNNAMED "
    "--add-exports java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED "
    "--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED "
    "--add-exports java.rmi/sun.rmi.server=ALL-UNNAMED "
    "--add-exports java.sql/java.sql=ALL-UNNAMED "
    "--add-opens java.base/java.lang.module=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.loader=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.reflect=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.math=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.module=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED "
    "--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"
)

# hashCode=0 forces deterministic identity hash codes, required by the bulk
# writer's token sort (from the eth-transformation spark-submit config).
_HASHCODE_OPTS = "-XX:+UnlockExperimentalVMOptions -XX:hashCode=0"


def _registrator_for(cassandra_version):
    major_minor = ".".join(str(cassandra_version).split(".")[:2])
    registrator = _KRYO_REGISTRATOR_BY_VERSION.get(major_minor)
    if registrator is None:
        raise ValueError(
            f"Unsupported Cassandra version for sidecar bulk write: "
            f"{cassandra_version!r}. cassandra-analytics 0.3.0 supports major.minor "
            f"in {sorted(_KRYO_REGISTRATOR_BY_VERSION)}."
        )
    return registrator


def sidecar_spark_config(cassandra_version):
    """Builder configs replicating cassandra-analytics' programmatic setup.

    Returns a ``dict`` to apply to the SparkSession builder. ``cassandra_version``
    must match the target cluster — it selects the Kryo registrator AND the
    SSTable format the bulk writer generates. The ``extraJavaOptions`` are
    additive in cassandra-analytics; any caller-provided value must include them.
    """
    java_opts = f"{_HASHCODE_OPTS} {_JDK11_OPTIONS}"
    return {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.referenceTracking": "false",
        "spark.kryo.registrator": (
            f"{_SBW_KRYO_REGISTRATOR},{_registrator_for(cassandra_version)}"
        ),
        "spark.cassandra_analytics.cassandra.version": str(cassandra_version),
        "spark.driver.extraJavaOptions": java_opts,
        "spark.executor.extraJavaOptions": java_opts,
    }


def bulk_write(
    df,
    keyspace,
    table,
    columns,
    *,
    contact_points,
    local_dc,
    consistency_level="LOCAL_QUORUM",
):
    """Bulk-write ``df`` to ``keyspace.table`` via the Sidecar SSTable path.

    ``columns`` are the target Cassandra column names in table order; the
    DataFrame is projected onto them (cassandra-analytics matches by exact
    name). The clustering tables are flat ``int`` columns with matching names,
    so none of the eth transformation's UDT/varint ``alignToSchema`` is needed.
    """
    if not contact_points or not local_dc:
        raise ValueError("sidecar bulk_write requires contact_points and local_dc")
    (
        df.select(*columns)
        .write.format(CASSANDRA_DATA_SINK)
        .option("sidecar_contact_points", contact_points)
        .option("keyspace", keyspace)
        .option("table", table)
        .option("local_dc", local_dc)
        .option("bulk_writer_cl", consistency_level)
        .option("number_splits", "-1")
        .option("data_transport", "DIRECT")
        .mode("append")
        .save()
    )
