"""Cassandra Sidecar bulk-write path (cassandra-analytics), shared helpers.

The bulk writer streams SSTables through the Sidecar service on the Cassandra
nodes instead of pushing rows through the native protocol. Two consumers:
the Scala full-transform launcher (``transformation/spark_jar.py``) and
PySpark jobs that bulk-write DataFrames — e.g. the delta-lake -> raw keyspace
rebuild, which imports ``bulk_write_dataframe`` alongside the address
normalizer. No pyspark import here: the DataFrame is duck-typed.
"""

from typing import Dict, List, Optional

# cassandra-analytics is Provided in graphsense-spark, so it is NOT in the fat
# jar — and it is absent from PySpark venvs. Jobs add it via --packages.
SIDECAR_PACKAGE = "org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.3.0"

# Spark datasource of the cassandra-analytics bulk writer. The 0.3.0 jar ships
# no DataSourceRegister service entry, so the FQCN is the only reliable form.
SIDECAR_WRITE_FORMAT = "org.apache.cassandra.spark.sparksql.CassandraDataSink"

# JDK module flags the Cassandra SSTable bulk writer needs. The temp-dir
# redirect is appended separately (it depends on spark.local.dir).
_SIDECAR_MODULE_FLAGS = (
    "--add-exports java.base/jdk.internal.misc=ALL-UNNAMED "
    "--add-exports java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens java.base/sun.nio.ch=ALL-UNNAMED"
)

# The bulk writer's task results carry DecoratedKey (wraps a ByteBuffer),
# which Java serialization rejects ("not serializable result:
# java.nio.HeapByteBuffer"). Kryo + the analytics registrator are required.
_KRYO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
_SIDECAR_KRYO_REGISTRATOR = (
    "org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator"
)


def sidecar_spark_properties(spark_props: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of ``spark_props`` with the SSTable-writer JVM flags.

    Appends the module flags and the temp-dir redirect (SSTable staging and
    Vert.x cache must live on spark.local.dir, not the root disk) to both
    driver and executor extraJavaOptions.

    The properties must be in force when the JVMs launch: pass them to
    spark-submit via --conf, or set them on the SparkConf before the session
    is created in a plain ``python job.py`` launch. Setting
    spark.driver.extraJavaOptions on an already-running driver (e.g. inside a
    spark-submit'ed script) is silently ignored by Spark.
    """
    local_dir = spark_props.get("spark.local.dir")
    if not local_dir:
        raise ValueError(
            "sidecar writer needs spark.local.dir set to redirect the "
            "SSTable/Vert.x temp dir off the root disk"
        )
    # spark.local.dir may be a comma-separated list; java.io.tmpdir needs one
    tmp_dir = local_dir.split(",")[0]
    props = dict(spark_props)
    # Vert.x creates its cache dir as `<cacheDirBase>-<uuid>` — a SIBLING of
    # the given path, not inside it. Point the base at a subpath so the
    # sibling lands inside spark.local.dir; the bare dir would need a
    # writable PARENT, which e.g. a docker mountpoint's root-owned parent
    # is not (observed as AccessDeniedException at the first bulk write).
    jvm = (
        f"{_SIDECAR_MODULE_FLAGS} -Djava.io.tmpdir={tmp_dir} "
        f"-Dvertx.cacheDirBase={tmp_dir}/vertx-cache"
    )
    for key in ("spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions"):
        existing = props.get(key, "").strip()
        props[key] = f"{existing} {jvm}".strip() if existing else jvm

    props["spark.serializer"] = _KRYO_SERIALIZER
    registrator = props.get("spark.kryo.registrator", "").strip()
    if registrator and _SIDECAR_KRYO_REGISTRATOR not in registrator.split(","):
        props["spark.kryo.registrator"] = f"{registrator},{_SIDECAR_KRYO_REGISTRATOR}"
    else:
        props.setdefault("spark.kryo.registrator", _SIDECAR_KRYO_REGISTRATOR)
    return props


def sidecar_packages(packages: List[str]) -> List[str]:
    """Return a copy of ``packages`` with the analytics package appended once."""
    pkgs = list(packages)
    if SIDECAR_PACKAGE not in pkgs:
        pkgs.append(SIDECAR_PACKAGE)
    return pkgs


def sidecar_writer_options(
    *,
    keyspace: str,
    table: str,
    contact_points: List[str],
    local_dc: Optional[str] = None,
    consistency_level: str = "LOCAL_QUORUM",
    options: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """DataFrameWriter options for the bulk writer (WriterOptions names).

    ``options`` adds tuning knobs (number_splits,
    commit_threads_per_instance, …) or overrides the consistency defaults;
    the identity options (keyspace, table, contact points) can only come
    from the explicit arguments. LOCAL_* consistency levels require
    ``local_dc`` (the writer's replication validation rejects them without
    one), and LOCAL_*/EACH_QUORUM only work on NetworkTopologyStrategy
    keyspaces.
    """
    if not contact_points:
        raise ValueError("sidecar contact_points must be set")
    if consistency_level.startswith("LOCAL_") and not local_dc:
        raise ValueError(
            f"local_dc is required for {consistency_level} (the bulk writer "
            "validates keyspace replication against the local datacenter)"
        )
    opts = {
        "keyspace": keyspace,
        "table": table,
        "sidecar_contact_points": ",".join(contact_points),
        "bulk_writer_cl": consistency_level,
    }
    if local_dc:
        opts["local_dc"] = local_dc
    if options:
        identity_overrides = {"keyspace", "table", "sidecar_contact_points"} & set(
            options
        )
        if identity_overrides:
            raise ValueError(
                f"options may not override {sorted(identity_overrides)} — "
                "pass them as explicit arguments"
            )
        opts.update(options)
    return opts


def bulk_write_dataframe(
    df,
    *,
    keyspace: str,
    table: str,
    contact_points: List[str],
    local_dc: Optional[str] = None,
    consistency_level: str = "LOCAL_QUORUM",
    options: Optional[Dict[str, str]] = None,
) -> None:
    """Bulk-write a Spark DataFrame into ``keyspace.table`` via the Sidecar.

    The table must exist; column names must match. The job's Spark session
    must be built with ``sidecar_spark_properties`` and ``sidecar_packages``.
    """
    opts = sidecar_writer_options(
        keyspace=keyspace,
        table=table,
        contact_points=contact_points,
        local_dc=local_dc,
        consistency_level=consistency_level,
        options=options,
    )
    df.write.format(SIDECAR_WRITE_FORMAT).options(**opts).mode("append").save()
