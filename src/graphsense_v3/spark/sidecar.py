"""Cassandra Sidecar bulk writes from PySpark.

The bulk writer is `cassandra-analytics`, a Spark **DataSource**
(``org.apache.cassandra.spark.sparksql.CassandraDataSink``), so it is not
Scala-only: graphsense-spark's ``SidecarBulkWriter.scala`` is a wrapper that
sets its options and works around its quirks. Everything that wrapper does is
reproduced here.

Why bother: the connector CQL path writes at ``throughputMBPerSec`` and is what
the TRON transform moved *off* because it was too slow and too disruptive. The
bulk path generates SSTables on the executors and streams them in, bypassing the
CQL write path entirely.

**Not yet exercised against a real cluster from PySpark.** Prove it on a small
block range before committing a long run to it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

# The coordinate and the JDK module flags are gslib's, not a second copy:
# cassandra-analytics is `Provided` in graphsense-spark, so the package has to
# be added at submit time either way.
from graphsenselib.transformation.spark_jar import (
    SIDECAR_PACKAGE,
    _SIDECAR_MODULE_FLAGS,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

#: What `KryoRegister.setup` sets, which the Scala path calls before the
#: session is built. Replicated as plain properties because a PySpark session
#: is configured through the builder, not through a SparkConf we can hand to a
#: Java static.
KRYO_REGISTRATOR = "org.apache.cassandra.spark.KryoRegister"
CASSANDRA_VERSION = "4.0.0"

#: The connector maps Cassandra `varint` to Spark DecimalType(38, 0). A real
#: `decimal` column carries its own precision, so this isolates varint cleanly.
VARINT_PRECISION = 38


def session_config(
    spark_config: dict, contact_points: list, local_dc: Optional[str]
) -> dict:
    """Spark properties the bulk writer needs, merged over ``spark_config``.

    Must be applied *before* the session is created: the Kryo registrator and
    the SSTable writer's JDK module flags cannot be set afterwards.
    """
    if not contact_points:
        raise ValueError("sidecar writes need at least one sidecar contact point")
    local_dir = spark_config.get("spark.local.dir")
    if not local_dir:
        raise ValueError(
            "sidecar writes need spark.local.dir set (in the spark_config "
            "profile) to redirect the SSTable/Vert.x temp dir off the root disk"
        )

    props = dict(spark_config)
    props["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
    registrators = [
        r for r in props.get("spark.kryo.registrator", "").split(",") if r.strip()
    ]
    if KRYO_REGISTRATOR not in registrators:
        registrators.append(KRYO_REGISTRATOR)
    props["spark.kryo.registrator"] = ",".join(registrators)
    props.setdefault("spark.cassandra_analytics.cassandra.version", CASSANDRA_VERSION)

    # The executors' temp dir is spark.local.dir; the DRIVER's is its own, for
    # the same reason it needs its own SPARK_LOCAL_DIRS -- the cluster's nvme
    # path does not exist inside the driver container.
    from graphsense_v3.spark.session import ensure_driver_scratch

    for key, temp in (
        ("spark.driver.extraJavaOptions", ensure_driver_scratch(props) or local_dir),
        ("spark.executor.extraJavaOptions", local_dir),
    ):
        jvm = (
            f"{_SIDECAR_MODULE_FLAGS} -Djava.io.tmpdir={temp} "
            f"-Dvertx.cacheDirBase={temp}"
        )
        existing = props.get(key, "").strip()
        props[key] = f"{existing} {jvm}".strip() if existing else jvm

    packages = [p for p in props.get("spark.jars.packages", "").split(",") if p.strip()]
    if SIDECAR_PACKAGE not in packages:
        packages.append(SIDECAR_PACKAGE)
    props["spark.jars.packages"] = ",".join(packages)
    return props


def _cast_varints(column, data_type):
    """Rebuild ``column`` with every varint-shaped decimal cast to string."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, DecimalType, MapType, StructType

    if isinstance(data_type, DecimalType):
        if data_type.precision == VARINT_PRECISION and data_type.scale == 0:
            return column.cast("string")
        return column
    if isinstance(data_type, StructType):
        return F.struct(
            *[
                _cast_varints(column.getField(f.name), f.dataType).alias(f.name)
                for f in data_type.fields
            ]
        )
    if isinstance(data_type, ArrayType):
        return F.transform(
            column, lambda item: _cast_varints(item, data_type.elementType)
        )
    if isinstance(data_type, MapType):
        return F.transform_values(
            column, lambda _, value: _cast_varints(value, data_type.valueType)
        )
    return column


def cast_varints_to_string(df: "DataFrame") -> "DataFrame":
    """Every varint column -- top level or nested -- as a string.

    cassandra-analytics 0.3.0's ``BigIntegerConverter`` rejects
    ``java.math.BigDecimal``, which is what Spark's DecimalType emits at
    row-read time, so a varint has to arrive as a string; the converter parses
    it back. No-op for a frame with no varint.
    """
    return df.select(
        *[
            _cast_varints(df[field.name], field.dataType).alias(field.name)
            for field in df.schema.fields
        ]
    )


def write(
    df: "DataFrame",
    keyspace: str,
    table: str,
    *,
    contact_points: list,
    local_dc: str,
    consistency_level: str = "LOCAL_QUORUM",
    number_splits: int = 1,
) -> None:
    """Bulk-write one frame through the sidecar."""
    (
        cast_varints_to_string(df)
        .write.format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
        .option("sidecar_contact_points", ",".join(contact_points))
        .option("keyspace", keyspace)
        .option("table", table)
        .option("local_dc", local_dc)
        .option("bulk_writer_cl", consistency_level)
        .option("number_splits", str(number_splits))
        .option("data_transport", "DIRECT")
        .mode("append")
        .save()
    )
