"""SparkSession for the v3 backfill.

Thin wrapper over ``graphsenselib.transformation.spark.create_spark_session``:
the package set (Cassandra connector, delta-spark, hadoop-aws) and the S3 and
Cassandra wiring are already correct there, and duplicating them would be a
second thing to keep in step with the cluster's hadoop version.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def ensure_driver_scratch(spark_config: dict | None) -> str | None:
    """Make sure the driver has somewhere writable for its block manager.

    ``spark.local.dir`` applies to the driver as well as the executors, and the
    cluster profiles point it at ``/var/data/nvme4/...`` -- a disk that exists
    on the worker hosts. In client mode the driver runs in a container, so the
    right answer is to MOUNT that path into it, which is what the Scala
    transform runs do::

        -v "$DRIVER_SCRATCH":/var/data/nvme4/spark

    That gives the driver a real disk rather than the container's writable
    layer, and leaves one meaningful ``spark.local.dir``.

    When it is not mounted, Spark dies at startup with
    ``AccessDeniedException: /var/data`` before a stage runs. Rather than fail
    that way, fall back to a temp dir via ``SPARK_LOCAL_DIRS`` -- which
    overrides ``spark.local.dir`` for the process that has it set, and is not
    propagated to executors, so they keep the nvme. Returns the fallback path
    if one was needed, else None.
    """
    configured = (spark_config or {}).get("spark.local.dir")
    if os.environ.get("SPARK_LOCAL_DIRS"):
        return None
    if configured and os.access(configured, os.W_OK):
        return None

    path = os.path.join(tempfile.gettempdir(), "graphsense-v3-driver")
    os.makedirs(path, exist_ok=True)
    os.environ["SPARK_LOCAL_DIRS"] = path
    if configured:
        logger.warning(
            "spark.local.dir=%s is not writable here, so the DRIVER falls back "
            "to %s. Mount the real disk into the container "
            "(-v <host path>:%s) -- a container's writable layer is small and "
            "driver broadcasts land here. Executors are unaffected.",
            configured,
            path,
            configured,
        )
    return path


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

    # Must happen before the JVM launches: it reads the environment at startup.
    ensure_driver_scratch(spark_config)

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
