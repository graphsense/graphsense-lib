"""Spark settings a v3 backfill needs on top of the configured profile.

The per-currency profiles in ``graphsense.yaml`` were written for the Scala
transform. Most of them carry over, but three differences between that job and
this one are not tuning -- they decide whether the run works at all:

* the Scala job does not read the Delta Lake through this session,
* it writes through the sidecar rather than the CQL connector, and
* it has no Python workers.

:data:`REQUIRED` therefore wins over the configured profile, and every override
is logged. :data:`TUNING` only fills in what the profile does not set.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: Both extensions, in one value. `create_spark_session` sets the Delta
#: extension and then applies spark_config LAST, so a profile naming only the
#: Cassandra extension (`transform-utxo`, `transform-eth`, `transform-trx` all
#: do) silently REPLACES it -- and the lake read then fails with "delta is not
#: a valid Spark SQL Data Source". The Scala job never hit this because it does
#: not read Delta through the same session.
SQL_EXTENSIONS = ",".join(
    [
        "io.delta.sql.DeltaSparkSessionExtension",
        "com.datastax.spark.connector.CassandraSparkExtensions",
    ]
)

#: Applied after the configured profile: correctness, not preference.
REQUIRED: dict[str, str] = {
    "spark.sql.extensions": SQL_EXTENSIONS,
    # Without this the connector writes an explicit NULL for every absent
    # value, and an explicit NULL is a TOMBSTONE. v3 frames are legitimately
    # sparse -- first_log_index on a transaction with no logs, the fiat map for
    # a block with no known rate, the address of a nulldata output -- so a
    # backfill would lay down billions of tombstones on a brand-new keyspace.
    # This is the same defect the review found on the delta-updater side
    # (`abstractupdater.py:102`, missing auto_none_to_unset).
    "spark.cassandra.output.ignoreNulls": "true",
    # `day_from_timestamp` is deliberately timezone-free, but `date_format` in
    # the exchange-rate join is not.
    "spark.sql.session.timeZone": "UTC",
}

#: Filled in only where the configured profile is silent.
TUNING: dict[str, str] = {
    # The ordinal window sorts ~5e9 rows on BTC. The Scala UTXO profile uses
    # 800, which leaves ~6M rows a partition; 2000 is what the TRON profile
    # settled on for comparable volume, and AQE coalesces where that is too many.
    "spark.sql.shuffle.partitions": "2000",
    "spark.default.parallelism": "2000",
    # The relation join fans a transaction out to inputs x outputs, which varies
    # by orders of magnitude per row. Skew handling is not enabled by any
    # existing profile and is the single most useful addition for this job.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Set by transform-eth and transform-trx, not by transform-utxo. The
    # default (4096 rows) spills almost immediately in a window this size.
    "spark.sql.windowExec.buffer.spill.threshold": "2097152",
    # Python workers hold pandas batches OFF-heap, so they are not covered by
    # spark.executor.memory. The Scala profiles never needed this.
    "spark.executor.pyspark.memory": "8g",
}

#: Keys a profile must set for the run to work on this cluster, with the reason.
#: Reported rather than invented: the right values are cluster facts.
EXPECTED: dict[str, str] = {
    "spark.local.dir": (
        "shuffle spills land on the ~23G root disk otherwise; the transform-* "
        "profiles point this at /var/data/nvme4/spark/local_storage"
    ),
    "spark.archives": (
        "the pandas UDFs import graphsense_v3 ON THE EXECUTORS, so the packaged "
        "environment has to be shipped -- see the `pubkey` profile, which does "
        "exactly this (spark.archives + spark.executorEnv.PYTHONPATH)"
    ),
    "spark.executorEnv.PYTHONPATH": "as spark.archives",
}


def resolve(configured: dict) -> dict:
    """Merge ``configured`` with the v3 defaults and required overrides."""
    resolved = dict(TUNING)
    resolved.update(configured or {})
    for key, value in REQUIRED.items():
        previous = resolved.get(key)
        if previous is not None and previous != value:
            logger.warning(
                "spark: overriding %s=%r with %r (required by the v3 job)",
                key,
                previous,
                value,
            )
        resolved[key] = value
    return resolved


def warnings(configured: dict) -> list[str]:
    """Settings this job needs that only the cluster's owner can supply."""
    return [
        f"{key} is not set: {why}"
        for key, why in EXPECTED.items()
        if not (configured or {}).get(key)
    ]
