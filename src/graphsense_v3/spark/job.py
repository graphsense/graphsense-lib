"""End-to-end backfill: Delta Lake -> v3 raw -> v3 derived.

Built to be run once against a real cluster rather than iterated on, so it
reports what each stage cost and refuses early on anything it cannot finish.

Driven by ``graphsense-v3 run``, which resolves everything it needs from
``graphsense.yaml``. ``--dry-run`` builds every frame and runs the conformance
checks without writing, which catches a column or key mismatch in seconds
instead of hours in.
"""

# NOTE: no `from __future__ import annotations` -- pandas UDFs reach this module
# through the loaders it imports.

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from graphsense_v3.schema import Kind, NETWORKS, Family, schema_for
from graphsense_v3.schema.definitions import MARKER_COMPLETE, MARKERS
from graphsense_v3.spark import (
    raw_account,
    raw_utxo,
    summary,
    derived_account,
    derived_utxo,
    writer,
)
from graphsense_v3.settings import RunSettings, assert_v3_keyspace
from graphsense_v3.spark.source import DeltaLake

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def exchange_rates_by_block(
    spark: "SparkSession", blocks: "DataFrame", rates: "DataFrame"
) -> "DataFrame":
    """``(asset, block_id, fiat_values)`` from the date-keyed rate table.

    Rates are not in the lake -- the existing gslib ``exchange-rates`` path owns
    that table -- so they are read from a keyspace and joined onto blocks by
    date. One table covers the native coin and every token alike, so this is one
    join rather than two. An asset with no rate for a block simply gets no row,
    and the transform then contributes no fiat for it rather than a zero.
    """
    from pyspark.sql import functions as F

    from graphsense_v3.spark.columns import day_from_timestamp

    dated = blocks.select(
        F.col("block_id"),
        F.date_format(day_from_timestamp(F.col("timestamp")), "yyyy-MM-dd").alias(
            "date"
        ),
    )
    return dated.join(rates, on="date", how="inner").select(
        F.col("asset"),
        F.col("block_id"),
        F.col("fiat_values").cast("map<string,double>").alias("fiat_values"),
    )


def read_cassandra(spark: "SparkSession", keyspace: str, table: str) -> "DataFrame":
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load()
    )


class Stage:
    """Times a stage and logs it. A benchmark run's whole output, really."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __enter__(self) -> "Stage":
        self.started = time.monotonic()
        logger.info("START %s", self.name)
        return self

    def __exit__(self, *exc) -> None:
        elapsed = time.monotonic() - self.started
        outcome = "FAILED" if exc[0] else "done"
        logger.info("%s  %s in %.1fs", outcome, self.name, elapsed)


def mark_complete(
    spark: "SparkSession", network: str, kind: Kind, keyspace: str
) -> None:
    """Record that every table of ``keyspace`` has been written.

    The last write of the run, and the only thing that distinguishes a finished
    keyspace from one whose job died halfway. A reader without this marker is
    measuring missing data and cannot tell that it is.
    """
    now = datetime.now(timezone.utc)
    frame = spark.createDataFrame(
        [(MARKER_COMPLETE, now.isoformat(), int(now.timestamp()))],
        schema="key STRING, value STRING, updated_at BIGINT",
    )
    writer.write(frame, schema_for(network, kind).table(MARKERS), keyspace)
    logger.info("marked %s complete", keyspace)


def run(
    spark: "SparkSession",
    settings: "RunSettings",
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    dry_run: bool = False,
    stages: Optional[tuple] = None,
) -> None:
    """Backfill one network, per :class:`RunSettings`.

    The derived stage reads its inputs from the frames the raw stage just
    built rather than from Cassandra, so the lake is scanned once.
    """
    network = settings.network
    family = NETWORKS[network]
    config = settings.config
    wanted = stages or ("raw", "derived")

    # Everything decidable from the arguments alone is decided here, before a
    # byte is read. A run that cannot finish should say so at submit time, not
    # after a full pass over the lake. The keyspace names were already checked
    # when the settings were built, and are checked again on every write.
    assert_v3_keyspace(settings.raw_keyspace)
    assert_v3_keyspace(settings.derived_keyspace)

    lake = DeltaLake(spark, settings.lake_root, network)
    loader = raw_utxo if family is Family.UTXO else raw_account

    with Stage(f"preflight {network}"):
        problems = loader.preflight(
            lake, network, start_block=start_block, end_block=end_block
        )
        for problem in problems:
            logger.error("PREFLIGHT: %s", problem)
        if problems:
            raise SystemExit(
                f"{len(problems)} preflight problem(s); fix or override before a run"
            )

    raw_schema = schema_for(network, Kind.RAW)
    with Stage("build raw frames"):
        raw_frames = loader.build(
            lake,
            network,
            settings.raw_keyspace,
            start_block=start_block,
            end_block=end_block,
            config=config,
        )
        for name, frame in raw_frames.items():
            writer.check(frame, raw_schema.table(name))

    if "raw" in wanted and not dry_run:
        for name, frame in raw_frames.items():
            with Stage(f"write raw.{name}"):
                writer.write(
                    frame,
                    raw_schema.table(name),
                    settings.raw_keyspace,
                    sidecar=settings.sidecar,
                )
        with Stage("write raw.summary_statistics"):
            writer.write(
                summary.statistics_for(spark, raw_frames),
                raw_schema.table("summary_statistics"),
                settings.raw_keyspace,
            )
        mark_complete(spark, network, Kind.RAW, settings.raw_keyspace)

    if "derived" not in wanted:
        if dry_run:
            logger.info("dry run: %d raw frames conform", len(raw_frames))
        return

    _run_derived(
        spark,
        network,
        family,
        raw_frames,
        settings.derived_keyspace,
        settings.rates_keyspace,
        config=config,
        dry_run=dry_run,
        sidecar=settings.sidecar,
    )


def _run_derived(
    spark: "SparkSession",
    network: str,
    family: Family,
    raw_frames: dict,
    derived_keyspace: str,
    rates_keyspace: str,
    *,
    config,
    dry_run: bool,
    sidecar: Optional[dict] = None,
) -> None:
    """The derived stage, from the frames the raw stage just built.

    Reading its inputs from those frames rather than back out of Cassandra means
    the lake is scanned once -- but it also means Spark recomputes them, so on a
    real run the two heavy frames are worth persisting first if the cluster has
    the memory.
    """
    rates = exchange_rates_by_block(
        spark,
        raw_frames["block"],
        read_cassandra(spark, rates_keyspace, "exchange_rates"),
    )

    schema = schema_for(network, Kind.DERIVED)
    with Stage("build derived frames"):
        if family is Family.UTXO:
            frames = derived_utxo.build(
                raw_frames["transaction_io"],
                raw_frames["transaction"],
                rates,
                network,
                config=config,
            )
        else:
            # token_configuration and token_exchange_rates are curated, not
            # derived, so they are read from the keyspace that already holds
            # them rather than rebuilt here.
            frames = derived_account.build(
                raw_frames["trace"],
                raw_frames["log"],
                read_cassandra(spark, rates_keyspace, "token_configuration"),
                rates,
                network,
                config=config,
            )
        for name, frame in frames.items():
            writer.check(frame, schema.table(name))

    if dry_run:
        logger.info("dry run: %d derived frames conform", len(frames))
        return

    for name, frame in frames.items():
        with Stage(f"write derived.{name}"):
            writer.write(frame, schema.table(name), derived_keyspace, sidecar=sidecar)
    # Counts, so it runs after the frames it counts are materialised.
    with Stage("write derived.summary_statistics"):
        writer.write(
            summary.statistics_for(spark, raw_frames, frames),
            schema.table("summary_statistics"),
            derived_keyspace,
        )
    mark_complete(spark, network, Kind.DERIVED, derived_keyspace)


def main(argv: Optional[list] = None) -> None:
    """Kept so the module stays spark-submittable; `graphsense-v3 run` is the
    normal entry point."""
    from graphsense_v3.cli import cli

    cli(args=argv, standalone_mode=True)


if __name__ == "__main__":
    main()
