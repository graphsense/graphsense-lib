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
from graphsense_v3.settings import RATE_FORWARD_FILL_SECONDS
from graphsense_v3.spark.source import DeltaLake

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


#: Every rate table this loader accepts, and how to tell them apart.
#:
#:   v2 raw          exchange_rates(date)                 map, NO asset column
#:   v2 raw          token_exchange_rates(asset, date)    account only
#:   v3 raw          exchange_rates(asset, date)          merged
#:   v3 derived      exchange_rates(asset, block_id...)   merged, per block
#:
#: The presence of an `asset` column is what distinguishes a v3 table from a v2
#: one, and a `block_id` column is what says the dates have already been
#: resolved. v2's *transformed* tables are deliberately NOT accepted -- their
#: fiat_values is a positional `list<float>`, whose meaning depends on an
#: ordering stored elsewhere, and silently reading it against the wrong ordering
#: is exactly the defect v3's map replaced.


def normalise_rates(rates: "DataFrame", *, symbol: str) -> "DataFrame":
    """Any accepted rate table -> ``(asset, <key>, fiat_values)``.

    ``<key>`` is whichever of ``date`` or ``block_id`` the source carries;
    :func:`rates_by_block` handles both.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType

    field = rates.schema["fiat_values"].dataType
    if isinstance(field, ArrayType):
        raise SystemExit(
            "this rate table stores fiat_values as a positional list, so its "
            "meaning depends on a currency ordering held elsewhere. Point "
            "--rates-keyspace at a raw keyspace, whose fiat_values is a map."
        )

    key = "block_id" if "block_id" in rates.columns else "date"
    asset = F.col("asset") if "asset" in rates.columns else F.lit(symbol)
    return rates.select(
        asset.alias("asset"),
        F.col(key),
        F.col("fiat_values").cast("map<string,double>").alias("fiat_values"),
    )


def rates_by_block(blocks: "DataFrame", rates: "DataFrame") -> "DataFrame":
    """``(asset, block_id, fiat_values)``, resolving dates against the blocks.

    A source that is already per-block needs no join. A block whose date has no
    rate simply gets no row, and the transform then contributes no fiat for it
    rather than a zero.
    """
    from pyspark.sql import functions as F

    from graphsense_v3.spark.columns import day_from_timestamp

    if "block_id" in rates.columns:
        return rates.select("asset", "block_id", "fiat_values")

    dated = blocks.select(
        F.col("block_id"),
        F.date_format(day_from_timestamp(F.col("timestamp")), "yyyy-MM-dd").alias(
            "date"
        ),
    )
    return dated.join(rates, on="date", how="inner").select(
        "asset", "block_id", "fiat_values"
    )


def rated_tip(rates: "DataFrame") -> Optional[int]:
    """The highest block a rate exists for, or None if there are none.

    Rates arrive a day at a time, so this normally sits a few hundred blocks
    below the chain tip -- the current day has no rate row until the day closes.
    """
    from pyspark.sql import functions as F

    row = rates.agg(F.max("block_id").alias("tip")).collect()[0]
    return None if row["tip"] is None else int(row["tip"])


def forward_fill_rates(
    rates: "DataFrame", blocks: "DataFrame", *, within_seconds: int
) -> "DataFrame":
    """``rates`` plus rows for recent unrated blocks, at the last known rate.

    The window is measured in BLOCK TIME rather than a block count, because a
    count means something different on every chain -- a day is ~576 LTC blocks
    and ~28,800 TRON ones.

    Carried per asset: each asset's own last rate extends forward, so a token
    whose feed stopped earlier than the native coin's does not silently borrow
    the coin's freshness.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    timed = rates.join(blocks.select("block_id", "timestamp"), on="block_id")
    newest = Window.partitionBy("asset").orderBy(F.desc("block_id"))
    last = (
        timed.withColumn("_rank", F.row_number().over(newest))
        .where(F.col("_rank") == 1)
        .select(
            F.col("asset").alias("_asset"),
            F.col("block_id").alias("_block_id"),
            F.col("timestamp").alias("_timestamp"),
            F.col("fiat_values").alias("_fiat_values"),
        )
    )
    filled = (
        blocks.select("block_id", "timestamp")
        .join(
            F.broadcast(last),
            (F.col("block_id") > F.col("_block_id"))
            & (F.col("timestamp") <= F.col("_timestamp") + F.lit(within_seconds)),
        )
        .select(
            F.col("_asset").alias("asset"),
            F.col("block_id"),
            F.col("_fiat_values").alias("fiat_values"),
        )
    )
    return rates.unionByName(filled)


def zero_fill_rates(rates: "DataFrame", blocks: "DataFrame") -> "DataFrame":
    """``rates`` plus a zero row for every block still without one.

    **v2 materialises a row for EVERY block**, carrying ``[0, 0]`` where the
    feed has nothing -- `ltc_transformed_20260727` holds block 1000 as exactly
    that, because the rate source starts 2015-01-01 and LTC genesis is 2011. So
    "no rate" is not a state v2 ever serves, and v3 must not invent it: the REST
    rates service raises ``BlockNotFoundException`` for a block with no row, so
    an absent row is a failed request rather than a missing number.

    Zeros are honest here in a way forward-filling would not be. These blocks
    are BEFORE the feed began; there is no earlier rate to carry, and zero is
    what v2 already reports for them.
    """
    from pyspark.sql import functions as F

    codes = sorted(
        row[0]
        for row in rates.select(F.explode(F.map_keys("fiat_values")).alias("code"))
        .distinct()
        .collect()
    )
    zero = F.create_map(*[item for code in codes for item in (F.lit(code), F.lit(0.0))])
    assets = rates.select("asset").distinct()
    # Every (block, asset) pair, so a chain with tokens gets a row per asset
    # rather than one null-asset row per block.
    every = blocks.select("block_id").crossJoin(F.broadcast(assets))
    return every.join(rates, on=["block_id", "asset"], how="left").select(
        F.col("asset"),
        F.col("block_id"),
        F.coalesce(F.col("fiat_values"), zero).alias("fiat_values"),
    )


def exchange_rates_by_block(
    spark: "SparkSession", network: str, keyspace: str, blocks: "DataFrame"
) -> "DataFrame":
    """``(asset, block_id, fiat_values)`` from an existing rate keyspace.

    Rates are not in the lake -- the gslib ``exchange-rates`` path owns them --
    so they come from a live keyspace, which may be either generation.
    """
    from graphsense_v3.spark.derived_account import NATIVE

    symbol = NATIVE[network][0] if network in NATIVE else network.upper()
    rates = normalise_rates(
        read_cassandra(spark, keyspace, "exchange_rates"), symbol=symbol
    )
    # v3 merged the token rates in, so a v3 source is already complete. Only a
    # v2 account keyspace has a second table to union.
    is_v2 = "asset" not in read_cassandra(spark, keyspace, "exchange_rates").columns
    if is_v2 and NETWORKS[network] is Family.ACCOUNT:
        tokens = normalise_rates(
            read_cassandra(spark, keyspace, "token_exchange_rates"), symbol=symbol
        )
        rates = rates.unionByName(tokens)
    return rates_by_block(blocks, rates)


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


def sample(frames: dict, label: str, rows: int = 10) -> None:
    """Force a few rows of every frame, so a dry run actually EXECUTES.

    Conformance checks only resolve schemas -- Spark builds the plan and runs
    nothing, so a pandas UDF that fails on the executors (a missing import, an
    unencodable address) would pass a dry run and then fail hours into the real
    one. Materialising a handful of rows runs the whole DAG, UDFs included.

    It is not free: the plan still shuffles, so a dry run belongs on a BOUNDED
    block range. That is what it is for.
    """
    for name, frame in frames.items():
        with Stage(f"dry-run {label}.{name}"):
            got = frame.limit(rows).count()
            logger.info(
                "  %s.%s produced %d of the first %d rows", label, name, got, rows
            )


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
    accept_preflight: bool = False,
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

    # Before ANY read: one moment for every table, not one moment per table.
    pinned = lake.pin(loader.LAKE_TABLES)
    logger.info(
        "lake pinned: %s",
        ", ".join(f"{table}@{version}" for table, version in sorted(pinned.items())),
    )

    with Stage(f"preflight {network}"):
        problems = loader.preflight(
            lake, network, start_block=start_block, end_block=end_block
        )
        for problem in problems:
            logger.error("PREFLIGHT: %s", problem)
        if problems and not accept_preflight:
            raise SystemExit(
                f"{len(problems)} preflight problem(s); fix them, or re-run with "
                "--accept-preflight if they are understood and the run should "
                "proceed anyway"
            )
        if problems:
            # Loud, and repeated at the point of override: the run that
            # proceeds past a preflight problem is the one whose log gets read
            # months later by someone asking why a partition is 300 MB.
            logger.warning(
                "PROCEEDING PAST %d PREFLIGHT PROBLEM(S) because "
                "--accept-preflight was given",
                len(problems),
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
        # Not a table, so it must come out before anything is checked or
        # written; see raw_utxo.SPINE for why it travels in this dict.
        single_address_io = raw_frames.pop(raw_utxo.SPINE, None)
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

    if dry_run:
        # Always, even when the derived stage follows: the derived frames are
        # built from a few of the raw ones, so sampling them alone would leave
        # the spending tables, the prefix index and block_by_date unexecuted.
        logger.info("dry run: %d raw frames conform", len(raw_frames))
        sample(raw_frames, "raw")

    if "derived" not in wanted:
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
        single_address_io=single_address_io,
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
    single_address_io=None,
) -> None:
    """The derived stage, from the frames the raw stage just built.

    Reading its inputs from those frames rather than back out of Cassandra means
    the lake is scanned once -- but it also means Spark recomputes them, so on a
    real run the two heavy frames are worth persisting first if the cluster has
    the memory.
    """
    rates = exchange_rates_by_block(spark, network, rates_keyspace, raw_frames["block"])

    # Every derived row must be SERVABLE, and a block with no rate is not: the
    # REST rates service raises rather than degrading, so a transaction in an
    # unrated block fails the whole call. v2 never emits such a block -- it
    # materialises a row for every one. Two steps, in this order, because they
    # answer different gaps:
    #
    #   1. the TAIL gets the last real rate carried forward, since rates land a
    #      day at a time and the tip is always unrated for up to ~24h;
    #   2. everything still missing gets ZEROS, which is the head of the chain,
    #      before the rate feed began -- there is no earlier rate to carry, and
    #      zero is what v2 already reports there.
    if rated_tip(rates) is None:
        raise SystemExit(
            f"{rates_keyspace} has no exchange rates for any block in range; "
            "the derived keyspace would be entirely unservable"
        )
    rates = forward_fill_rates(
        rates, raw_frames["block"], within_seconds=RATE_FORWARD_FILL_SECONDS
    )
    rates = zero_fill_rates(rates, raw_frames["block"])

    schema = schema_for(network, Kind.DERIVED)
    with Stage("build derived frames"):
        if family is Family.UTXO:
            frames = derived_utxo.build(
                raw_frames["transaction_io"],
                raw_frames["transaction"],
                raw_frames["block"],
                rates,
                network,
                config=config,
                single_address_io=single_address_io,
            )
        else:
            # token_configuration and token_exchange_rates are curated, not
            # derived, so they are read from the keyspace that already holds
            # them rather than rebuilt here.
            frames = derived_account.build(
                raw_frames["trace"],
                raw_frames["log"],
                read_cassandra(spark, rates_keyspace, "token_configuration"),
                raw_frames["block"],
                rates,
                network,
                config=config,
                # Balances need the fees, and a fee is on the transaction, not
                # on any transfer. TRON keeps them in a table of their own; on
                # eth they are gas_used * gas_price off the receipt.
                transactions=raw_frames["transaction"],
                fees=raw_frames.get("fee"),
            )
        for name, frame in frames.items():
            writer.check(frame, schema.table(name))

    if dry_run:
        logger.info("dry run: %d derived frames conform", len(frames))
        sample(frames, "derived")
        return

    for name, frame in frames.items():
        with Stage(f"write derived.{name}"):
            writer.write(frame, schema.table(name), derived_keyspace, sidecar=sidecar)
    from pyspark.sql import functions as F

    with Stage("write derived.exchange_rates"):
        # The rates the derived tables were priced with, carried INTO the
        # keyspace that was priced with them. Declared by the schema and never
        # written until now, which left a v3 reader with nowhere to get a
        # block's rate except the v2 keyspace -- exactly the coupling a
        # self-contained v3 keyspace removes. Same rows, re-keyed by
        # (asset, block_id_group).
        from graphsense_v3.spark.columns import id_group

        writer.write(
            rates.select(
                F.col("asset"),
                id_group(F.col("block_id"), config.block_bucket_size).alias(
                    "block_id_group"
                ),
                F.col("block_id").cast("int").alias("block_id"),
                F.col("fiat_values"),
            ),
            schema.table("exchange_rates"),
            derived_keyspace,
            sidecar=sidecar,
        )
    if family is Family.ACCOUNT:
        with Stage("write derived.token_configuration"):
            # Curated, not derived -- but a reader needs the decimals and the
            # peg to interpret a token value at all, so it belongs here too.
            writer.write(
                read_cassandra(spark, rates_keyspace, "token_configuration"),
                schema.table("token_configuration"),
                derived_keyspace,
            )
    with Stage("write derived.configuration"):
        # The derived keyspace carries its OWN constants: a reader computing
        # address_bucket has to have them, and sending it to the raw keyspace
        # for them would make it need that keyspace's name too.
        from graphsense_v3.config import configuration_row

        writer.write(
            configuration_row(spark, config, derived_keyspace),
            schema.table("configuration"),
            derived_keyspace,
        )
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
