"""End-to-end backfill: Delta Lake -> v3 raw -> v3 transformed.

Built to be run once against a real cluster rather than iterated on, so it
reports what each stage cost and refuses early on anything it cannot finish.

    spark-submit --py-files ... -m graphsense_v3.spark.job \\
        --network btc --lake s3a://.../btc \\
        --raw-keyspace btc_raw_v3 --transformed-keyspace btc_transformed_v3 \\
        --rates-keyspace btc_raw_20260101

``--dry-run`` builds every frame and runs the conformance checks without
writing, which catches a column or key mismatch in seconds instead of hours in.
"""

# NOTE: no `from __future__ import annotations` -- pandas UDFs reach this module
# through the loaders it imports.

import argparse
import logging
import time
from typing import TYPE_CHECKING, Optional

from graphsense_v3.config import config_for
from graphsense_v3.schema import Kind, NETWORKS, Family, schema_for
from graphsense_v3.spark import raw_account, raw_utxo, transformed_utxo, writer
from graphsense_v3.spark.source import DeltaLake

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def exchange_rates_by_block(
    spark: "SparkSession", blocks: "DataFrame", rates: "DataFrame"
) -> "DataFrame":
    """``(block_id, fiat_values)`` from a date-keyed rate table.

    Rates are not in the lake -- the existing gslib ``exchange-rates`` path owns
    that table -- so they are read from a keyspace and joined onto blocks by
    date. A block whose date has no rate simply gets none, and the transform
    then contributes no fiat for it rather than a zero.
    """
    from pyspark.sql import functions as F

    from graphsense_v3.spark.columns import day_from_timestamp

    dated = blocks.select(
        F.col("block_id"),
        F.date_format(day_from_timestamp(F.col("timestamp")), "yyyy-MM-dd").alias(
            "date"
        ),
    )
    return dated.join(rates, on="date", how="left").select(
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


def run(
    spark: "SparkSession",
    network: str,
    lake_root: str,
    raw_keyspace: str,
    transformed_keyspace: Optional[str] = None,
    rates_keyspace: Optional[str] = None,
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    """Backfill one network.

    The transformed stage reads its inputs from the frames the raw stage just
    built rather than from Cassandra, so the lake is scanned once.
    """
    network = network.lower()
    family = NETWORKS[network]
    config = config_for(network)

    # Everything that can be decided from the arguments alone is decided here,
    # before a byte is read. A run that cannot finish should say so at submit
    # time, not after a full pass over the lake.
    if transformed_keyspace is not None:
        if family is not Family.UTXO:
            raise SystemExit(
                "the transformed stage is UTXO-only so far; run the raw stage "
                "for an account network and leave --transformed-keyspace unset"
            )
        if rates_keyspace is None:
            raise SystemExit("--rates-keyspace is required for the transformed stage")

    lake = DeltaLake(spark, lake_root, network)
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
            raw_keyspace,
            start_block=start_block,
            end_block=end_block,
            config=config,
        )
        for name, frame in raw_frames.items():
            writer.check(frame, raw_schema.table(name))

    if not dry_run:
        for name, frame in raw_frames.items():
            with Stage(f"write raw.{name}"):
                writer.write(frame, raw_schema.table(name), raw_keyspace)

    if transformed_keyspace is None or rates_keyspace is None:
        return  # already validated above; this narrows the types

    _run_transformed(
        spark,
        network,
        raw_frames,
        transformed_keyspace,
        rates_keyspace,
        config=config,
        dry_run=dry_run,
    )


def _run_transformed(
    spark: "SparkSession",
    network: str,
    raw_frames: dict,
    transformed_keyspace: str,
    rates_keyspace: str,
    *,
    config,
    dry_run: bool,
) -> None:
    """The transformed stage, from the frames the raw stage just built.

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

    schema = schema_for(network, Kind.TRANSFORMED)
    with Stage("build transformed frames"):
        frames = transformed_utxo.build(
            raw_frames["transaction_io"],
            raw_frames["transaction"],
            rates,
            network,
            config=config,
        )
        for name, frame in frames.items():
            writer.check(frame, schema.table(name))

    if dry_run:
        logger.info("dry run: %d transformed frames conform", len(frames))
        return

    for name, frame in frames.items():
        with Stage(f"write transformed.{name}"):
            writer.write(frame, schema.table(name), transformed_keyspace)


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", required=True, choices=sorted(NETWORKS))
    parser.add_argument("--lake", required=True, help="Delta Lake root for the network")
    parser.add_argument("--raw-keyspace", required=True)
    parser.add_argument("--transformed-keyspace")
    parser.add_argument("--rates-keyspace", help="keyspace holding exchange_rates")
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--cassandra", nargs="+", default=[], help="Cassandra contact points"
    )
    parser.add_argument("--cassandra-username")
    parser.add_argument("--cassandra-password")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    from graphsense_v3.spark.session import create_session

    spark = create_session(
        f"graphsense-v3-{args.network}",
        cassandra_nodes=args.cassandra,
        cassandra_username=args.cassandra_username,
        cassandra_password=args.cassandra_password,
    )
    try:
        run(
            spark,
            args.network,
            args.lake,
            args.raw_keyspace,
            args.transformed_keyspace,
            args.rates_keyspace,
            start_block=args.start_block,
            end_block=args.end_block,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
