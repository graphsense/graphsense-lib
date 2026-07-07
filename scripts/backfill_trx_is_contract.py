#!/usr/bin/env python
"""Backfill the ``is_contract`` flag on TRON (TRX) transformed addresses.

Why this exists
---------------
TRON contract detection used to recognise a contract only when it was deployed
by a top-level ``CreateSmartContract`` transaction (signalled by
``receipt_contract_address`` on the tx). Contracts deployed by a *factory*
contract -- where the top-level tx is a ``TriggerSmartContract`` and the new
contract appears only as an internal ``create`` trace -- were never flagged
(e.g. the PEPE TRC20 token ``TMacq4TDUw5q8NFBwmbY4RLXvzvG5JTkvi``).

The Spark transform (``graphsense-spark`` ``computeContracts``) already unions
both signals; the incremental Python delta-updater was fixed to match. This
script repairs already-ingested keyspaces *in place* without a full
re-transform: it recomputes the complete contract-address set from the raw
keyspace and flips ``is_contract = true`` on the matching rows of the
transformed ``address`` table. Only that one column is written (a partial CQL
update); all value / tx-count / balance columns are left untouched.

It is idempotent and only writes rows whose flag is not already ``true``.

Contract address sources (mirrors Spark ``computeContracts`` for TRX):
  * raw ``trace``        : ``note = 'create'`` and ``rejected = false``
                           -> ``transferto_address`` (factory / internal CREATE)
  * raw ``transaction``  : ``receipt_status = 1`` and
                           ``receipt_contract_address`` is not null
                           -> ``receipt_contract_address`` (top-level deploy)

Matching is an exact join on the raw 21-byte (0x41-prefixed) address blob
against the transformed ``address.address`` column, so it needs no knowledge of
the address-prefix or bucket-size layout.

Connection / Spark details are read from ``graphsense.yaml`` (same convention as
the scripts/pubkey/* jobs): ``--env`` selects the environment (Cassandra nodes +
credentials and, with ``--currency``, the raw/transformed keyspace names) and
``--spark-profile`` selects a ``spark_config`` profile.

Safety
------
The script is dry-run by default and writes nothing until ``--write`` is given.
It only ever sets ``is_contract = true`` (it can never blank a field or un-flag
a contract), it only touches rows that already exist in the ``address`` table
(inner join), and it writes only the primary key + ``is_contract`` (a partial
CQL upsert -- every other column is left untouched). The address match is an
exact byte join, so the only failure mode is *under*-matching (writing nothing),
never mis-flagging a different address.

Example (run in the graphsense-lib image, like scripts/pubkey/*):

    docker run --rm --network host \
      -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
      -v /path/to/graphsense.yaml:/graphsense.yaml:ro \
      -v $PWD/scripts/backfill_trx_is_contract.py:/backfill_trx_is_contract.py:ro \
      -v gs-backfill-ivy:/root/.ivy2 \
      ghcr.io/graphsense/graphsense-lib:<tag> \
      python /backfill_trx_is_contract.py --env <env> --spark-profile <profile>

That is a dry run. Add ``--write`` to actually apply. Reads the full
(column-pruned) ``trace``, ``transaction`` and ``address`` tables once, so run
it off-peak.
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger("backfill_trx_is_contract")

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill is_contract on TRX transformed addresses.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--env", required=True, help="graphsense.yaml environment.")
    p.add_argument(
        "--currency",
        default="trx",
        help="Currency key in the environment (only 'trx' is supported).",
    )
    p.add_argument(
        "--raw-keyspace",
        default=None,
        help="Override the raw keyspace (default: from env/currency).",
    )
    p.add_argument(
        "--transformed-keyspace",
        default=None,
        help="Override the transformed keyspace (default: from env/currency).",
    )
    p.add_argument(
        "--spark-profile",
        default="",
        help=(
            "spark_config profile to use (nested baseline+profiles form). "
            "Empty (default) uses the default/baseline spark config."
        ),
    )
    p.add_argument(
        "--min-block",
        type=int,
        default=None,
        help="Only scan raw rows with block_id >= this (full scan if unset).",
    )
    p.add_argument(
        "--max-block",
        type=int,
        default=None,
        help="Only scan raw rows with block_id <= this (full scan if unset).",
    )
    p.add_argument(
        "--write",
        action="store_true",
        help=(
            "Actually write the updates. Without this flag the script runs in "
            "dry-run mode (the safe default): it computes and reports counts but "
            "writes nothing."
        ),
    )
    p.add_argument("--local", action="store_true", help="Run Spark in local[*] mode.")
    return p.parse_args(argv)


def build_contract_addresses(spark, args):
    """Return a DataFrame with a single distinct ``address`` (binary) column."""
    from pyspark.sql import functions as F

    def read(table: str):
        df = (
            spark.read.format(CASSANDRA_FORMAT)
            .options(keyspace=args.raw_keyspace, table=table)
            .load()
        )
        if args.min_block is not None:
            df = df.filter(F.col("block_id") >= args.min_block)
        if args.max_block is not None:
            df = df.filter(F.col("block_id") <= args.max_block)
        return df

    # Factory / internal CREATE deployments live only in traces.
    trace_deploys = (
        read("trace")
        .filter((F.col("note") == "create") & (F.col("rejected") == F.lit(False)))
        .filter(F.col("transferto_address").isNotNull())
        .select(F.col("transferto_address").alias("address"))
    )

    # Top-level CreateSmartContract deployments carry a receipt contract address.
    tx_deploys = (
        read("transaction")
        .filter(
            (F.col("receipt_status") == 1)
            & F.col("receipt_contract_address").isNotNull()
        )
        .select(F.col("receipt_contract_address").alias("address"))
    )

    return trace_deploys.union(tx_deploys).distinct()


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.currency != "trx":
        raise SystemExit("This backfill only supports --currency trx.")

    from pyspark.sql import functions as F

    from graphsenselib.config import get_config
    from graphsenselib.transformation.spark import create_spark_session

    # Resolve connection / Spark details from graphsense.yaml (pubkey convention).
    config = get_config()
    env_config = config.get_environment(args.env)
    keyspace = config.get_keyspace_config(args.env, args.currency)
    if args.raw_keyspace is None:
        args.raw_keyspace = keyspace.raw_keyspace_name
    if args.transformed_keyspace is None:
        args.transformed_keyspace = keyspace.transformed_keyspace_name

    # Empty profile -> default/baseline spark config (mirrors scripts/pubkey/*).
    spark_config = (
        config.get_spark_config(args.spark_profile)
        if args.spark_profile
        else config.get_spark_config()
    )
    # Defensive: never write a null/tombstone for any column. Our write only
    # ever sets is_contract=true on existing rows and never carries a null, but
    # this guarantees a null can never blank out a column even if the job changes.
    spark_config = {**spark_config, "spark.cassandra.output.ignoreNulls": "true"}

    logger.info(
        "env=%s currency=%s raw=%s transformed=%s nodes=%s",
        args.env,
        args.currency,
        args.raw_keyspace,
        args.transformed_keyspace,
        env_config.cassandra_nodes,
    )

    spark = create_spark_session(
        app_name=f"backfill-trx-is-contract-{args.env}",
        local=args.local,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        spark_config=spark_config,
        spark_packages=config.get_spark_packages(),
    )

    try:
        contract_addresses = build_contract_addresses(spark, args).cache()
        n_contracts = contract_addresses.count()
        logger.info("Distinct contract addresses found in raw: %d", n_contracts)

        addresses = (
            spark.read.format(CASSANDRA_FORMAT)
            .options(keyspace=args.transformed_keyspace, table="address")
            .load()
            .select("address_id_group", "address_id", "address", "is_contract")
        )

        # Inner-join keeps only contract addresses that exist in the transformed
        # keyspace; the filter skips rows already flagged (idempotent, fewer
        # writes). is_contract may be null on legacy rows -> treat as not set.
        to_update = addresses.join(
            contract_addresses, on="address", how="inner"
        ).filter(F.col("is_contract").isNull() | (F.col("is_contract") == F.lit(False)))

        updates = to_update.select(
            "address_id_group", "address_id", F.lit(True).alias("is_contract")
        ).cache()

        # Hard safety guard: the write must carry ONLY the primary key plus
        # is_contract. Any extra column here would be written to the address
        # table and could overwrite real data. Fail loudly rather than risk it.
        expected_cols = ["address_id_group", "address_id", "is_contract"]
        if updates.columns != expected_cols:
            raise SystemExit(
                f"Refusing to proceed: write columns {updates.columns} != "
                f"expected {expected_cols}."
            )

        n_updates = updates.count()
        logger.info(
            "Transformed addresses needing is_contract=true: %d "
            "(already-true rows skipped)",
            n_updates,
        )

        if not args.write:
            logger.info(
                "DRY RUN (default): writing nothing. Pass --write to apply. "
                "Sample of pending updates:"
            )
            updates.show(20, truncate=False)
            return 0

        if n_updates == 0:
            logger.info("Nothing to update. Done.")
            return 0

        # Partial column write: only the primary key + is_contract are present,
        # so the connector issues a CQL upsert (INSERT of just these columns)
        # that sets is_contract and leaves every other column on the row
        # untouched. mode MUST be "append" -- "overwrite" would TRUNCATE the
        # whole address table first. Never change this to overwrite.
        logger.info(
            "Writing is_contract=true to %s.address for %d rows ...",
            args.transformed_keyspace,
            n_updates,
        )
        (
            updates.write.format(CASSANDRA_FORMAT)
            .options(keyspace=args.transformed_keyspace, table="address")
            .mode("append")
            .save()
        )
        logger.info("Wrote is_contract=true for %d addresses.", n_updates)
        return 0
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
