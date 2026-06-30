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

Example
-------
    uv run python scripts/backfill_trx_is_contract.py \
        --raw-keyspace trx_raw \
        --transformed-keyspace trx_transformed \
        --cassandra-host db1:9042,db2:9042 \
        --dry-run

Drop ``--dry-run`` to actually write. Reads the full (column-pruned) ``trace``,
``transaction`` and ``address`` tables once, so run it off-peak.
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
    p.add_argument("--raw-keyspace", required=True, help="TRX raw keyspace.")
    p.add_argument(
        "--transformed-keyspace", required=True, help="TRX transformed keyspace."
    )
    p.add_argument(
        "--cassandra-host",
        default="localhost:9042",
        help="Comma-separated host:port list.",
    )
    p.add_argument("--username", default=None, help="Cassandra username (optional).")
    p.add_argument("--password", default=None, help="Cassandra password (optional).")
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
        "--dry-run",
        action="store_true",
        help="Compute and report counts but do not write any updates.",
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

    from pyspark.sql import functions as F

    from graphsenselib.transformation.spark import create_spark_session

    nodes = [h.strip() for h in args.cassandra_host.split(",") if h.strip()]
    spark = create_spark_session(
        app_name=f"backfill-trx-is-contract-{args.transformed_keyspace}",
        local=args.local,
        cassandra_nodes=nodes,
        cassandra_username=args.username,
        cassandra_password=args.password,
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
        n_updates = updates.count()
        logger.info(
            "Transformed addresses needing is_contract=true: %d "
            "(already-true rows skipped)",
            n_updates,
        )

        if args.dry_run:
            logger.info("--dry-run set: not writing. Sample of pending updates:")
            updates.show(20, truncate=False)
            return 0

        if n_updates == 0:
            logger.info("Nothing to update. Done.")
            return 0

        # Partial column write: only the primary key + is_contract are present,
        # so the connector issues a CQL upsert that touches just is_contract and
        # leaves every other column on the row untouched.
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
