"""Diff the new pubkey_v2 lookup against the legacy pubkey table.

Correctness oracle for the cross-chain pubkey-update backfill: the legacy
``pubkey.pubkey_by_address`` table (written by an older script) is compared
row-by-row against the new ``pubkey_v2.pubkey_by_address`` table.

It checks ONE direction — every legacy row must still exist (and match) in the
new table (old ⊆ new). The reverse (rows only in new) is NOT a failure: the new
job may cover a wider block range than the old script, so new-only rows are
expected and are deliberately not enumerated. Buckets over a left join
legacy -> new on ``address``:

    match           - legacy address present in new, identical pubkey -> agreement
    mismatch        - present in new but DIFFERENT pubkey blob         -> investigate
    missing_in_new  - legacy address absent from new                  -> see split

``missing_in_new`` is further split by how many distinct chains the legacy
pubkey spans (over the legacy table):

    missing_single_network - pubkey seen on one chain  -> EXPECTED (the new job
                             is cross-chain-only, so single-network pubkeys are
                             intentionally not reproduced)
    missing_multi_network  - pubkey seen on >=2 chains -> REAL regression (e.g. a
                             pubkey legacy only saw inside a multisig/P2PK script)

and prints a few example addresses per warning bucket for spot-checking. Pass
``--sample-fraction`` to check a random sample of the legacy table for a faster
first pass (default 1.0 = full table, exact counts).

Reuses graphsenselib's Spark session so the Cassandra connector, auth and
spark profile match the job exactly. Run on the Spark driver host:

    uv run python scripts/pubkey/diff.py --env prod \
        --old-keyspace pubkey --new-keyspace pubkey_v2

Read-only: it never writes to Cassandra.
"""

# Ops report script: print() is the intended human-facing output channel.
# ruff: noqa: T201

import argparse
import logging


def classify_chain(address: str) -> str:
    """Coarse source chain of a legacy address, for splitting missing_in_new.

    Used only to decide whether a missing legacy pubkey was single-network
    (expected: the new job is cross-chain-only) or multi-network (a real
    regression). BTC and BCH legacy base58 ('1'/'3') are indistinguishable by
    address, so they fold into one 'btc/bch' chain — a pubkey seen only on
    btc+bch reads as single-chain here, which is acceptable for a diagnostic.
    """
    if not address:
        return "unknown"
    a = address
    if a.startswith("0x"):
        return "evm"
    if a.startswith("T"):
        return "trx"
    if a.startswith("ltc1") or a[0] in ("L", "M"):
        return "ltc"
    if a.startswith("bitcoincash:"):
        return "bch"
    if a.startswith("t1") or a.startswith("t3"):
        return "zec"
    if a[0] in ("D", "A", "9"):
        return "doge"
    if a.startswith("bc1") or a[0] in ("1", "3"):
        return "btc/bch"
    return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, help="graphsense.yaml environment")
    parser.add_argument("--old-keyspace", default="pubkey", help="legacy keyspace")
    parser.add_argument("--new-keyspace", default="pubkey_v2", help="new keyspace")
    parser.add_argument("--table", default="pubkey_by_address")
    parser.add_argument(
        "--examples", type=int, default=10, help="example rows to show per bucket"
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="sample this fraction of the LEGACY table (0<f<=1; default 1.0=full)",
    )
    args = parser.parse_args()

    from pyspark.sql import functions as F

    from graphsenselib.config import get_config
    from graphsenselib.transformation.spark import create_spark_session

    config = get_config()
    env_config = config.get_environment(args.env)

    spark = create_spark_session(
        app_name=f"pubkey-v2-diff-{args.env}",
        local=False,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        spark_config=config.spark_config or {},
    )

    def load(keyspace: str):
        return (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=args.table, keyspace=keyspace)
            .load()
            .select("address", "pubkey")
        )

    if not (0.0 < args.sample_fraction <= 1.0):
        raise SystemExit("--sample-fraction must be in (0, 1].")

    try:
        old = load(args.old_keyspace).withColumnRenamed("pubkey", "pubkey_old")
        if args.sample_fraction < 1.0:
            old = old.sample(fraction=args.sample_fraction, seed=42)
        new = load(args.new_keyspace).withColumnRenamed("pubkey", "pubkey_new")

        # One direction only: does every legacy row still exist (and match) in
        # the new table? A left join keeps exactly the legacy rows; new-only
        # rows are never enumerated (expected when new covers a wider range).
        joined = old.join(new, "address", "left").withColumn(
            "bucket",
            F.when(F.col("pubkey_new").isNull(), F.lit("missing_in_new"))
            .when(F.col("pubkey_old") == F.col("pubkey_new"), F.lit("match"))
            .otherwise(F.lit("mismatch")),
        )
        joined = joined.cache()

        scope = (
            "full"
            if args.sample_fraction >= 1.0
            else f"{args.sample_fraction:.4g} sample"
        )
        print(
            f"\n=== legacy '{args.old_keyspace}' ⊆ '{args.new_keyspace}'? ({scope}) ==="
        )
        counts = {
            r["bucket"]: r["count"] for r in joined.groupBy("bucket").count().collect()
        }
        print(f"  legacy rows checked : {sum(counts.values()):,}")
        for bucket in ("match", "mismatch", "missing_in_new"):
            print(f"  {bucket:15s}: {counts.get(bucket, 0):,}")

        missing = counts.get("missing_in_new", 0)
        mismatch = counts.get("mismatch", 0)
        if missing:
            print(
                f"\n  WARNING: {missing:,} legacy addresses are MISSING from "
                f"'{args.new_keyspace}' (regression)."
            )
        if mismatch:
            print(
                f"\n  WARNING: {mismatch:,} legacy addresses map to a DIFFERENT "
                "pubkey blob in the new table (must investigate)."
            )
        if not missing and not mismatch:
            print("\n  OK: every legacy row is present and matching in the new table.")

        # Split missing_in_new by how many distinct chains the legacy pubkey
        # spans: single-network misses are EXPECTED (the new job is
        # cross-chain-only); multi-network misses are real regressions (e.g. a
        # pubkey legacy only ever saw inside a multisig/P2PK script).
        if missing:
            from pyspark.sql.types import StringType

            chain_udf = F.udf(classify_chain, StringType())
            # Per-pubkey network span over the (possibly sampled) legacy table.
            pk_networks = (
                old.withColumn("chain", chain_udf(F.col("address")))
                .groupBy("pubkey_old")
                .agg(F.countDistinct("chain").alias("n_networks"))
            )
            missing_bucketed = (
                joined.filter(F.col("bucket") == "missing_in_new")
                .select("address", "pubkey_old")
                .join(pk_networks, "pubkey_old", "left")
                .withColumn(
                    "miss_bucket",
                    F.when(
                        F.col("n_networks") >= 2, F.lit("missing_multi_network")
                    ).otherwise(F.lit("missing_single_network")),
                )
                .cache()
            )
            mb = {
                r["miss_bucket"]: r["count"]
                for r in missing_bucketed.groupBy("miss_bucket").count().collect()
            }
            single = mb.get("missing_single_network", 0)
            multi = mb.get("missing_multi_network", 0)
            print("\n  missing_in_new split:")
            print(f"    single-network (EXPECTED, cross-chain-only): {single:,}")
            print(f"    multi-network  (REGRESSION, investigate)   : {multi:,}")
            if args.sample_fraction < 1.0:
                print(
                    "    NOTE: sampled run — n_networks is a lower bound, so the "
                    "multi-network (regression) count is reliable but the "
                    "single-network count may be inflated."
                )
            if multi:
                print("\n--- examples: missing_in_new / multi-network (regression) ---")
                (
                    missing_bucketed.filter(
                        F.col("miss_bucket") == "missing_multi_network"
                    )
                    .select("address", "pubkey_old", "n_networks")
                    .limit(args.examples)
                    .show(truncate=80, vertical=False)
                )

        for bucket in ("missing_in_new", "mismatch"):
            if counts.get(bucket, 0):
                print(f"\n--- examples: {bucket} ---")
                (
                    joined.filter(F.col("bucket") == bucket)
                    .select("address", "pubkey_old", "pubkey_new")
                    .limit(args.examples)
                    .show(truncate=80, vertical=False)
                )
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
