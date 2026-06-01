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
    missing_in_new  - legacy address absent from new                  -> regression

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
