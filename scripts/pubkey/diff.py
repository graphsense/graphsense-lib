"""Diff the new pubkey_v2 lookup against the legacy pubkey table.

Correctness oracle for the cross-chain pubkey-update backfill: the legacy
``pubkey.pubkey_by_address`` table (written by an older script) is compared
row-by-row against the new ``pubkey_v2.pubkey_by_address`` table.

It reports a breakdown over the full outer join on ``address``:

    only_old  - address present in legacy, MISSING in new  -> potential regression
    only_new  - address present in new only                -> new coverage / bug
    match     - present in both, identical pubkey blob      -> agreement
    mismatch  - present in both, DIFFERENT pubkey blob      -> must investigate

and prints a few example addresses per bucket for spot-checking.

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

    try:
        old = load(args.old_keyspace).withColumnRenamed("pubkey", "pubkey_old")
        new = load(args.new_keyspace).withColumnRenamed("pubkey", "pubkey_new")

        joined = old.join(new, "address", "full_outer").withColumn(
            "bucket",
            F.when(F.col("pubkey_old").isNull(), F.lit("only_new"))
            .when(F.col("pubkey_new").isNull(), F.lit("only_old"))
            .when(F.col("pubkey_old") == F.col("pubkey_new"), F.lit("match"))
            .otherwise(F.lit("mismatch")),
        )
        joined = joined.cache()

        print("\n=== pubkey_v2 vs legacy pubkey: bucket counts ===")
        counts = {
            r["bucket"]: r["count"] for r in joined.groupBy("bucket").count().collect()
        }
        for bucket in ("only_old", "only_new", "match", "mismatch"):
            print(f"  {bucket:9s}: {counts.get(bucket, 0):,}")

        only_old = counts.get("only_old", 0)
        mismatch = counts.get("mismatch", 0)
        if only_old:
            print(
                f"\n  WARNING: {only_old:,} addresses are in legacy but missing "
                "from the new table (possible regression)."
            )
        if mismatch:
            print(
                f"\n  WARNING: {mismatch:,} addresses map to a DIFFERENT pubkey "
                "blob in the two tables (must investigate)."
            )

        for bucket in ("only_old", "mismatch", "only_new"):
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
