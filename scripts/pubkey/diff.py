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

``missing_in_new`` is further split by each legacy pubkey's REAL source-network
count, read from the ``observed`` Delta table -- the same criterion the job
materialises on (``size(collect_set(network)) >= 2``). Requires ``--sink-path``:

    missing_single_source - pubkey seen on one source network -> EXPECTED (the
                            new job is cross-chain-only, so single-source pubkeys
                            are intentionally not reproduced)
    missing_absent        - pubkey never extracted on any chain -> extraction gap
    missing_cross_source  - pubkey seen on >=2 networks but not materialised ->
                            REAL regression (a detection bug)

NB: an earlier version inferred the network count from the address *prefix*, but
both legacy and the new job derive every pubkey to all chains, so a single-source
pubkey's multi-chain derived addresses made it look "multi-network". The observed
join uses the true source networks, so it stays exact even with
``--sample-fraction``.

and prints a few example addresses per warning bucket for spot-checking. Pass
``--sample-fraction`` to check a random sample of the legacy table for a faster
first pass (default 1.0 = full table, exact counts).

Reuses graphsenselib's Spark session so the Cassandra connector, auth and
spark profile match the job exactly. Run on the Spark driver host:

    uv run python scripts/pubkey/diff.py --env <env> \
        --old-keyspace pubkey --new-keyspace pubkey_v2 \
        --sink-path s3://<pubkey-sink> --s3-config <s3-config>

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
    parser.add_argument(
        "--sink-path",
        default=None,
        help=(
            "Delta base path of the pubkey sink; enables the real "
            "source-network split of missing_in_new (reads <sink-path>/observed)."
        ),
    )
    parser.add_argument(
        "--s3-config",
        dest="s3_config",
        default=None,
        help="s3_configs entry for S3/MinIO creds (required if sink-path is s3://).",
    )
    args = parser.parse_args()

    from pyspark.sql import functions as F

    from graphsenselib.config import get_config
    from graphsenselib.transformation.spark import create_spark_session

    config = get_config()
    env_config = config.get_environment(args.env)
    s3_credentials = (
        config.get_s3_credentials(args.s3_config) if args.s3_config else None
    )

    spark = create_spark_session(
        app_name=f"pubkey-v2-diff-{args.env}",
        local=False,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        s3_credentials=s3_credentials,
        spark_config=config.get_spark_config(),
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

        # Split missing_in_new by each legacy pubkey's REAL source-network count,
        # read from the observed Delta table (size(collect_set(network)) is the
        # exact criterion the job materialises on). Inferring it from the address
        # prefix overcounts, because every pubkey is derived to all chains.
        # `observed` is compressed-only but legacy stores a mix of compressed and
        # uncompressed blobs, so normalise the legacy key to compressed before the
        # join (else every uncompressed legacy key falsely lands in missing_absent).
        if missing and args.sink_path:
            from pyspark.sql.types import BinaryType

            from graphsenselib.pubkey.extract import _to_compressed

            @F.udf(returnType=BinaryType())
            def _compress(pk):
                return _to_compressed(bytes(pk)) if pk is not None else None

            base = args.sink_path.rstrip("/").replace("s3://", "s3a://")
            observed = spark.read.format("delta").load(f"{base}/observed")
            spans = observed.groupBy("pubkey").agg(
                F.size(F.collect_set("network")).alias("n_networks")
            )
            miss = joined.filter(F.col("bucket") == "missing_in_new").select(
                "address", _compress(F.col("pubkey_old")).alias("pubkey")
            )
            missing_bucketed = (
                miss.join(spans, "pubkey", "left")
                .withColumn(
                    "miss_bucket",
                    F.when(F.col("pubkey").isNull(), F.lit("missing_uncompressible"))
                    .when(F.col("n_networks").isNull(), F.lit("missing_absent"))
                    .when(F.col("n_networks") >= 2, F.lit("missing_cross_source"))
                    .otherwise(F.lit("missing_single_source")),
                )
                .cache()
            )
            mb = {
                r["miss_bucket"]: r["count"]
                for r in missing_bucketed.groupBy("miss_bucket").count().collect()
            }
            print("\n  missing_in_new split (by real observed source networks):")
            print(
                "    single-source (EXPECTED, cross-chain-only): "
                f"{mb.get('missing_single_source', 0):,}"
            )
            print(
                "    absent_from_observed (extraction gap)     : "
                f"{mb.get('missing_absent', 0):,}"
            )
            print(
                "    uncompressible legacy key (malformed)     : "
                f"{mb.get('missing_uncompressible', 0):,}"
            )
            print(
                "    cross-source  (REGRESSION, investigate)   : "
                f"{mb.get('missing_cross_source', 0):,}"
            )
            for bucket, title in (
                ("missing_cross_source", "cross-source (REAL regression)"),
                ("missing_absent", "absent from observed (extraction gap)"),
            ):
                if mb.get(bucket, 0):
                    print(f"\n--- examples: missing_in_new / {title} ---")
                    (
                        missing_bucketed.filter(F.col("miss_bucket") == bucket)
                        .select("address", "n_networks", "pubkey")
                        .limit(args.examples)
                        .show(truncate=80, vertical=False)
                    )
        elif missing:
            print(
                "\n  missing_in_new split skipped: pass --sink-path <delta sink> "
                "(and --s3-config if on S3) to classify the missing rows by real "
                "observed source-network count."
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
