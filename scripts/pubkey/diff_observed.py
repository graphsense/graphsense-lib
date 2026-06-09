"""Definitive regression check: classify legacy-missing pubkeys by their REAL
source-network count from the ``observed`` Delta table.

``diff.py`` splits ``missing_in_new`` using ``classify_chain``, which infers a
chain from the *address prefix*. But both legacy and the new pipeline derive a
pubkey to addresses on every DERIVATION_CHAIN, so a pubkey *seen on one source
chain* still has multi-chain derived addresses and gets mislabelled
"multi-network". This script instead joins the missing legacy pubkeys against
``observed`` and counts the distinct source ``network`` values each was actually
seen signing on -- the same criterion the materialisation uses
(``size(collect_set(network)) >= 2``).

The legacy key is normalised to compressed (``_to_compressed``) before the join,
because ``observed`` is compressed-only while legacy stores a mix of compressed
and uncompressed blobs (joining raw would mislabel every uncompressed key absent).

Buckets of missing_in_new:
    single_source             - seen on exactly 1 network (EXPECTED: cross-chain-only)
    absent_from_observed      - compressed key never extracted on any chain (true gap)
    uncompressible_legacy_key - legacy blob can't be compressed (malformed/off-curve)
    cross_source (>=2)        - seen on >=2 networks but NOT materialised (REGRESSION)

Example (same image/mounts as diff.py):

    docker run --rm --network host \
      -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
      -v /path/to/graphsense.yaml:/graphsense.yaml:ro \
      -v $PWD/scripts/pubkey/diff_observed.py:/diff_observed.py:ro \
      ghcr.io/graphsense/graphsense-lib:<tag> \
      python /diff_observed.py --env <env> \
        --old-keyspace pubkey --new-keyspace pubkey_v2 \
        --sink-path s3://<pubkey-sink> --s3-config <s3-config>
"""

# Ops report script: print() is the intended human-facing output channel.
# ruff: noqa: T201

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, help="graphsense.yaml environment")
    parser.add_argument("--old-keyspace", default="pubkey", help="legacy keyspace")
    parser.add_argument("--new-keyspace", default="pubkey_v2", help="new keyspace")
    parser.add_argument("--table", default="pubkey_by_address")
    parser.add_argument(
        "--sink-path", required=True, help="Delta base path of the pubkey sink"
    )
    parser.add_argument(
        "--s3-config",
        dest="s3_config",
        default=None,
        help="s3_configs entry for S3/MinIO creds (required if sink-path is s3://)",
    )
    parser.add_argument(
        "--examples", type=int, default=20, help="example rows to show per bucket"
    )
    parser.add_argument(
        "--spark-profile",
        default="pubkey",
        help=(
            "spark_config profile to use (nested baseline+profiles form); "
            "defaults to 'pubkey'. Falls back to the default config if the "
            "profile is absent or spark_config is flat. Pass '' for the default."
        ),
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
    base = args.sink_path.rstrip("/").replace("s3://", "s3a://")

    # Spark profile (default 'pubkey'); fall back to the default/baseline config
    # when the profile is absent or spark_config is in flat (legacy) form.
    if args.spark_profile:
        try:
            spark_config = config.get_spark_config(args.spark_profile)
        except ValueError as exc:
            print(
                f"NOTE: spark profile {args.spark_profile!r} unavailable "
                f"({exc}); using the default spark config."
            )
            spark_config = config.get_spark_config()
    else:
        spark_config = config.get_spark_config()

    spark = create_spark_session(
        app_name=f"pubkey-diff-observed-{args.env}",
        local=False,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        s3_credentials=s3_credentials,
        spark_config=spark_config,
    )

    def load(keyspace: str):
        return (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=args.table, keyspace=keyspace)
            .load()
            .select("address", "pubkey")
        )

    # `observed` stores COMPRESSED keys only, but legacy `pubkey_by_address`
    # stores a mix of compressed and uncompressed blobs. Joining on the raw blob
    # would mislabel every uncompressed legacy key as absent. Normalise the
    # legacy key to compressed first so the join compares like-for-like.
    from pyspark.sql.types import BinaryType

    from graphsenselib.pubkey.extract import _to_compressed

    @F.udf(returnType=BinaryType())
    def compress(pk):
        if pk is None:
            return None
        return _to_compressed(bytes(pk))

    try:
        old = load(args.old_keyspace)
        new = load(args.new_keyspace).select("address")
        # Legacy addresses absent from the new table, with their legacy pubkey
        # normalised to compressed form for the observed join.
        missing = old.join(new, "address", "left_anti").withColumn(
            "pubkey_c", compress(F.col("pubkey"))
        )

        # Real per-pubkey source-network span from observed (compressed keys).
        observed = spark.read.format("delta").load(f"{base}/observed")
        spans = observed.groupBy("pubkey").agg(
            F.size(F.collect_set("network")).alias("n_networks")
        )

        joined = missing.join(spans, missing["pubkey_c"] == spans["pubkey"], "left")
        bucketed = (
            joined.select(
                missing["address"].alias("address"),
                missing["pubkey"].alias("pubkey"),
                F.col("pubkey_c"),
                F.col("n_networks"),
            )
            .withColumn(
                "miss_bucket",
                # legacy blob that can't be compressed at all (malformed/off-curve)
                # could never be in observed; report it apart from real gaps.
                F.when(F.col("pubkey_c").isNull(), F.lit("uncompressible_legacy_key"))
                .when(F.col("n_networks").isNull(), F.lit("absent_from_observed"))
                .when(F.col("n_networks") >= 2, F.lit("cross_source_REGRESSION"))
                .otherwise(F.lit("single_source_expected")),
            )
            .cache()
        )

        print("\n=== missing_in_new by REAL observed source-network count ===")
        print("    (legacy key normalised to compressed before the observed join)")
        counts = {
            r["miss_bucket"]: r["count"]
            for r in bucketed.groupBy("miss_bucket").count().collect()
        }
        total = sum(counts.values())
        print(f"  missing legacy addresses checked : {total:,}")
        for b in (
            "single_source_expected",
            "absent_from_observed",
            "uncompressible_legacy_key",
            "cross_source_REGRESSION",
        ):
            print(f"  {b:28s}: {counts.get(b, 0):,}")

        # The buckets above count ADDRESSES, but legacy derives one pubkey to an
        # address on every chain, so a single absent pubkey inflates the address
        # count ~10x. Report the DISTINCT absent pubkeys to size the real gap.
        if counts.get("absent_from_observed", 0):
            distinct_absent = (
                bucketed.filter(F.col("miss_bucket") == "absent_from_observed")
                .select("pubkey_c")
                .distinct()
                .count()
            )
            print(
                f"\n  absent_from_observed = {distinct_absent:,} DISTINCT pubkeys "
                f"(across {counts['absent_from_observed']:,} derived addresses)"
            )

        if counts.get("cross_source_REGRESSION", 0):
            print("\n--- examples: cross_source (>=2 networks, REAL regression) ---")
            (
                bucketed.filter(F.col("miss_bucket") == "cross_source_REGRESSION")
                .select("address", "n_networks", "pubkey")
                .limit(args.examples)
                .show(truncate=80, vertical=False)
            )
        if counts.get("absent_from_observed", 0):
            print(
                "\n--- examples: absent_from_observed (compressed key never extracted) ---"
            )
            (
                bucketed.filter(F.col("miss_bucket") == "absent_from_observed")
                .select("address", "pubkey", "pubkey_c")
                .limit(args.examples)
                .show(truncate=80, vertical=False)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
