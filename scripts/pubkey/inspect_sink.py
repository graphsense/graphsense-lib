"""Inspect the cross-chain pubkey Delta sink: which chains ran, and how deep.

Quick operational check for a pubkey-update / pubkey-detect backfill. Reads the
shared Delta tables under ``--sink-path`` (no Cassandra) and prints:

  * ``state``    - last_processed_block per source network (which chains ran)
  * ``observed`` - row count per source network (extraction coverage)

Example (same image/mounts as diff.py):

    docker run --rm --network host \
      -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
      -v /path/to/graphsense.yaml:/graphsense.yaml:ro \
      -v $PWD/scripts/pubkey/inspect_sink.py:/inspect_sink.py:ro \
      ghcr.io/graphsense/graphsense-lib:<tag> \
      python /inspect_sink.py --env <env> --sink-path s3://<pubkey-sink> --s3-config <s3-config>
"""

# Ops report script: print() is the intended human-facing output channel.
# ruff: noqa: T201

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, help="graphsense.yaml environment")
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
        "--spark-profile",
        default="pubkey",
        help=(
            "spark_config profile to use (nested baseline+profiles form); "
            "defaults to 'pubkey'. Falls back to the default config if the "
            "profile is absent or spark_config is flat. Pass '' for the default."
        ),
    )
    args = parser.parse_args()

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
        app_name=f"pubkey-inspect-{args.env}",
        local=False,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        s3_credentials=s3_credentials,
        spark_config=spark_config,
    )
    try:
        print("\n=== state (last_processed_block per network) ===")
        spark.read.format("delta").load(f"{base}/state").orderBy("network").show(
            100, truncate=False
        )

        print("\n=== observed rows per source network ===")
        observed = spark.read.format("delta").load(f"{base}/observed")
        observed.groupBy("network").count().orderBy("network").show(100, truncate=False)

        print("\n=== observed: distinct pubkeys, and pubkeys seen on >= 2 networks ===")
        from pyspark.sql import functions as F

        spans = observed.groupBy("pubkey").agg(
            F.size(F.collect_set("network")).alias("n_networks")
        )
        total = spans.count()
        cross = spans.filter(F.col("n_networks") >= 2).count()
        print(f"  distinct pubkeys in observed : {total:,}")
        print(f"  cross-chain (>=2 networks)   : {cross:,}")
        print(f"  single-network               : {total - cross:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
