"""Decisive probe for the ``absent_from_observed`` gap reported by
``diff_observed.py``: are those legacy pubkeys physically present on-chain in a
UTXO source we DO extract (btc/bch/ltc/zec), or are they simply not in our
extracted data at all?

For each absent compressed pubkey we take its 32-byte X-coordinate
(``pubkey_c[1:33]``) -- shared by the compressed (``02/03||X``) and uncompressed
(``04||X||Y``) on-chain encodings -- and substring-search it (hex) against the
raw ``transaction`` Delta of each UTXO chain: every input ``script_hex``, every
``txinwitness`` element, and every output ``script_hex``. This bypasses the
extractor's script parsing entirely, so it distinguishes two very different
causes:

    on_chain_FOUND  - the X-coord appears in a script we ingested but the
                      extractor never produced the key  -> PARSER GAP (a bug)
    not_in_utxo     - the X-coord appears in NO extracted UTXO script         ->
                      the key came from a source we don't extract (doge is a
                      DERIVATION_CHAIN but NOT an extraction source; or an
                      account-only key; or legacy had a wider source)

Sampled (``--sample``) because it is a full scan of the raw transaction tables.
Scope it to one chain with ``--chains`` to keep a single run cheap.

Each chain's raw ``transaction`` Delta is resolved from the graphsense config
(``keyspaces.<chain>.ingest_config.raw_keyspace_file_sinks["delta"]``), exactly
as ``transformation pubkey-update`` resolves its ``--source-path``, so no path
needs to be passed. Override a single chain with ``--source-path`` if needed.

    docker run --rm --network host \
      -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
      -v /path/to/graphsense.yaml:/graphsense.yaml:ro \
      -v $PWD/scripts/pubkey/probe_absent_onchain.py:/probe.py:ro \
      ghcr.io/graphsense/graphsense-lib:<tag> \
      python /probe.py --env <env> \
        --old-keyspace pubkey --new-keyspace pubkey_v2 \
        --sink-path s3://<pubkey-sink> --s3-config <s3-config> \
        --chains btc ltc zec bch --sample 300
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
        "--sink-path", required=True, help="Delta base path of the pubkey sink"
    )
    parser.add_argument("--s3-config", dest="s3_config", default=None)
    parser.add_argument(
        "--source-path",
        default=None,
        help=(
            "override the ingested-delta dir (reads <dir>/transaction); only "
            "valid with a single --chains entry. Default: resolve per chain "
            "from the config, like pubkey-update does."
        ),
    )
    parser.add_argument(
        "--chains",
        nargs="+",
        default=["btc", "ltc", "zec", "bch"],
        help="UTXO source chains to scan",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=300,
        help="number of distinct absent pubkeys to probe (full-scan per chain)",
    )
    parser.add_argument("--examples", type=int, default=20)
    args = parser.parse_args()

    from pyspark.sql import functions as F
    from pyspark.sql.types import BinaryType

    from graphsenselib.config import get_config
    from graphsenselib.pubkey.extract import _to_compressed
    from graphsenselib.transformation.spark import create_spark_session

    config = get_config()
    env_config = config.get_environment(args.env)
    s3_credentials = (
        config.get_s3_credentials(args.s3_config) if args.s3_config else None
    )
    sink = args.sink_path.rstrip("/").replace("s3://", "s3a://")

    if args.source_path and len(args.chains) != 1:
        raise SystemExit("--source-path is only valid with exactly one --chains entry.")

    def source_dir(chain: str) -> str:
        """Resolve a chain's ingested-delta dir like pubkey-update does."""
        if args.source_path:
            path = args.source_path
        else:
            ks = config.get_keyspace_config(args.env, chain)
            sink_cfg = (
                ks.ingest_config.raw_keyspace_file_sinks.get("delta")
                if ks.ingest_config
                else None
            )
            if sink_cfg is None:
                raise SystemExit(
                    f"No delta source sink configured for {chain} in env {args.env}."
                )
            path = sink_cfg.directory
        return path.rstrip("/").replace("s3://", "s3a://")

    spark = create_spark_session(
        app_name=f"pubkey-probe-absent-{args.env}",
        local=False,
        cassandra_nodes=env_config.cassandra_nodes,
        cassandra_username=env_config.username,
        cassandra_password=env_config.password,
        s3_credentials=s3_credentials,
        spark_config=config.spark_config or {},
    )

    @F.udf(returnType=BinaryType())
    def compress(pk):
        return _to_compressed(bytes(pk)) if pk is not None else None

    def load(keyspace: str):
        return (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=args.table, keyspace=keyspace)
            .load()
            .select("address", "pubkey")
        )

    try:
        # Rebuild the absent_from_observed set exactly as diff_observed does:
        # legacy addresses absent from new, whose compressed key is not in observed.
        old = load(args.old_keyspace)
        new = load(args.new_keyspace).select("address")
        missing = (
            old.join(new, "address", "left_anti")
            .withColumn("pubkey_c", compress(F.col("pubkey")))
            .filter(F.col("pubkey_c").isNotNull())
        )
        observed = spark.read.format("delta").load(f"{sink}/observed")
        observed_keys = observed.select("pubkey").distinct()
        absent = (
            missing.join(
                observed_keys,
                missing["pubkey_c"] == observed_keys["pubkey"],
                "left_anti",
            )
            .select("pubkey_c")
            .distinct()
        )

        # Sample, then index by the 32-byte X-coordinate hex (lowercase),
        # shared by compressed/uncompressed on-chain encodings.
        sampled = absent.limit(args.sample).collect()
        needles = {bytes(r["pubkey_c"])[1:33].hex() for r in sampled}
        print(f"\n=== probing {len(needles):,} distinct absent pubkeys on-chain ===")
        print(f"    chains scanned: {', '.join(args.chains)}")
        needles_bc = spark.sparkContext.broadcast(needles)

        from pyspark.sql.types import ArrayType, StringType

        @F.udf(returnType=ArrayType(StringType()))
        def matched_xcoords(input_scripts, witnesses, output_scripts):
            """Return the X-coord hexes from the needle set found in this tx."""
            found = []
            ndl = needles_bc.value
            blobs = []
            for s in input_scripts or []:
                if s:
                    blobs.append(s)
            for w in witnesses or []:
                for elem in w or []:
                    if elem:
                        blobs.append(elem)
            for s in output_scripts or []:
                if s:
                    blobs.append(s)
            joined = " ".join(blobs).lower()
            for x in ndl:
                if x in joined:
                    found.append(x)
            return found

        all_found: set[str] = set()
        for chain in args.chains:
            path = source_dir(chain)
            tx = spark.read.format("delta").load(f"{path}/transaction")
            in_scripts = F.transform(F.col("inputs"), lambda i: i["script_hex"])
            in_wit = F.transform(F.col("inputs"), lambda i: i["txinwitness"])
            out_scripts = F.transform(F.col("outputs"), lambda o: o["script_hex"])
            hits = (
                tx.select(
                    matched_xcoords(in_scripts, in_wit, out_scripts).alias("found")
                )
                .select(F.explode("found").alias("xcoord"))
                .distinct()
            )
            chain_found = {r["xcoord"] for r in hits.collect()}
            print(
                f"  {chain:5s}: {len(chain_found):,} of {len(needles):,} found on-chain"
            )
            all_found |= chain_found

        on_chain = len(all_found)
        not_found = len(needles) - on_chain
        print("\n=== verdict ===")
        print(f"  on_chain_FOUND (PARSER GAP) : {on_chain:,}")
        print(f"  not_in_utxo (source gap)    : {not_found:,}")
        if on_chain:
            print("\n--- example X-coords found on-chain but never extracted ---")
            for x in sorted(all_found)[: args.examples]:
                print(f"    {x}")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
