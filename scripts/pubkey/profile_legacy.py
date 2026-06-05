"""Profile the legacy ``pubkey.pubkey_by_address`` keyspace to recover the spec
of the (lost) script that filled it.

The script that built the production ``pubkey`` keyspace is gone, but its output
survives. This read-only profiler answers the questions we need before
rebuilding the new ``pubkey_v2`` job as a verified superset:

  1. all-pubkeys or cross-chain-only?   -> per-pubkey family/address-count dists
  2. observed or derived addresses?     -> --check-derivable subset test
  3. which chains / script types?       -> address-prefix classification
  4. cardinality                        -> total rows + distinct pubkeys

READ-ONLY: it only ``.load()``s the table and aggregates. It never writes to
Cassandra. Still a full distributed token-range scan though (same access path as
diff.py), so run it deliberately, off-peak, on a host with cluster access.

Run on the Spark driver host:

    uv run python scripts/pubkey/profile_legacy.py \
        --cassandra-host <cassandra-host> \
        --username <user> --password <pass> \
        --keyspace pubkey --table pubkey_by_address \
        --check-derivable 2000

If you have an env in ~/.graphsense.yaml that already points at the right
cluster you can pass ``--env <name>`` instead of the explicit host/creds.
"""

# Ops report script: print() is the intended human-facing output channel.
# ruff: noqa: T201

import argparse
import logging
from typing import List, Optional, Tuple

EVM_ADDR_LEN = 42  # "0x" + 40 hex


def classify(address: Optional[str]) -> Tuple[str, str]:
    """Map an address to (family, addr_type) by prefix.

    family is one of: evm | tron | utxo | unknown. NOTE the base58 UTXO
    prefixes overlap across chains (BTC/BCH-legacy both start '1'/'3', LTC '3'
    collides too), so utxo sub-types are best-effort. The ``evm`` and ``tron``
    families are unambiguous, and the long bech32 (P2WSH) signal is reliable —
    those are what the cross-chain / observed-vs-derived questions actually hinge
    on.
    """
    if not address:
        return ("unknown", "null")
    a = address
    if a.startswith("0x") and len(a) == EVM_ADDR_LEN:
        return ("evm", "eth/evm_0x")
    if a.startswith("T") and 30 <= len(a) <= 40:
        return ("tron", "tron_base58")
    if a.startswith("bc1"):
        return ("utxo", "btc_p2wsh" if len(a) > 50 else "btc_p2wpkh")
    if a.startswith("ltc1"):
        return ("utxo", "ltc_p2wsh" if len(a) > 50 else "ltc_p2wpkh")
    if (
        a.startswith("bitcoincash:")
        or a.startswith("bchreg:")
        or a.startswith("bchtest:")
    ):
        return ("utxo", "bch_cashaddr")
    if a.startswith("t1"):
        return ("utxo", "zec_t1_p2pkh")
    if a.startswith("t3"):
        return ("utxo", "zec_t3_p2sh")
    first = a[0]
    base58_map = {
        "1": "p2pkh(btc/bch)",
        "3": "p2sh(btc/bch/ltc-or-nested-segwit)",
        "L": "ltc_p2pkh",
        "M": "ltc_p2pkh_or_p2sh",
        "D": "doge_p2pkh",
        "A": "doge_p2sh",
        "9": "doge_p2sh",
    }
    if first in base58_map:
        return ("utxo", base58_map[first])
    return ("unknown", f"prefix:{a[:3]}")


def _derive_set(pk_hex: str, chains: List[str]) -> List[str]:
    """All addresses the NEW pipeline would derive for a compressed pubkey hex."""
    from graphsenselib.utils.pubkey_to_address import convert_pubkey_to_addresses

    out: List[str] = []
    try:
        nested = convert_pubkey_to_addresses(pk_hex, currencies=chains)
    except Exception:
        return out
    for forms in nested.values():
        if not isinstance(forms, dict):
            continue
        for key, val in forms.items():
            if key != "error" and isinstance(val, str) and val:
                out.append(val)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", help="graphsense.yaml env (for cluster/creds)")
    parser.add_argument("--cassandra-host", help="override: Cassandra contact host")
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--keyspace", default="pubkey")
    parser.add_argument("--table", default="pubkey_by_address")
    parser.add_argument(
        "--examples", type=int, default=15, help="example rows per section"
    )
    parser.add_argument(
        "--check-derivable",
        type=int,
        default=0,
        metavar="N",
        help="sample N pubkeys and report how many stored addresses are NOT "
        "reproducible by the new derivation (observed-script signal). 0=skip.",
    )
    args = parser.parse_args()

    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    from graphsenselib.config import get_config
    from graphsenselib.transformation.spark import create_spark_session

    config = get_config()
    cassandra_nodes = None
    username = args.username
    password = args.password
    if args.env:
        env_config = config.get_environment(args.env)
        cassandra_nodes = env_config.cassandra_nodes
        username = username or env_config.username
        password = password or env_config.password
    if args.cassandra_host:
        cassandra_nodes = [args.cassandra_host]
    if not cassandra_nodes:
        raise SystemExit("Provide --cassandra-host or --env with a configured cluster.")

    spark = create_spark_session(
        app_name=f"pubkey-legacy-profile-{args.keyspace}",
        local=False,
        cassandra_nodes=cassandra_nodes,
        cassandra_username=username,
        cassandra_password=password,
        spark_config=config.spark_config or {},
    )

    classify_schema = StructType(
        [
            StructField("family", StringType(), False),
            StructField("atype", StringType(), False),
        ]
    )
    classify_udf = F.udf(classify, classify_schema)

    try:
        raw = (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=args.table, keyspace=args.keyspace)
            .load()
            .select("address", "pubkey")
        )
        df = (
            raw.withColumn("cl", classify_udf(F.col("address")))
            .select(
                F.col("address"),
                F.lower(F.hex(F.col("pubkey"))).alias("pk"),
                F.col("cl.family").alias("family"),
                F.col("cl.atype").alias("atype"),
            )
            .cache()
        )

        total = df.count()
        distinct_pk = df.select("pk").distinct().count()
        print("\n=== cardinality ===")
        print(f"  total (address,pubkey) rows : {total:,}")
        print(f"  distinct pubkeys            : {distinct_pk:,}")
        print(f"  avg addresses / pubkey      : {total / max(distinct_pk, 1):.2f}")

        print("\n=== address breakdown by (family, type) ===")
        (
            df.groupBy("family", "atype")
            .agg(
                F.count("*").alias("addresses"),
                F.countDistinct("pk").alias("pubkeys"),
            )
            .orderBy(F.col("addresses").desc())
            .show(50, truncate=False)
        )

        # Per-pubkey shape: which families and how many distinct addresses.
        perpk = (
            df.groupBy("pk")
            .agg(
                F.sort_array(F.collect_set("family")).alias("families"),
                F.countDistinct("address").alias("n_addr"),
            )
            .cache()
        )

        print("\n=== family-set distribution per pubkey ===")
        print("  (does a pubkey live on evm AND tron AND utxo, or just one?)")
        (
            perpk.groupBy("families")
            .agg(F.count("*").alias("pubkeys"))
            .orderBy(F.col("pubkeys").desc())
            .show(50, truncate=False)
        )

        print("\n=== addresses-per-pubkey histogram ===")
        print("  (low+uniform -> observed; high+uniform -> derive-all-forms;")
        print("   any '1' bucket -> legacy stored single-chain singletons too)")
        bucketed = perpk.withColumn(
            "bucket",
            F.when(F.col("n_addr") == 1, F.lit("1"))
            .when(F.col("n_addr") == 2, F.lit("2"))
            .when(F.col("n_addr") == 3, F.lit("3"))
            .when(F.col("n_addr") <= 6, F.lit("4-6"))
            .when(F.col("n_addr") <= 10, F.lit("7-10"))
            .otherwise(F.lit("11+")),
        )
        (
            bucketed.groupBy("bucket")
            .agg(F.count("*").alias("pubkeys"))
            .orderBy("bucket")
            .show(truncate=False)
        )

        print("\n--- examples: single-address pubkeys (if any) ---")
        singletons = perpk.filter(F.col("n_addr") == 1)
        if singletons.head(1):
            (
                singletons.join(df, "pk")
                .select("pk", "address", "family", "atype")
                .limit(args.examples)
                .show(truncate=False)
            )
        else:
            print("  (none — every pubkey maps to >=2 addresses)")

        print("\n--- examples: highest address-count pubkeys ---")
        top = perpk.orderBy(F.col("n_addr").desc()).limit(args.examples)
        top.select("pk", "n_addr", "families").show(truncate=False)

        if args.check_derivable > 0:
            print(f"\n=== observed-vs-derived test (sample {args.check_derivable}) ===")
            print("  stored addresses NOT reproducible by convert_pubkey_to_addresses")
            print("  => legacy stored OBSERVED (script-hash/multisig) addresses.")
            chains = ["btc", "doge", "ltc", "zec", "eth", "trx", "bch"]
            derive_udf = F.udf(
                lambda pk: _derive_set(pk, chains), ArrayType(StringType())
            )
            sample_pk = perpk.select("pk").limit(args.check_derivable)
            stored = (
                df.join(sample_pk, "pk")
                .groupBy("pk")
                .agg(F.collect_set("address").alias("stored"))
            )
            checked = (
                stored.withColumn("derived", derive_udf(F.col("pk")))
                .withColumn(
                    "not_derivable",
                    F.array_except(F.col("stored"), F.col("derived")),
                )
                .withColumn("n_stored", F.size("stored"))
                .withColumn("n_not_derivable", F.size("not_derivable"))
                .cache()
            )
            agg = checked.agg(
                F.count("*").alias("pubkeys"),
                F.sum("n_stored").alias("stored_addrs"),
                F.sum("n_not_derivable").alias("not_derivable_addrs"),
                F.sum(F.when(F.col("n_not_derivable") > 0, 1).otherwise(0)).alias(
                    "pubkeys_with_undrivable"
                ),
            ).collect()[0]
            print(f"  pubkeys sampled               : {agg['pubkeys']:,}")
            print(f"  stored addresses              : {agg['stored_addrs']:,}")
            print(f"  NOT reproducible by derivation: {agg['not_derivable_addrs']:,}")
            print(
                f"  pubkeys w/ >=1 such address   : {agg['pubkeys_with_undrivable']:,}"
            )
            print("\n  --- examples of non-derivable stored addresses ---")
            (
                checked.filter(F.col("n_not_derivable") > 0)
                .select("pk", "not_derivable")
                .limit(args.examples)
                .show(truncate=False)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
