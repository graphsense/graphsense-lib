"""Validate the account-model delta field mapping used by pubkey extraction.

This is the single riskiest surface in the new pubkey job: ``extract_pubkey_account``
maps delta ``transaction`` columns (``v`` smallint, ``r``/``s`` varint, the
``access_list`` UDT, big-endian varint blobs) into the RPC shape the signature
helpers consume. The unit tests only exercise *synthetic* rows — this exercises
*real* delta rows and checks that the recovered key's address equals the stored
``from_address``.

For ETH a correctly-recovered key's address IS ``from`` by definition, so the
match rate should be ~100%. A low rate means the field mapping is wrong and must
be fixed before any keyspace write. It splits the outcome instead of silently
dropping (which the in-pipeline self-check would do):

    match         - recovered address == from_address          -> mapping OK
    mismatch      - recovered a (valid) key, address != from    -> MAPPING BUG
    recover_error - reconstruction/recovery threw               -> MAPPING BUG
    no_sig/zero_sig/no_from - row had no usable signature/from  -> skipped

TRX is reported as recovery-success only (informational): there ``from_address``
is the contract owner, which legitimately differs from the signer for multisig /
permission accounts, and tron-address formatting differs — so no match check.

READ-ONLY and bounded: reads only a narrow ``block_id`` range from the delta
``transaction`` table with a small ``--sample`` limit (partition-pruned, columnar
— not a full scan) and never writes anything.

Runs anywhere with Spark + read access to the delta. Compute can be local
(``--local`` => local[*], no cluster needed), but the delta source is normally
on S3, so you also need network reach to the bucket and an ``s3_configs`` entry
with credentials in your graphsense config (passed via ``--s3-config NAME``):

    uv run python scripts/pubkey/validate_account_fieldmap.py --local \
        --source-path s3://.../eth/delta --s3-config prod --currency eth \
        --start-block 20000000 --end-block 20010000 --sample 500
"""

# Ops report script: print() is the intended human-facing output channel.
# ruff: noqa: T201

import argparse
import logging
from collections import Counter
from typing import Any, Dict, Optional, Tuple


def _diagnose(row: Dict[str, Any], currency: str) -> Tuple[str, Optional[Any]]:
    """Re-run the exact extract_pubkey_account mapping but report the 3-way
    comparison instead of dropping on mismatch."""
    from graphsenselib.pubkey.extract import (
        _DEFAULT_CHAIN_ID,
        _as_int_be,
        _delta_row_to_rpc_shape,
        _normalize_evm_addr,
    )
    from graphsenselib.utils.signature import (
        eth_get_msg_hash_from_signature_data,
        eth_get_signature_data_from_rpc_json,
        eth_recover_pubkey,
    )

    v, r, s = row.get("v"), row.get("r"), row.get("s")
    if v is None or r is None or s is None:
        return ("no_sig", None)
    try:
        v_int, r_int, s_int = _as_int_be(v), _as_int_be(r), _as_int_be(s)
        if r_int == 0 and s_int == 0:
            return ("zero_sig", None)
        if currency == "trx":
            tx_hash = row.get("tx_hash")
            if tx_hash is None:
                return ("no_txhash", None)
            msg_hash = bytes(tx_hash)
            # Recovery success is the only signal we report for TRX.
            eth_recover_pubkey((v_int, r_int, s_int), msg_hash)
            return ("trx_recovered", None)

        chain_id = _DEFAULT_CHAIN_ID.get(currency, 1) or 1
        rpc_shape = _delta_row_to_rpc_shape(row, chain_id)
        sig_data = eth_get_signature_data_from_rpc_json(rpc_shape)
        msg_hash = eth_get_msg_hash_from_signature_data(sig_data)
        recovered = eth_recover_pubkey((v_int, r_int, s_int), msg_hash)
        derived = recovered.to_address()[2:]
        expected = _normalize_evm_addr(row.get("from_address"))
        if expected is None:
            return ("no_from", derived)
        if derived == expected:
            return ("match", None)
        return ("mismatch", (derived, expected, int(rpc_shape.get("type", 0))))
    except Exception as e:
        return ("recover_error", repr(e))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-path", required=True, help="delta source root")
    parser.add_argument("--currency", required=True, choices=["eth", "trx"])
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument("--sample", type=int, default=500, help="max rows to check")
    parser.add_argument("--examples", type=int, default=15)
    parser.add_argument(
        "--s3-config",
        help="name of an s3_configs entry for S3/MinIO creds; required when "
        "--source-path is on s3:// (the delta source usually is)",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="run Spark in local[*] mode (no cluster compute needed; you still "
        "need network + creds to read the S3 delta)",
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
    if not config.is_loaded():
        config.load()  # populate from ~/.graphsense.yaml (s3_configs, spark_config)
    source_path = args.source_path.rstrip("/").replace("s3://", "s3a://")
    is_s3 = source_path.startswith("s3a://")

    s3_credentials = None
    if args.s3_config:
        s3_credentials = config.get_s3_credentials(args.s3_config)
    elif is_s3:
        available = sorted((config.s3_configs or {}).keys())
        raise SystemExit(
            "Source path is on S3 but --s3-config was not given. Available "
            f"s3_configs: {available or 'none defined in your local config'}."
        )

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
        app_name=f"pubkey-fieldmap-validate-{args.currency}",
        local=args.local,
        # No Cassandra needed; placeholder keeps the connector config happy.
        cassandra_nodes=["localhost:9042"],
        s3_credentials=s3_credentials,
        spark_config=spark_config,
        spark_packages=config.get_spark_packages(),
    )

    cols = [
        "block_id",
        "tx_hash",
        "from_address",
        "transaction_type",
        "nonce",
        "to_address",
        "value",
        "gas",
        "gas_price",
        "input",
        "max_fee_per_gas",
        "max_priority_fee_per_gas",
        "max_fee_per_blob_gas",
        "blob_versioned_hashes",
        "access_list",
        "v",
        "r",
        "s",
    ]
    try:
        df = spark.read.format("delta").load(f"{source_path}/transaction")
        present = [c for c in cols if c in df.columns]
        sub = (
            df.filter(
                (df["block_id"] >= args.start_block)
                & (df["block_id"] <= args.end_block)
            )
            .select(*present)
            .limit(args.sample)
        )
        rows = [r.asDict(recursive=True) for r in sub.collect()]
        print(
            f"\n=== account field-map validation: {args.currency}, "
            f"blocks [{args.start_block}, {args.end_block}], "
            f"{len(rows)} rows ==="
        )
        if not rows:
            raise SystemExit("No rows in that block range — widen it.")

        status = Counter()
        examples: Dict[str, list] = {"mismatch": [], "recover_error": []}
        for row in rows:
            st, detail = _diagnose(row, args.currency)
            status[st] += 1
            if st in examples and len(examples[st]) < args.examples:
                examples[st].append((row.get("tx_hash"), detail))

        for st, n in status.most_common():
            print(f"  {st:15s}: {n:,}")

        if args.currency == "trx":
            ok = status.get("trx_recovered", 0)
            attempted = ok + status.get("recover_error", 0)
            if attempted:
                print(
                    f"\n  TRX recovery success: {ok}/{attempted} "
                    f"({100.0 * ok / attempted:.1f}%) — informational "
                    "(no owner-address match check)."
                )
        else:
            match = status.get("match", 0)
            mismatch = status.get("mismatch", 0)
            checked = match + mismatch
            if checked:
                rate = 100.0 * match / checked
                print(
                    f"\n  ETH from-address match rate: {match}/{checked} ({rate:.2f}%)"
                )
                if rate >= 99.9:
                    print("  => field mapping looks CORRECT.")
                else:
                    print(
                        "  => WARNING: low match rate -> field mapping likely "
                        "WRONG. Do not write a keyspace until fixed."
                    )

        for st in ("mismatch", "recover_error"):
            if examples.get(st):
                print(f"\n  --- examples: {st} ---")
                for tx_hash, detail in examples[st]:
                    h = (
                        tx_hash.hex()
                        if isinstance(tx_hash, (bytes, bytearray))
                        else tx_hash
                    )
                    print(f"    {h}  {detail}")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
