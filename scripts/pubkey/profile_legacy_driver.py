"""Profile the legacy ``pubkey.pubkey_by_address`` keyspace with the plain
Cassandra driver — NO Spark, so no full-cluster scan.

The script that built the production ``pubkey`` keyspace is lost; its output is
the surviving spec. This samples it cheaply and runs the same questions as
profile_legacy.py, but using token-range sampling + primary-key point reads
instead of a distributed scan, so it does not block the cluster.

READ-ONLY. Sampling strategy:
  * pick ``--segments`` random ring positions; from each, read ``LIMIT k`` rows
    via ``WHERE token(address) >= ?`` (a bounded, paged sequential slice).
  * total sample ~= --sample-size rows.

Tests:
  1. address breakdown by (family, type)              -> chain/script coverage
  2. observed-vs-derived (per row, free)              -> are stored addresses
     reproducible by convert_pubkey_to_addresses? non-derivable => legacy stored
     OBSERVED script (P2SH/P2WSH/multisig) addresses the new design can't rebuild
  3. --check-crosschain: for each sampled pubkey, derive addrs across all chains
     and POINT-READ each (cheap PK lookups) to see which families are actually
     present -> cross-chain-only vs single-chain-singletons

Run on a host with cluster access (you run it, not me):

    uv run python scripts/pubkey/profile_legacy_driver.py \
        --cassandra-host ikn-vie02-spark01-db0 \
        --username <user> --password <pass> \
        --sample-size 20000 --segments 40 --check-crosschain

For an exact-ish row count without scanning, use nodetool on a node instead:
    nodetool tablestats pubkey.pubkey_by_address   # "Number of partition keys (estimate)"
"""

# Ops report script: print() is the intended human-facing output channel.
# ruff: noqa: T201

import argparse
import random
from collections import Counter
from typing import List, Optional, Tuple

EVM_ADDR_LEN = 42
TOKEN_MIN = -(2**63)
TOKEN_MAX = 2**63 - 1
DERIVE_CHAINS = ["btc", "doge", "ltc", "zec", "eth", "trx", "bch"]


def classify(address: Optional[str]) -> Tuple[str, str]:
    """(family, addr_type) by prefix. evm/tron/long-bech32 reliable; base58 UTXO
    sub-types overlap across BTC/BCH/LTC (best-effort)."""
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
    if a.startswith(("bitcoincash:", "bchreg:", "bchtest:")):
        return ("utxo", "bch_cashaddr")
    if a.startswith("t1"):
        return ("utxo", "zec_t1_p2pkh")
    if a.startswith("t3"):
        return ("utxo", "zec_t3_p2sh")
    base58_map = {
        "1": "p2pkh(btc/bch)",
        "3": "p2sh(btc/bch/ltc-or-nested-segwit)",
        "L": "ltc_p2pkh",
        "M": "ltc_p2pkh_or_p2sh",
        "D": "doge_p2pkh",
        "A": "doge_p2sh",
        "9": "doge_p2sh",
    }
    if a[0] in base58_map:
        return ("utxo", base58_map[a[0]])
    return ("unknown", f"prefix:{a[:3]}")


def derive_addresses(pk_hex: str) -> List[str]:
    """All addresses the NEW pipeline would derive for a compressed pubkey hex.

    Returns [] for pubkey blobs that aren't a valid 33/65-byte key hex (those
    legacy rows are stored in a form the new derivation can't consume at all).
    """
    from graphsenselib.utils.pubkey_to_address import convert_pubkey_to_addresses

    out: List[str] = []
    try:
        nested = convert_pubkey_to_addresses(pk_hex, currencies=DERIVE_CHAINS)
    except Exception:
        return out
    for forms in nested.values():
        if isinstance(forms, dict):
            for key, val in forms.items():
                if key != "error" and isinstance(val, str) and val:
                    out.append(val)
    return out


def sample_rows(session, keyspace: str, table: str, sample_size: int, segments: int):
    """Token-range sample: read bounded slices from random ring positions."""
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement

    per_segment = max(1, sample_size // max(1, segments))
    rows: List[Tuple[str, bytes]] = []
    seen = 0
    for _ in range(segments):
        anchor = random.randint(TOKEN_MIN, TOKEN_MAX)
        stmt = SimpleStatement(
            f"SELECT address, pubkey FROM {keyspace}.{table} "
            f"WHERE token(address) >= {anchor} LIMIT {per_segment}",
            fetch_size=min(per_segment, 1000),
            consistency_level=ConsistencyLevel.LOCAL_ONE,
        )
        for r in session.execute(stmt):
            pk = r.pubkey
            rows.append((r.address, bytes(pk) if pk is not None else b""))
            seen += 1
    print(f"  sampled rows: {seen:,} from {segments} ring segments")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cassandra-host", required=True)
    parser.add_argument("--port", type=int, default=9042)
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--keyspace", default="pubkey")
    parser.add_argument("--table", default="pubkey_by_address")
    parser.add_argument("--sample-size", type=int, default=20000)
    parser.add_argument("--segments", type=int, default=40)
    parser.add_argument("--examples", type=int, default=20)
    parser.add_argument(
        "--check-crosschain",
        action="store_true",
        help="for each sampled pubkey, point-read its derived addresses to see "
        "which families are present in the table (cross-chain test)",
    )
    parser.add_argument(
        "--crosschain-pubkeys",
        type=int,
        default=2000,
        help="cap distinct pubkeys used for the cross-chain point-read test",
    )
    args = parser.parse_args()

    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    from cassandra.concurrent import execute_concurrent_with_args

    auth = (
        PlainTextAuthProvider(username=args.username, password=args.password)
        if args.username
        else None
    )
    cluster = Cluster(
        [args.cassandra_host], port=args.port, auth_provider=auth, connect_timeout=15
    )
    session = cluster.connect()
    session.default_timeout = 120

    try:
        print(f"\n=== sampling {args.keyspace}.{args.table} (token-range) ===")
        rows = sample_rows(
            session, args.keyspace, args.table, args.sample_size, args.segments
        )
        if not rows:
            raise SystemExit("No rows sampled — check host/keyspace/creds.")

        # --- Test 1: address breakdown by (family, type) ---
        fam_type = Counter()
        fam = Counter()
        for address, _pk in rows:
            f, t = classify(address)
            fam_type[(f, t)] += 1
            fam[f] += 1
        print("\n=== address breakdown by (family, type) ===")
        for (f, t), n in fam_type.most_common():
            print(f"  {n:>10,}  {f:<8} {t}")
        print("  families:", dict(fam))

        # --- Test 2: observed-vs-derived (per row, free) ---
        print("\n=== observed-vs-derived (per sampled row) ===")
        derivable = 0
        not_derivable = 0
        bad_pk_form = 0
        nd_examples = []
        nd_type = Counter()
        for address, pk in rows:
            pk_hex = pk.hex()
            derived = set(derive_addresses(pk_hex))
            if not derived:
                bad_pk_form += 1
                continue
            if address in derived:
                derivable += 1
            else:
                not_derivable += 1
                nd_type[classify(address)[1]] += 1
                if len(nd_examples) < args.examples:
                    nd_examples.append((address, pk_hex, len(pk)))
        total_checked = derivable + not_derivable
        print(f"  rows with usable pubkey form  : {total_checked:,}")
        print(f"  address IS derivable          : {derivable:,}")
        print(f"  address NOT derivable         : {not_derivable:,}")
        print(f"  unusable pubkey blob form     : {bad_pk_form:,}")
        if total_checked:
            pct = 100.0 * not_derivable / total_checked
            print(f"  => {pct:.2f}% of stored addresses are OBSERVED (not derivable)")
        if not_derivable:
            print("  non-derivable address types:", dict(nd_type))
            print("  --- examples (address, pubkey_hex, pubkey_len_bytes) ---")
            for a, h, ln in nd_examples:
                print(f"    {a}  {h}  len={ln}")

        # --- Test 3: cross-chain via point reads ---
        if args.check_crosschain:
            print("\n=== cross-chain test (point reads per pubkey) ===")
            distinct_pk = []
            seen_pk = set()
            for _address, pk in rows:
                h = pk.hex()
                if h and h not in seen_pk:
                    seen_pk.add(h)
                    distinct_pk.append(pk)
                if len(distinct_pk) >= args.crosschain_pubkeys:
                    break
            select_stmt = session.prepare(
                f"SELECT address FROM {args.keyspace}.{args.table} WHERE address = ?"
            )
            famset_hist = Counter()
            examples_single = []
            for pk in distinct_pk:
                derived = derive_addresses(pk.hex())
                if not derived:
                    continue
                results = execute_concurrent_with_args(
                    session,
                    select_stmt,
                    [(a,) for a in derived],
                    concurrency=20,
                )
                present_families = set()
                present_addrs = []
                for ok, res in results:
                    if ok:
                        hit = res.one()
                        if hit is not None:
                            present_addrs.append(hit.address)
                            present_families.add(classify(hit.address)[0])
                key = tuple(sorted(present_families))
                famset_hist[key] += 1
                if len(key) <= 1 and len(examples_single) < args.examples:
                    examples_single.append((pk.hex(), present_addrs[:5]))
            print(f"  distinct pubkeys checked: {sum(famset_hist.values()):,}")
            print("  family-set present in table (per pubkey):")
            for key, n in famset_hist.most_common():
                print(
                    f"    {n:>8,}  {list(key) if key else '[none-derivable-present]'}"
                )
            print("\n  NOTE: only single-key derivable forms are point-read, so a")
            print("  pubkey stored ONLY under a script/observed address shows as")
            print("  few/no present families here — cross-reference Test 2.")
            if examples_single:
                print("\n  --- examples: single-family / sparse pubkeys ---")
                for h, addrs in examples_single:
                    print(f"    {h}  present={addrs}")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
