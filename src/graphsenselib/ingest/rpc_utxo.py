"""Fast JSON-RPC based block and transaction exporter for UTXO chains.

Replaces bitcoin-etl's ExportBlocksJob with direct batch JSON-RPC calls.
Output dict format is identical to bitcoin-etl's mappers so that downstream
code (enrich_txs, prepare_blocks_inplace, prepare_transactions_inplace_parquet)
works without changes.

Supported chains: BTC, LTC, BCH, ZEC.
Uses getblock(hash, verbosity) which returns blocks with full decoded
transaction data in a single RPC call — no separate getrawtransaction needed.

Verbosity levels:
  - 2: Standard decoded transactions (all chains).
  - 3: Decoded transactions with ``prevout`` on each input containing the
    spent output's value, address, and type. Supported by Bitcoin Core 23.0+
    and BCHN 26.0.0+. Default for BTC/BCH.
"""

import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from graphsenselib.ingest.rpc_eth import BatchRpcClient, validate_rpc_fields
from graphsenselib.utils.pubkey_to_address import base58check_encode, hash160

logger = logging.getLogger(__name__)

# ZCash shielded address type (matches bitcoin-etl)
ADDRESS_TYPE_SHIELDED = "shielded"


# ---------------------------------------------------------------------------
# Field validation sets (same pattern as rpc.py for ETH)
# ---------------------------------------------------------------------------

# -- Block -------------------------------------------------------------------

_BLOCK_KNOWN_KEYS = frozenset(
    {
        "hash",
        "height",
        "time",
        "size",
        "strippedsize",
        "weight",
        "version",
        "merkleroot",
        "nonce",
        "bits",
        "tx",
    }
)

_BLOCK_BLACKLIST = frozenset(
    {
        "confirmations",  # changes with each new block
        "difficulty",  # derived from bits
        "chainwork",  # cumulative chain work, not stored
        "nTx",  # redundant with len(tx)
        "previousblockhash",  # not stored
        "nextblockhash",  # only present for non-tip blocks, not stored
        "versionHex",  # hex representation of version, redundant
        "mediantime",  # median of last 11 blocks, not stored
        "target",  # compact target threshold, derived from bits
        "coinbase_tx",  # parsed coinbase returned by some node variants; redundant with tx[0]
        # ZEC-specific consensus/mining fields
        "solution",  # Equihash PoW solution
        "anchor",  # Sprout note commitment tree anchor
        "finalsaplingroot",  # Sapling tree root hash
        "finalorchardroot",  # NU5 Orchard tree root hash
        "chainhistoryroot",  # chain history Merkle root
        "blockcommitments",  # NU5 block commitments hash
        "authdataroot",  # NU5 auth data Merkle root
        "trees",  # note commitment tree state
        "chainSupply",  # chain supply info
        "valuePools",  # shielded pool balances
        # LTC-specific
        "mweb",  # MimbleWimble Extension Block data
        # BCH-specific
        "ablastate",  # Adaptive Block Limit Algorithm state (BCHN 27+)
    }
)

# -- Transaction -------------------------------------------------------------

_TX_KNOWN_KEYS = frozenset(
    {
        "txid",
        "size",
        "vsize",
        "version",
        "locktime",
        "vin",
        "vout",
        # ZCash shielded I/O
        "vjoinsplit",
        "valueBalance",
    }
)

_TX_BLACKLIST = frozenset(
    {
        "hash",  # wtxid (witness hash), different from txid for segwit
        "weight",  # derived from size/vsize
        "hex",  # raw transaction hex, not stored
        "fee",  # convenience field at verbosity 3, we compute it ourselves
        # ZCash extras we don't parse
        "vShieldedSpend",
        "vShieldedOutput",
        "bindingSig",
        "overwintered",
        "expiryheight",
        "valueBalanceZat",
        "authDigest",
        "authdigest",  # same as authDigest, lowercase in some ZEC block ranges
        "versiongroupid",  # ZEC Overwinter/Sapling version group ID
        "joinSplitPubKey",  # ZEC Sprout joinsplit public key
        "joinSplitSig",  # ZEC Sprout joinsplit signature
        "orchard",  # ZEC NU5 Orchard bundle (actions, proof, etc.)
        # Zebra adds these per-transaction lookup fields that zcashd does not
        # emit inside getblock; none carry parser-relevant data.
        "blockhash",
        "blocktime",
        "confirmations",
        "height",
        "in_active_chain",
        "time",
    }
)

# -- Input (vin entry) ------------------------------------------------------

_VIN_KNOWN_KEYS = frozenset(
    {
        "coinbase",  # present instead of txid/vout for coinbase txs
        "txid",
        "vout",
        "scriptSig",
        "sequence",
        "txinwitness",
        "prevout",  # verbosity 3 only
    }
)

_VIN_BLACKLIST = frozenset(
    {
        "ismweb",  # LTC: flags vin from MimbleWimble Extension Block
        "tokenData",  # BCH: CashTokens data on input (category, amount, nft)
    }
)

# -- scriptSig --------------------------------------------------------------

_SCRIPT_SIG_KNOWN_KEYS = frozenset(
    {
        "asm",
        "hex",
    }
)

_SCRIPT_SIG_BLACKLIST = frozenset()

# -- prevout (verbosity 3) --------------------------------------------------

_PREVOUT_KNOWN_KEYS = frozenset(
    {
        "value",
        "scriptPubKey",
    }
)

_PREVOUT_BLACKLIST = frozenset(
    {
        "generated",  # whether UTXO is from coinbase
        "height",  # block height of the UTXO
        "tokenData",  # BCH: CashTokens data (category, amount, nft)
    }
)

# -- Output (vout entry) ----------------------------------------------------

_VOUT_KNOWN_KEYS = frozenset(
    {
        "value",
        "n",
        "scriptPubKey",
    }
)

_VOUT_BLACKLIST = frozenset(
    {
        "ismweb",  # LTC: flags vout from MimbleWimble Extension Block
        "valueSat",  # ZEC: integer satoshi value, redundant with value
        "valueZat",  # ZEC: integer zatoshi value, redundant with value
        "tokenData",  # BCH: CashTokens data (category, amount, nft)
    }
)

# -- scriptPubKey (shared by vout and prevout) -------------------------------

_SCRIPT_PUB_KEY_KNOWN_KEYS = frozenset(
    {
        "asm",
        "hex",
        "type",
        "address",  # modern Bitcoin Core (singular)
        "addresses",  # older nodes (list)
        "reqSigs",  # legacy multisig field
    }
)

_SCRIPT_PUB_KEY_BLACKLIST = frozenset(
    {
        "desc",  # output descriptor (Bitcoin Core 22+)
    }
)

# -- vjoinsplit (ZCash Sprout) -----------------------------------------------

_JOINSPLIT_KNOWN_KEYS = frozenset(
    {
        "vpub_old",
        "vpub_new",
    }
)

_JOINSPLIT_BLACKLIST = frozenset(
    {
        "valid",
        "description",
        "nullifiers",
        "commitments",
        "onetimePubKey",
        "randomSeed",
        "macs",
        "proof",
        "vpub_oldZat",
        "vpub_newZat",
        "anchor",  # Sprout anchor hash
        "ciphertexts",  # encrypted note ciphertexts
    }
)


def _btc_to_satoshi(value):
    """Convert BTC float to satoshis (int).

    IEEE 754 double has 53 bits of mantissa. Max Bitcoin value (21M BTC =
    2.1e15 satoshis) fits in 51 bits, so round(float * 1e8) is exact.
    """
    if value is None:
        return None
    return round(value * 100_000_000)


def _nonce_to_hex(nonce):
    """Convert nonce to hex string. BTC returns int, ZEC returns hex string."""
    if nonce is None:
        return None
    if isinstance(nonce, str):
        return nonce
    return format(nonce, "x")


def _script_hex_to_non_standard_address(script_hex):
    """Replicate bitcoin-etl's btc_script_service.script_hex_to_non_standard_address."""
    if script_hex is None:
        script_hex = ""
    script_bytes = bytearray.fromhex(script_hex)
    script_hash = hashlib.sha256(script_bytes).hexdigest()[:40]
    return "nonstandard" + script_hash


# P2PKH/t1 address version byte per network, used to derive the address of a
# P2PK output that a node classifies as type "pubkey" but returns without an
# address. zcashd populates it; Zebra and modern Bitcoin Core do not. BCH
# legacy addresses share BTC's version bytes.
_PUBKEY_ADDRESS_VERSION = {
    "btc": b"\x00",
    "bch": b"\x00",
    "ltc": b"\x30",
    "zec": b"\x1c\xb8",  # Zcash t1 prefix
}


def _p2pk_address_from_script(script_hex, network):
    """Derive the P2PKH-style address of a P2PK locking script.

    A P2PK script is ``<push 33|65> <pubkey> OP_CHECKSIG`` and has no canonical
    address, so nodes report type "pubkey" but frequently omit the address. We
    derive ``base58check(version, hash160(pubkey))`` — the same value zcashd
    returns — so output parsing is identical regardless of whether the node
    populated the address. Returns ``None`` if ``script_hex`` is not a
    well-formed P2PK script (caller falls back to the nonstandard synthetic id).
    """
    if not script_hex:
        return None
    try:
        script = bytes.fromhex(script_hex)
    except ValueError:
        return None
    if len(script) < 2 or script[-1] != 0xAC:  # trailing OP_CHECKSIG
        return None
    push_len = script[0]
    if push_len not in (33, 65) or len(script) != push_len + 2:
        return None
    pubkey = script[1:-1]
    valid = (push_len == 33 and pubkey[0] in (0x02, 0x03)) or (
        push_len == 65 and pubkey[0] in (0x04, 0x06, 0x07)
    )
    if not valid:
        return None
    version = _PUBKEY_ADDRESS_VERSION.get(network, b"\x00")
    return base58check_encode(version, hash160(pubkey))


def _parse_input(vin_entry, index, network="btc"):
    """Parse a single vin entry to input dict matching bitcoin-etl format.

    When verbosity 3 data is available, the ``prevout`` object on each input
    contains the spent output's value, address, and type — no external
    database lookup required.
    """
    validate_rpc_fields(vin_entry.keys(), _VIN_KNOWN_KEYS, _VIN_BLACKLIST, "vin")
    script_sig = vin_entry.get("scriptSig") or {}
    if script_sig:
        validate_rpc_fields(
            script_sig.keys(),
            _SCRIPT_SIG_KNOWN_KEYS,
            _SCRIPT_SIG_BLACKLIST,
            "scriptSig",
        )
    prevout = vin_entry.get("prevout")

    input_dict = {
        "index": index,
        "spent_transaction_hash": vin_entry.get("txid"),
        "spent_output_index": vin_entry.get("vout"),
        "script_asm": script_sig.get("asm"),
        "script_hex": script_sig.get("hex"),
        "sequence": vin_entry.get("sequence"),
        "required_signatures": None,
        "type": None,
        "addresses": [],
        "value": None,
        "txinwitness": vin_entry.get("txinwitness"),
    }

    if prevout:
        validate_rpc_fields(
            prevout.keys(), _PREVOUT_KNOWN_KEYS, _PREVOUT_BLACKLIST, "prevout"
        )
        input_dict["value"] = _btc_to_satoshi(prevout.get("value"))
        spk = prevout.get("scriptPubKey") or {}
        if spk:
            validate_rpc_fields(
                spk.keys(),
                _SCRIPT_PUB_KEY_KNOWN_KEYS,
                _SCRIPT_PUB_KEY_BLACKLIST,
                "prevout.scriptPubKey",
            )
        address = spk.get("address")
        addresses = spk.get("addresses")
        if not addresses:
            addresses = [address] if address else []
        output_type = spk.get("type")

        # Non-standard address detection (same logic as _parse_output).
        # P2PK prevouts have type "pubkey" but no address — derive it directly
        # (keeping the node's type). Anything else falls back to the synthetic
        # nonstandard id, storing the script hex so enrich_txs can try
        # parse_script later.
        if (not addresses) and output_type != "nulldata":
            prevout_hex = spk.get("hex", "")
            derived = (
                _p2pk_address_from_script(prevout_hex, network)
                if output_type == "pubkey"
                else None
            )
            if derived is not None:
                addresses = [derived]
            else:
                output_type = "nonstandard"
                addresses = [_script_hex_to_non_standard_address(prevout_hex)]
                input_dict["prevout_script_hex"] = prevout_hex

        input_dict["addresses"] = addresses
        input_dict["type"] = output_type

    return input_dict


def _parse_output(vout_entry, network="btc"):
    """Parse a single vout entry to output dict matching bitcoin-etl format."""
    validate_rpc_fields(vout_entry.keys(), _VOUT_KNOWN_KEYS, _VOUT_BLACKLIST, "vout")
    script_pub_key = vout_entry.get("scriptPubKey") or {}
    if script_pub_key:
        validate_rpc_fields(
            script_pub_key.keys(),
            _SCRIPT_PUB_KEY_KNOWN_KEYS,
            _SCRIPT_PUB_KEY_BLACKLIST,
            "scriptPubKey",
        )
    addresses = script_pub_key.get("addresses")
    if not addresses:
        a = script_pub_key.get("address")
        addresses = [a] if a else []

    output_type = script_pub_key.get("type")
    required_signatures = script_pub_key.get("reqSigs")

    # When the node omits the address: P2PK outputs (type "pubkey") have no
    # canonical address but we can derive one (keeping the node's type), so a
    # node that populates it (zcashd) and one that does not (Zebra) parse
    # identically. Everything else falls back to bitcoin-etl's synthetic
    # nonstandard id.
    if (not addresses) and output_type != "nulldata":
        script_hex = script_pub_key.get("hex", "")
        derived = (
            _p2pk_address_from_script(script_hex, network)
            if output_type == "pubkey"
            else None
        )
        if derived is not None:
            addresses = [derived]
            # P2PK always requires exactly one signature; nodes that omit the
            # address omit reqSigs too (Zebra), so fill it to match zcashd.
            if required_signatures is None:
                required_signatures = 1
        else:
            output_type = "nonstandard"
            addresses = [_script_hex_to_non_standard_address(script_hex)]

    return {
        "index": vout_entry.get("n"),
        "script_asm": script_pub_key.get("asm"),
        "script_hex": script_pub_key.get("hex"),
        "required_signatures": required_signatures,
        "type": output_type,
        "addresses": addresses,
        "value": _btc_to_satoshi(vout_entry.get("value")),
    }


def _make_shielded_input(index, value):
    return {
        "index": index,
        "spent_transaction_hash": None,
        "spent_output_index": None,
        "script_asm": None,
        "script_hex": None,
        "sequence": None,
        "required_signatures": None,
        "type": ADDRESS_TYPE_SHIELDED,
        "addresses": [],
        "value": value,
        "txinwitness": None,
    }


def _make_shielded_output(index, value):
    return {
        "index": index,
        "script_asm": None,
        "script_hex": None,
        "required_signatures": None,
        "type": ADDRESS_TYPE_SHIELDED,
        "addresses": [],
        "value": value,
    }


def _parse_btc_block_and_txs(raw_block, network="btc"):
    """Parse a raw getblock(hash, 2) response into (block_dict, [tx_dicts]).

    Handles coinbase detection, value conversion, non-standard addresses,
    and ZCash shielded I/O. Output format matches bitcoin-etl's mapper output.
    """
    validate_rpc_fields(raw_block.keys(), _BLOCK_KNOWN_KEYS, _BLOCK_BLACKLIST, "block")

    block_hash = raw_block["hash"]
    block_number = raw_block["height"]
    block_timestamp = raw_block["time"]

    raw_txs = raw_block.get("tx") or []
    coinbase_param = None
    txs = []

    for tx_index, raw_tx in enumerate(raw_txs):
        validate_rpc_fields(raw_tx.keys(), _TX_KNOWN_KEYS, _TX_BLACKLIST, "transaction")
        vin = raw_tx.get("vin") or []
        vout = raw_tx.get("vout") or []

        # Detect and handle coinbase
        is_coinbase = False
        inputs = []
        input_idx = 0
        for vin_entry in vin:
            if "coinbase" in vin_entry:
                validate_rpc_fields(
                    vin_entry.keys(), _VIN_KNOWN_KEYS, _VIN_BLACKLIST, "vin"
                )
                coinbase_param = vin_entry["coinbase"]
                is_coinbase = True
            else:
                inputs.append(_parse_input(vin_entry, input_idx, network))
                input_idx += 1

        outputs = [_parse_output(v, network) for v in vout]

        # ZCash: joinsplit shielded I/O
        # vpub_old = value from transparent → shielded (consumed) = OUTPUT
        # vpub_new = value from shielded → transparent (produced) = INPUT
        # Same convention as Sapling valueBalance: z→t = input, t→z = output
        vjoinsplit = raw_tx.get("vjoinsplit")
        if vjoinsplit:
            for js in vjoinsplit:
                validate_rpc_fields(
                    js.keys(),
                    _JOINSPLIT_KNOWN_KEYS,
                    _JOINSPLIT_BLACKLIST,
                    "vjoinsplit",
                )
                vpub_old = _btc_to_satoshi(js.get("vpub_old")) or 0
                vpub_new = _btc_to_satoshi(js.get("vpub_new")) or 0
                if vpub_new > 0:
                    inputs.append(_make_shielded_input(len(inputs), vpub_new))
                if vpub_old > 0:
                    outputs.append(_make_shielded_output(len(outputs), vpub_old))

        # ZCash: Sapling value balance
        value_balance = raw_tx.get("valueBalance")
        if value_balance is not None and value_balance != 0:
            value_balance_sat = _btc_to_satoshi(value_balance)
            if value_balance_sat > 0:
                inputs.append(_make_shielded_input(len(inputs), value_balance_sat))
            elif value_balance_sat < 0:
                outputs.append(_make_shielded_output(len(outputs), -value_balance_sat))

        # Calculate values AFTER adding shielded I/O so totals include
        # both transparent and shielded amounts (matching bitcoin-etl).
        output_value = sum(o["value"] for o in outputs if o["value"] is not None)
        input_value = sum(inp["value"] for inp in inputs if inp["value"] is not None)

        if is_coinbase:
            fee = 0
        else:
            fee = input_value - output_value

        txs.append(
            {
                "type": "transaction",
                "hash": raw_tx.get("txid"),
                "size": raw_tx.get("size"),
                "virtual_size": raw_tx.get("vsize"),
                "version": raw_tx.get("version"),
                "lock_time": raw_tx.get("locktime"),
                "block_number": block_number,
                "block_hash": block_hash,
                "block_timestamp": block_timestamp,
                "is_coinbase": is_coinbase,
                "index": tx_index,
                "inputs": inputs,
                "outputs": outputs,
                "input_count": len(inputs),
                "output_count": len(outputs),
                "input_value": input_value,
                "output_value": output_value,
                "fee": fee,
            }
        )

    block = {
        "type": "block",
        "hash": block_hash,
        "size": raw_block.get("size"),
        "stripped_size": raw_block.get("strippedsize"),
        "weight": raw_block.get("weight"),
        "number": block_number,
        "version": raw_block.get("version"),
        "merkle_root": raw_block.get("merkleroot"),
        "timestamp": block_timestamp,
        "nonce": _nonce_to_hex(raw_block.get("nonce")),
        "bits": raw_block.get("bits"),
        "coinbase_param": coinbase_param,
        "transaction_count": len(raw_txs),
    }

    return block, txs


_GETRAWTX_BATCH_SIZE = 50
_MAX_OUTPUT_CACHE_ENTRIES = 2**23  # ~8M txs

# Cache entry tuple layout (avoids dict overhead, saves ~200 bytes per entry)
_CE_VALUE = 0
_CE_ADDRESSES = 1
_CE_TYPE = 2
_CE_SCRIPT_HEX = 3  # only set for non-standard outputs, None otherwise


def _make_cache_entry(value, addresses, output_type, script_hex):
    """Create a compact cache entry tuple. Only stores script_hex for
    non-standard outputs (where it's needed for address derivation)."""
    needs_script = not addresses or output_type == "nonstandard"
    return (value, addresses, output_type, script_hex if needs_script else None)


class BtcBlockExporter:
    """Export UTXO blocks and transactions via direct batch JSON-RPC.

    Replaces bitcoin-etl's ExportBlocksJob + BtcService for all UTXO chains
    (BTC, LTC, BCH, ZEC). Uses getblock(hash, verbosity) which returns
    blocks with full decoded transaction data in a single RPC call.

    Two-phase design:
      Phase 1: getblockhash for all blocks in one batch (fast, ~15ms).
      Phase 2: getblock for each block individually via ThreadPoolExecutor.

    Individual getblock requests (not batched) allow Bitcoin Core to
    parallelize across its RPC threads. Batched requests are processed
    sequentially within a single connection, which is slower for heavy blocks.

    When verbosity < 3, inputs lack value/addresses/type. If
    ``resolve_inputs=True`` (default), the exporter resolves them via
    batched ``getrawtransaction`` calls after block fetching.
    """

    def __init__(
        self,
        provider_uri,
        max_workers=5,
        timeout=300,
        verbosity=2,
        resolve_inputs=True,
        network="btc",
        fail_on_unresolved_inputs=True,
    ):
        self.client = BatchRpcClient(provider_uri, timeout=timeout)
        self.max_workers = max_workers
        self.verbosity = verbosity
        self.resolve_inputs = resolve_inputs
        self.fail_on_unresolved_inputs = fail_on_unresolved_inputs
        # Network code (btc/ltc/bch/zec) selects address version bytes when a
        # node omits the address for a P2PK output (see _p2pk_address_from_script).
        self.network = network
        # Cumulative output cache across batches for input resolution.
        # Maps tx_hash → {output_index → {value, addresses, type, script_hex}}.
        # When blocks are processed in small batches, inputs in batch N may
        # reference outputs from batch M<N. The cache ensures those outputs
        # are available without requiring getrawtransaction (which needs txindex).
        self._output_cache = {}

    def get_current_block_number(self):
        """Get the current block height from the node."""
        return self.client.make_request("getblockcount", [])

    def _fetch_all_hashes(self, block_numbers):
        """Batch getblockhash for all block numbers in one RPC call."""
        rpc_requests = [
            {
                "jsonrpc": "2.0",
                "method": "getblockhash",
                "params": [bn],
                "id": bn,
            }
            for bn in block_numbers
        ]
        results = self.client.make_batch_request(rpc_requests)
        result_map = {r["id"]: r for r in results}

        hashes = []
        for bn in block_numbers:
            r = result_map.get(bn)
            if r is None:
                raise ValueError(f"Missing response for getblockhash({bn})")
            if "error" in r and r["error"] is not None:
                raise ValueError(f"RPC error for getblockhash({bn}): {r['error']}")
            hashes.append(r["result"])
        return hashes

    def _fetch_single_block(self, block_hash):
        """Fetch a single block via getblock(hash, verbosity) and parse it.

        Returns (block, txs, rpc_seconds, parse_seconds).
        """
        rpc_request = {
            "jsonrpc": "2.0",
            "method": "getblock",
            "params": [block_hash, self.verbosity],
            "id": 0,
        }
        t0 = time.monotonic()
        results = self.client.make_batch_request([rpc_request])
        t_rpc = time.monotonic() - t0

        r = results[0]
        if "error" in r and r["error"] is not None:
            raise ValueError(f"RPC error for getblock({block_hash}): {r['error']}")
        raw_block = r["result"]
        if raw_block is None:
            raise ValueError(f"Block not found for hash {block_hash}")

        t0 = time.monotonic()
        block, txs = _parse_btc_block_and_txs(raw_block, self.network)
        t_parse = time.monotonic() - t0

        return block, txs, t_rpc, t_parse

    def _batch_getrawtransaction(self, tx_hashes):
        """Fetch decoded transactions via batched getrawtransaction calls.

        Returns dict mapping tx_hash → {output_index → {value, addresses, type}}.
        """
        hashes = list(tx_hashes)
        chunks = [
            hashes[i : i + _GETRAWTX_BATCH_SIZE]
            for i in range(0, len(hashes), _GETRAWTX_BATCH_SIZE)
        ]

        output_map = {}

        def fetch_chunk(chunk):
            rpc_requests = [
                {
                    "jsonrpc": "2.0",
                    "method": "getrawtransaction",
                    "params": [h, 1],
                    "id": idx,
                }
                for idx, h in enumerate(chunk)
            ]
            results = self.client.make_batch_request(rpc_requests)
            chunk_map = {}
            for r in results:
                if r.get("error") or not r.get("result"):
                    tx_hash = (
                        chunk[r["id"]]
                        if r.get("id") is not None and r["id"] < len(chunk)
                        else "unknown"
                    )
                    logger.warning(
                        f"getrawtransaction failed for {tx_hash}: "
                        f"error={r.get('error')}"
                    )
                    continue
                raw_tx = r["result"]
                by_index = {}
                for vout in raw_tx.get("vout", []):
                    parsed = _parse_output(vout, self.network)
                    by_index[parsed["index"]] = _make_cache_entry(
                        parsed["value"],
                        parsed["addresses"],
                        parsed["type"],
                        parsed.get("script_hex"),
                    )
                chunk_map[raw_tx["txid"]] = by_index
            return chunk_map

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(fetch_chunk, c) for c in chunks]
            for f in as_completed(futures):
                output_map.update(f.result())

        return output_map

    def _resolve_unresolved_inputs(self, transactions):
        """Resolve inputs that lack value/addresses (verbosity 2).

        Uses a three-tier resolution strategy:
        1. Cumulative output cache from previous batches (free).
        2. Within-batch outputs (free).
        3. Batch getrawtransaction for remaining unknowns (requires txindex).

        The cumulative cache (``self._output_cache``) persists across calls so
        that outputs from earlier batches are available when blocks are
        processed in small chunks.
        """
        # Phase 1: add current batch outputs to cumulative cache
        for tx in transactions:
            by_index = {}
            for out in tx["outputs"]:
                by_index[out["index"]] = _make_cache_entry(
                    out["value"],
                    out["addresses"],
                    out["type"],
                    out.get("script_hex"),
                )
            self._output_cache[tx["hash"]] = by_index

        # Collect unresolved spent tx hashes not in cache
        unresolved = set()
        for tx in transactions:
            for inp in tx["inputs"]:
                sth = inp["spent_transaction_hash"]
                if inp["value"] is None and sth and sth not in self._output_cache:
                    unresolved.add(sth)

        # Phase 2: fetch remaining from node (needs txindex)
        t0 = time.monotonic()
        if unresolved:
            fetched = self._batch_getrawtransaction(unresolved)
            self._output_cache.update(fetched)
        t_fetch = time.monotonic() - t0

        # Phase 3: apply resolution, evict spent outputs, recompute values
        n_resolved = 0
        spent_keys = []  # (tx_hash, output_index) to evict after resolution
        for tx in transactions:
            changed = False
            for inp in tx["inputs"]:
                if inp["value"] is None and inp["spent_transaction_hash"]:
                    sth = inp["spent_transaction_hash"]
                    idx = inp["spent_output_index"]
                    resolved = self._output_cache.get(sth, {}).get(idx)
                    if resolved:
                        inp["value"] = resolved[_CE_VALUE]
                        inp["addresses"] = resolved[_CE_ADDRESSES]
                        inp["type"] = resolved[_CE_TYPE]
                        if resolved[_CE_SCRIPT_HEX]:
                            inp["prevout_script_hex"] = resolved[_CE_SCRIPT_HEX]
                        spent_keys.append((sth, idx))
                        changed = True
                        n_resolved += 1
            if changed:
                input_value = sum(
                    i["value"] for i in tx["inputs"] if i["value"] is not None
                )
                tx["input_value"] = input_value
                if not tx["is_coinbase"]:
                    tx["fee"] = input_value - tx["output_value"]

        # Evict spent outputs to bound cache to UTXO set size.
        # Each output can only be spent once, so removing it is safe.
        for sth, idx in spent_keys:
            by_idx = self._output_cache.get(sth)
            if by_idx is not None:
                by_idx.pop(idx, None)
                if not by_idx:
                    del self._output_cache[sth]

        # Trim cache if it exceeds the max size to prevent unbounded growth.
        if len(self._output_cache) > _MAX_OUTPUT_CACHE_ENTRIES:
            excess = len(self._output_cache) - _MAX_OUTPUT_CACHE_ENTRIES // 2
            keys_to_remove = list(self._output_cache.keys())[:excess]
            for k in keys_to_remove:
                del self._output_cache[k]
            logger.warning(
                f"Output cache exceeded {_MAX_OUTPUT_CACHE_ENTRIES} entries, "
                f"trimmed {excess} oldest entries"
            )

        # Inputs still unresolved after all three phases keep value/addresses =
        # None. Writing them null silently corrupts fees, balances, address
        # relations and clustering downstream, so fail fast by default (see
        # fail_on_unresolved_inputs). A null input almost always means the node is
        # missing txindex=1 or is pruned. Typically the underlying getrawtransaction
        # failure was already logged in _batch_getrawtransaction.
        unresolved_inputs = [
            (tx["hash"], inp["spent_transaction_hash"], inp["spent_output_index"])
            for tx in transactions
            for inp in tx["inputs"]
            if inp["value"] is None and inp["spent_transaction_hash"]
        ]
        if unresolved_inputs:
            tx_hash, sth, idx = unresolved_inputs[0]
            detail = (
                f"{len(unresolved_inputs)} spent input(s) could not be resolved "
                f"after cache + getrawtransaction (e.g. tx {tx_hash} "
                f"spends {sth}:{idx})"
            )
            if self.fail_on_unresolved_inputs:
                raise RuntimeError(
                    f"{detail}. Their value/addresses would be null, corrupting "
                    "fees/balances/relations/clustering downstream. The node is "
                    "likely missing txindex=1 or is pruned. Set config "
                    "fail_on_unresolved_inputs=false to write null inputs anyway, "
                    "or fill_unresolved_inputs=true to fill dummy values."
                )
            logger.warning(
                "%s; writing them with null value/addresses "
                "(input_value/fee understated).",
                detail,
            )

        logger.info(
            f"[source-timing] input resolution: {len(unresolved)} txs fetched "
            f"in {t_fetch:.2f}s, {n_resolved} inputs resolved, "
            f"cache size: {len(self._output_cache)} txs"
        )

    def export_blocks_and_transactions(self, start_block, end_block):
        """Export blocks and transactions for a block range.

        Returns (blocks, transactions) with dict format identical to
        bitcoin-etl's BtcBlockMapper and BtcTransactionMapper output.
        """
        t0 = time.monotonic()
        block_numbers = list(range(start_block, end_block + 1))

        # Phase 1: fetch all block hashes in one batch (fast)
        hashes = self._fetch_all_hashes(block_numbers)

        # Phase 2: fetch each block individually for maximum node parallelism
        all_blocks_by_num = {}
        all_txs_by_block = {}
        total_rpc_s = 0.0
        total_parse_s = 0.0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_single_block, bh): bn
                for bn, bh in zip(block_numbers, hashes)
            }
            for future in as_completed(futures):
                block, txs, t_rpc, t_parse = future.result()
                total_rpc_s += t_rpc
                total_parse_s += t_parse
                all_blocks_by_num[block["number"]] = block
                for tx in txs:
                    all_txs_by_block.setdefault(tx["block_number"], []).append(tx)

        # Reassemble in block order
        t0_reassemble = time.monotonic()
        blocks = []
        transactions = []
        for bn in block_numbers:
            blocks.append(all_blocks_by_num[bn])
            block_txs = all_txs_by_block.get(bn, [])
            block_txs.sort(key=lambda t: t["index"])
            transactions.extend(block_txs)
        t_reassemble = time.monotonic() - t0_reassemble

        t_blocks = time.monotonic() - t0

        # Phase 3: resolve unresolved inputs via getrawtransaction
        # (only when verbosity < 3 and not using Cassandra resolver)
        t_resolve = 0.0
        if self.resolve_inputs and self.verbosity < 3 and transactions:
            t0_resolve = time.monotonic()
            self._resolve_unresolved_inputs(transactions)
            t_resolve = time.monotonic() - t0_resolve

        dt = t_blocks + t_resolve
        n_blocks = len(block_numbers)
        resolve_str = f"  resolve={t_resolve:.2f}s" if t_resolve > 0 else ""
        blks_per_sec = f"{n_blocks / dt:.1f}" if dt > 0 else "inf"
        logger.info(
            f"[source-timing] UTXO {n_blocks} blocks ({start_block}-{end_block}): "
            f"total={dt:.2f}s  fetch={t_blocks:.2f}s  "
            f"rpc_sum={total_rpc_s:.2f}s  parse_sum={total_parse_s:.2f}s  "
            f"reassemble={t_reassemble:.3f}s{resolve_str}  "
            f"{len(transactions)} txs  "
            f"({blks_per_sec} blk/s)"
        )

        return blocks, transactions
