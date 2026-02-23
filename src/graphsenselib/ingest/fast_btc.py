"""Fast JSON-RPC based block and transaction exporter for UTXO chains.

Replaces bitcoin-etl's ExportBlocksJob with direct batch JSON-RPC calls.
Output dict format is identical to bitcoin-etl's mappers so that downstream
code (enrich_txs, prepare_blocks_inplace, prepare_transactions_inplace_parquet)
works without changes.

Supported chains: BTC, LTC, BCH, ZEC.
All use getblock(hash, verbosity=2) which returns blocks with full decoded
transaction data in a single RPC call — no separate getrawtransaction needed.
"""

import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from graphsenselib.ingest.fast_rpc import BatchRpcClient

logger = logging.getLogger(__name__)

# ZCash shielded address type (matches bitcoin-etl)
ADDRESS_TYPE_SHIELDED = "shielded"


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


def _parse_input(vin_entry, index):
    """Parse a single vin entry to input dict matching bitcoin-etl format."""
    script_sig = vin_entry.get("scriptSig") or {}
    return {
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


def _parse_output(vout_entry):
    """Parse a single vout entry to output dict matching bitcoin-etl format."""
    script_pub_key = vout_entry.get("scriptPubKey") or {}
    addresses = script_pub_key.get("addresses")
    if not addresses:
        a = script_pub_key.get("address")
        addresses = [a] if a else []

    output_type = script_pub_key.get("type")

    # Non-standard address detection (matching bitcoin-etl's _add_non_standard_addresses)
    if (not addresses) and output_type != "nulldata":
        output_type = "nonstandard"
        script_hex = script_pub_key.get("hex", "")
        addresses = [_script_hex_to_non_standard_address(script_hex)]

    return {
        "index": vout_entry.get("n"),
        "script_asm": script_pub_key.get("asm"),
        "script_hex": script_pub_key.get("hex"),
        "required_signatures": script_pub_key.get("reqSigs"),
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


def _parse_btc_block_and_txs(raw_block):
    """Parse a raw getblock(hash, 2) response into (block_dict, [tx_dicts]).

    Handles coinbase detection, value conversion, non-standard addresses,
    and ZCash shielded I/O. Output format matches bitcoin-etl's mapper output.
    """
    block_hash = raw_block["hash"]
    block_number = raw_block["height"]
    block_timestamp = raw_block["time"]

    raw_txs = raw_block.get("tx") or []
    coinbase_param = None
    txs = []

    for tx_index, raw_tx in enumerate(raw_txs):
        vin = raw_tx.get("vin") or []
        vout = raw_tx.get("vout") or []

        # Detect and handle coinbase
        is_coinbase = False
        inputs = []
        input_idx = 0
        for vin_entry in vin:
            if "coinbase" in vin_entry:
                coinbase_param = vin_entry["coinbase"]
                is_coinbase = True
            else:
                inputs.append(_parse_input(vin_entry, input_idx))
                input_idx += 1

        outputs = [_parse_output(v) for v in vout]

        # Calculate values (matching graphsense-bitcoin-etl behavior:
        # sum of empty list = 0, not None)
        output_value = sum(o["value"] for o in outputs if o["value"] is not None)
        input_value = sum(inp["value"] for inp in inputs if inp["value"] is not None)

        if is_coinbase:
            fee = 0
        else:
            fee = input_value - output_value

        # ZCash: joinsplit shielded I/O
        vjoinsplit = raw_tx.get("vjoinsplit")
        if vjoinsplit:
            for js in vjoinsplit:
                pub_input = _btc_to_satoshi(js.get("vpub_old")) or 0
                pub_output = _btc_to_satoshi(js.get("vpub_new")) or 0
                if pub_input > 0:
                    inputs.append(_make_shielded_input(len(inputs), pub_input))
                if pub_output > 0:
                    outputs.append(_make_shielded_output(len(outputs), pub_output))

        # ZCash: Sapling value balance
        value_balance = raw_tx.get("valueBalance")
        if value_balance is not None and value_balance != 0:
            value_balance_sat = _btc_to_satoshi(value_balance)
            if value_balance_sat > 0:
                inputs.append(_make_shielded_input(len(inputs), value_balance_sat))
            elif value_balance_sat < 0:
                outputs.append(_make_shielded_output(len(outputs), -value_balance_sat))

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


class FastBtcBlockExporter:
    """Export UTXO blocks and transactions via direct batch JSON-RPC.

    Replaces bitcoin-etl's ExportBlocksJob + BtcService for all UTXO chains
    (BTC, LTC, BCH, ZEC). Uses getblock(hash, verbosity=2) which returns
    blocks with full decoded transaction data in a single RPC call.

    Two-phase design:
      Phase 1: getblockhash for all blocks in one batch (fast, ~15ms).
      Phase 2: getblock for each block individually via ThreadPoolExecutor.

    Individual getblock requests (not batched) allow Bitcoin Core to
    parallelize across its RPC threads. Batched requests are processed
    sequentially within a single connection, which is slower for heavy blocks.
    """

    def __init__(self, provider_uri, max_workers=10, timeout=300):
        self.client = BatchRpcClient(provider_uri, timeout=timeout)
        self.max_workers = max_workers

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
        """Fetch a single block via getblock(hash, 2) and parse it."""
        rpc_request = {
            "jsonrpc": "2.0",
            "method": "getblock",
            "params": [block_hash, 2],
            "id": 0,
        }
        results = self.client.make_batch_request([rpc_request])
        r = results[0]
        if "error" in r and r["error"] is not None:
            raise ValueError(f"RPC error for getblock({block_hash}): {r['error']}")
        raw_block = r["result"]
        if raw_block is None:
            raise ValueError(f"Block not found for hash {block_hash}")
        return _parse_btc_block_and_txs(raw_block)

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

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_single_block, bh): bn
                for bn, bh in zip(block_numbers, hashes)
            }
            for future in as_completed(futures):
                block, txs = future.result()
                all_blocks_by_num[block["number"]] = block
                for tx in txs:
                    all_txs_by_block.setdefault(tx["block_number"], []).append(tx)

        # Reassemble in block order
        blocks = []
        transactions = []
        for bn in block_numbers:
            blocks.append(all_blocks_by_num[bn])
            block_txs = all_txs_by_block.get(bn, [])
            block_txs.sort(key=lambda t: t["index"])
            transactions.extend(block_txs)

        dt = time.monotonic() - t0
        n_blocks = len(block_numbers)
        logger.info(
            f"[source-timing] UTXO {n_blocks} blocks ({start_block}-{end_block}): "
            f"total={dt:.2f}s  {len(transactions)} txs  "
            f"({n_blocks / dt:.1f} blk/s)"
        )

        return blocks, transactions
