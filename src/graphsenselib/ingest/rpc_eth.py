"""Fast JSON-RPC based block, transaction, receipt, and log exporters.

Replaces ethereum-etl's ExportBlocksJob and ExportReceiptsJob with direct
batch JSON-RPC calls. Output dict format is identical to ethereum-etl's mappers.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import orjson
import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utility converters (matching ethereum-etl's utils)
# ---------------------------------------------------------------------------


def hex_to_dec(hex_string):
    """Convert hex string to int. Returns None for None, passes through ints."""
    if hex_string is None:
        return None
    if isinstance(hex_string, int):
        return hex_string
    return int(hex_string, 16)


def to_normalized_address(address):
    """Lowercase an address. Returns None for None, non-strings pass through."""
    if address is None:
        return None
    if not isinstance(address, str):
        return address
    return address.lower()


def to_float_or_none(val):
    """Convert to float, or None if val is None."""
    if val is None:
        return None
    return float(val)


# ---------------------------------------------------------------------------
# BatchRpcClient
# ---------------------------------------------------------------------------


class BatchRpcClient:
    """Thread-safe JSON-RPC batch client using requests.Session."""

    def __init__(self, provider_uri, timeout=600):
        self.provider_uri = provider_uri
        self.timeout = timeout
        self._local = threading.local()

    def _get_session(self):
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def _reset_session(self):
        """Close and discard the current thread-local session."""
        session = getattr(self._local, "session", None)
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
            del self._local.session

    def _is_connection_error(self, exc):
        """Return True if the exception indicates a broken/incomplete connection."""
        from requests.exceptions import ConnectionError, ChunkedEncodingError

        if isinstance(exc, (ConnectionError, ChunkedEncodingError)):
            return True
        # IncompleteRead surfaces inside ConnectionError or on its own
        exc_str = str(exc)
        if "IncompleteRead" in exc_str or "Connection broken" in exc_str:
            return True
        return False

    def make_batch_request(self, rpc_requests, max_retries=5):
        """POST a JSON-RPC batch and return list of responses."""
        session = self._get_session()
        last_error: Exception = Exception("no retries attempted")
        for attempt in range(max_retries):
            try:
                response = session.post(
                    self.provider_uri,
                    data=orjson.dumps(rpc_requests),
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                result = orjson.loads(response.content)
                if not isinstance(result, list):
                    result = [result]
                return result
            except Exception as e:
                last_error = e
                if self._is_connection_error(e):
                    self._reset_session()
                    session = self._get_session()
                if attempt < max_retries - 1:
                    wait = min(2**attempt, 30)
                    logger.warning(
                        f"Batch RPC retry {attempt + 1}/{max_retries}: {e}. "
                        f"Waiting {wait}s."
                    )
                    time.sleep(wait)
        raise last_error

    def make_request(self, method, params, max_retries=5):
        """Single JSON-RPC call with retries. Returns the 'result' field."""
        session = self._get_session()
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }
        last_error: Exception = Exception("no retries attempted")
        for attempt in range(max_retries):
            try:
                response = session.post(
                    self.provider_uri,
                    data=orjson.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = orjson.loads(response.content)
                if data.get("error") is not None:
                    raise ValueError(f"RPC error for {method}: {data['error']}")
                return data["result"]
            except ValueError:
                raise  # RPC-level errors are not retryable
            except Exception as e:
                last_error = e
                if self._is_connection_error(e):
                    self._reset_session()
                    session = self._get_session()
                if attempt < max_retries - 1:
                    wait = min(2**attempt, 30)
                    logger.warning(
                        f"RPC retry {attempt + 1}/{max_retries} for {method}: "
                        f"{e}. Waiting {wait}s."
                    )
                    time.sleep(wait)
        raise last_error

    def get_latest_block_number(self):
        """eth_blockNumber -> int."""
        result = self.make_request("eth_blockNumber", [])
        return int(result, 16)


# ---------------------------------------------------------------------------
# Field validation at parse level
# ---------------------------------------------------------------------------


def validate_rpc_fields(json_keys, known_keys, blacklist, context):
    """Raise if the RPC response contains fields we neither parse nor blacklist.

    Parameters
    ----------
    json_keys : iterable of str
        Keys present in the raw JSON-RPC response object.
    known_keys : frozenset of str
        camelCase keys that the parse function actively reads.
    blacklist : frozenset of str
        camelCase keys we intentionally ignore (e.g. derived / redundant fields).
    context : str
        Human-readable label used in the error message (e.g. "block", "transaction").
    """
    unknown = set(json_keys) - known_keys - blacklist
    if unknown:
        raise ValueError(
            f"Unknown RPC fields {sorted(unknown)} in {context}. "
            f"Add them to the parser (known_keys) or to the field blacklist."
        )


# -- Block -------------------------------------------------------------------

_BLOCK_KNOWN_KEYS = frozenset(
    {
        "number",
        "hash",
        "parentHash",
        "nonce",
        "sha3Uncles",
        "logsBloom",
        "transactionsRoot",
        "stateRoot",
        "receiptsRoot",
        "miner",
        "difficulty",
        "totalDifficulty",
        "size",
        "extraData",
        "gasLimit",
        "gasUsed",
        "timestamp",
        "transactions",
        "baseFeePerGas",
        "withdrawalsRoot",
        "withdrawals",
        "blobGasUsed",
        "excessBlobGas",
        "parentBeaconBlockRoot",
        "uncles",
        "requestsHash",
    }
)

_BLOCK_BLACKLIST = frozenset(
    {
        "mixHash",  # PoW-era, post-merge repurposed as prevRandao
    }
)

# -- Transaction -------------------------------------------------------------

_TX_KNOWN_KEYS = frozenset(
    {
        "hash",
        "nonce",
        "blockHash",
        "blockNumber",
        "transactionIndex",
        "from",
        "to",
        "value",
        "gas",
        "gasPrice",
        "input",
        "maxFeePerGas",
        "maxPriorityFeePerGas",
        "type",
        "maxFeePerBlobGas",
        "blobVersionedHashes",
        "v",
        "yParity",
        "r",
        "s",
        "accessList",
        "authorizationList",
    }
)

_TX_BLACKLIST = frozenset(
    {
        "chainId",  # EIP-155, always 0x1 on mainnet
    }
)

# -- Receipt -----------------------------------------------------------------

_RECEIPT_KNOWN_KEYS = frozenset(
    {
        "transactionHash",
        "transactionIndex",
        "blockHash",
        "blockNumber",
        "cumulativeGasUsed",
        "gasUsed",
        "contractAddress",
        "root",
        "status",
        "effectiveGasPrice",
        "l1Fee",
        "l1GasUsed",
        "l1GasPrice",
        "l1FeeScalar",
        "blobGasPrice",
        "blobGasUsed",
    }
)

_RECEIPT_BLACKLIST = frozenset(
    {
        "from",  # redundant with transaction
        "to",  # redundant with transaction
        "type",  # tx type, redundant with transaction
        "logs",  # parsed separately via parse_log_json
        "logsBloom",  # bloom filter, not stored
    }
)

# -- Log ---------------------------------------------------------------------

_LOG_KNOWN_KEYS = frozenset(
    {
        "logIndex",
        "transactionHash",
        "transactionIndex",
        "blockHash",
        "blockNumber",
        "address",
        "data",
        "topics",
    }
)

_LOG_BLACKLIST = frozenset(
    {
        "type",  # always "log", not useful
        "removed",  # reorg flag, not stored
        "blockTimestamp",  # redundant with block-level timestamp
    }
)

# -- Withdrawal (nested in block) -------------------------------------------

_WITHDRAWAL_KNOWN_KEYS = frozenset(
    {
        "index",
        "validatorIndex",
        "address",
        "amount",
    }
)

_WITHDRAWAL_BLACKLIST = frozenset()


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_block_json(json_block):
    """Convert eth_getBlockByNumber JSON response to dict matching
    ethereum-etl's EthBlockMapper.block_to_dict() output."""
    validate_rpc_fields(json_block.keys(), _BLOCK_KNOWN_KEYS, _BLOCK_BLACKLIST, "block")
    transactions = json_block.get("transactions") or []

    withdrawals_raw = json_block.get("withdrawals") or []
    withdrawals = []
    for w in withdrawals_raw:
        validate_rpc_fields(
            w.keys(), _WITHDRAWAL_KNOWN_KEYS, _WITHDRAWAL_BLACKLIST, "withdrawal"
        )
        withdrawals.append(
            {
                "index": hex_to_dec(w.get("index")),
                "validator_index": hex_to_dec(w.get("validatorIndex")),
                "address": w.get("address"),
                "amount": hex_to_dec(w.get("amount")),
            }
        )

    return {
        "type": "block",
        "number": hex_to_dec(json_block.get("number")),
        "hash": json_block.get("hash"),
        "parent_hash": json_block.get("parentHash"),
        "nonce": json_block.get("nonce"),
        "sha3_uncles": json_block.get("sha3Uncles"),
        "logs_bloom": json_block.get("logsBloom"),
        "transactions_root": json_block.get("transactionsRoot"),
        "state_root": json_block.get("stateRoot"),
        "receipts_root": json_block.get("receiptsRoot"),
        "miner": to_normalized_address(json_block.get("miner")),
        "difficulty": hex_to_dec(json_block.get("difficulty")),
        "total_difficulty": hex_to_dec(json_block.get("totalDifficulty")),
        "size": hex_to_dec(json_block.get("size")),
        "extra_data": json_block.get("extraData"),
        "gas_limit": hex_to_dec(json_block.get("gasLimit")),
        "gas_used": hex_to_dec(json_block.get("gasUsed")),
        "timestamp": hex_to_dec(json_block.get("timestamp")),
        "transaction_count": len(transactions),
        "base_fee_per_gas": hex_to_dec(json_block.get("baseFeePerGas")),
        "withdrawals_root": json_block.get("withdrawalsRoot"),
        "withdrawals": withdrawals,
        "blob_gas_used": hex_to_dec(json_block.get("blobGasUsed")),
        "excess_blob_gas": hex_to_dec(json_block.get("excessBlobGas")),
        "parent_beacon_block_root": json_block.get("parentBeaconBlockRoot"),
        "uncles": json_block.get("uncles") or [],
        "requests_hash": json_block.get("requestsHash"),
    }


def parse_transaction_json(json_tx, block_timestamp):
    """Convert transaction from eth_getBlockByNumber(bn, true) response to
    dict matching ethereum-etl's EthTransactionMapper.transaction_to_dict()."""
    validate_rpc_fields(json_tx.keys(), _TX_KNOWN_KEYS, _TX_BLACKLIST, "transaction")
    return {
        "type": "transaction",
        "hash": json_tx.get("hash"),
        "nonce": hex_to_dec(json_tx.get("nonce")),
        "block_hash": json_tx.get("blockHash"),
        "block_number": hex_to_dec(json_tx.get("blockNumber")),
        "block_timestamp": block_timestamp,
        "transaction_index": hex_to_dec(json_tx.get("transactionIndex")),
        "from_address": to_normalized_address(json_tx.get("from")),
        "to_address": to_normalized_address(json_tx.get("to")),
        "value": hex_to_dec(json_tx.get("value")),
        "gas": hex_to_dec(json_tx.get("gas")),
        "gas_price": hex_to_dec(json_tx.get("gasPrice")),
        "input": json_tx.get("input"),
        "max_fee_per_gas": hex_to_dec(json_tx.get("maxFeePerGas")),
        "max_priority_fee_per_gas": hex_to_dec(json_tx.get("maxPriorityFeePerGas")),
        "transaction_type": hex_to_dec(json_tx.get("type")),
        "max_fee_per_blob_gas": hex_to_dec(json_tx.get("maxFeePerBlobGas")),
        "blob_versioned_hashes": json_tx.get("blobVersionedHashes") or [],
        "v": hex_to_dec(json_tx.get("v")),
        "y_parity": hex_to_dec(json_tx.get("yParity")),
        "r": hex_to_dec(json_tx.get("r")),
        "s": hex_to_dec(json_tx.get("s")),
        "access_list": json_tx.get("accessList") or [],
        "authorization_list": json_tx.get("authorizationList") or [],
    }


def parse_receipt_json(json_receipt):
    """Convert eth_getTransactionReceipt response to dict matching
    ethereum-etl's EthReceiptMapper.receipt_to_dict()."""
    validate_rpc_fields(
        json_receipt.keys(), _RECEIPT_KNOWN_KEYS, _RECEIPT_BLACKLIST, "receipt"
    )
    return {
        "type": "receipt",
        "transaction_hash": json_receipt.get("transactionHash"),
        "transaction_index": hex_to_dec(json_receipt.get("transactionIndex")),
        "block_hash": json_receipt.get("blockHash"),
        "block_number": hex_to_dec(json_receipt.get("blockNumber")),
        "cumulative_gas_used": hex_to_dec(json_receipt.get("cumulativeGasUsed")),
        "gas_used": hex_to_dec(json_receipt.get("gasUsed")),
        "contract_address": to_normalized_address(json_receipt.get("contractAddress")),
        "root": json_receipt.get("root"),
        "status": hex_to_dec(json_receipt.get("status")),
        "effective_gas_price": hex_to_dec(json_receipt.get("effectiveGasPrice")),
        "l1_fee": hex_to_dec(json_receipt.get("l1Fee")),
        "l1_gas_used": hex_to_dec(json_receipt.get("l1GasUsed")),
        "l1_gas_price": hex_to_dec(json_receipt.get("l1GasPrice")),
        "l1_fee_scalar": to_float_or_none(json_receipt.get("l1FeeScalar")),
        "blob_gas_price": hex_to_dec(json_receipt.get("blobGasPrice")),
        "blob_gas_used": hex_to_dec(json_receipt.get("blobGasUsed")),
    }


def parse_log_json(json_log):
    """Convert log from receipt response to dict matching
    ethereum-etl's EthReceiptLogMapper.receipt_log_to_dict()."""
    validate_rpc_fields(json_log.keys(), _LOG_KNOWN_KEYS, _LOG_BLACKLIST, "log")
    return {
        "type": "log",
        "log_index": hex_to_dec(json_log.get("logIndex")),
        "transaction_hash": json_log.get("transactionHash"),
        "transaction_index": hex_to_dec(json_log.get("transactionIndex")),
        "block_hash": json_log.get("blockHash"),
        "block_number": hex_to_dec(json_log.get("blockNumber")),
        "address": json_log.get("address"),
        "data": json_log.get("data"),
        "topics": json_log.get("topics") or [],
    }


# ---------------------------------------------------------------------------
# Transaction enrichment
# ---------------------------------------------------------------------------

_RECEIPT_FIELDS = [
    ("cumulative_gas_used", "receipt_cumulative_gas_used"),
    ("gas_used", "receipt_gas_used"),
    ("contract_address", "receipt_contract_address"),
    ("root", "receipt_root"),
    ("status", "receipt_status"),
    ("effective_gas_price", "receipt_effective_gas_price"),
    ("l1_fee", "receipt_l1_fee"),
    ("l1_gas_used", "receipt_l1_gas_used"),
    ("l1_gas_price", "receipt_l1_gas_price"),
    ("l1_fee_scalar", "receipt_l1_fee_scalar"),
    ("blob_gas_price", "receipt_blob_gas_price"),
    ("blob_gas_used", "receipt_blob_gas_used"),
]


def enrich_transactions(transactions, receipts):
    """Join transactions with receipts by hash, adding receipt_* fields.

    Replaces ethereumetl.streaming.enrich.enrich_transactions().
    Unlike the ethereumetl version, this preserves all original tx fields
    (including v, r, s).
    """
    receipt_by_hash = {r["transaction_hash"]: r for r in receipts}

    result = []
    for tx in transactions:
        receipt = receipt_by_hash.get(tx["hash"])
        if receipt is None:
            raise ValueError(f"Receipt not found for transaction {tx['hash']}")
        for src_field, dst_field in _RECEIPT_FIELDS:
            tx[dst_field] = receipt.get(src_field)
        result.append(tx)

    if len(result) != len(transactions):
        raise ValueError(
            f"Transaction count mismatch: {len(result)} enriched vs "
            f"{len(transactions)} input"
        )
    return result


# ---------------------------------------------------------------------------
# Exporters
# ---------------------------------------------------------------------------


class BlockExporter:
    """Export blocks and transactions via batch eth_getBlockByNumber calls."""

    def __init__(self, client, batch_size=50, max_workers=20):
        self.client = client
        self.batch_size = batch_size
        self.max_workers = max_workers

    def _fetch_batch(self, block_numbers):
        """Fetch a batch of blocks with full transactions."""
        rpc_requests = [
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(bn), True],
                "id": bn,
            }
            for bn in block_numbers
        ]
        results = self.client.make_batch_request(rpc_requests)
        result_map = {r["id"]: r for r in results}

        blocks = []
        transactions = []
        for bn in block_numbers:
            r = result_map.get(bn)
            if r is None:
                raise ValueError(f"Missing response for block {bn}")
            if r.get("error") is not None:
                raise ValueError(f"RPC error for block {bn}: {r['error']}")
            json_block = r["result"]
            if json_block is None:
                raise ValueError(f"Block {bn} not found")

            block = parse_block_json(json_block)
            block_timestamp = block["timestamp"]

            raw_txs = json_block.get("transactions") or []
            for json_tx in raw_txs:
                transactions.append(parse_transaction_json(json_tx, block_timestamp))
            blocks.append(block)

        return blocks, transactions

    def _fetch_batch_headers(self, block_numbers):
        """Fetch block headers without transactions (detailed=false)."""
        rpc_requests = [
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(bn), False],
                "id": bn,
            }
            for bn in block_numbers
        ]
        results = self.client.make_batch_request(rpc_requests)
        result_map = {r["id"]: r for r in results}

        blocks = []
        for bn in block_numbers:
            r = result_map.get(bn)
            if r is None:
                raise ValueError(f"Missing response for block {bn}")
            if r.get("error") is not None:
                raise ValueError(f"RPC error for block {bn}: {r['error']}")
            json_block = r["result"]
            if json_block is None:
                raise ValueError(f"Block {bn} not found")
            blocks.append(parse_block_json(json_block))
        return blocks

    def export_blocks_and_transactions(self, start_block, end_block):
        """Export blocks and transactions for a block range.

        Returns (blocks, transactions) matching the interface of
        AccountStreamerAdapter.export_blocks_and_transactions().
        """
        block_numbers = list(range(start_block, end_block + 1))

        batches = [
            block_numbers[i : i + self.batch_size]
            for i in range(0, len(block_numbers), self.batch_size)
        ]

        all_blocks_by_num = {}
        all_txs_by_block = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_batch, batch_bns): batch_bns
                for batch_bns in batches
            }
            for future in as_completed(futures):
                batch_blocks, batch_txs = future.result()
                for b in batch_blocks:
                    all_blocks_by_num[b["number"]] = b
                for tx in batch_txs:
                    all_txs_by_block.setdefault(tx["block_number"], []).append(tx)

        # Reassemble in block order
        blocks = []
        transactions = []
        for bn in range(start_block, end_block + 1):
            blocks.append(all_blocks_by_num[bn])
            block_txs = all_txs_by_block.get(bn, [])
            block_txs.sort(key=lambda t: t["transaction_index"])
            transactions.extend(block_txs)

        return blocks, transactions

    def export_block_headers(self, start_block, end_block):
        """Export block headers (without transactions) for a block range.

        Uses detailed=false which is ~10x faster than detailed=true for
        blocks with many transactions (e.g., 0.07s vs 3.26s for 100 Tron blocks).
        """
        block_numbers = list(range(start_block, end_block + 1))

        batches = [
            block_numbers[i : i + self.batch_size]
            for i in range(0, len(block_numbers), self.batch_size)
        ]

        all_blocks_by_num = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_batch_headers, batch_bns): batch_bns
                for batch_bns in batches
            }
            for future in as_completed(futures):
                batch_blocks = future.result()
                for b in batch_blocks:
                    all_blocks_by_num[b["number"]] = b

        return [all_blocks_by_num[bn] for bn in range(start_block, end_block + 1)]


class ReceiptExporter:
    """Export receipts and logs via batch eth_getTransactionReceipt calls."""

    def __init__(self, client, batch_size=50, max_workers=20):
        self.client = client
        self.batch_size = batch_size
        self.max_workers = max_workers

    def _fetch_batch(self, tx_hashes):
        """Fetch receipts for a batch of transaction hashes."""
        rpc_requests = [
            {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
                "id": idx,
            }
            for idx, tx_hash in enumerate(tx_hashes)
        ]
        results = self.client.make_batch_request(rpc_requests)
        result_map = {r["id"]: r for r in results}

        receipts = []
        logs = []
        for idx, tx_hash in enumerate(tx_hashes):
            r = result_map.get(idx)
            if r is None:
                raise ValueError(f"Missing response for receipt {tx_hash}")
            if r.get("error") is not None:
                raise ValueError(f"RPC error for receipt {tx_hash}: {r['error']}")
            json_receipt = r["result"]
            if json_receipt is None:
                raise ValueError(f"Receipt not found for tx {tx_hash}")

            receipt = parse_receipt_json(json_receipt)
            receipts.append(receipt)

            for json_log in json_receipt.get("logs") or []:
                logs.append(parse_log_json(json_log))

        return receipts, logs

    def export_receipts_and_logs(self, transaction_hashes):
        """Export receipts and logs for given transaction hashes.

        Returns (receipts, logs) matching the interface of
        AccountStreamerAdapter.export_receipts_and_logs().
        """
        tx_hashes = list(transaction_hashes)

        batches = [
            tx_hashes[i : i + self.batch_size]
            for i in range(0, len(tx_hashes), self.batch_size)
        ]

        all_receipts = []
        all_logs = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_batch, batch_hashes): batch_hashes
                for batch_hashes in batches
            }
            for future in as_completed(futures):
                batch_receipts, batch_logs = future.result()
                all_receipts.extend(batch_receipts)
                all_logs.extend(batch_logs)

        # Sort receipts by (block_number, transaction_index)
        all_receipts.sort(key=lambda r: (r["block_number"], r["transaction_index"]))
        # Sort logs by (block_number, log_index)
        all_logs.sort(key=lambda lg: (lg["block_number"], lg["log_index"]))

        return all_receipts, all_logs


class BlockReceiptExporter:
    """Export receipts and logs via batch eth_getBlockReceipts calls.

    Uses 1 RPC call per block instead of 1 per transaction, which is
    significantly faster for blocks with many transactions.
    """

    def __init__(self, client, batch_size=20, max_workers=10):
        self.client = client
        self.batch_size = batch_size
        self.max_workers = max_workers

    def _fetch_batch(self, block_numbers):
        """Fetch receipts for a batch of blocks."""
        rpc_requests = [
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockReceipts",
                "params": [hex(bn)],
                "id": bn,
            }
            for bn in block_numbers
        ]
        results = self.client.make_batch_request(rpc_requests)
        result_map = {r["id"]: r for r in results}

        receipts = []
        logs = []
        for bn in block_numbers:
            r = result_map.get(bn)
            if r is None:
                raise ValueError(f"Missing response for block receipts {bn}")
            if r.get("error") is not None:
                raise ValueError(f"RPC error for block receipts {bn}: {r['error']}")
            block_receipts = r["result"]
            if block_receipts is None:
                # Empty block or block not found — treat as no receipts
                continue

            for json_receipt in block_receipts:
                receipt = parse_receipt_json(json_receipt)
                receipts.append(receipt)

                for json_log in json_receipt.get("logs") or []:
                    logs.append(parse_log_json(json_log))

        return receipts, logs

    def export_receipts_and_logs(self, start_block, end_block):
        """Export receipts and logs for a block range.

        Returns (receipts, logs) with the same dict format as
        ReceiptExporter.export_receipts_and_logs().
        """
        block_numbers = list(range(start_block, end_block + 1))

        batches = [
            block_numbers[i : i + self.batch_size]
            for i in range(0, len(block_numbers), self.batch_size)
        ]

        all_receipts = []
        all_logs = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_batch, batch_bns): batch_bns
                for batch_bns in batches
            }
            for future in as_completed(futures):
                batch_receipts, batch_logs = future.result()
                all_receipts.extend(batch_receipts)
                all_logs.extend(batch_logs)

        # Sort receipts by (block_number, transaction_index)
        all_receipts.sort(key=lambda r: (r["block_number"], r["transaction_index"]))
        # Sort logs by (block_number, log_index)
        all_logs.sort(key=lambda lg: (lg["block_number"], lg["log_index"]))

        return all_receipts, all_logs


# ---------------------------------------------------------------------------
# Block range for date (replaces EthService.get_block_range_for_date)
# ---------------------------------------------------------------------------


def get_block_range_for_date(client, target_date):
    """Binary search for (start_block, end_block) of a calendar date.

    Replaces ethereumetl's EthService.get_block_range_for_date().
    """
    if isinstance(target_date, datetime):
        target_date = target_date.date()

    # Create UTC boundaries for the target date
    start_dt = datetime(
        target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
    )
    end_dt = start_dt + timedelta(days=1)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    latest_block = client.get_latest_block_number()

    def get_block_timestamp(block_number):
        result = client.make_request("eth_getBlockByNumber", [hex(block_number), False])
        if result is None:
            return None
        return hex_to_dec(result.get("timestamp"))

    # Binary search for the first block with timestamp >= start_ts
    lo, hi = 0, latest_block
    while lo < hi:
        mid = (lo + hi) // 2
        ts = get_block_timestamp(mid)
        if ts is None or ts < start_ts:
            lo = mid + 1
        else:
            hi = mid
    start_block = lo

    # Binary search for the last block with timestamp < end_ts
    lo, hi = start_block, latest_block
    while lo < hi:
        mid = (lo + hi + 1) // 2
        ts = get_block_timestamp(mid)
        if ts is None or ts < end_ts:
            lo = mid
        else:
            hi = mid - 1
    end_block = lo

    return start_block, end_block
