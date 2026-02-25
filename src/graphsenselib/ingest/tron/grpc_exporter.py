"""Combined gRPC exporter for Tron: all data from a single gRPC pass.

Replaces ALL HTTP data fetching (blocks, receipts) with gRPC, keeping only
lightweight HTTP block headers (for fields not in gRPC like logs_bloom).

Phase 1: GetBlockByNum2 for transactions + types
Phase 2: GetTransactionInfoByBlockNum for traces + fees + receipts + logs

Combined gRPC time: ~1.2s for 100 blocks with 30 workers.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from graphsenselib.ingest.tron.export_traces_job import decode_block_to_traces

logger = logging.getLogger(__name__)

try:
    import grpc

    from graphsenselib.ingest.tron.grpc.api.tron_api_pb2 import NumberMessage
    from graphsenselib.ingest.tron.grpc.api.tron_api_pb2_grpc import WalletStub
    from graphsenselib.ingest.tron.grpc.core import contract_pb2
    from graphsenselib.utils.grpc import get_channel

    _has_grpc = True
except ImportError:
    _has_grpc = False


# ---------------------------------------------------------------------------
# Address conversion
# ---------------------------------------------------------------------------


def _grpc_addr_to_hex(addr_bytes):
    """Convert Tron gRPC address bytes to 0x-prefixed hex string.

    gRPC returns 21-byte addresses with 0x41 prefix (Tron mainnet).
    JSON-RPC returns 20-byte EVM addresses. Strip the prefix to match.
    """
    if not addr_bytes:
        return None
    if len(addr_bytes) == 21 and addr_bytes[0] == 0x41:
        return "0x" + addr_bytes[1:].hex()
    if len(addr_bytes) == 20:
        return "0x" + addr_bytes.hex()
    # Unexpected length — return as-is
    return "0x" + addr_bytes.hex()


# ---------------------------------------------------------------------------
# Contract parameter decoding
# ---------------------------------------------------------------------------


def _extract_owner_address_generic(param_bytes):
    """Extract owner_address from raw protobuf bytes using wire format.

    Scans for the first length-delimited field that looks like a 21-byte
    Tron address (starts with 0x41). Works for all contract types including
    V2 types that don't have compiled protobuf classes.
    """
    pos = 0
    while pos < len(param_bytes):
        tag_byte = param_bytes[pos]
        wire_type = tag_byte & 0x07
        pos += 1

        if wire_type == 2:  # length-delimited
            length = 0
            shift = 0
            while pos < len(param_bytes):
                b = param_bytes[pos]
                pos += 1
                length |= (b & 0x7F) << shift
                if (b & 0x80) == 0:
                    break
                shift += 7

            if pos + length > len(param_bytes):
                break
            value = param_bytes[pos : pos + length]
            pos += length

            if length == 21 and value[0] == 0x41:
                return _grpc_addr_to_hex(value)

        elif wire_type == 0:  # varint
            while pos < len(param_bytes) and param_bytes[pos] & 0x80:
                pos += 1
            if pos < len(param_bytes):
                pos += 1
        elif wire_type == 5:  # 32-bit
            pos += 4
        elif wire_type == 1:  # 64-bit
            pos += 8
        else:
            break

    return None


def _extract_all_addresses_generic(param_bytes):
    """Extract all 21-byte Tron addresses from raw protobuf bytes.

    Returns list of (hex_address, field_number) tuples.
    Used for V2 contract types without compiled protobuf classes.
    """
    addresses = []
    pos = 0
    while pos < len(param_bytes):
        tag_byte = param_bytes[pos]
        field_number = tag_byte >> 3
        wire_type = tag_byte & 0x07
        pos += 1

        if wire_type == 2:  # length-delimited
            length = 0
            shift = 0
            while pos < len(param_bytes):
                b = param_bytes[pos]
                pos += 1
                length |= (b & 0x7F) << shift
                if (b & 0x80) == 0:
                    break
                shift += 7

            if pos + length > len(param_bytes):
                break
            value = param_bytes[pos : pos + length]
            pos += length

            if length == 21 and value[0] == 0x41:
                addresses.append((_grpc_addr_to_hex(value), field_number))

        elif wire_type == 0:  # varint
            while pos < len(param_bytes) and param_bytes[pos] & 0x80:
                pos += 1
            if pos < len(param_bytes):
                pos += 1
        elif wire_type == 5:  # 32-bit
            pos += 4
        elif wire_type == 1:  # 64-bit
            pos += 8
        else:
            break

    return addresses


def _decode_contract_param(contract):
    """Decode gRPC contract parameter to (from_address, to_address, value, input_data).

    Matches the output of the Tron JSON-RPC eth-compatibility layer.
    """
    type_name = contract.parameter.type_url.split("/")[-1]
    param_bytes = contract.parameter.value

    from_addr = None
    to_addr = None
    value = 0
    input_data = "0x"

    if type_name == "protocol.TransferContract":
        msg = contract_pb2.TransferContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        to_addr = _grpc_addr_to_hex(msg.to_address)
        value = msg.amount

    elif type_name == "protocol.TriggerSmartContract":
        msg = contract_pb2.TriggerSmartContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        to_addr = _grpc_addr_to_hex(msg.contract_address)
        value = msg.call_value
        input_data = ("0x" + msg.data.hex()) if msg.data else "0x"

    elif type_name == "protocol.TransferAssetContract":
        msg = contract_pb2.TransferAssetContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        to_addr = _grpc_addr_to_hex(msg.to_address)
        value = msg.amount

    elif type_name == "protocol.CreateSmartContract":
        msg = contract_pb2.CreateSmartContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)

    elif type_name == "protocol.AccountCreateContract":
        msg = contract_pb2.AccountCreateContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        to_addr = _grpc_addr_to_hex(msg.account_address)

    elif type_name == "protocol.FreezeBalanceContract":
        msg = contract_pb2.FreezeBalanceContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        to_addr = (
            _grpc_addr_to_hex(msg.receiver_address) if msg.receiver_address else None
        )
        value = msg.frozen_balance

    elif type_name == "protocol.UnfreezeBalanceContract":
        msg = contract_pb2.UnfreezeBalanceContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        to_addr = (
            _grpc_addr_to_hex(msg.receiver_address) if msg.receiver_address else None
        )

    elif type_name == "protocol.WithdrawBalanceContract":
        msg = contract_pb2.WithdrawBalanceContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)

    elif type_name == "protocol.VoteWitnessContract":
        msg = contract_pb2.VoteWitnessContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        if msg.votes:
            to_addr = _grpc_addr_to_hex(msg.votes[0].vote_address)
            value = msg.votes[0].vote_count

    elif type_name == "protocol.ExchangeTransactionContract":
        msg = contract_pb2.ExchangeTransactionContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)
        value = msg.quant

    elif type_name == "protocol.AccountPermissionUpdateContract":
        msg = contract_pb2.AccountPermissionUpdateContract()
        msg.ParseFromString(param_bytes)
        from_addr = _grpc_addr_to_hex(msg.owner_address)

    else:
        # Generic decode for V2 types and any future contract types.
        # Extract all addresses: first is from (owner), second is to (receiver).
        addresses = _extract_all_addresses_generic(param_bytes)
        if addresses:
            from_addr = addresses[0][0]
            if len(addresses) > 1:
                to_addr = addresses[1][0]

    return from_addr, to_addr, value, input_data


# ---------------------------------------------------------------------------
# Energy price derivation
# ---------------------------------------------------------------------------


def _derive_energy_price(results):
    """Derive the energy price (getEnergyFee) from TransactionInfo data.

    Scans all TransactionInfos for a tx that paid energy with TRX.
    The energy price is a chain-wide parameter, constant within any
    block range processed in a single batch.

    Returns 0 if no tx in the batch paid energy (e.g., genesis blocks
    with no smart contract activity).
    """
    for _, _, tx_info_list in results:
        for tx_info in tx_info_list.transactionInfo:
            receipt = tx_info.receipt
            if receipt.energy_fee > 0:
                # paid_energy = total - free_from_staking - free_from_deployer
                paid_energy = (
                    receipt.energy_usage_total
                    - receipt.energy_usage
                    - receipt.origin_energy_usage
                )
                if paid_energy > 0:
                    return receipt.energy_fee // paid_energy
    return 0


# ---------------------------------------------------------------------------
# Combined gRPC exporter
# ---------------------------------------------------------------------------


class TronCombinedGrpcExporter:
    """Combined gRPC exporter for Tron transactions, types, traces, and fees.

    Uses GetBlockByNum2 (transactions + types) and GetTransactionInfoByBlockNum
    (traces + fees) in a single parallel pass per block, replacing:
    - HTTP eth_getBlockByNumber(detailed=true) for transactions (~3.26s)
    - gRPC GetBlockByNum2 for types (~0.31s)
    - gRPC GetTransactionInfoByBlockNum for traces/fees (~0.00s overlapped)

    Combined gRPC time: ~0.89s for 100 blocks with 30 workers.
    """

    def __init__(self, grpc_endpoint, max_workers=30):
        if not _has_grpc:
            raise ImportError(
                "TronCombinedGrpcExporter requires grpc. "
                "Install gslib with ingest dependencies."
            )
        self.grpc_endpoint = grpc_endpoint
        self.max_workers = max_workers
        self._channel = None
        self._stub = None

    def close(self):
        """Close the persistent gRPC channel."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None

    def _reset_channel(self):
        """Reset channel so next export() creates a fresh connection."""
        if self._channel is not None:
            self._channel.close()
        self._channel = None
        self._stub = None

    def _fetch_and_decode_block(self, block_num, wallet_stub, retries=5, timeout=180):
        """Fetch and decode a single block via gRPC with retry logic."""
        attempt = 0
        base_delay = 1.0
        max_delay = 15.0
        backoff_multiplier = 2.0

        while attempt < retries:
            try:
                msg = NumberMessage(num=block_num)
                block_ext = wallet_stub.GetBlockByNum2(msg, timeout=timeout)
                tx_info_list = wallet_stub.GetTransactionInfoByBlockNum(
                    msg, timeout=timeout
                )
                return block_num, block_ext, tx_info_list
            except grpc.RpcError as e:
                attempt += 1
                if attempt >= retries:
                    raise Exception(
                        f"Failed to fetch block {block_num} after {retries} attempts. "
                        f"Last error: {e}"
                    )
                delay = min(base_delay * (backoff_multiplier ** (attempt - 1)), max_delay)
                logger.error(
                    f"gRPC error fetching block {block_num}, "
                    f"attempt {attempt}/{retries}: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)

        raise Exception(
            f"Failed to fetch block {block_num} after {retries} attempts"
        )

    def export(self, start_block, end_block):
        """Export all data for a block range via gRPC.

        Returns (transactions, hash_to_type, traces, fees, receipts, logs) where:
        - transactions: list of tx dicts matching parse_transaction_json format
        - hash_to_type: dict of {tx_hash: tron_contract_type}
        - traces: list of trace dicts matching decode_block_to_traces format
        - fees: list of fee dicts matching decode_fees format
        - receipts: list of receipt dicts matching parse_receipt_json format
        - logs: list of log dicts matching parse_log_json format
        """

        def _run():
            t_channel = time.monotonic()

            # Reuse persistent channel across export() calls to avoid
            # ~0.04s warmup overhead per batch.
            if self._channel is None:
                self._channel = get_channel(self.grpc_endpoint).__enter__()
                self._stub = WalletStub(self._channel)
                # Warm up: first call establishes the HTTP/2 connection.
                self._stub.GetBlockByNum2(
                    NumberMessage(num=start_block), timeout=30
                )
            t_channel_ready = time.monotonic() - t_channel

            wallet_stub = self._stub

            def fetch_block(bn):
                return self._fetch_and_decode_block(bn, wallet_stub)

            t_fetch = time.monotonic()
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(
                    executor.map(
                        fetch_block, range(start_block, end_block + 1)
                    )
                )
            t_fetch_done = time.monotonic() - t_fetch

            # Derive energy_price from the first tx with paid energy.
            # The energy price is a chain-wide parameter (getEnergyFee),
            # constant within any 100-block range.
            energy_price = _derive_energy_price(results)

            # Results are in block order (executor.map preserves order)
            all_txs = []
            hash_to_type = {}
            all_traces = []
            all_fees = []
            all_receipts = []
            all_logs = []

            for block_num, block_ext, tx_info_list in results:
                block_hash = "0x" + block_ext.blockid.hex()
                block_timestamp = (
                    block_ext.block_header.raw_data.timestamp // 1000
                )

                # Build tx_hash → tx_info lookup for receipt/log extraction
                tx_info_by_hash = {}
                for tx_info in tx_info_list.transactionInfo:
                    tx_info_by_hash["0x" + tx_info.id.hex()] = tx_info

                # Running counters per block
                block_log_index = 0
                cumulative_gas_used = 0

                # Extract transactions and types from BlockExtention
                for tx_idx, tx_ext in enumerate(block_ext.transactions):
                    tx_raw = tx_ext.transaction
                    contract = tx_raw.raw_data.contract[0]
                    tx_hash = "0x" + tx_ext.txid.hex()

                    # Decode contract parameter for from/to/value/input
                    from_addr, to_addr, value, input_data = _decode_contract_param(
                        contract
                    )

                    # Is this a contract creation?
                    type_name = contract.parameter.type_url.split("/")[-1]
                    is_create = type_name == "protocol.CreateSmartContract"

                    # Extract v, r, s from signature
                    v, r, s = 0, 0, 0
                    if tx_raw.signature:
                        sig = tx_raw.signature[0]
                        if len(sig) >= 65:
                            r = int.from_bytes(sig[:32], "big")
                            s = int.from_bytes(sig[32:64], "big")
                            v_byte = sig[64]
                            # sig[64] may already be v (27/28) or recovery id (0/1)
                            v = v_byte if v_byte >= 27 else v_byte + 27

                    # Look up TransactionInfo for gas_used and per-tx energy price
                    tx_info = tx_info_by_hash.get(tx_hash)

                    # Tron JSON-RPC returns gas = energy_usage_total (actual
                    # energy consumed), not the fee_limit. Match that behavior.
                    gas = 0
                    tx_gas_price = energy_price  # batch-level fallback
                    if tx_info is not None:
                        ti_rcpt = tx_info.receipt
                        gas = ti_rcpt.energy_usage_total
                        # Derive per-tx energy price from receipt to match
                        # what the HTTP JSON-RPC returns as gasPrice.
                        if ti_rcpt.energy_fee > 0:
                            paid = (
                                ti_rcpt.energy_usage_total
                                - ti_rcpt.energy_usage
                                - ti_rcpt.origin_energy_usage
                            )
                            if paid > 0:
                                tx_gas_price = ti_rcpt.energy_fee // paid
                        # WithdrawBalanceContract: value is the withdrawn
                        # amount from TransactionInfo, not the contract param.
                        if type_name == "protocol.WithdrawBalanceContract":
                            value = tx_info.withdraw_amount

                    tx_dict = {
                        "type": "transaction",
                        "hash": tx_hash,
                        "nonce": 0,  # Tron doesn't use nonces
                        "block_hash": block_hash,
                        "block_number": block_num,
                        "block_timestamp": block_timestamp,
                        "transaction_index": tx_idx,
                        "from_address": from_addr,
                        "to_address": to_addr,
                        "value": value,
                        "gas": gas,
                        "gas_price": tx_gas_price,
                        "input": input_data,
                        "max_fee_per_gas": None,
                        "max_priority_fee_per_gas": None,
                        "transaction_type": contract.type,
                        "max_fee_per_blob_gas": None,
                        "blob_versioned_hashes": [],
                        "v": v,
                        "r": r,
                        "s": s,
                    }
                    all_txs.append(tx_dict)
                    hash_to_type[tx_hash] = contract.type

                    # Build receipt and fee from TransactionInfo
                    if tx_info is not None:
                        ti_receipt = tx_info.receipt
                        gas_used = ti_receipt.energy_usage_total
                        cumulative_gas_used += gas_used

                        # contract_address: only for contract creation txs
                        # (matches HTTP JSON-RPC which returns null for calls)
                        contract_addr = None
                        if is_create and tx_info.contract_address:
                            contract_addr = _grpc_addr_to_hex(
                                tx_info.contract_address
                            )

                        receipt_dict = {
                            "type": "receipt",
                            "transaction_hash": tx_hash,
                            "transaction_index": tx_idx,
                            "block_hash": block_hash,
                            "block_number": block_num,
                            "cumulative_gas_used": cumulative_gas_used,
                            "gas_used": gas_used,
                            "contract_address": contract_addr,
                            "root": None,
                            "status": 1 if tx_info.result == 0 else 0,
                            "effective_gas_price": tx_gas_price,
                            "l1_fee": None,
                            "l1_gas_used": None,
                            "l1_gas_price": None,
                            "l1_fee_scalar": None,
                            "blob_gas_price": None,
                            "blob_gas_used": None,
                        }
                        all_receipts.append(receipt_dict)

                        # Fee dict (merged from decode_fees to avoid
                        # redundant TransactionInfo iteration)
                        all_fees.append({
                            "block_id": block_num,
                            "fee": tx_info.fee,
                            "tx_hash": tx_hash,
                            "energy_usage": ti_receipt.energy_usage,
                            "energy_fee": ti_receipt.energy_fee,
                            "origin_energy_usage": ti_receipt.origin_energy_usage,
                            "energy_usage_total": ti_receipt.energy_usage_total,
                            "net_usage": ti_receipt.net_usage,
                            "net_fee": ti_receipt.net_fee,
                            "result": ti_receipt.result,
                            "energy_penalty_total": ti_receipt.net_fee,
                        })

                        # Build logs from TransactionInfo
                        for log_entry in tx_info.log:
                            log_addr = _grpc_addr_to_hex(log_entry.address)
                            log_dict = {
                                "type": "log",
                                "log_index": block_log_index,
                                "transaction_hash": tx_hash,
                                "transaction_index": tx_idx,
                                "block_hash": block_hash,
                                "block_number": block_num,
                                "address": log_addr,
                                "data": (
                                    ("0x" + log_entry.data.hex())
                                    if log_entry.data
                                    else "0x"
                                ),
                                "topics": [
                                    "0x" + topic.hex()
                                    for topic in log_entry.topics
                                ],
                            }
                            all_logs.append(log_dict)
                            block_log_index += 1
                    else:
                        # No TransactionInfo for this tx — create minimal receipt
                        receipt_dict = {
                            "type": "receipt",
                            "transaction_hash": tx_hash,
                            "transaction_index": tx_idx,
                            "block_hash": block_hash,
                            "block_number": block_num,
                            "cumulative_gas_used": cumulative_gas_used,
                            "gas_used": 0,
                            "contract_address": None,
                            "root": None,
                            "status": 1,
                            "effective_gas_price": tx_gas_price,
                            "l1_fee": None,
                            "l1_gas_used": None,
                            "l1_gas_price": None,
                            "l1_fee_scalar": None,
                            "blob_gas_price": None,
                            "blob_gas_used": None,
                        }
                        all_receipts.append(receipt_dict)

                        # Zero-fee entry for tx without TransactionInfo
                        all_fees.append({
                            "block_id": block_num,
                            "fee": 0,
                            "tx_hash": tx_hash,
                            "energy_usage": 0,
                            "energy_fee": 0,
                            "origin_energy_usage": 0,
                            "energy_usage_total": 0,
                            "net_usage": 0,
                            "net_fee": 0,
                            "result": 0,
                            "energy_penalty_total": 0,
                        })

                # Extract traces from TransactionInfoList
                traces = decode_block_to_traces(block_num, tx_info_list)
                all_traces.extend(traces)

            t_decode_done = time.monotonic() - t_fetch - t_fetch_done
            logger.info(
                f"[grpc-exporter] channel={t_channel_ready:.3f}s  "
                f"fetch={t_fetch_done:.3f}s  "
                f"decode={t_decode_done:.3f}s  "
                f"txs={len(all_txs)}  traces={len(all_traces)}  fees={len(all_fees)}  "
                f"receipts={len(all_receipts)}  logs={len(all_logs)}  "
                f"energy_price={energy_price}"
            )

            return (
                all_txs, hash_to_type, all_traces, all_fees,
                all_receipts, all_logs,
            )

        # Retry logic for the entire block range
        attempt = 0
        retries = 5
        base_delay = 2.0
        max_delay = 15.0
        backoff_multiplier = 2.0

        while attempt < retries:
            try:
                return _run()
            except Exception as e:
                attempt += 1
                # Reset channel so next attempt gets a fresh connection
                self._reset_channel()
                if attempt >= retries:
                    raise Exception(
                        f"Failed to export block range {start_block}-{end_block} "
                        f"after {retries} attempts. Last error: {e}"
                    )
                delay = min(base_delay * (backoff_multiplier ** (attempt - 1)), max_delay)
                logger.error(
                    f"Error exporting block range {start_block}-{end_block}, "
                    f"attempt {attempt}/{retries}: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)

        raise Exception(
            f"Failed to export block range {start_block}-{end_block} "
            f"after {retries} attempts"
        )
