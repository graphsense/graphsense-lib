import asyncio
import time

from typing import Dict, Optional, Any, Tuple, List, AsyncGenerator
from graphsenselib.utils.httpx import RetryHTTPClient
from graphsenselib.utils import strip_0x
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.defi.bridging.models import (
    Bridge,
    BridgeSendTransfer,
    BridgeReceiveTransfer,
    BridgeSendReference,
    BridgeReceiveReference,
)
from graphsenselib.utils.logging import logger
from graphsenselib.utils.accountmodel import is_native_placeholder
from graphsenselib.datatypes.abi import decode_logs_db, log_signatures
from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.defi.models import Trace
from graphsenselib.datatypes.common import NodeType


UTXO_NETWORKS = ["btc", "bch", "ltc", "zec"]
ACCOUNT_NETWORKS = ["eth", "trx"]

# Networks where we can search for THORChain receives (TransferOut logs for EVM, OP_RETURN for UTXO)
SUPPORTED_RECEIVE_NETWORKS = UTXO_NETWORKS + ["eth"]
THOR_TO_GRAPHSENSE_NETWORK = {
    "BTC": "btc",
    "ETH": "eth",
    "LTC": "ltc",
    "BCH": "bch",
}
GRAPHSENSE_TO_THOR_NETWORK = {v: k for k, v in THOR_TO_GRAPHSENSE_NETWORK.items()}
THORNODE_URLS = [
    "https://thornode.ninerealms.com/thorchain/tx/status/",
    "https://thornode-v1.ninerealms.com/thorchain/tx/details/",
]

# Known THORChain router addresses (for validating deposit addresses)
# These are the main router contracts that receive funds from deposit addresses
THORCHAIN_ROUTER_ADDRESSES = {
    "eth": [
        "0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146",  # Current ETH router
        "0x3624525075b88b24ecc29ce226b0cec1ffcb6976",  # Previous router
        "0xc145990e84155416144c532e31f89b840ca8c2ce",  # Historical router
    ],
}

# THORChain event topic signatures (keccak256 hashes)
# These are used for efficient DB queries since topic0 is part of the clustering key
THORCHAIN_TRANSFEROUT_TOPIC = bytes.fromhex(
    "a9cd03aa3c1b4515114539cd53d22085129d495cb9e9f9af77864526240f1bf7"
)
ERC20_TRANSFER_TOPIC = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


async def try_thornode_endpoints(tx_hash_upper: str):
    """
    Try all THORNODE_URLS endpoints for the given transaction hash.
    Returns the first successful and decodable JSON response.
    Used as fallback when OP_RETURN data is not available in DB.
    """
    client = RetryHTTPClient()

    for base_url in THORNODE_URLS:
        try:
            url = f"{base_url}{tx_hash_upper}"
            logger.debug(f"Trying THORChain endpoint: {url}")

            response = await client.get(url)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get("out_txs") is None or data.get("tx") is None:
                        continue

                    logger.debug(f"Successfully got decodable response from {url}")
                    return data
                except Exception as json_error:
                    logger.warning(
                        f"Response from {url} returned 200 but was not decodable JSON: {json_error}"
                    )
                    continue
            else:
                logger.debug(f"Endpoint {url} returned status {response.status_code}")
        except Exception as e:
            logger.warning(f"Error trying endpoint {base_url}: {e}")
            continue

    raise ValueError(
        "All THORChain endpoints failed or returned non-decodable responses"
    )


def parse_op_return_memo(script_hex: str) -> Optional[str]:
    """
    Parse memo from OP_RETURN script hex.

    OP_RETURN format:
    - 6a = OP_RETURN opcode
    - Next byte(s) = length (or OP_PUSHDATA1/2/4 for longer data)
    - Remaining = data bytes (memo as UTF-8)

    Returns decoded memo string or None if not valid OP_RETURN.
    """
    if not script_hex or not script_hex.startswith("6a"):
        return None

    script_bytes = bytes.fromhex(script_hex)

    if len(script_bytes) < 2:
        return None

    # Skip OP_RETURN (0x6a)
    pos = 1
    length_byte = script_bytes[pos]

    # Handle OP_PUSHDATA variants
    if length_byte <= 0x4B:  # Direct push (0-75 bytes)
        data_start = pos + 1
        data_len = length_byte
    elif length_byte == 0x4C:  # OP_PUSHDATA1
        if len(script_bytes) < pos + 2:
            return None
        data_start = pos + 2
        data_len = script_bytes[pos + 1]
    elif length_byte == 0x4D:  # OP_PUSHDATA2
        if len(script_bytes) < pos + 3:
            return None
        data_start = pos + 3
        data_len = int.from_bytes(script_bytes[pos + 1 : pos + 3], "little")
    else:
        return None

    if len(script_bytes) < data_start + data_len:
        return None

    data = script_bytes[data_start : data_start + data_len]
    return data.decode("utf-8", errors="replace")


THORCHAIN_MEMO_PREFIXES = ["=", "s", "S", "+", "-", "~"]


def is_thorchain_memo(memo: str) -> bool:
    """
    Check if a memo string looks like a THORChain memo.

    THORChain memos start with action indicators:
    - =, s, S: swap
    - +: add liquidity
    - -: withdraw liquidity
    - ~: loan
    """
    if not memo or len(memo) < 2:
        return False
    return memo[0] in THORCHAIN_MEMO_PREFIXES


def extract_memo_from_utxo_tx(tx: Dict[str, Any]) -> Optional[str]:
    """
    Extract memo from UTXO transaction OP_RETURN output.

    Returns the memo string if found, None otherwise.
    """
    for output in tx.get("outputs", []) or []:
        script_hex = getattr(output, "script_hex", None)
        if script_hex:
            if isinstance(script_hex, bytes):
                script_hex = script_hex.hex()
            memo = parse_op_return_memo(script_hex)
            if memo:
                return memo
    return None


def is_thorchain_utxo_deposit(tx: Dict[str, Any]) -> bool:
    """
    Check if a UTXO transaction is a THORChain deposit based on OP_RETURN memo.

    Returns True if the transaction has an OP_RETURN output with a THORChain memo.
    """
    memo = extract_memo_from_utxo_tx(tx)
    return memo is not None and is_thorchain_memo(memo)


def is_thorchain_utxo_receive(tx: Dict[str, Any]) -> bool:
    """
    Check if a UTXO transaction is a THORChain receive (OUT: or REFUND:).

    Returns True if the transaction has an OP_RETURN with OUT: or REFUND: prefix.
    """
    memo = extract_memo_from_utxo_tx(tx)
    if memo is None:
        return False
    parsed = decode_withdrawal(memo)
    return parsed.get("is_withdrawal", False)


async def get_utxo_tx_with_memo(
    db: Cassandra, network: str, tx_hash: str
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Get UTXO transaction from DB and extract OP_RETURN memo.

    Returns (tx_dict, memo) where memo is extracted from OP_RETURN output.
    """
    tx = await db.get_tx_by_hash(network, tx_hash.lower())

    if tx is None:
        raise ValueError(f"Transaction {tx_hash} not found in {network}")

    memo = extract_memo_from_utxo_tx(tx)
    return tx, memo


class SwapDecoder:
    """Minimal decoder for THORChain swap memos"""

    # Common asset abbreviations
    ASSETS = {
        "r": "THOR.RUNE",
        "b": "BTC.BTC",
        "e": "ETH.ETH",
        "a": "AVAX.AVAX",
        "d": "DOGE.DOGE",
        "l": "LTC.LTC",
        "g": "GAIA.ATOM",
    }

    def decode(self, memo: str) -> Dict[str, Any]:
        """
        Decode a swap memo

        Returns dict with:
        - is_swap: bool
        - asset: target asset
        - destination: destination address
        - limit: minimum output (optional)
        - streaming_interval: blocks between swaps (optional)
        - streaming_quantity: number of swaps (optional)
        - affiliate: affiliate address (optional)
        - affiliate_fee: fee in basis points (optional)
        """
        if not memo:
            return {"is_swap": False, "error": "Empty memo"}

        parts = memo.split(":")
        if not parts:
            return {"is_swap": False, "error": "Invalid format"}

        # Check if it's a swap
        func = parts[0].strip().lower()
        if func not in ["=", "s", "swap", "=<"]:
            return {
                "is_swap": False,
                "error": f"Not a swap memo (function: {parts[0]})",
            }

        result = {"is_swap": True, "raw_memo": memo}

        # Parse asset
        if len(parts) > 1 and parts[1]:
            result["asset"] = self._expand_asset(parts[1])

        # Parse destination address
        if len(parts) > 2 and parts[2]:
            if "/" in parts[2]:
                addrs = parts[2].split("/")
                result["destination"] = addrs[0]
                if len(addrs) > 1:
                    result["refund_address"] = addrs[1]
            else:
                result["destination"] = parts[2]

        # Parse limit/streaming params
        if len(parts) > 3 and parts[3]:
            streaming = parts[3].split("/")
            if streaming[0]:
                result["limit"] = self._to_int(streaming[0])
            if len(streaming) > 1 and streaming[1]:
                result["streaming_interval"] = int(streaming[1])
            if len(streaming) > 2 and streaming[2]:
                result["streaming_quantity"] = int(streaming[2])

        # Parse affiliate
        if len(parts) > 4 and parts[4]:
            result["affiliate"] = parts[4]

        # Parse affiliate fee
        try:
            if len(parts) > 5 and parts[5]:
                result["affiliate_fee"] = int(parts[5])
        except ValueError:
            # some txs have non-integer affiliate fees
            # e.g. 0x6264da18732889def93336b2bd6f14d70471d9e7bb626d7012f683d6061e3480_I460
            result["affiliate_fee"] = None

        return result

    def _expand_asset(self, asset: str) -> str:
        """Expand asset abbreviation or return as-is"""
        # If it's already in chain.asset format, return it
        if "." in asset:
            return asset
        # Check abbreviations
        return self.ASSETS.get(asset.lower(), asset)

    def _to_int(self, value: str) -> Optional[int]:
        """Convert to int, handling scientific notation"""
        if not value:
            return None
        try:
            # Handle scientific notation like 1e6
            if "e" in value.lower():
                return int(float(value))
            return int(value)
        except ValueError:
            return None


# Simple usage function
def decode_swap(memo: str) -> Dict[str, Any]:
    """Decode a THORChain swap memo"""
    decoder = SwapDecoder()
    return decoder.decode(memo)


class TransferOutDecoder:
    """Minimal decoder for THORChain TransferOut event memos"""

    def decode(self, memo: str) -> Dict[str, Any]:
        """
        Decode a TransferOut memo

        TransferOut memos typically have format:
        OUT:<TXID> or REFUND:<TXID>

        Returns dict with:
        - is_withdrawal: bool
        - type: 'OUT' or 'REFUND'
        - tx_id: transaction ID
        """
        if not memo:
            return {"is_withdrawal": False, "error": "Empty memo"}

        parts = memo.split(":")
        if not parts:
            return {"is_withdrawal": False, "error": "Invalid format"}

        # Check if it's a transfer out
        func = parts[0].strip().upper()
        if func not in ["OUT", "REFUND"]:
            return {
                "is_withdrawal": False,
                "error": f"Not a transfer out memo (function: {parts[0]})",
            }

        result = {"is_withdrawal": True, "type": func, "raw_memo": memo}

        # Parse transaction ID
        if len(parts) > 1 and parts[1]:
            result["tx_id"] = parts[1].strip()

        # Some OUT memos might have additional info
        if len(parts) > 2:
            result["additional_info"] = ":".join(parts[2:])

        return result


def decode_withdrawal(memo: str) -> Dict[str, Any]:
    """Decode a THORChain TransferOut memo"""
    decoder = TransferOutDecoder()
    return decoder.decode(memo)


async def get_bridges_from_thorchain_send_from_tx_hash_account(
    network: str,
    db: Cassandra,
    tx_hash: str,
) -> AsyncGenerator[BridgeSendTransfer, None]:
    """Get bridge send transfers from transaction hash for account-based networks"""
    if network in ACCOUNT_NETWORKS:
        tx_hash_bytes = bytes.fromhex(strip_0x(tx_hash))
        tx = await db.get_tx_by_hash(network, tx_hash_bytes)

        if tx is None:
            raise ValueError(f"Transaction {tx_hash} not found in DB")

        block_number = tx["block_id"]
        # Get all logs and traces in the block
        # todo wasteful
        logs_set = await db.get_logs_in_block_eth(
            network, block_number, tx_hash=tx_hash_bytes
        )
        traces_set = await db.get_traces_in_block(
            network, block_number, tx_hash=tx_hash_bytes
        )

        traces_set = Trace.dicts_to_normalized(network, traces_set, tx)

        dlogs_logs = decode_logs_db(logs_set, log_signatures_local=log_signatures)
        dlogs_filtered = [dlog for dlog, _ in dlogs_logs]
        logs_raw_filtered = [log for _, log in dlogs_logs]
        traces_filtered = [trace for trace in traces_set]

        async for bridge_send, _ in get_bridges_from_thorchain_send(
            network, db, tx, dlogs_filtered, logs_raw_filtered, traces_filtered
        ):
            yield bridge_send


def extract_memo_from_input(tx: Dict[str, Any]) -> Optional[str]:
    """
    Extract THORChain memo from transaction input data.

    Direct deposits to THORChain vaults include the memo as ASCII in the input field.
    """
    input_data = tx.get("input")
    if not input_data:
        return None

    if isinstance(input_data, bytes):
        input_bytes = input_data
    else:
        input_bytes = bytes.fromhex(input_data)

    if len(input_bytes) == 0:
        return None

    # Try to decode as UTF-8
    try:
        memo = input_bytes.decode("utf-8", errors="strict")
        if is_thorchain_memo(memo):
            return memo
    except (UnicodeDecodeError, ValueError):
        pass

    return None


async def is_thorchain_deposit_address(
    db: Cassandra, network: str, address: str
) -> bool:
    """
    Check if an address is a THORChain deposit address.

    A THORChain deposit address is identified by:
    1. Having only one outgoing neighbor
    2. That neighbor being a known THORChain router address

    Args:
        db: Cassandra database connection
        network: Network identifier (e.g., 'eth')
        address: Address to check (hex string with or without 0x prefix)

    Returns:
        True if the address is a THORChain deposit address
    """

    # Normalize address format
    if address.startswith("0x"):
        address_bytes = bytes.fromhex(address[2:])
    else:
        address_bytes = bytes.fromhex(address)

    # Get outgoing neighbors (only first page, we just need to check if there's exactly 1)
    neighbors, _ = await db.list_neighbors(
        currency=network,
        id=address_bytes,
        is_outgoing=True,
        node_type=NodeType.ADDRESS,
        targets=None,
        page=None,
        pagesize=10,  # Small page size - we only need to check if there's exactly 1
    )

    if len(neighbors) != 1:
        logger.debug(
            f"Address {address} has {len(neighbors)} outgoing neighbors, expected 1"
        )
        return False

    # Get the neighbor address
    neighbor = neighbors[0]
    neighbor_address_id = neighbor.get("dst_address_id")
    if neighbor_address_id is None:
        return False

    # Get the actual address from the ID
    neighbor_address = await db.get_address_by_address_id(network, neighbor_address_id)
    if neighbor_address is None:
        return False

    neighbor_address_hex = "0x" + neighbor_address.hex()

    # Check if neighbor is a known THORChain router
    known_routers = THORCHAIN_ROUTER_ADDRESSES.get(network, [])
    if neighbor_address_hex.lower() in [r.lower() for r in known_routers]:
        logger.debug(
            f"Address {address} is a THORChain deposit address "
            f"(neighbor {neighbor_address_hex} is a known router)"
        )
        return True

    logger.debug(
        f"Address {address} neighbor {neighbor_address_hex} is not a known THORChain router"
    )
    return False


async def get_bridges_from_thorchain_send(
    network: str,
    db: Optional[Cassandra],
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
) -> AsyncGenerator[Tuple[BridgeSendTransfer, BridgeReceiveReference], None]:
    """
    Extract bridge send information from THORChain deposit transactions.

    Handles two types of deposits:
    1. Router deposits: Use the Deposit log event (contains asset and memo)
    2. Direct vault deposits: Memo is in the transaction input data (native ETH only)
       - Requires validation that the receiver is a THORChain deposit address

    Args:
        network: Network identifier (e.g., 'eth')
        db: Cassandra database connection (required for direct vault deposit validation)
        tx: Transaction data
        dlogs: Decoded logs
        logs_raw: Raw logs
        traces: Transaction traces

    Example tx: 6d65123e246d752de3f39e0fdf5b788baad35a29b7e95b74c714e6c7c1ea61dd (Bybit hack bridge to BTC)
    """

    def from_hex(address):
        return "0x" + address.hex()

    from_address = from_hex(tx["from_address"])

    deposits = [dlog for dlog in dlogs if dlog["name"] == "Deposit"]

    # Handle the two deposit types
    if len(deposits) == 1:
        # Type 1: Router deposit with Deposit log
        deposit = deposits[0]
        from_asset = deposit["parameters"]["asset"]
        memo = deposit["parameters"]["memo"]
    elif len(deposits) == 0:
        # Type 2: Direct vault deposit - memo in input data
        memo = extract_memo_from_input(tx)
        if memo is None:
            logger.debug(
                f"No Deposit log and no memo in input for tx {tx['tx_hash'].hex()}"
            )
            return

        # Validate that the receiver is a THORChain deposit address
        to_address = from_hex(tx["to_address"])
        if db is None:
            logger.warning(
                f"Cannot validate THORChain deposit address without db connection "
                f"for tx {tx['tx_hash'].hex()}"
            )
            return

        is_deposit_addr = await is_thorchain_deposit_address(db, network, to_address)
        if not is_deposit_addr:
            logger.debug(
                f"Receiver {to_address} is not a THORChain deposit address "
                f"for tx {tx['tx_hash'].hex()}"
            )
            return

        # Direct vault deposits are always native ETH
        from_asset = "0x0000000000000000000000000000000000000000"
    else:
        logger.warning(
            f"Expected 0 or 1 Deposit logs, got {len(deposits)} for tx {tx['tx_hash'].hex()}"
        )
        return

    swap_info = decode_swap(memo)

    if swap_info["is_swap"]:
        to_address = swap_info["destination"]
        asset_code = swap_info["asset"]  # e.g. BTC.BTC
        to_network_thor = asset_code.split(".")[0]
        # asset = asset_code.split(".")[1]
        to_network = THOR_TO_GRAPHSENSE_NETWORK.get(to_network_thor)

        # Check if the target network is supported
        if to_network is None:
            logger.warning(
                f"Skipping thorchain transaction: unsupported network '{to_network_thor}' in memo: {memo}"
            )
            return

        # to_asset = "native" if asset == network else asset
        # to_amount = swap_info["limit"]  # should be determined from the linked tx

    else:
        logger.warning(f"Skipping thorchain action, not implemented memo: {memo}")
        return

    from_network = network

    if is_native_placeholder(from_asset):
        from_asset = "native"

    # correlate the payment on the target network
    # get the traces with nonzero value and the transfers
    token_transfers = [dlog for dlog in dlogs if dlog["name"] == "Transfer"]

    eth_transfers = [
        trace
        for trace in traces
        if trace.value > 0 and trace.from_address == from_address
    ]

    from_tx = tx["tx_hash"].hex()
    # Handle both ETH transfers (native) and token transfers (ERC-20)
    if len(token_transfers) > 0:
        # ERC-20 token transfer case
        # Find token transfers from the sender
        sender_token_transfers = [
            (i, dlog)
            for i, dlog in enumerate(dlogs)
            if dlog["name"] == "Transfer"
            and dlog["parameters"]["from"].lower() == from_address.lower()
        ]

        if len(sender_token_transfers) != 1:
            raise ValueError(
                f"Expected exactly one token transfer from sender, got {len(sender_token_transfers)}"
            )

        log_index, transfer_log = sender_token_transfers[0]
        from_amount = transfer_log["parameters"]["value"]
        log_index_raw = logs_raw[log_index]["log_index"]

        from_payment = SubTransactionIdentifier(
            tx_hash=from_tx,
            tx_type=SubTransactionType.ERC20,
            sub_index=log_index_raw,
        ).to_string()

    elif len(eth_transfers) > 0:
        # ETH transfer case (native token)
        if len(eth_transfers) != 1:
            raise ValueError(
                f"Expected exactly one ETH transfer from sender, got {len(eth_transfers)}"
            )

        trace_index = eth_transfers[0].trace_index
        from_amount = eth_transfers[0].value

        from_payment = SubTransactionIdentifier(
            tx_hash=from_tx,
            tx_type=SubTransactionType.InternalTx,
            sub_index=trace_index,
        ).to_string()
    else:
        raise ValueError("No token transfers or ETH transfers found from sender")

    # Get timestamp from tx (could be 'block_timestamp' or 'timestamp')
    from_timestamp = tx.get("block_timestamp") or tx.get("timestamp")

    yield (
        BridgeSendTransfer(
            fromAddress=from_address,
            fromAsset=from_asset,
            fromAmount=from_amount,
            fromPayment=from_payment,
            fromNetwork=from_network,
        ),
        BridgeReceiveReference(
            toAddress=to_address,
            toNetwork=to_network,
            fromTxHash=from_tx,
            fromTimestamp=from_timestamp,
            targetAssetCode=asset_code,  # e.g., "ETH.ETH" or "ETH.USDT-0x..."
        ),
    )


async def get_bridges_from_thorchain_receive(
    network: str,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
) -> AsyncGenerator[Tuple[BridgeReceiveTransfer, BridgeSendReference], None]:
    """
    TODO: This doesnt work yet for BTC. Would need OP_RETURN data.
    """
    hash_field_name = "tx_hash" if network in ACCOUNT_NETWORKS else "tx_hash"
    tx_hash = tx[hash_field_name].hex()

    relevant_dlogs = [
        dlog
        for dlog in dlogs
        if dlog["name"] == "TransferOut" and "thorchain" in dlog["log_def"]["tags"]
    ]
    assert len(relevant_dlogs) == 1, "Expected exactly one TransferOut log"
    relevant_dlog = relevant_dlogs[0]

    params = relevant_dlog["parameters"]
    # vault = params["vault"]
    to_address = params["to"]
    asset_log = params["asset"]
    # amount = params["amount"]
    memo = params["memo"]

    parsed_memo = decode_withdrawal(memo)

    if "error" in parsed_memo:
        # e.g. 9b1ad0f967a06891e444477fbd254bea40179acd074ea697a305a2b4e91cbe86_I506
        logger.warning(
            f"There was an error in the thorchain memo: {parsed_memo['error']}, skipping."
        )
        return  # Early return for async generator

    to_tx_hash = parsed_memo["tx_id"].lower()

    if not parsed_memo["is_withdrawal"]:
        raise ValueError("Not a withdrawal")

    # Determine asset and get transfer payment identifier
    if is_native_placeholder(asset_log):
        to_asset = "native"
        # Find ETH transfer trace
        eth_transfers = [
            trace
            for trace in traces
            if trace.value > 0 and trace.to_address == to_address
        ]
        if len(eth_transfers) != 1:
            raise ValueError(
                f"Expected exactly one ETH transfer, got {len(eth_transfers)}"
            )

        trace_index = eth_transfers[0].trace_index
        to_amount = eth_transfers[0].value

        to_payment = SubTransactionIdentifier(
            tx_hash=tx_hash,
            tx_type=SubTransactionType.InternalTx,
            sub_index=trace_index,
        ).to_string()
    else:
        to_asset = asset_log
        # Find ERC20 transfer
        token_transfers = [
            (i, dlog)
            for i, dlog in enumerate(dlogs)
            if dlog["name"] == "Transfer" and dlog["parameters"]["to"] == to_address
        ]
        if len(token_transfers) != 1:
            raise ValueError(
                f"Expected exactly one token transfer, got {len(token_transfers)}"
            )

        log_index, transfer_log = token_transfers[0]
        to_amount = transfer_log["parameters"]["value"]
        log_index_raw = logs_raw[log_index]["log_index"]

        to_payment = SubTransactionIdentifier(
            tx_hash=tx_hash.lower(),
            tx_type=SubTransactionType.ERC20,
            sub_index=log_index_raw,
        ).to_string()

    yield (
        BridgeReceiveTransfer(
            toAddress=to_address,
            toAsset=to_asset,
            toAmount=int(to_amount),
            toPayment=to_payment,
            toNetwork=network,
        ),
        BridgeSendReference(
            fromTxHash=to_tx_hash,
        ),
    )


def combine_bridge_transfers(
    bridge_send: BridgeSendTransfer, bridge_receive: BridgeReceiveTransfer
) -> Bridge:
    """
    Combine a BridgeSendTransfer and BridgeReceiveTransfer into a full Bridge object.
    """
    return Bridge(
        fromAddress=bridge_send.fromAddress,
        fromAsset=bridge_send.fromAsset,
        fromAmount=bridge_send.fromAmount,
        fromPayment=bridge_send.fromPayment,
        fromNetwork=bridge_send.fromNetwork,
        toAddress=bridge_receive.toAddress,
        toAsset=bridge_receive.toAsset,
        toAmount=bridge_receive.toAmount,
        toPayment=bridge_receive.toPayment,
        toNetwork=bridge_receive.toNetwork,
    )


async def get_full_bridges_from_thorchain_send(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Dict[str, Any]],
) -> AsyncGenerator[Bridge, None]:
    """
    Get full Bridge objects from THORCHAIN send (deposit) transactions.
    Combines send transfers with their corresponding receive transfers.
    """

    result = [
        item
        async for item in get_bridges_from_thorchain_send(
            network, db, tx, dlogs, logs_raw, traces
        )
    ]
    if len(result) == 0:
        logger.warning(f"No send transfers found for {tx['tx_hash'].hex()}")
        return  # This stops the generator iteration

    send_transfers, receive_references = zip(*result)
    matcher = ThorchainTransactionMatcher(network, db)

    for send_transfer, receive_reference in zip(send_transfers, receive_references):
        receive_transfers = await matcher.match_receiving_transactions(
            receive_reference
        )
        if receive_transfers is None:
            continue

        for receive_transfer in receive_transfers:
            yield combine_bridge_transfers(send_transfer, receive_transfer)


async def get_full_bridges_from_thorchain_receive(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Dict[str, Any]],
) -> AsyncGenerator[Bridge, None]:
    """
    Get full Bridge objects from THORCHAIN receive (withdrawal) transactions.
    """
    result = []
    async for item in get_bridges_from_thorchain_receive(
        network, tx, dlogs, logs_raw, traces
    ):
        if item:
            result.append(item)

    if len(result) == 0:
        logger.warning(f"No receive transfers found for {tx['tx_hash'].hex()}")
        return  # This is fine in async generators - just stops iteration

    receive_transfers, send_references = zip(*result)
    matcher = ThorchainTransactionMatcher(network, db)

    for receive_transfer, send_reference in zip(receive_transfers, send_references):
        send_transfers = await matcher.match_sending_transactions(send_reference)
        if send_transfers is None:
            continue

        for send_transfer in send_transfers:
            yield combine_bridge_transfers(send_transfer, receive_transfer)


async def preliminary_utxo_handling_receive(
    db: Cassandra,
    network: str,
    receive_reference: BridgeReceiveReference,
) -> Optional[List[BridgeReceiveTransfer]]:
    """
    Find the receiving UTXO transaction using OP_RETURN memo from DB.
    The memo contains OUT:<original_tx_hash> which links to the deposit.
    Falls back to Thornode API if OP_RETURN data is not available.
    """
    # Get all incoming transactions for the recipient address
    address = receive_reference.toAddress
    txs, _ = await db.list_address_txs(network, address, direction="in", order="asc")

    has_script_hex = False
    for tx_ref in txs:
        tx_hash = tx_ref["tx_hash"].hex()
        tx, memo = await get_utxo_tx_with_memo(db, network, tx_hash)

        # Check if any output has script_hex (to know if we should fallback)
        for output in tx.get("outputs", []) or []:
            if getattr(output, "script_hex", None):
                has_script_hex = True
                break

        if memo is None:
            continue

        # Parse the memo to check if it's the matching OUT:<fromTxHash>
        parsed = decode_withdrawal(memo)
        if not parsed.get("is_withdrawal"):
            continue

        if parsed.get("tx_id", "").lower() != receive_reference.fromTxHash.lower():
            continue

        # Found the matching transaction - get the output value to recipient
        to_amount = 0
        for output in tx.get("outputs", []) or []:
            output_addresses = getattr(output, "address", None) or []
            if address in output_addresses:
                to_amount = getattr(output, "value", 0)
                break

        return [
            BridgeReceiveTransfer(
                toAddress=receive_reference.toAddress,
                toAsset="native",
                toAmount=to_amount,
                toPayment=tx_hash,
                toNetwork=receive_reference.toNetwork,
            )
        ]

    # Fallback to Thornode API if no script_hex data available (older blocks)
    if not has_script_hex:
        logger.debug(
            f"No script_hex data in DB for {receive_reference.toAddress}, "
            "falling back to Thornode API"
        )
        return await _thornode_fallback_receive(receive_reference)

    return None


async def _thornode_fallback_receive(
    receive_reference: BridgeReceiveReference,
) -> Optional[List[BridgeReceiveTransfer]]:
    """Fallback to Thornode API when OP_RETURN data is not available in DB."""
    tx_hash_upper = receive_reference.fromTxHash.upper()

    thorchain_data = await try_thornode_endpoints(tx_hash_upper)

    # Find the outbound transaction for the target network
    target_out_tx = None
    for out_tx in thorchain_data.get("out_txs", []):
        if out_tx["chain"].lower() == receive_reference.toNetwork and not out_tx.get(
            "refund", False
        ):
            target_out_tx = out_tx
            break

    if target_out_tx:
        toPayment = target_out_tx["id"].lower()
        if target_out_tx["coins"]:
            toAmount = target_out_tx["coins"][0]["amount"]
        else:
            raise ValueError("No coins found")
    else:
        raise ValueError("No outbound transaction found")

    return [
        BridgeReceiveTransfer(
            toAddress=receive_reference.toAddress,
            toAsset="native",
            toAmount=toAmount,
            toPayment=toPayment,
            toNetwork=receive_reference.toNetwork,
        )
    ]


async def preliminary_utxo_handling_send(
    db: Cassandra, fromNetwork: str, send_reference: BridgeSendReference
) -> Optional[List[BridgeSendTransfer]]:
    """
    Get UTXO send transaction details using DB with OP_RETURN memo.
    Falls back to Thornode API if OP_RETURN data is not available.
    """
    tx, memo = await get_utxo_tx_with_memo(db, fromNetwork, send_reference.fromTxHash)

    if tx is None:
        raise ValueError(f"Transaction {send_reference.fromTxHash} not found")

    # Check if script_hex data is available
    has_script_hex = False
    for output in tx.get("outputs", []) or []:
        if getattr(output, "script_hex", None):
            has_script_hex = True
            break

    # If no script_hex data, fall back to Thornode API
    if not has_script_hex:
        logger.debug(
            f"No script_hex data in DB for tx {send_reference.fromTxHash}, "
            "falling back to Thornode API"
        )
        return await _thornode_fallback_send(fromNetwork, send_reference)

    # Parse the swap memo to validate it's a THORChain deposit
    if memo:
        swap_info = decode_swap(memo)
        if not swap_info.get("is_swap"):
            logger.warning(f"UTXO tx {send_reference.fromTxHash} is not a swap: {memo}")

    # Find the sender address and amount from inputs
    # For UTXO, the sender is typically the first input's address
    from_address = None
    from_amount = tx.get("total_input", 0)

    inputs = tx.get("inputs", []) or []
    if inputs:
        first_input = inputs[0]
        input_addresses = getattr(first_input, "address", None) or []
        if input_addresses:
            from_address = (
                input_addresses[0]
                if isinstance(input_addresses, list)
                else input_addresses
            )

    return [
        BridgeSendTransfer(
            fromAddress=from_address or "unknown",
            fromAsset="native",
            fromAmount=from_amount,
            fromNetwork=fromNetwork,
            fromPayment=send_reference.fromTxHash,
        )
    ]


async def _thornode_fallback_send(
    fromNetwork: str, send_reference: BridgeSendReference
) -> Optional[List[BridgeSendTransfer]]:
    """Fallback to Thornode API when OP_RETURN data is not available in DB."""
    tx_hash_upper = send_reference.fromTxHash.upper()

    thorchain_data = await try_thornode_endpoints(tx_hash_upper)

    # Find the inbound transaction (UTXO)
    tx = thorchain_data.get("tx")
    if not tx or tx.get("chain", "").lower() not in UTXO_NETWORKS:
        raise ValueError("No UTXO inbound transaction found")
    fromAddress = tx["from_address"]
    fromAsset = "native"
    fromAmount = tx["coins"][0]["amount"] if tx["coins"] else None

    fromPayment = send_reference.fromTxHash
    if not fromAmount:
        raise ValueError("No UTXO amount found in inbound transaction")

    return [
        BridgeSendTransfer(
            fromAddress=fromAddress,
            fromAsset=fromAsset,
            fromAmount=fromAmount,
            fromNetwork=fromNetwork,
            fromPayment=fromPayment,
        )
    ]


class ThorchainTransactionMatcher:
    def __init__(self, network: str, db: Cassandra):
        self.network = network  # base network
        self.db = db

    async def match_receiving_transactions(
        self,
        receive_reference: BridgeReceiveReference,
        min_height: int = 0,
    ) -> Optional[List[BridgeReceiveTransfer]]:
        """
        We want to find the fromTransferHash in a tx in the incoming txs of the address
        1. Get the incoming txs of the address
        2. For UTXO check the OP_RETURN, for ETH check the logs - WE currently dont have that in UTXO

        Multiple receiving transfers are possible
        """

        def get_tags(dlog):
            return dlog["log_def"]["tags"]

        if receive_reference.toNetwork in UTXO_NETWORKS:
            return await preliminary_utxo_handling_receive(
                self.db, receive_reference.toNetwork, receive_reference
            )

        elif receive_reference.toNetwork == "eth":
            matched = []
            address = receive_reference.toAddress
            address_bytes = bytes.fromhex(address[2:])

            # Determine token_currency filter based on target asset
            # THORChain asset codes: "ETH.ETH" (native), "ETH.USDT-0x..." (token)
            token_currency = None
            if receive_reference.targetAssetCode:
                asset_parts = receive_reference.targetAssetCode.split(".")
                if len(asset_parts) >= 2:
                    asset_symbol = asset_parts[1]  # e.g., "ETH" or "USDT-0x..."
                    if "-" not in asset_symbol:
                        # Native asset (no contract address)
                        token_currency = "ETH"
                    # For tokens, we leave token_currency=None to fetch all
                    # (we could parse the contract address for more specific filtering)

            # Note: We considered estimating min_height from the source transaction
            # timestamp to skip older blocks, but this optimization is tricky because:
            # 1. THORChain may process swaps before BTC confirmations (mempool detection)
            # 2. Mathematical block estimation can be inaccurate
            # 3. The DB query for block-by-timestamp is expensive (ALLOW FILTERING)
            # The parallel bucket querying optimization already provides good performance,
            # so we skip timestamp-based filtering for now.

            # Get incoming transactions for the destination address
            # Note: Only pass min_height if > 0, as min_height=0 triggers expensive lookups
            t_list_start = time.perf_counter()
            txs, _ = await self.db.list_address_txs(
                receive_reference.toNetwork,
                address_bytes,
                direction="in",
                min_height=min_height if min_height else None,
                order="asc",
                token_currency=token_currency,
                pagesize=100,
            )
            t_list_end = time.perf_counter()
            logger.debug(
                f"[PERF] list_address_txs: {t_list_end - t_list_start:.3f}s, found {len(txs)} txs (token_currency={token_currency})"
            )

            if not txs:
                return matched

            # Filter by known THORChain router addresses
            known_routers = THORCHAIN_ROUTER_ADDRESSES.get(
                receive_reference.toNetwork, []
            )
            known_routers_bytes = [
                bytes.fromhex(addr[2:].lower()) for addr in known_routers
            ]

            filtered_txs = [
                tx for tx in txs if tx.get("from_address") in known_routers_bytes
            ]

            if filtered_txs:
                logger.debug(
                    f"[PERF] router filter: {len(txs)} -> {len(filtered_txs)} txs"
                )
                txs = filtered_txs
            else:
                logger.debug(
                    f"[PERF] no txs from known routers, checking all {len(txs)} txs"
                )

            # OPTIMIZATION: Query logs by topic instead of tx_hash
            # topic0 is part of the clustering key, so no ALLOW FILTERING needed
            # Group txs by block to minimize queries
            t_logs_start = time.perf_counter()

            # Build a mapping of block -> [txs in that block]
            blocks_to_txs = {}
            for tx in txs:
                block_id = tx["height"]
                if block_id not in blocks_to_txs:
                    blocks_to_txs[block_id] = []
                blocks_to_txs[block_id].append(tx)

            # Fetch TransferOut logs for each unique block (one query per block, no ALLOW FILTERING)
            log_tasks = [
                self.db.get_logs_in_block_eth(
                    receive_reference.toNetwork,
                    block_id,
                    topic=THORCHAIN_TRANSFEROUT_TOPIC,  # Filter by topic - efficient!
                )
                for block_id in blocks_to_txs.keys()
            ]
            all_logs_results = await asyncio.gather(*log_tasks)

            # Build mapping of block -> logs
            block_to_logs = {}
            for block_id, logs_result in zip(blocks_to_txs.keys(), all_logs_results):
                logs = (
                    logs_result.current_rows
                    if hasattr(logs_result, "current_rows")
                    else logs_result
                )
                block_to_logs[block_id] = logs

            t_logs_end = time.perf_counter()
            logger.debug(
                f"[PERF] topic-filtered log fetch for {len(blocks_to_txs)} blocks: {t_logs_end - t_logs_start:.3f}s"
            )

            # Process logs to find matching TransferOut
            for tx in txs:
                tx_hash = tx["tx_hash"].hex()
                tx_hash_bytes = tx["tx_hash"]
                block_of_tx = tx["height"]

                # Get logs for this block and filter by tx_hash in Python
                block_logs = block_to_logs.get(block_of_tx, [])
                tx_logs = [
                    log for log in block_logs if log.get("tx_hash") == tx_hash_bytes
                ]
                decoded_logs = decode_logs_db(tx_logs)

                for dlog, log in decoded_logs:
                    if dlog["name"] == "TransferOut" and "thorchain" in get_tags(dlog):
                        params = dlog["parameters"]
                        value_thorchain_log = params["amount"]
                        asset_thorchain_log = params["asset"]
                        memo = params["memo"]

                        decoded_memo = decode_withdrawal(memo)
                        if (
                            decoded_memo["is_withdrawal"]
                            and decoded_memo["tx_id"].lower()
                            == strip_0x(receive_reference.fromTxHash).lower()
                        ):
                            pass
                        else:
                            continue
                    else:
                        continue

                    if is_native_placeholder(asset_thorchain_log):
                        asset = "native"
                        to_address_log = params["to"].lower()

                        # Fetch traces to find the specific internal tx
                        traces_result = await self.db.get_traces_in_block(
                            receive_reference.toNetwork,
                            block_of_tx,
                            tx_hash=tx_hash_bytes,
                        )
                        traces = (
                            traces_result.current_rows
                            if hasattr(traces_result, "current_rows")
                            else traces_result
                        )

                        # Find the trace matching recipient and amount
                        matching_trace_index = None
                        for trace in traces:
                            trace_to = trace.get("to_address")
                            trace_value = trace.get("value", 0)
                            if trace_to is not None:
                                trace_to_hex = (
                                    "0x" + trace_to.hex()
                                    if isinstance(trace_to, bytes)
                                    else trace_to
                                )
                                if (
                                    trace_to_hex.lower() == to_address_log
                                    and trace_value == value_thorchain_log
                                ):
                                    matching_trace_index = trace.get("trace_index")
                                    break

                        if matching_trace_index is not None:
                            transfer = SubTransactionIdentifier(
                                tx_hash=tx_hash,
                                tx_type=SubTransactionType.InternalTx,
                                sub_index=matching_trace_index,
                            ).to_string()
                        else:
                            # Fallback to tx hash if no matching trace found
                            logger.warning(
                                f"No matching trace found for TransferOut in tx {tx_hash}, "
                                f"to={to_address_log}, amount={value_thorchain_log}"
                            )
                            transfer = tx_hash

                        matched.append(
                            BridgeReceiveTransfer(
                                toAddress=receive_reference.toAddress,
                                toAsset=asset,
                                toAmount=value_thorchain_log,
                                toPayment=transfer,
                                toNetwork=receive_reference.toNetwork,
                            )
                        )
                        break

                    # For token transfers, we need to fetch the ERC20 Transfer logs
                    # (not included in TransferOut topic query)
                    transfer_logs_result = await self.db.get_logs_in_block_eth(
                        receive_reference.toNetwork,
                        block_of_tx,
                        topic=ERC20_TRANSFER_TOPIC,
                    )
                    transfer_logs_raw = (
                        transfer_logs_result.current_rows
                        if hasattr(transfer_logs_result, "current_rows")
                        else transfer_logs_result
                    )
                    # Filter to this tx's Transfer logs
                    tx_transfer_logs = [
                        log
                        for log in transfer_logs_raw
                        if log.get("tx_hash") == tx_hash_bytes
                    ]
                    decoded_transfer_logs = decode_logs_db(tx_transfer_logs)

                    transfers = [
                        dlog
                        for dlog in decoded_transfer_logs
                        if dlog[0]["name"] == "Transfer"
                    ]

                    if len(transfers) != 1:
                        logger.warning(
                            f"Expected 1 transfer in token TransferOut tx {tx_hash}, got {len(transfers)}"
                        )
                        continue
                    transfer = transfers[0][0]
                    transfer_tx = transfers[0][1]

                    value_transfer_log = transfer["parameters"]["value"]

                    assert value_thorchain_log == value_transfer_log, (
                        f"Value mismatch: {value_thorchain_log} != {value_transfer_log}"
                    )
                    asset = transfer["address"].lower()
                    toPayment = SubTransactionIdentifier(
                        tx_hash=transfer_tx["tx_hash"].hex().lower(),
                        tx_type=SubTransactionType.ERC20,
                        sub_index=transfer_tx["log_index"],
                    ).to_string()
                    matched.append(
                        BridgeReceiveTransfer(
                            toAddress=receive_reference.toAddress,
                            toAsset=asset,
                            toAmount=int(value_transfer_log),
                            toPayment=toPayment,
                            toNetwork=receive_reference.toNetwork,
                        )
                    )

            return matched

    async def match_sending_transactions(
        self, send_reference: BridgeSendReference
    ) -> Optional[List[BridgeSendTransfer]]:
        currencies_supported = self.db.config["currencies"]
        hits = []
        tx_hash_hex = strip_0x(send_reference.fromTxHash).lower()
        for n in currencies_supported:
            try:
                if n in UTXO_NETWORKS:
                    tx_param = tx_hash_hex
                else:
                    tx_param = bytes.fromhex(tx_hash_hex)
                hit = await self.db.get_tx_by_hash(n, tx_param)
                hits.append({"currency": n, "tx": hit})
            except Exception:
                pass

        # Get all networks that have matches
        networks_with_matches = []
        for hit in hits:
            if hit["tx"] is not None:
                networks_with_matches.append(hit["currency"])

        if len(networks_with_matches) == 0:
            logger.warning(
                f"No txs found for sender tx {send_reference.fromTxHash} on any network"
            )
            return None

        if len(networks_with_matches) > 1:
            logger.warning(
                f"Found matches on multiple networks for tx {send_reference.fromTxHash}: {networks_with_matches}"
            )
            return None

        fromNetwork = networks_with_matches[0]

        # Get transactions for the matching network
        txs = [
            x["tx"]["tx_hash"].hex()
            for x in hits
            if x["currency"] == fromNetwork and x["tx"] is not None
        ]
        # remove postfixes _ if exists and get the prefix
        txs = [tx.split("_")[0] if "_" in tx else tx for tx in txs]
        # get unique
        txs = list(set(txs))

        if len(txs) != 1:
            logger.warning(
                f"Expected exactly one tx, got {len(txs)} for {send_reference.fromTxHash}"
            )
            return None

        if fromNetwork in UTXO_NETWORKS:
            return await preliminary_utxo_handling_send(
                self.db, fromNetwork, send_reference
            )

        elif fromNetwork in ACCOUNT_NETWORKS:
            bridges_generator = get_bridges_from_thorchain_send_from_tx_hash_account(
                fromNetwork, self.db, send_reference.fromTxHash
            )
            return [bridge async for bridge in bridges_generator]

        else:
            raise ValueError(f"Unsupported network: {fromNetwork}")


async def find_thorchain_receive_for_utxo_send(
    db: Cassandra,
    utxo_network: str,
    utxo_tx_hash: str,
    memo: str,
    from_timestamp: Optional[int] = None,
) -> Optional[BridgeReceiveTransfer]:
    """
    Find the receiving transaction for a UTXO THORChain deposit.

    Parses the memo to extract destination, then uses ThorchainTransactionMatcher
    to find the matching receive transaction.

    Args:
        db: Cassandra database connection
        utxo_network: Source UTXO network (e.g., 'btc')
        utxo_tx_hash: Hash of the UTXO transaction
        memo: THORChain memo from the OP_RETURN
        from_timestamp: Unix timestamp of the source transaction (for min_height estimation)

    Returns:
        BridgeReceiveTransfer if matching receive found, None otherwise
    """
    swap_info = decode_swap(memo)
    if not swap_info.get("is_swap"):
        logger.debug(f"UTXO tx {utxo_tx_hash} memo is not a swap: {memo}")
        return None

    target_address = swap_info.get("destination")
    asset_code = swap_info.get("asset")  # e.g., "ETH.ETH"

    if not target_address or not asset_code:
        logger.debug(f"Missing destination or asset in memo: {memo}")
        return None

    # Extract target network from asset code
    target_network_thor = asset_code.split(".")[0]
    target_network = THOR_TO_GRAPHSENSE_NETWORK.get(target_network_thor)

    if target_network is None:
        logger.debug(f"Unsupported target network in memo: {target_network_thor}")
        return None

    # Only search on networks where we can find TransferOut logs (EVM) or OP_RETURN (UTXO)
    if target_network not in SUPPORTED_RECEIVE_NETWORKS:
        logger.debug(
            f"Target network {target_network} not supported for receive matching"
        )
        return None

    # Build reference and use existing matcher
    receive_ref = BridgeReceiveReference(
        toAddress=target_address,
        toNetwork=target_network,
        fromTxHash=utxo_tx_hash,
        fromTimestamp=from_timestamp,
        toAsset=asset_code,  # e.g., "ETH.ETH" or "ETH.USDT-0x..."
    )

    matcher = ThorchainTransactionMatcher(target_network, db)
    receives = await matcher.match_receiving_transactions(receive_ref)

    if receives and len(receives) > 0:
        return receives[0]
    return None


async def get_bridges_from_thorchain_utxo_send(
    db: Cassandra,
    network: str,
    tx: Dict[str, Any],
) -> AsyncGenerator[Bridge, None]:
    """
    Get bridge objects from a UTXO THORChain deposit transaction.

    This is the main entry point for detecting UTXO → ETH/UTXO bridges.
    It parses the OP_RETURN memo to identify THORChain deposits, then
    uses cross-chain confirmation to find the matching receive transaction.

    Args:
        db: Cassandra database connection
        network: Source UTXO network (e.g., 'btc', 'ltc')
        tx: UTXO transaction data

    Yields:
        Bridge objects for detected bridges
    """
    if network not in UTXO_NETWORKS:
        return

    # Extract memo from OP_RETURN
    memo = extract_memo_from_utxo_tx(tx)
    if memo is None:
        return

    if not is_thorchain_memo(memo):
        return

    tx_hash = tx["tx_hash"].hex() if isinstance(tx["tx_hash"], bytes) else tx["tx_hash"]

    # Parse swap info to get destination details
    swap_info = decode_swap(memo)
    if not swap_info.get("is_swap"):
        logger.debug(f"UTXO tx {tx_hash} has THORChain memo but not a swap: {memo}")
        return

    # Build send transfer from UTXO tx
    from_address = None
    from_amount = tx.get("total_input", 0)

    inputs = tx.get("inputs", []) or []
    if inputs:
        first_input = inputs[0]
        input_addresses = getattr(first_input, "address", None) or []
        if input_addresses:
            from_address = (
                input_addresses[0]
                if isinstance(input_addresses, list)
                else input_addresses
            )

    send_transfer = BridgeSendTransfer(
        fromAddress=from_address or "unknown",
        fromAsset="native",
        fromAmount=from_amount,
        fromNetwork=network,
        fromPayment=tx_hash,
    )

    # Try to find matching receive transaction via cross-chain confirmation
    # Get timestamp for min_height estimation
    from_timestamp = tx.get("block_timestamp") or tx.get("timestamp")
    receive_transfer = await find_thorchain_receive_for_utxo_send(
        db, network, tx_hash, memo, from_timestamp=from_timestamp
    )

    if receive_transfer is not None:
        # Found complete bridge
        yield combine_bridge_transfers(send_transfer, receive_transfer)
    else:
        # Return partial bridge (send only, receive pending/failed)
        logger.debug(
            f"No matching receive found for UTXO tx {tx_hash}, returning partial bridge"
        )
        # For partial bridges, we still need the target info from the memo
        target_address = swap_info.get("destination", "unknown")
        asset_code = swap_info.get("asset", "")
        target_network_thor = asset_code.split(".")[0] if asset_code else ""
        target_network = THOR_TO_GRAPHSENSE_NETWORK.get(target_network_thor, "unknown")

        # Create a partial receive transfer with zero amount (pending)
        partial_receive = BridgeReceiveTransfer(
            toAddress=target_address,
            toAsset="native",
            toAmount=0,  # Unknown until confirmed
            toPayment="pending",
            toNetwork=target_network,
        )
        yield combine_bridge_transfers(send_transfer, partial_receive)


async def get_bridges_from_thorchain_utxo_receive(
    db: Cassandra,
    network: str,
    tx: Dict[str, Any],
) -> AsyncGenerator[Bridge, None]:
    """
    Get bridge objects from a UTXO THORChain receive transaction.

    This handles the case where a UTXO tx has an OUT: or REFUND: memo,
    indicating it's the receive side of a bridge (e.g., ETH -> BTC).

    Args:
        db: Cassandra database connection
        network: UTXO network (e.g., 'btc')
        tx: UTXO transaction data

    Yields:
        Bridge objects for detected bridges
    """
    if network not in UTXO_NETWORKS:
        return

    memo = extract_memo_from_utxo_tx(tx)
    if memo is None:
        return

    parsed = decode_withdrawal(memo)
    if not parsed.get("is_withdrawal"):
        return

    source_tx_hash = parsed.get("tx_id")
    if not source_tx_hash:
        logger.warning(f"OUT memo missing tx_id: {memo}")
        return

    tx_hash = tx["tx_hash"].hex() if isinstance(tx["tx_hash"], bytes) else tx["tx_hash"]

    # Find recipient address and amount from outputs
    to_address = None
    to_amount = 0
    for output in tx.get("outputs", []) or []:
        output_addresses = getattr(output, "address", None) or []
        script_hex = getattr(output, "script_hex", None)
        if script_hex:
            if isinstance(script_hex, bytes):
                script_hex = script_hex.hex()
            if script_hex.startswith("6a"):
                continue
        if output_addresses:
            to_address = (
                output_addresses[0]
                if isinstance(output_addresses, list)
                else output_addresses
            )
            to_amount = getattr(output, "value", 0)
            break

    receive_transfer = BridgeReceiveTransfer(
        toAddress=to_address or "unknown",
        toAsset="native",
        toAmount=to_amount,
        toNetwork=network,
        toPayment=tx_hash,
    )

    send_reference = BridgeSendReference(fromTxHash=source_tx_hash)
    matcher = ThorchainTransactionMatcher(network, db)
    send_transfers = await matcher.match_sending_transactions(send_reference)

    if send_transfers:
        for send_transfer in send_transfers:
            yield combine_bridge_transfers(send_transfer, receive_transfer)
    else:
        logger.debug(
            f"No matching send found for UTXO receive tx {tx_hash}, "
            f"source tx: {source_tx_hash}"
        )


# Example test cases
if __name__ == "__main__":
    swap_examples = [
        "=:b:bc1q6fxj6446h2gr40wtfkzqyfk848gw4q4t8akr2p:0/1/0:dx:30",
        "=:ETH.ETH:0x19317e026ef473d44D746d364062539Ba7Cb0fa3:117890211/1/0:wr:100",
    ]
    for memo in swap_examples:
        print(f"\nMemo: {memo}")  # noqa: T201
        result = decode_swap(memo)
        if result["is_swap"]:
            print(f"  Target asset: {result.get('asset')}")  # noqa: T201
            print(f"  Destination: {result.get('destination')}")  # noqa: T201
            if "limit" in result:
                print(f"  Limit: {result['limit']}")  # noqa: T201
            if "affiliate" in result:
                # noqa: T201
                print(  # noqa: T201
                    f"  Affiliate: {result['affiliate']} (fee: {result.get('affiliate_fee')} bps)"
                )
        else:
            print(f"  Not a swap: {result.get('error')}")  # noqa: T201

    transfer_examples = [
        "OUT:EA7D80B3EB709319A6577AF6CF4DEFF67975D4F5A93CD8817E7FF04A048D1C5C",
        "REFUND:1234567890ABCDEF",
        "out:abcdef123456",  # lowercase
        "=:BTC.BTC:bc1q...",  # Not a transfer out
    ]

    for memo in transfer_examples:
        print(f"\nMemo: {memo}")  # noqa: T201
        result = decode_withdrawal(memo)
        if result["is_withdrawal"]:
            print(f"  Type: {result['type']}")  # noqa: T201
            print(f"  TX ID: {result.get('tx_id')}")  # noqa: T201
            if "additional_info" in result:
                print(f"  Additional: {result['additional_info']}")  # noqa: T201
        else:
            print(f"  Not a transfer out: {result.get('error')}")  # noqa: T201
