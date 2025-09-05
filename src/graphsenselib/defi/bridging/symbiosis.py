from typing import Dict, Optional, Any, List
from graphsenselib.utils.httpx import RetryHTTPClient
from graphsenselib.utils import strip_0x
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.defi.bridging.models import Bridge
from graphsenselib.utils.logging import logger
from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.datatypes.abi import decode_logs_db, log_signatures
from graphsenselib.utils.address import AddressConverterTrx
from graphsenselib.defi.models import Trace

ACCOUNT_NETWORKS = ["eth", "trx"]
SYMBIOSIS_CHAIN_ID_TO_NETWORK = {1: "eth", 728126428: "trx", 13863860: "btc"}


async def _search_symbiosis_api(tx_hash: str) -> Optional[Dict[str, Any]]:
    """Search for transaction in Symbiosis API"""
    try:
        client = RetryHTTPClient()
        response = await client.get(
            "https://api.symbiosis.finance/explorer/v1/transactions",
            params={"search": strip_0x(tx_hash)},
        )
        if response.status_code == 200:
            records = response.json().get("records", [])
            return records[0] if len(records) == 1 else None
        return None
    except Exception as e:
        logger.warning(f"Symbiosis API error: {e}")
        return None


def _get_asset_from_token(token: Dict[str, Any]) -> str:
    """Convert token info to asset identifier"""
    if (
        not token
        or token.get("address") == "0x0000000000000000000000000000000000000000"
    ):
        return "native"
    return token.get("address", "").lower()


def _normalize_address(address: str, network: str) -> str:
    """Normalize address for different networks"""
    if network == "trx":
        ac = AddressConverterTrx()
        address = "0x" + ac.to_canonical_address_str(address)

    address = address.lower()
    if not address.startswith("0x"):
        address = "0x" + address
    return address


async def get_bridges_from_symbiosis_decoded_logs(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
) -> Optional[List[Bridge]]:
    """Get bridge transfers from Symbiosis API using decoded logs for payment identifiers"""

    tx_hash = tx.get("tx_hash")
    if not tx_hash:
        return None

    if isinstance(tx_hash, bytes):
        tx_hash = tx_hash.hex()

    # Get bridge data from Symbiosis API
    record = await _search_symbiosis_api(tx_hash)
    if not record:
        return None

    # Extract bridge information
    from_route = record.get("from_route", [])
    to_route = record.get("to_route", [])
    if not from_route or not to_route:
        return None

    from_network = SYMBIOSIS_CHAIN_ID_TO_NETWORK.get(record.get("from_chain_id"))
    to_network = SYMBIOSIS_CHAIN_ID_TO_NETWORK.get(record.get("to_chain_id"))
    if not from_network or not to_network:
        return None

    # Extract amounts and assets
    from_info = from_route[0]
    to_info = to_route[-1]  # Last entry is final asset

    # Create payment identifiers
    from_payment = _create_payment_identifier(
        record["from_tx_hash"],
        record["from_address"],
        _get_asset_from_token(from_info.get("token")),
        dlogs,
        logs_raw,
        traces,
        from_network,
    )

    to_payment = await _create_payment_identifier_for_receive(
        db,
        to_network,
        record["to_tx_hash"],
        record["to_address"],
        _get_asset_from_token(to_info.get("token")),
    )

    if not from_payment or not to_payment:
        return None

    return [
        Bridge(
            fromAddress=record["from_address"],
            fromAsset=_get_asset_from_token(from_info.get("token")),
            fromAmount=int(from_info.get("amount", 0)),
            fromPayment=from_payment,
            fromNetwork=from_network,
            toAddress=record["to_address"],
            toAsset=_get_asset_from_token(to_info.get("token")),
            toAmount=int(to_info.get("amount", 0)),
            toPayment=to_payment,
            toNetwork=to_network,
        )
    ]


def _create_payment_identifier(
    tx_hash: str,
    address: str,
    asset: str,
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
    network: str,
) -> str:
    """Create payment identifier using already-available transfers and traces"""
    if not tx_hash or network not in ACCOUNT_NETWORKS:
        return tx_hash

    normalized_address = _normalize_address(address, network)

    try:
        if asset == "native":
            # Find traces where the user is the sender (from_address) - for send side
            relevant_traces = [
                trace
                for trace in traces
                if trace.from_address == address.lower() and trace.value > 0
            ]

            if relevant_traces:
                trace = relevant_traces[0]  # Take first match
                return SubTransactionIdentifier(
                    tx_hash=tx_hash,
                    tx_type=SubTransactionType.InternalTx,
                    sub_index=trace.trace_index,
                ).to_string()
        else:
            # Find Transfer events where the user is the sender (from parameter) - for send side
            for dlog, log_raw in zip(dlogs, logs_raw):
                if (
                    dlog.get("name") == "Transfer"
                    and dlog.get("address", "").lower() == asset.lower()
                ):
                    params = dlog.get("parameters", {})
                    # Only match if user is the sender (from), not recipient (to)
                    if params.get("from", "").lower() == normalized_address:
                        return SubTransactionIdentifier(
                            tx_hash=tx_hash,
                            tx_type=SubTransactionType.ERC20,
                            sub_index=log_raw.get("log_index", 0),
                        ).to_string()
    except Exception as e:
        logger.warning(f"Error creating payment identifier: {e}")

    return tx_hash


async def _create_payment_identifier_for_receive(
    db: Cassandra, network: str, tx_hash: str, address: str, asset: str
) -> Optional[str]:
    """Create payment identifier for receive transaction by fetching its data"""
    if not tx_hash or network not in ACCOUNT_NETWORKS:
        return tx_hash

    try:
        tx_hash_bytes = bytes.fromhex(strip_0x(tx_hash))
        tx = await db.get_tx_by_hash(network, tx_hash_bytes)
        if not tx:
            return tx_hash

        block_number = tx["block_id"]
        normalized_address = _normalize_address(address, network)

        if asset == "native":
            traces_raw = await db.get_traces_in_block(
                network, block_number, tx_hash=tx_hash_bytes
            )
            traces = Trace.dicts_to_normalized(network, traces_raw, tx)

            # Find traces where the user is the recipient (to_address)
            relevant_traces = [
                trace
                for trace in traces
                if trace.to_address == address.lower() and trace.value > 0
            ]

            if relevant_traces:
                trace = relevant_traces[0]  # Take first match
                return SubTransactionIdentifier(
                    tx_hash=tx_hash,
                    tx_type=SubTransactionType.InternalTx,
                    sub_index=trace.trace_index,
                ).to_string()
        else:
            logs = await db.get_logs_in_block_eth(
                network, block_number, tx_hash=tx_hash_bytes
            )
            decoded_logs = decode_logs_db(logs, log_signatures_local=log_signatures)

            # Find Transfer events where the user is the recipient (to parameter)
            for decoded_log, raw_log in decoded_logs:
                if (
                    decoded_log["name"] == "Transfer"
                    and decoded_log["address"].lower() == asset.lower()
                    and raw_log["tx_hash"] == tx_hash_bytes
                ):
                    params = decoded_log["parameters"]
                    # Only match if user is the recipient (to), not sender (from)
                    if params.get("to", "").lower() == normalized_address:
                        return SubTransactionIdentifier(
                            tx_hash=tx_hash,
                            tx_type=SubTransactionType.ERC20,
                            sub_index=raw_log["log_index"],
                        ).to_string()
    except Exception as e:
        logger.warning(f"Error creating receive payment identifier: {e}")

    return tx_hash
