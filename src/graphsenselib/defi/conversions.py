import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Union, Any, Tuple

from graphsenselib.defi.bridging.thorchain import (
    get_full_bridges_from_thorchain_send,
    get_full_bridges_from_thorchain_receive,
    extract_memo_from_input,
    is_thorchain_deposit_address,
    is_thorchain_utxo_deposit,
    is_thorchain_utxo_receive,
    get_bridges_from_thorchain_utxo_send,
    get_bridges_from_thorchain_utxo_receive,
    UTXO_NETWORKS,
)
from graphsenselib.defi.bridging.symbiosis import (
    get_bridges_from_symbiosis_decoded_logs,
)
from graphsenselib.datatypes.abi import decode_logs_dict
from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.models import Bridge, BridgeStrategy
from graphsenselib.defi.swapping.models import ExternalSwap
from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.defi.models import Trace

# todo What doesnt work yet:
# Swaps that have a specified to address https://etherscan.io/tx/0x1f76090132cd8b58f7a4f8724141ca500ca65ed84d646aa200bb0dd6ec45503f

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DetectedConversions:
    """What get_conversions_from_db finds in a tx: the swaps and bridges it
    detects, and the bridges it finds only half of (defi/bridging/delegated.py),
    which an external backend may complete."""

    conversions: Tuple[Union[ExternalSwap, Bridge], ...] = ()
    half_bridges: Tuple[Bridge, ...] = ()


async def is_direct_vault_deposit(
    network: str, db: Cassandra, tx: Dict[str, Any]
) -> bool:
    """
    Check if a transaction is a direct THORChain vault deposit.

    Direct vault deposits have:
    1. A THORChain memo in the transaction input data
    2. The receiver is a THORChain deposit address (single outgoing neighbor to a router)
    """
    # Check for memo in input data
    memo = extract_memo_from_input(tx)
    if memo is None:
        return False

    # Get receiver address
    to_address = tx.get("to_address")
    if to_address is None:
        return False

    if isinstance(to_address, bytes):
        to_address = "0x" + to_address.hex()

    # Validate receiver is a THORChain deposit address
    return await is_thorchain_deposit_address(db, network, to_address)


def get_bridge_strategy_from_decoded_logs(dlogs: list) -> BridgeStrategy:
    """Determine the bridge detection strategy from decoded logs."""
    if not dlogs:
        return BridgeStrategy.UNKNOWN

    names = [dlog["name"] for dlog in dlogs]

    def get_tags(dlog):
        return dlog["log_def"]["tags"]

    tags = [tag for dlog in dlogs for tag in get_tags(dlog)]

    if "bridging" in tags:
        if "thorchain" in tags:
            if "Deposit" in names:
                return BridgeStrategy.THORCHAIN_SEND
            elif "TransferOut" in names:
                return BridgeStrategy.THORCHAIN_RECEIVE
        elif "symbiosis" in tags:
            return BridgeStrategy.SYMBIOSIS

    return BridgeStrategy.UNKNOWN


async def get_conversions_from_db(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    visualize: bool = False,
    included_bridges: Tuple[str, ...] = (),
    cow_protocol_swaps: bool = True,
) -> DetectedConversions:
    """
    Extract all conversion information (swaps and bridges) from decoded logs.

    cow_protocol_swaps: report the orders of CoW Protocol settlements as
    swaps (one per order), and link the eth-flow ETH sell orders placed in tx
    to their settlements; False ignores both.

    Returns:
        The swaps and bridges detected, and the bridges of included_bridges
        that graphsense-lib finds only half of. Both are empty if nothing is
        detected.
    """

    # Check for UTXO THORChain transactions FIRST (before fetching logs)
    # UTXO networks don't have logs - detection is via OP_RETURN memo
    if network in UTXO_NETWORKS:
        if "thorchain" in included_bridges:
            tx_hash = (
                tx["tx_hash"].hex()
                if isinstance(tx["tx_hash"], bytes)
                else tx["tx_hash"]
            )
            if is_thorchain_utxo_deposit(tx):
                logger.debug(f"UTXO THORChain deposit detected for tx {tx_hash}")
                bridges = []
                async for bridge in get_bridges_from_thorchain_utxo_send(
                    db, network, tx
                ):
                    bridges.append(bridge)
                return DetectedConversions(tuple(bridges))
            elif is_thorchain_utxo_receive(tx):
                logger.debug(f"UTXO THORChain receive detected for tx {tx_hash}")
                bridges = []
                async for bridge in get_bridges_from_thorchain_utxo_receive(
                    db, network, tx
                ):
                    bridges.append(bridge)
                return DetectedConversions(tuple(bridges))
        return DetectedConversions()

    # For EVM networks, fetch logs and traces. Both scans read the same
    # tx's block partition independently of each other, so run them
    # concurrently instead of one after another.
    tx_logs_raw, tx_traces = await asyncio.gather(
        db.fetch_transaction_logs(network, tx),
        db.fetch_transaction_traces(network, tx),
    )
    tx_traces = Trace.dicts_to_normalized(network, tx_traces, tx)
    conversions = await _conversions_from_logs(
        network,
        db,
        tx,
        tx_logs_raw,
        tx_traces,
        visualize=visualize,
        included_bridges=included_bridges,
        cow_protocol_swaps=cow_protocol_swaps,
    )
    return DetectedConversions(
        tuple(conversions),
        half_bridges(network, tx_logs_raw or (), included_bridges),
    )


async def _conversions_from_logs(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    tx_logs_raw: List[Dict[str, Any]],
    tx_traces: List[Trace],
    visualize: bool,
    included_bridges: Tuple[str, ...],
    cow_protocol_swaps: bool,
) -> List[Union[ExternalSwap, Bridge]]:
    """The swaps and bridges of an EVM tx, detected from its raw logs and
    normalized traces (get_conversions_from_db)."""
    # Check for direct vault deposits (no logs, memo in input data)
    # This must be checked before the early return for no logs
    if (not tx_logs_raw or len(tx_logs_raw) == 0) and "thorchain" in included_bridges:
        if await is_direct_vault_deposit(network, db, tx):
            logger.debug(
                f"Direct vault deposit detected for tx {tx['tx_hash'].hex() if isinstance(tx['tx_hash'], bytes) else tx['tx_hash']}"
            )
            bridges = []
            async for bridge in get_full_bridges_from_thorchain_send(
                network, db, tx, [], [], tx_traces
            ):
                bridges.append(bridge)
            return bridges

    if not tx_logs_raw:
        logger.debug(f"No logs found for transaction {tx['tx_hash']}")
        return []

    decoded_logs_and_logs = decode_logs_dict(tx_logs_raw)

    if len(decoded_logs_and_logs) == 0:
        logger.debug(f"No decoded logs found for transaction {tx['tx_hash']}")
        return []

    decoded_log_data, tx_logs_raw_filtered = zip(*decoded_logs_and_logs)

    conversions = []

    bridge_result = await get_bridges_from_decoded_logs(
        network,
        db,
        tx,
        decoded_log_data,
        tx_logs_raw_filtered,
        tx_traces,
        included_bridges,
    )
    conversions += bridge_result

    from graphsenselib.defi.swaps import get_swap_from_decoded_logs

    if tx.get("input"):
        swap_results = get_swap_from_decoded_logs(
            decoded_log_data,
            tx_logs_raw_filtered,
            tx_traces,
            visualize,
            transaction_input=tx["input"],
            cow_protocol=cow_protocol_swaps,
        )
    else:
        swap_results = get_swap_from_decoded_logs(
            decoded_log_data,
            tx_logs_raw_filtered,
            tx_traces,
            visualize,
            cow_protocol=cow_protocol_swaps,
        )
    conversions += swap_results

    if cow_protocol_swaps:
        from graphsenselib.defi.swapping.cow_ethflow import (
            get_eth_flow_swaps,
            is_eth_flow_placement,
        )

        if any(map(is_eth_flow_placement, decoded_log_data)):
            conversions += await get_eth_flow_swaps(
                network, db, tx, list(decoded_log_data), tx_traces
            )

    return conversions


async def get_bridges_from_decoded_logs(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
    included_bridges: Tuple[str, ...] = (),
) -> List[Bridge]:
    """
    Extract bridging information from decoded logs.
    Currently only handles simple Withdrawal events tagged as bridging.

    Args:
        dlogs: List of decoded log dictionaries
        logs_raw: List of raw log dictionaries
        traces: List of trace dictionaries (unused for now)

    Returns:
        Bridge object if bridging withdrawal detected, None otherwise
    """

    # Sort logs by log_index
    dlogs, logs_raw = zip(
        *sorted(zip(dlogs, logs_raw), key=lambda x: x[1]["log_index"])
    )

    strategy = get_bridge_strategy_from_decoded_logs(dlogs)

    bridges = None

    if strategy == BridgeStrategy.THORCHAIN_SEND and "thorchain" in included_bridges:
        bridge_generator = get_full_bridges_from_thorchain_send(
            network, db, tx, dlogs, logs_raw, traces
        )
        if bridge_generator is not None:
            bridges = []
            async for bridge in bridge_generator:
                bridges.append(bridge)
        else:
            bridges = []

    elif (
        strategy == BridgeStrategy.THORCHAIN_RECEIVE and "thorchain" in included_bridges
    ):
        bridge_generator = get_full_bridges_from_thorchain_receive(
            network, db, tx, dlogs, logs_raw, traces
        )
        if bridge_generator is not None:
            bridges = []
            async for bridge in bridge_generator:
                bridges.append(bridge)
        else:
            bridges = []

    elif strategy == BridgeStrategy.SYMBIOSIS and "symbiosis" in included_bridges:
        bridges = await get_bridges_from_symbiosis_decoded_logs(
            network, db, tx, dlogs, logs_raw, traces
        )

    if bridges is None:
        return []
    else:
        return bridges
