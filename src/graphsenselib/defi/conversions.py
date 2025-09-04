from typing import Dict, List, Optional, Union, Any, Tuple

from graphsenselib.defi.bridging.thorchain import (
    get_full_bridges_from_thorchain_send,
    get_full_bridges_from_thorchain_receive,
)
from graphsenselib.defi.bridging.symbiosis import (
    get_bridges_from_symbiosis_decoded_logs,
)
from graphsenselib.datatypes.abi import decode_logs_dict
from ..defi.swaps import get_swap_from_decoded_logs
from graphsenselib.utils.logging import logger
from graphsenselib.defi.bridging.models import Bridge, BridgeStrategy
from graphsenselib.defi.swapping.models import ExternalSwap
from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.defi.models import Trace

# todo What doesnt work yet:
# Swaps that have a specified to address https://etherscan.io/tx/0x1f76090132cd8b58f7a4f8724141ca500ca65ed84d646aa200bb0dd6ec45503f


def get_bridge_strategy_from_decoded_logs(dlogs: list) -> BridgeStrategy:
    """Determine the bridge detection strategy from decoded logs."""
    if not dlogs:
        return BridgeStrategy.UNKNOWN

    names = [dlog["name"] for dlog in dlogs]

    def get_tags(dlog):
        return dlog["log_def"]["tags"]

    tags = [tag for dlog in dlogs for tag in get_tags(dlog)]
    # final_log_tags = dlogs[-1]["log_def"]["tags"] if dlogs else []
    final_log_name = dlogs[-1]["name"] if dlogs else ""

    if "bridging" in tags:
        if "wormhole" in tags:
            if final_log_name == "LogMessagePublished":
                return BridgeStrategy.WORMHOLE_AUTOMATIC_RELAY
            else:
                # todo we might not get this case if we dont have the final log decoded. Bring all logs here (not just decoded)
                raise ValueError(
                    "Not a LogMessagePublished as a final log, could be a cross chain swap or sth"
                )
        elif "stargate" in tags:
            if "Swap" in names:
                return BridgeStrategy.STARGATE
        elif "thorchain" in tags:
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
) -> List[Union[ExternalSwap, Bridge]]:
    """
    Extract all conversion information (swaps and bridges) from decoded logs.

    Returns:
        List of dictionaries with 'type' key ('swap' or 'bridge') and 'data' key containing
        the conversion information. Empty list if no conversions detected.
    """

    tx_logs_raw = await db.fetch_transaction_logs(network, tx)
    tx_traces = await db.fetch_transaction_traces(network, tx)

    if not tx_logs_raw:
        logger.info(f"No logs found for transaction {tx['tx_hash']}")
        return []

    decoded_logs_and_logs = decode_logs_dict(tx_logs_raw)

    if len(decoded_logs_and_logs) == 0:
        logger.debug(f"No decoded logs found for transaction {tx['tx_hash']}")
        return []

    decoded_log_data, tx_logs_raw_filtered = zip(*decoded_logs_and_logs)

    conversions = []

    tx_traces = Trace.dicts_to_normalized(network, tx_traces, tx)

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

    swap_results = get_swap_from_decoded_logs(
        decoded_log_data, tx_logs_raw_filtered, tx_traces, visualize
    )
    conversions += swap_results

    return conversions


def get_bridges_from_wormhole(
    dlogs: List[Dict[str, Any]], logs_raw: List[Dict[str, Any]]
) -> Optional[List[Bridge]]:
    # TODO: Unfinished

    # log_message_published = [
    #    dlog for dlog in dlogs if dlog["name"] == "LogMessagePublished"
    # ]
    # sender = log_message_published[0]["parameters"]["sender"]
    # sequence = log_message_published[0]["parameters"]["sequence"]
    # nonce = log_message_published[0]["parameters"]["nonce"]
    # payload = log_message_published[0]["parameters"]["payload"]
    # consistency_level = log_message_published[0]["parameters"]["consistencyLevel"]
    # first byte of payload is the message type
    # payload_bytes = bytes.fromhex(payload[2:])
    # transfer = decode_wormhole_payload(payload_bytes)

    logger.warning(
        "Wormhole tx found. Support not yet implemented, probably not interesting for now, no tron"
    )
    return None


def get_bridges_from_stargate(
    dlogs: List[Dict[str, Any]], logs_raw: List[Dict[str, Any]]
) -> Optional[List[Bridge]]:
    # TODO: Unfinished

    swaps = [dlog for dlog in dlogs if dlog["name"] == "Swap"]
    # todo should we call them differently than the actual log is called? because swap is ambiguous
    assert len(swaps) == 1, "Expected exactly one swap"
    # swap = swaps[0]
    # chainId = swap["parameters"]["chainId"]
    # dstPoolId = swap["parameters"]["dstPoolId"]
    # from_ = swap["parameters"]["from"]
    # amountSD = swap["parameters"]["amountSD"]
    # eqReward = swap["parameters"]["eqReward"]
    # eqFee = swap["parameters"]["eqFee"]
    # protocolFee = swap["parameters"]["protocolFee"]
    # lpFee = swap["parameters"]["lpFee"]

    logger.warning(
        "Stargate tx found. Support not yet implemented, probably not interesting for now, no tron"
    )
    return None


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

    if strategy == BridgeStrategy.WORMHOLE_AUTOMATIC_RELAY:
        bridges = get_bridges_from_wormhole(dlogs, logs_raw)

    elif strategy == BridgeStrategy.STARGATE:
        bridges = get_bridges_from_stargate(dlogs, logs_raw)

    elif strategy == BridgeStrategy.THORCHAIN_SEND and "thorchain" in included_bridges:
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
