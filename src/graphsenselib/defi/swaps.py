from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from graphsenselib.utils.accountmodel import ETH_PLACEHOLDER_ADDRESS

try:
    import networkx as nx
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
except ImportError:
    raise Exception(
        "Please install graphsense-lib with conversions optional dependencies."
    )
from graphsenselib.utils.logging import logger

from graphsenselib.utils.accountmodel import ensure_0x_prefix, normalize_asset
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.defi.swapping.models import (
    ExternalSwap,
    SwapStrategy,
    get_swap_strategy_from_decoded_logs,
)
from graphsenselib.defi.models import Trace


@dataclass
class AssetFlow:
    """Represents an asset flow between addresses."""

    from_address: str
    to_address: str
    asset: str
    amount: int
    source_type: str
    source_index: int


@dataclass
class AmountInfo:
    """Represents amount information for an address."""

    asset: str
    amount: int
    source_type: str
    source_index: int


def extract_asset_flows(
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
) -> Tuple[List[AssetFlow], List[AssetFlow], List[AssetFlow], List[AssetFlow]]:
    """Extract asset flows from transfers, withdrawals, deposits and traces."""

    # Extract transfers
    transfers = [dlog for dlog in dlogs if dlog["name"] == "Transfer"]
    transfer_asset_flows = [
        AssetFlow(
            from_address=dlog["parameters"]["from"],
            to_address=dlog["parameters"]["to"],
            asset=dlog["address"].lower(),
            amount=dlog["parameters"]["value"],
            source_type="erc20",
            source_index=logs_raw[next(i for i, d in enumerate(dlogs) if d == dlog)][
                "log_index"
            ],
        )
        for i, dlog in enumerate(transfers)
    ]

    # Extract withdrawals
    withdrawals = [dlog for dlog in dlogs if dlog["name"] == "Withdrawal"]
    withdrawal_asset_flows = [
        AssetFlow(
            from_address=dlog["parameters"]["src"],
            to_address=dlog["address"].lower(),
            asset=dlog["address"].lower(),
            amount=dlog["parameters"]["value"],
            source_type="erc20",
            source_index=logs_raw[next(i for i, d in enumerate(dlogs) if d == dlog)][
                "log_index"
            ],
        )
        for dlog in withdrawals  # WETH to WETH contract
    ]

    # Extract deposits
    deposits = [dlog for dlog in dlogs if dlog["name"] == "Deposit"]
    deposit_asset_flows = [
        AssetFlow(
            from_address=dlog["address"].lower(),
            to_address=dlog["parameters"]["dst"],
            asset=dlog["address"].lower(),
            amount=dlog["parameters"]["wad"],
            source_type="erc20",
            source_index=logs_raw[next(i for i, d in enumerate(dlogs) if d == dlog)][
                "log_index"
            ],
        )
        for dlog in deposits
    ]

    # Extract traces
    relevant_traces = [trace for trace in traces if trace.is_call and trace.value != 0]
    traces_asset_flows = [
        AssetFlow(
            from_address=trace.from_address,
            to_address=trace.to_address,
            asset=ETH_PLACEHOLDER_ADDRESS,
            amount=trace.value,
            source_type="trace",
            source_index=trace.trace_index,
        )
        for trace in relevant_traces
    ]

    return (
        transfer_asset_flows,
        withdrawal_asset_flows,
        deposit_asset_flows,
        traces_asset_flows,
    )


def get_asset_flows_of_address(
    address: str, all_asset_flows: List[AssetFlow]
) -> Tuple[List[AmountInfo], List[AmountInfo]]:
    """Get incoming and outgoing asset flows for a specific address."""
    outgoing_amounts = [
        AmountInfo(
            asset=flow.asset,
            amount=flow.amount,
            source_type=flow.source_type,
            source_index=flow.source_index,
        )
        for flow in all_asset_flows
        if flow.from_address.lower() == address.lower()
    ]
    incoming_amounts = [
        AmountInfo(
            asset=flow.asset,
            amount=flow.amount,
            source_type=flow.source_type,
            source_index=flow.source_index,
        )
        for flow in all_asset_flows
        if flow.to_address.lower() == address.lower()
    ]

    return outgoing_amounts, incoming_amounts


def create_payment_identifier(
    tx_hash_hex: str, source_type: str, source_index: int
) -> str:
    """Create a payment identifier based on transaction hash, source type, and index."""
    txh0x = ensure_0x_prefix(tx_hash_hex)

    if source_type == "erc20":
        payment = SubTransactionIdentifier(
            tx_hash=txh0x,
            tx_type=SubTransactionType.ERC20,
            sub_index=source_index,
        ).to_string()
    else:  # trace
        payment = SubTransactionIdentifier(
            tx_hash=txh0x,
            tx_type=SubTransactionType.InternalTx,
            sub_index=source_index,
        ).to_string()

    return payment


def get_swap_from_eulerian_path(
    all_asset_flows: List[AssetFlow],
    transfer_asset_flows: List[AssetFlow],
    traces: List[Trace],
    logs_raw: List[Dict[str, Any]],
    version: str,
) -> ExternalSwap:
    """Extract swap information from eulerian path analysis."""
    trace0 = traces[0]
    assert (
        trace0.trace_address == ""
    )  # this means the trace is the first trace of the tx
    sender = trace0.from_address

    # get all the amounts outgoing from the sender
    outgoing_amounts, incoming_amounts = get_asset_flows_of_address(
        sender, all_asset_flows
    )

    # CASE WHERE THE SENDER OF THE TX IS NOT THE SWAPPER
    if len(outgoing_amounts) == 0 and len(incoming_amounts) == 0:
        # todo this might not be true if the first transfer is ETH
        sender = transfer_asset_flows[0].from_address
        outgoing_amounts, incoming_amounts = get_asset_flows_of_address(
            sender, all_asset_flows
        )
        version += "sender-not-swapper"

    tx_hash = logs_raw[0]["tx_hash"].hex()

    if not len(outgoing_amounts) == 1:
        logger.warning(
            f"Expected exactly one outgoing amount, got {len(outgoing_amounts)}, {tx_hash}"
        )
        return None
    if not len(incoming_amounts) == 1:
        logger.warning(
            f"Expected exactly one incoming amount, got {len(incoming_amounts)}, {tx_hash}"
        )
        return None

    fromAsset = outgoing_amounts[0].asset
    fromAmount = outgoing_amounts[0].amount
    from_source_type = outgoing_amounts[0].source_type
    from_source_index = outgoing_amounts[0].source_index

    toAsset = incoming_amounts[0].asset
    toAmount = incoming_amounts[0].amount
    to_source_type = incoming_amounts[0].source_type
    to_source_index = incoming_amounts[0].source_index

    tx_hash_hex = logs_raw[0]["tx_hash"].hex()
    fromPayment = create_payment_identifier(
        tx_hash_hex, from_source_type, from_source_index
    )
    toPayment = create_payment_identifier(tx_hash_hex, to_source_type, to_source_index)

    return ExternalSwap(
        fromAddress=sender,
        toAddress=sender,
        fromAsset=normalize_asset(fromAsset),
        toAsset=normalize_asset(toAsset),
        fromAmount=fromAmount,
        toAmount=toAmount,
        fromPayment=fromPayment,
        toPayment=toPayment,
    )


# def handle_order_record_swap(
#    dlogs: List[Dict[str, Any]], logs_raw: List[Dict[str, Any]]
# ) -> ExternalSwap:
#    """Handle OrderRecord type swaps."""
#    relevant_logs = [dlog for dlog in dlogs if dlog["name"] == "OrderRecord"]
#    relevant_logs_i = [
#        i for i, dlog in enumerate(dlogs) if dlog["name"] == "OrderRecord"
#    ]
#    assert len(relevant_logs) == 1, "Expected exactly one OrderRecord log"
#    dlog = relevant_logs[0]
#    log_raw = logs_raw[relevant_logs_i[0]]
#
#    # todo maybe not optimal - this is now just the log of the OrderRecord
#    # and not of the transfers
#    fromPayment = SubTransactionIdentifier(
#        tx_hash=ensure_0x_prefix(log_raw["tx_hash"].hex()),
#        tx_type=SubTransactionType.ERC20,
#        sub_index=int(log_raw["log_index"]),
#    ).to_string()
#
#    params = dlog["parameters"]
#
#    fromAmount = params["fromAmount"]
#    toAmount = params["toAmount"]
#    fromAsset = params["fromToken"]
#    toAsset = params["toToken"]
#    sender = params["sender"]
#
#    return ExternalSwap(
#        fromAddress=sender,
#        toAddress=sender,
#        fromAsset=normalize_asset(fromAsset),
#        toAsset=normalize_asset(toAsset),
#        fromAmount=fromAmount,
#        toAmount=toAmount,
#        fromPayment=fromPayment,
#        toPayment=fromPayment,
#    )


def filter_graph_for_eulerian_path(
    G: nx.DiGraph, all_asset_flows: List[AssetFlow]
) -> Tuple[nx.DiGraph, List[AssetFlow]]:
    """Filter graph by removing dangling nodes to make it eulerian."""
    nodes = G.nodes

    # First try: Remove edges where the source node has no outgoing edges
    missing_outgoing = set()
    for node in nodes:
        if G.in_degree(node) == 1:
            if G.out_degree(node) == 0:
                missing_outgoing.add(node)

    all_asset_flows_filtered = [
        flow
        for flow in all_asset_flows
        if flow.from_address.lower() not in missing_outgoing
        and flow.to_address.lower() not in missing_outgoing
    ]
    all_from_tos_filtered = [
        (flow.from_address, flow.to_address) for flow in all_asset_flows_filtered
    ]

    G_filtered = nx.DiGraph()
    G_filtered.add_edges_from(all_from_tos_filtered)

    return G_filtered, all_asset_flows_filtered


def visualize_graph(
    G,
    tx_hash: str = "unknown",
    swap_edges: list = None,
    transfer_edges: list = None,
    tx_sender: str = None,
):
    plt.figure(figsize=(10, 8))
    pos = nx.spring_layout(G)
    plt.title(f"Transaction {tx_hash}")

    # Prepare edge colors
    swap_edges = swap_edges or []
    transfer_edges = transfer_edges or []

    # Convert to sets for easier comparison (normalize to lowercase)
    swap_edges_set = {(edge[0].lower(), edge[1].lower()) for edge in swap_edges}
    transfer_edges_set = {(edge[0].lower(), edge[1].lower()) for edge in transfer_edges}

    # Create edge color list matching the order of edges in the graph
    edge_colors = []
    for edge in G.edges():
        edge_normalized = (edge[0].lower(), edge[1].lower())
        if edge_normalized in swap_edges_set:
            if edge_normalized in transfer_edges_set:
                edge_colors.append("green")  # swap+transfer edges
            else:
                edge_colors.append("red")  # swap-only edges
        else:
            edge_colors.append("black")  # other edges

    # Create node color list
    node_colors = []
    for node in G.nodes():
        if tx_sender and node.lower() == tx_sender.lower():
            node_colors.append("orange")  # highlight tx sender
        else:
            node_colors.append("lightblue")  # default color

    # Draw the graph with colored edges and nodes
    nx.draw(
        G,
        pos,
        with_labels=True,
        node_color=node_colors,
        node_size=1500,
        edge_color=edge_colors,
        arrowsize=20,
        font_size=8,
        width=2,
        connectionstyle="arc3,rad=0.1",
    )

    # Add legend

    legend_elements = [
        Patch(facecolor="orange", label="Transaction Sender"),
        Patch(facecolor="lightblue", label="Other Addresses"),
        Patch(facecolor="green", label="Swap + Transfer Edges"),
        Patch(facecolor="red", label="Swap-only Edges"),
        Patch(facecolor="black", label="Other Edges"),
    ]
    plt.legend(handles=legend_elements, loc="upper right", bbox_to_anchor=(0.1, 0.1))

    # add enough padding
    plt.show()


def handle_general_swap(
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
    visualize: bool = False,
) -> Optional[ExternalSwap]:
    """Handle general swap detection using graph analysis."""

    tx_hash = logs_raw[0]["tx_hash"].hex()

    # Validate minimum requirements
    transfers = [dlog for dlog in dlogs if dlog["name"] == "Transfer"]
    if len(transfers) < 2:
        logger.warning(f"Not enough transfers to detect a general swap , {tx_hash}")
        return None

    # Extract all asset flows
    (
        transfer_asset_flows,
        withdrawal_asset_flows,
        deposit_asset_flows,
        traces_asset_flows,
    ) = extract_asset_flows(dlogs, logs_raw, traces)

    all_asset_flows = (
        []
        + transfer_asset_flows
        + traces_asset_flows  # might not need it if we track withdrawals and deposits
        + withdrawal_asset_flows
        + deposit_asset_flows
    )
    all_from_tos = [(flow.from_address, flow.to_address) for flow in all_asset_flows]

    # Extract swap edges for visualization
    swaps = [dlog for dlog in dlogs if dlog["name"] == "Swap"]
    swap_froms = [
        dlog["parameters"]["sender"] for dlog in swaps if "sender" in dlog["parameters"]
    ]
    swap_intermediates = [dlog["address"] for dlog in swaps]
    swap_tos = [
        dlog["parameters"]["to"]
        if "to" in dlog["parameters"]
        else dlog["parameters"]["sender"]
        for dlog in swaps
    ]

    swap_from_tos = list(zip(swap_froms, swap_intermediates)) + list(
        zip(swap_intermediates, swap_tos)
    )
    swap_from_tos = [(x[0].lower(), x[1].lower()) for x in swap_from_tos]

    # Extract transaction sender from traces
    tx_sender = None
    if traces:
        trace0 = traces[0]
        if trace0.trace_address is None:
            return None
        if trace0.trace_address == "":
            tx_sender = trace0.from_address

    transfer_edges = [
        (flow.from_address, flow.to_address) for flow in transfer_asset_flows
    ]

    # Build graph
    G = nx.DiGraph()
    G.add_edges_from(all_from_tos)
    assert nx.is_weakly_connected(G), "Graph is not weakly connected"

    if visualize:
        visualize_graph(G, tx_hash, swap_from_tos, transfer_edges, tx_sender)

    # Check if we can find a eulerian path
    if nx.is_eulerian(G):
        return get_swap_from_eulerian_path(
            all_asset_flows, transfer_asset_flows, traces, logs_raw, version="swap"
        )

    # Handle non-eulerian paths by filtering
    G_filtered, all_asset_flows_filtered = filter_graph_for_eulerian_path(
        G, all_asset_flows
    )

    if nx.is_eulerian(G_filtered):
        return get_swap_from_eulerian_path(
            all_asset_flows_filtered,
            transfer_asset_flows,
            traces,
            logs_raw,
            version="swap-prune-dangling-out",
        )

    # Second filter attempt: Remove edges where the target node has no incoming edges
    nodes = G.nodes
    missing_incoming = set()
    for node in nodes:
        if G.out_degree(node) == 1:
            if G.in_degree(node) == 0:
                missing_incoming.add(node)

    all_asset_flows_filtered = [
        flow
        for flow in all_asset_flows
        if flow.from_address.lower() not in missing_incoming
        and flow.to_address.lower() not in missing_incoming
    ]
    all_from_tos_filtered = [
        (flow.from_address, flow.to_address) for flow in all_asset_flows_filtered
    ]
    G_filtered = nx.DiGraph()
    G_filtered.add_edges_from(all_from_tos_filtered)

    # Check if filtered graph is eulerian
    if nx.is_eulerian(G_filtered):
        logger.warning(
            f"Graph had dangling incoming edges, can be e.g. "
            f"because of a user sending a tx to a MEV bot contract, {tx_hash}"
            f"So far these have almost exclusively been MEV bots."
        )
        return None

    return get_swap_from_eulerian_path(
        all_asset_flows,
        transfer_asset_flows,
        traces,
        logs_raw,
        version="swap-non-eulerian",
    )


def get_swap_from_decoded_logs(
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
    visualize: bool = False,
) -> List[ExternalSwap]:
    """
    Main function to extract swap information from decoded logs.

    This function has been refactored to use modular components for better maintainability.
    """
    # Sort dlogs and raw logs
    dlogs, logs_raw = zip(
        *sorted(zip(dlogs, logs_raw), key=lambda x: x[1]["log_index"])
    )

    # Determine strategy
    strategy = get_swap_strategy_from_decoded_logs(dlogs)

    # if strategy == SwapStrategy.IGNORE:
    swaps = []

    # if strategy == SwapStrategy.ORDER_RECORD:
    #    swaps += [handle_order_record_swap(dlogs, logs_raw)]
    if strategy == SwapStrategy.SWAP:
        swaps += [handle_general_swap(dlogs, logs_raw, traces, visualize)]

    swaps = [swap for swap in swaps if swap is not None]
    return swaps
