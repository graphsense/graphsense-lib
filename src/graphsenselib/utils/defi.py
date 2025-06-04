# from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from typing import Optional

import requests
from eth_abi import decode
from eth_abi.exceptions import (
    DecodingError,
    InsufficientDataBytes,
    NonEmptyPaddingBytes,
)
from eth_hash.auto import keccak
import networkx as nx

from .accountmodel import strip_0x

# todo What doesnt work yet:
# Swaps that have a specified to address https://etherscan.io/tx/0x1f76090132cd8b58f7a4f8724141ca500ca65ed84d646aa200bb0dd6ec45503f
# standardize 0x000 to 0xeee or other way around


# should be ETH not WETH back:
# 2025-06-03 17:40:17,808 - __main__ - INFO - Found swap: ExternalSwap(swapper='0x4acb6c4321253548a7d4bb9c84032cc4ee04bfd7', fromAmount=266694890725, toAmount=361721269610745, fromToken='0x8390a1da07e376ef7add4be859ba74fb83aa02d5', toToken='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', version='swap', swap_log='0x37bb440595bcf32d8506212b799f06424a41cb7aa21e653846f4d3da1bb36c9d_S28')


@dataclass(frozen=True)
class DexPair:
    t0: str
    t1: Optional[str]
    version: str
    pool_address: str
    pair_id: Optional[str]
    issuer: str
    creation_log: str

    def get_id(self) -> int:
        return hash(str(self))


@dataclass(frozen=True)
class ExternalSwap:
    swapper: str
    fromAmount: str
    toAmount: str
    fromToken: str
    toToken: Optional[str]
    version: str
    swap_log: str

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class TokenMetadata:
    adr: str
    name: Optional[str]
    ticker: Optional[str]
    decimals: Optional[int]


def get_pair_from_decoded_log(dlog, log_raw):
    name = dlog["name"]
    issuer = dlog["address"]

    creation_log = "0x" + log_raw["tx_hash"].hex() + "_L" + str(log_raw["log_index"])

    if name == "PairCreated":
        t0 = dlog["parameters"]["token0"]
        t1 = dlog["parameters"]["token1"]
        v = "uni2"
        pool_address = dlog["parameters"]["pair"]
        pair_id = None
    elif name == "PoolCreated":
        t0 = dlog["parameters"]["token0"]
        t1 = dlog["parameters"]["token1"]
        v = "uni3"
        pool_address = dlog["parameters"]["pool"]
        pair_id = None
    elif name == "Initialize":
        t0 = dlog["parameters"]["currency0"]
        t1 = dlog["parameters"]["currency1"]
        v = "uni4"
        pool_address = "0x000000000004444c5dc75cB358380D2e3dE08A90"
        pair_id = dlog["parameters"]["id"]
    elif name == "NewExchange":
        t0 = dlog["parameters"]["token"]
        t1 = None
        pool_address = dlog["parameters"]["exchange"]
        v = "uni1"
        pair_id = None
    else:
        raise ValueError(f"Trading pair of type {name} not supported")

    return DexPair(t0, t1, v, pool_address, pair_id, issuer, creation_log)


def get_strategy_from_decoded_logs(dlogs: list) -> str:
    names = [dlog["name"] for dlog in dlogs]

    def get_tags(dlog):
        return dlog["log_def"]["tags"]

    tags = [tag for dlog in dlogs for tag in get_tags(dlog)]
    final_log_tags = dlogs[-1]["log_def"]["tags"] if dlogs else []

    # just a guess but i think it should be last, so its not just routed through there?
    # Lets be conservative
    if "OrderRecord" == names[-1]:
        return "OrderRecord"
    elif (
        "settlement" in final_log_tags and "cow-protocol" in final_log_tags
    ) or "cross-chain" in tags:
        # e.g. https://etherscan.io/tx/0x8e7a3d044ed6873a5683ffe2f59b8cd68a3d786edaa64cdc4c05a9ae8ff97984
        # may settle multiple orders in one tx
        #
        return "Ignore"
    elif "swap" in tags:
        return "Swap"
    else:
        return


def visualize_graph(
    G,
    tx_hash: str = "unknown",
    swap_edges: list = None,
    transfer_edges: list = None,
    tx_sender: str = None,
):
    import matplotlib.pyplot as plt

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
    from matplotlib.patches import Patch

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


def get_swap_from_decoded_logs(
    dlogs: list, logs_raw: list, traces: list, visualize: bool = False
) -> Optional[ExternalSwap]:
    # Note token 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee is a placeholder for ETH

    # Probably have to check traces to handle that

    # log_indices = [log['log_index'] for log in logs_raw]
    # sort dlogs and raw logs

    # todo could be removed later if we just allow dicts in the first place
    if len(logs_raw) > 0 and not isinstance(logs_raw[0], dict):
        logs_raw = [log_raw._asdict() for log_raw in logs_raw]
    if len(traces) > 0 and not isinstance(traces[0], dict):
        traces = [trace._asdict() for trace in traces]

    dlogs, logs_raw = zip(
        *sorted(zip(dlogs, logs_raw), key=lambda x: x[1]["log_index"])
    )
    # logic to decide which handling function to use.
    strategy = get_strategy_from_decoded_logs(dlogs)

    if strategy == "Ignore":
        return None
    elif strategy == "OrderRecord":
        relevant_logs = [dlog for dlog in dlogs if dlog["name"] == "OrderRecord"]
        relevant_logs_i = [
            i for i, dlog in enumerate(dlogs) if dlog["name"] == "OrderRecord"
        ]
        assert len(relevant_logs) == 1, "Expected exactly one OrderRecord log"
        dlog = relevant_logs[0]
        log_raw = logs_raw[relevant_logs_i[0]]
        swap_log = f"0x{log_raw['tx_hash'].hex()}_S{log_raw['log_index']}"

        params = dlog["parameters"]

        fromAmount = params["fromAmount"]
        toAmount = params["toAmount"]
        fromToken = params["fromToken"]
        toToken = params["toToken"]
        sender = params["sender"]

        # log sender?
        version = "okx-router"

        return ExternalSwap(
            swapper=sender,
            fromAmount=fromAmount,
            toAmount=toAmount,
            fromToken=fromToken,
            toToken=toToken,
            version=version,
            swap_log=swap_log,
        )

    elif strategy == "Swap":
        tx_hash = logs_raw[0]["tx_hash"].hex()

        # if tx_hash.startswith("76f42"):
        #    print("asd")
        # get the first and last transfer and the corresponding tokens
        # make sure the sender of the first transfer is the receiver of the last one
        # if there are not enough transfers or this condition is not met, return

        swaps = [dlog for dlog in dlogs if dlog["name"] == "Swap"]  # noqa

        swap_froms = [
            dlog["parameters"]["sender"]
            for dlog in swaps
            if "sender" in dlog["parameters"]
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

        transfers = [dlog for dlog in dlogs if dlog["name"] == "Transfer"]

        if len(transfers) < 2:
            raise ValueError(  # noqa
                f"Not enough transfers to detect a general swap , {logs_raw[0]['tx_hash'].hex()}"
            )
            return

        relevant_traces = [
            trace
            for trace in traces
            if trace["call_type"] == "call" and trace["value"] != 0
        ]

        # (from, to, asset, amt)

        traces_asset_flows = [
            (
                "0x" + trace["from_address"].hex(),
                "0x" + trace["to_address"].hex(),
                "0x" + 40 * "e",
                trace["value"],
            )
            for trace in relevant_traces
        ]

        transfer_asset_flows = [
            (
                dlog["parameters"]["from"],
                dlog["parameters"]["to"],
                dlog["address"].lower(),
                dlog["parameters"]["value"],
            )
            for dlog in transfers
        ]

        withdrawals = [dlog for dlog in dlogs if dlog["name"] == "Withdrawal"]
        withdrawal_asset_flows = [
            (
                dlog["parameters"]["src"],
                dlog["address"].lower(),
                dlog["address"].lower(),
                dlog["parameters"]["value"],
            )
            for dlog in withdrawals  # WETH to WETH contract
        ]

        deposits = [dlog for dlog in dlogs if dlog["name"] == "Deposit"]

        deposit_asset_flows = [
            (
                dlog["address"].lower(),
                dlog["parameters"]["dst"],
                dlog["address"].lower(),
                dlog["parameters"]["wad"],
            )
            for dlog in deposits
        ]
        all_asset_flows = (
            []
            + transfer_asset_flows
            + traces_asset_flows  # might not need it if we track withdrawals and deposits
            + withdrawal_asset_flows
            + deposit_asset_flows
        )
        all_from_tos = [(x[0], x[1]) for x in all_asset_flows]

        # Extract transaction sender from traces
        tx_sender = None
        if traces:
            trace0 = traces[0]
            if trace0["trace_address"] == "":
                tx_sender = "0x" + trace0["from_address"].hex()

        transfer_edges = [(x[0], x[1]) for x in transfer_asset_flows]

        G = nx.DiGraph()
        G.add_edges_from(all_from_tos)
        assert nx.is_weakly_connected(G), "Graph is not weakly connected"

        if visualize:
            visualize_graph(G, tx_hash, swap_from_tos, transfer_edges, tx_sender)

        def get_asset_flows_of_address(address, all_asset_flows):
            outgoing_amounts = [
                (x[2], x[3])  # (asset, amount)
                for x in all_asset_flows
                if x[0].lower() == address.lower()
            ]
            incoming_amounts = [
                (x[2], x[3])  # (amount, asset)
                for x in all_asset_flows
                if x[1].lower() == address.lower()
            ]

            return outgoing_amounts, incoming_amounts

        def get_swap_from_eulerian_path(
            all_asset_flows, transfer_asset_flows, traces, version
        ):
            # 0x62e3c242b5e903071458ad90a160493d84911c77 is a pair
            trace0 = traces[0]
            assert trace0["trace_address"] == ""
            sender = "0x" + trace0["from_address"].hex()
            # get all the amounts outgoing from the sender
            outgoing_amounts, incoming_amounts = get_asset_flows_of_address(
                sender, all_asset_flows
            )

            # CASE WHERE THE SENDER OF THE TX IS NOT THE SWAPPER
            if len(outgoing_amounts) == 0 and len(incoming_amounts) == 0:
                # todo this might not work true if the first transfer is ETH
                sender = transfer_asset_flows[0][0]
                outgoing_amounts, incoming_amounts = get_asset_flows_of_address(
                    sender, all_asset_flows
                )
                version += "sender-not-swapper"

            if not len(outgoing_amounts) == 1:
                raise ValueError(
                    f"Expected exactly one outgoing amount, got {len(outgoing_amounts)}, {tx_hash}"
                )
            if not len(incoming_amounts) == 1:
                raise ValueError(
                    f"Expected exactly one incoming amount, got {len(incoming_amounts)}, {tx_hash}"
                )

            fromAmount = outgoing_amounts[0][1]
            toAmount = incoming_amounts[0][1]
            fromToken = outgoing_amounts[0][0]
            toToken = incoming_amounts[0][0]

            swap_log = f"0x{logs_raw[0]['tx_hash'].hex()}_S{logs_raw[0]['log_index']}"

            return ExternalSwap(
                swapper=sender,
                fromAmount=fromAmount,
                toAmount=toAmount,
                fromToken=fromToken,
                toToken=toToken,
                version=version,
                swap_log=swap_log,
            )

        # check if we can find a eulerian path
        if nx.is_eulerian(G):
            return get_swap_from_eulerian_path(
                all_asset_flows, transfer_asset_flows, traces, version="swap"
            )

        # handle non-eulerian paths
        # remove the links A-> B where there is no B -> C
        nodes = G.nodes

        missing_outgoing = set()
        for node in nodes:
            if G.in_degree(node) == 1:
                if G.out_degree(node) == 0:
                    missing_outgoing.add(node)

        # Remove edges where the source node has no outgoing edges
        all_asset_flows_filtered = [
            flow
            for flow in all_asset_flows
            if flow[0].lower() not in missing_outgoing
            and flow[1].lower() not in missing_outgoing
        ]
        all_from_tos_filtered = [(x[0], x[1]) for x in all_asset_flows_filtered]

        G_filtered = nx.DiGraph()
        G_filtered.add_edges_from(all_from_tos_filtered)

        # Check if filtered graph is eulerian
        if nx.is_eulerian(G_filtered):
            return get_swap_from_eulerian_path(
                all_asset_flows_filtered,
                transfer_asset_flows,
                traces,
                version="swap-prune-dangling-out",
            )

        missing_incoming = set()
        for node in nodes:
            if G.out_degree(node) == 1:
                if G.in_degree(node) == 0:
                    missing_incoming.add(node)
        # Remove edges where the target node has no incoming edges
        all_asset_flows_filtered = [
            flow
            for flow in all_asset_flows
            if flow[0].lower() not in missing_incoming
            and flow[1].lower() not in missing_incoming
        ]
        all_from_tos_filtered = [(x[0], x[1]) for x in all_asset_flows_filtered]
        G_filtered = nx.DiGraph()
        G_filtered.add_edges_from(all_from_tos_filtered)
        # Check if filtered graph is eulerian
        if nx.is_eulerian(G_filtered):
            raise ValueError(
                f"Graph had dangling incoming edges, can be e.g. "
                f"because of a user sending a tx to a MEV bot contract, {tx_hash}"
                f"So far these have almost exclusively been MEV bots."
            )

        return get_swap_from_eulerian_path(
            all_asset_flows, transfer_asset_flows, traces, version="swap-non-eulerian"
        )


def get_topic(signature: str) -> bytes:
    return keccak(signature.encode("utf-8"))


def get_function_selector(function_signature: str) -> str:
    return f"0x{get_topic(function_signature)[:4].hex()}"


def get_call_payload(to: str, payload: str, for_block: str):
    return {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {"to": to, "data": payload},
            for_block,
        ],
        "id": 1,
    }


def decode_string_result(result):
    return decode(["string"], result)[0]


def decode_bytes32_result(result):
    return decode(["bytes32"], result)[0]


def decode_uint8_result(result):
    try:
        return decode(["uint8"], result)[0]
    except NonEmptyPaddingBytes:
        return None


def decode_text_result(data):
    try:
        if "result" in data:
            bytes_text = bytes.fromhex(strip_0x(data["result"]))
            if len(bytes_text) == 0:
                text = None
            else:
                try:
                    text = decode_string_result(bytes_text)
                except OverflowError:
                    # might be byte32 encoded e.g. like 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
                    text = (
                        decode_bytes32_result(bytes_text).decode("utf-8").rstrip("\x00")
                    )
        else:
            text = None
    except (
        InsufficientDataBytes,
        NonEmptyPaddingBytes,
        DecodingError,
        UnicodeDecodeError,
    ):
        text = None

    return text


def get_token_details(
    rpc_url: str, address: str, for_block: str = "latest"
) -> TokenMetadata:
    payload = get_call_payload(address, get_function_selector("name()"), for_block)
    response = requests.post(
        rpc_url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()

    name = decode_text_result(data)

    payload = get_call_payload(address, get_function_selector("symbol()"), for_block)
    response = requests.post(
        rpc_url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()

    symbol = decode_text_result(data)

    payload = get_call_payload(address, get_function_selector("decimals()"), for_block)
    response = requests.post(
        rpc_url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()
    if "result" in data:
        bytes_decimals = bytes.fromhex(strip_0x(data["result"]))
        decimals = (
            None if len(bytes_decimals) == 0 else decode_uint8_result(bytes_decimals)
        )
    else:
        decimals = None

    return TokenMetadata(address, name, symbol, decimals)
