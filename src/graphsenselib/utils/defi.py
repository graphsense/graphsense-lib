# from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

import requests
from eth_abi import decode
from eth_abi.exceptions import (
    DecodingError,
    InsufficientDataBytes,
    NonEmptyPaddingBytes,
)
from eth_hash.auto import keccak

from .accountmodel import ensure_0x_prefix, strip_0x
from .transactions import SubTransactionIdentifier, SubTransactionType

# todo What doesnt work yet:
# Swaps that have a specified to address https://etherscan.io/tx/0x1f76090132cd8b58f7a4f8724141ca500ca65ed84d646aa200bb0dd6ec45503f


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
class InternalSwap:
    fromAddress: str
    toAddress: str
    fromAsset: str
    toAsset: str
    fromAmount: str
    toAmount: str
    fromPayment: str
    toPayment: str

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class ExternalSwap:
    fromAddress: str
    toAddress: str
    fromAsset: str
    toAsset: str
    fromAmount: str
    toAmount: str
    fromPayment: str  # log {tx_hash}_L{log_index} or {tx_hash}_I{trace_index}
    toPayment: str
    # swap_composition: Optional[List[InternalSwap]] = None  # individual swaps that make up this aggregated swap

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

    creation_log = SubTransactionIdentifier(
        tx_hash=ensure_0x_prefix(log_raw["tx_hash"].hex()),
        tx_type=SubTransactionType.GenericLog,
        sub_index=log_raw["log_index"],
    ).to_string()

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
    import networkx as nx

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


def normalize_asset(asset: str) -> str:
    if asset.lower() == "0x" + "e" * 40:
        return "native"
    elif asset.lower() == "0x" + "0" * 40:
        return "native"
    else:
        return asset.lower()


def get_swap_from_decoded_logs(
    dlogs: List[Dict], logs_raw: List[Dict], traces: List[Dict], visualize: bool = False
) -> Optional[ExternalSwap]:
    # Note token 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee is a placeholder for ETH

    # Probably have to check traces to handle that

    # log_indices = [log['log_index'] for log in logs_raw]
    # sort dlogs and raw logs

    try:
        import networkx as nx
    except ImportError:
        raise Exception(
            "Please install graphsense-lib with swap optional dependencies."
        )

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

        # todo maybe not optimal - this is now just the log of the OrderRecord
        # and not of the transfers
        fromPayment = SubTransactionIdentifier(
            tx_hash=ensure_0x_prefix(log_raw["tx_hash"].hex()),
            tx_type=SubTransactionType.ERC20,
            sub_index=int(log_raw["log_index"]),
        ).to_string()

        params = dlog["parameters"]

        fromAmount = params["fromAmount"]
        toAmount = params["toAmount"]
        fromAsset = params["fromToken"]
        toAsset = params["toToken"]
        sender = params["sender"]

        return ExternalSwap(
            fromAddress=sender,
            toAddress=sender,
            fromAsset=normalize_asset(fromAsset),
            toAsset=normalize_asset(toAsset),
            fromAmount=fromAmount,
            toAmount=toAmount,
            fromPayment=fromPayment,
            toPayment=fromPayment,
        )

    elif strategy == "Swap":
        tx_hash = logs_raw[0]["tx_hash"].hex()

        # if tx_hash.startswith("76f42"):
        #    print("asd")
        # get the first and last transfer and the corresponding tokens
        # make sure the sender of the first transfer is the receiver of the last one
        # if there are not enough transfers or this condition is not met, return

        swaps = [dlog for dlog in dlogs if dlog["name"] == "Swap"]

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
            raise ValueError(
                f"Not enough transfers to detect a general swap , {logs_raw[0]['tx_hash'].hex()}"
            )
            return

        relevant_traces = [
            trace
            for trace in traces
            if trace["call_type"] == "call" and trace["value"] != 0
        ]

        # (from, to, asset, amt, source_type, source_index)

        traces_asset_flows = [
            (
                "0x" + trace["from_address"].hex(),
                "0x" + trace["to_address"].hex(),
                "0x" + 40 * "e",
                trace["value"],
                "trace",
                trace["trace_index"],
            )
            for trace in relevant_traces
        ]

        transfer_asset_flows = [
            (
                dlog["parameters"]["from"],
                dlog["parameters"]["to"],
                dlog["address"].lower(),
                dlog["parameters"]["value"],
                "log",
                logs_raw[i]["log_index"],
            )
            for i, dlog in enumerate(transfers)
        ]

        withdrawals = [dlog for dlog in dlogs if dlog["name"] == "Withdrawal"]
        withdrawal_asset_flows = [
            (
                dlog["parameters"]["src"],
                dlog["address"].lower(),
                dlog["address"].lower(),
                dlog["parameters"]["value"],
                "log",
                logs_raw[next(i for i, d in enumerate(dlogs) if d == dlog)][
                    "log_index"
                ],
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
                "log",
                logs_raw[next(i for i, d in enumerate(dlogs) if d == dlog)][
                    "log_index"
                ],
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
                tx_sender = ensure_0x_prefix(trace0["from_address"].hex())

        transfer_edges = [(x[0], x[1]) for x in transfer_asset_flows]

        G = nx.DiGraph()
        G.add_edges_from(all_from_tos)
        assert nx.is_weakly_connected(G), "Graph is not weakly connected"

        if visualize:
            visualize_graph(G, tx_hash, swap_from_tos, transfer_edges, tx_sender)

        def get_asset_flows_of_address(address, all_asset_flows):
            outgoing_amounts = [
                (x[2], x[3], x[4], x[5])  # (asset, amount, source_type, source_index)
                for x in all_asset_flows
                if x[0].lower() == address.lower()
            ]
            incoming_amounts = [
                (x[2], x[3], x[4], x[5])  # (asset, amount, source_type, source_index)
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
            sender = ensure_0x_prefix(trace0["from_address"].hex())
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

            fromAsset = outgoing_amounts[0][0]
            fromAmount = outgoing_amounts[0][1]
            from_source_type = outgoing_amounts[0][2]
            from_source_index = outgoing_amounts[0][3]

            toAsset = incoming_amounts[0][0]
            toAmount = incoming_amounts[0][1]
            to_source_type = incoming_amounts[0][2]
            to_source_index = incoming_amounts[0][3]

            tx_hash_hex = logs_raw[0]["tx_hash"].hex()
            txh0x = ensure_0x_prefix(tx_hash_hex)

            if from_source_type == "log":
                fromPayment = SubTransactionIdentifier(
                    tx_hash=txh0x,
                    tx_type=SubTransactionType.GenericLog,
                    sub_index=from_source_index,
                ).to_string()
            else:  # trace
                fromPayment = SubTransactionIdentifier(
                    tx_hash=txh0x,
                    tx_type=SubTransactionType.InternalTx,
                    sub_index=from_source_index,
                ).to_string()

            if to_source_type == "log":
                toPayment = SubTransactionIdentifier(
                    tx_hash=txh0x,
                    tx_type=SubTransactionType.GenericLog,
                    sub_index=to_source_index,
                ).to_string()
            else:  # trace
                toPayment = SubTransactionIdentifier(
                    tx_hash=txh0x,
                    tx_type=SubTransactionType.InternalTx,
                    sub_index=to_source_index,
                ).to_string()

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
    return ensure_0x_prefix(get_topic(function_signature)[:4].hex())


def create_token_balance_request_payload(
    contract_address: str, account: str, block: str = "latest"
):
    return {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {
                "to": contract_address,
                "data": f"0x70a08231000000000000000000000000{strip_0x(account)}",
            },
            block,
        ],
        "id": 1,
    }


def create_base_balance_request_payload(contract_address: str, block: str = "latest"):
    return {
        "method": "eth_getBalance",
        "params": [contract_address, block],
        "id": 1,
        "jsonrpc": "2.0",
    }


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
