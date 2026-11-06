"""Graph model + GraphML writer (tx_mode=node).

One synthetic graph covers every case the export has to get right: a UTXO
tx with several inputs and outputs (change, bare multisig, off-graph
outputs), an account tx with a token sub-transfer, a tx with no address on
the graph, a tx the backend doesn't know, a failed lookup, an agg edge, a
swap, and three networks. The expected GraphML is a golden file; rerun
with ``UPDATE_GOLDEN=1`` to rewrite it after an intended change.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Any, Optional

import networkx as nx
import pytest

from graphsenselib.convert.gs_files import GsBuilder
from graphsenselib.convert.graph_convert import (
    ExportBackend,
    build_graph,
    convert,
    graph_from_payload,
    render,
)
from graphsenselib.convert.graph_convert import networks
from graphsenselib.pathfinder import RestBackend

GOLDEN = Path(__file__).parent / "golden"

# Synthetic ids; only their shape matters.
A, B, C = "1AAAinputAddress", "1BBBoutputAddress", "1CCCnotOnGraph"
MULTI = "1DDDmultisigCosigner"
E1 = "0xAbCdEf0000000000000000000000000000000001"  # checksum-cased in the file
E2 = "0x00000000000000000000000000000000000000e2"
T1 = "TXyzTronReceiver00000000000000000"
UTXO_TX = "aa" * 32
ISOLATED_TX = "bb" * 32
UNKNOWN_TX = "cc" * 32
FAILING_TX = "dd" * 32
ETH_TX = "ee" * 32
ETH_TOKEN_TX = f"{ETH_TX}_T0"
SWAP_OUT_TX = "ff" * 32
TRX_TX = "12" * 32


def _values(v: int) -> dict[str, Any]:
    return {"value": v, "fiat_values": [{"code": "usd", "value": v / 1000}]}


def _io(addresses: list[str], value: int) -> dict[str, Any]:
    return {"address": addresses, "value": _values(value)}


def _utxo(tx_hash: str, inputs: list, outputs: list, **kw: Any) -> dict[str, Any]:
    return {
        "tx_type": "utxo",
        "currency": "btc",
        "tx_hash": tx_hash,
        "coinbase": False,
        "height": 800000,
        "timestamp": 1690000000,
        "no_inputs": len(inputs),
        "no_outputs": len(outputs),
        "total_input": _values(sum(i["value"]["value"] for i in inputs)),
        "total_output": _values(sum(o["value"]["value"] for o in outputs)),
        "inputs": inputs,
        "outputs": outputs,
        **kw,
    }


def _account(
    identifier: str, frm: str, to: str, value: int, currency: str, network: str
) -> dict[str, Any]:
    return {
        "tx_type": "account",
        "identifier": identifier,
        "currency": currency.upper(),
        "network": network,
        "tx_hash": identifier.split("_")[0],
        "height": 19000000,
        "timestamp": 1700000000,
        "value": _values(value),
        "from_address": frm.lower() if frm.startswith("0x") else frm,
        "to_address": to.lower() if to.startswith("0x") else to,
    }


BODIES: dict[tuple[str, str], Optional[dict[str, Any]]] = {
    ("btc", UTXO_TX): _utxo(
        UTXO_TX,
        inputs=[_io([A], 70_000), _io([C], 40_000), _io([A], 10_000)],
        outputs=[
            _io([B], 50_000),
            _io([C], 20_000),
            _io([B, MULTI], 30_000),  # bare multisig: two addresses
            _io([], 0),  # OP_RETURN
            _io([A], 19_000),  # change
        ],
    ),
    ("btc", ISOLATED_TX): _utxo(
        ISOLATED_TX, inputs=[_io([C], 5_000)], outputs=[_io([C], 4_000)]
    ),
    ("btc", UNKNOWN_TX): None,
    # FAILING_TX is absent: its lookup raised.
    ("eth", ETH_TX): _account(ETH_TX, E1, E2, 10**18, "eth", "eth"),
    ("eth", ETH_TOKEN_TX): _account(ETH_TOKEN_TX, E2, E1, 2_500_000, "usdt", "eth"),
    ("eth", SWAP_OUT_TX): _account(SWAP_OUT_TX, E2, E1, 7, "weth", "eth"),
    ("trx", TRX_TX): _account(TRX_TX, E2, T1, 1_000_000, "trx", "trx"),
}

CONVERSIONS = [
    {
        "type": "dex_swap",
        "input_tx": ETH_TOKEN_TX,
        "output_tx": SWAP_OUT_TX,
        "edge_from": E1.lower(),
        "edge_to": E2,
    }
]


def _payload() -> list[Any]:
    b = GsBuilder(name="export test")
    b.add_address(A, network="btc", x=0, y=0, starting_point=True)
    b.add_address(
        B, network="btc", x=10, y=0, label="cash-out", color=(1.0, 0.0, 0.0, 0.5)
    )
    b.add_address(E1, network="eth", x=0, y=10)
    b.add_address(E2, network="eth", x=10, y=10)
    b.add_address(T1, network="trx", x=20, y=10)
    for tx, net in [
        (UTXO_TX, "btc"),
        (ISOLATED_TX, "btc"),
        (UNKNOWN_TX, "btc"),
        (FAILING_TX, "btc"),
        (ETH_TX, "eth"),
        (ETH_TOKEN_TX, "eth"),
        (SWAP_OUT_TX, "eth"),
        (TRX_TX, "trx"),
    ]:
        b.add_tx(tx, network=net, x=5, y=5)
    b.add_agg_edge(A, B, tx_ids=[UTXO_TX], network="btc")
    return b.to_payload()


def _graph():
    data = graph_from_payload(_payload())
    return build_graph(data, BODIES, conversions=CONVERSIONS, warnings=["from lookup"])


def _nx() -> nx.MultiDiGraph:
    g = nx.read_graphml(_write_tmp(render(_graph())))
    assert isinstance(g, nx.MultiDiGraph)
    return g


def _write_tmp(data: bytes) -> io.BytesIO:
    return io.BytesIO(data)


def _attr(d: dict, name: str) -> Any:
    return d["gs:v0:" + name]


def _edges(g: nx.MultiDiGraph, **match: Any) -> list[tuple[str, str, dict]]:
    return [
        (u, v, d)
        for u, v, d in g.edges(data=True)
        if all(d.get("gs:v0:" + k) == want for k, want in match.items())
    ]


def btc(address: str) -> str:
    return networks.address_id("btc", address)


def btc_tx(tx: str) -> str:
    return networks.tx_id("btc", tx)


def test_golden_graphml() -> None:
    out = render(_graph())
    golden = GOLDEN / "node_mode.graphml"
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_bytes(out)
    assert out.decode() == golden.read_text(encoding="utf-8")


def test_parses_in_networkx_with_typed_attributes() -> None:
    g = _nx()
    assert g.graph["gs:v0:format_version"] == "0"
    assert g.graph["gs:v0:tx_mode"] == "node"
    assert "from lookup" in g.graph["gs:v0:warnings"]
    node = g.nodes[btc(A)]
    assert node["label"] == A
    assert _attr(node, "starting_point") is True
    assert isinstance(_attr(node, "x"), float)
    assert g.nodes[btc(B)]["label"] == "cash-out"
    assert _attr(g.nodes[btc(B)], "color") == "#ff000080"
    tx = g.nodes[btc_tx(UTXO_TX)]
    assert _attr(tx, "height") == 800000
    assert _attr(tx, "timestamp") == "2023-07-22T04:26:40Z"


def test_utxo_one_edge_per_on_graph_input_and_output() -> None:
    g = _nx()
    tx = btc_tx(UTXO_TX)
    inputs = sorted(
        (u, _attr(d, "io_index"), _attr(d, "value"))
        for u, v, d in _edges(g, kind="input")
        if v == tx
    )
    # C's input is off the graph; A spends twice.
    assert inputs == [(btc(A), 0, "70000"), (btc(A), 2, "10000")]
    outputs = sorted(
        (v, _attr(d, "outpoint"), _attr(d, "value"), d.get("gs:v0:address_count"))
        for u, v, d in _edges(g, kind="output")
        if u == tx
    )
    assert outputs == [
        (btc(A), f"{UTXO_TX}:4", "19000", None),  # change
        (btc(B), f"{UTXO_TX}:0", "50000", None),
        (btc(B), f"{UTXO_TX}:2", "30000", 2),  # multisig, full value
    ]
    assert _attr(g.nodes[tx], "total_output") == "119000"
    assert _attr(g.nodes[tx], "no_outputs") == 5


def test_amounts_are_strings_with_asset_ids() -> None:
    g = _nx()
    for _, _, d in _edges(g, kind="input") + _edges(g, kind="output"):
        assert isinstance(_attr(d, "value"), str)
    btc_edge = _edges(g, kind="input", network="btc")[0][2]
    assert _attr(btc_edge, "asset_id") == (
        "bip122:000000000019d6689c085ae165831e93/slip44:0"
    )


def test_account_tx_and_token_sub_transfer_are_separate_nodes() -> None:
    g = _nx()
    e1, e2 = networks.address_id("eth", E1), networks.address_id("eth", E2)
    assert e1 == "eip155:1:" + E1.lower()  # one spelling for EVM addresses
    base, token = networks.tx_id("eth", ETH_TX), networks.tx_id("eth", ETH_TOKEN_TX)
    assert _attr(g.nodes[token], "sub_index") == "T0"
    assert _attr(g.nodes[token], "tx_hash") == ETH_TX
    assert _attr(g.nodes[token], "asset") == "usdt"
    assert "gs:v0:asset_id" not in g.nodes[token]  # tokens: not mapped yet
    assert g.has_edge(e1, base) and g.has_edge(base, e2)
    assert g.has_edge(e2, token) and g.has_edge(token, e1)
    assert _attr(g.edges[e1, base, f"{base}/in"], "value") == str(10**18)


def test_tx_without_on_graph_addresses_is_isolated_but_resolved() -> None:
    g = _nx()
    tx = btc_tx(ISOLATED_TX)
    assert g.degree(tx) == 0
    assert _attr(g.nodes[tx], "resolved") is True


def test_unknown_and_failed_txs_stay_unresolved_with_warnings() -> None:
    graph = _graph()
    g = _nx()
    for tx in (UNKNOWN_TX, FAILING_TX):
        node = g.nodes[btc_tx(tx)]
        assert _attr(node, "resolved") is False
        assert g.degree(btc_tx(tx)) == 0
    assert any("not found" in w and UNKNOWN_TX in w for w in graph.warnings)
    assert any("lookup failed" in w and FAILING_TX in w for w in graph.warnings)


def test_agg_edge_is_kept_and_marked_undirected() -> None:
    g = _nx()
    [(u, v, d)] = _edges(g, kind="agg")
    assert (u, v) == (btc(A), btc(B))
    assert _attr(d, "directed") is False
    assert _attr(d, "txs") == btc_tx(UTXO_TX)


def test_conversion_is_a_tx_to_tx_edge() -> None:
    g = _nx()
    [(u, v, d)] = _edges(g, kind="conversion")
    assert (u, v) == (
        networks.tx_id("eth", ETH_TOKEN_TX),
        networks.tx_id("eth", SWAP_OUT_TX),
    )
    assert _attr(d, "conversion_type") == "dex_swap"
    assert _attr(d, "from_address") == networks.address_id("eth", E1)


def test_mixed_networks_have_their_own_ids() -> None:
    g = _nx()
    assert _attr(g.nodes[networks.address_id("trx", T1)], "network") == "trx"
    # Tron has no verified CAIP-2 id yet: GraphSense fallback namespace.
    assert networks.address_id("trx", T1) == f"graphsense:trx:{T1}"
    # E2 -> TRX tx is not drawn: E2 lives on eth, not on trx.
    trx_tx = networks.tx_id("trx", TRX_TX)
    assert list(g.predecessors(trx_tx)) == []
    assert list(g.successors(trx_tx)) == [networks.address_id("trx", T1)]


def test_gs_bytes_and_json_payload_give_the_same_graph() -> None:
    b = GsBuilder(name="x")
    b.add_address(A, network="btc")
    from_bytes = graph_from_payload(b.to_bytes())
    assert from_bytes == graph_from_payload(b.to_payload())


def test_unknown_format_and_mode_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown graph format"):
        render(_graph(), "gexf")
    with pytest.raises(ValueError, match="unknown tx_mode"):
        render(_graph(), tx_mode="hyperedge")  # type: ignore[arg-type]


class _FakeBackend:
    async def tx_body(self, network: str, tx_id: str):
        if tx_id == FAILING_TX:
            raise RuntimeError("boom")
        return BODIES.get((network, tx_id))

    async def tx_conversions(self, network: str, tx_id: str):
        if tx_id == ETH_TOKEN_TX:
            return [
                {
                    "conversion_type": "dex_swap",
                    "from_asset_transfer": ETH_TOKEN_TX,
                    "to_asset_transfer": SWAP_OUT_TX,
                }
            ]
        return []

    async def tx_io_order(self, network: str, tx_id: str):
        body = BODIES.get((network, tx_id))
        if body is None:
            return None
        return [body["from_address"]], [body["to_address"]]


def test_backends_satisfy_the_protocol() -> None:
    import httpx

    assert isinstance(_FakeBackend(), ExportBackend)
    assert isinstance(RestBackend(httpx.AsyncClient()), ExportBackend)


async def test_convert_end_to_end_matches_the_offline_build() -> None:
    out = await convert(_payload(), _FakeBackend())
    assert out.graph is not None
    assert [e.id for e in out.graph.edges] == [e.id for e in _graph().edges]
    assert any(FAILING_TX in w for w in out.warnings)
    assert out.filename == "export_test.graphml"
    assert out.media_type == "application/graphml+xml"
    nx.read_graphml(_write_tmp(out.content))
