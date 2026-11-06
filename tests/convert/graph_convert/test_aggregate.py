"""tx_mode=aggregate: pair edges derived from the node graph."""

from __future__ import annotations

import io
import os

import networkx as nx
import pytest

from graphsenselib.convert.gs_files import GsBuilder
from graphsenselib.convert.graph_convert import (
    aggregate,
    build_graph,
    graph_from_payload,
    networks,
    render,
)

from .test_graphml import (
    A,
    B,
    C,
    E1,
    E2,
    ETH_TOKEN_TX,
    ETH_TX,
    GOLDEN,
    SWAP_OUT_TX,
    UTXO_TX,
    _graph,
    _io,
    _utxo,
)


def _nx(graph) -> nx.MultiDiGraph:
    return nx.read_graphml(io.BytesIO(render(graph)))


def _flows(g: nx.MultiDiGraph) -> dict[tuple[str, str, str], dict]:
    return {
        (u, v, d["gs:v0:asset"]): d
        for u, v, d in g.edges(data=True)
        if d["gs:v0:kind"] == "flow"
    }


def btc(a: str) -> str:
    return networks.address_id("btc", a)


def eth(a: str) -> str:
    return networks.address_id("eth", a)


def test_golden_graphml() -> None:
    out = render(_graph(), tx_mode="aggregate")
    golden = GOLDEN / "aggregate_mode.graphml"
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_bytes(out)
    assert out.decode() == golden.read_text(encoding="utf-8")


def test_only_address_nodes_remain() -> None:
    g = _nx(aggregate(_graph()))
    assert g.graph["gs:v0:tx_mode"] == "aggregate"
    assert {d["gs:v0:kind"] for _, d in g.nodes(data=True)} == {"address"}


def test_one_edge_per_pair_and_asset_never_summed_across_assets() -> None:
    flows = _flows(_nx(aggregate(_graph())))
    assert set(flows) == {
        (btc(A), btc(B), "btc"),
        (eth(E1), eth(E2), "eth"),
        (eth(E2), eth(E1), "usdt"),
        (eth(E2), eth(E1), "weth"),
    }
    assert flows[eth(E1), eth(E2), "eth"]["gs:v0:value"] == str(10**18)
    assert flows[eth(E2), eth(E1), "usdt"]["gs:v0:value"] == "2500000"
    assert flows[eth(E2), eth(E1), "weth"]["gs:v0:txs"] == networks.tx_id(
        "eth", SWAP_OUT_TX
    )


def test_utxo_value_omitted_when_several_senders() -> None:
    # UTXO_TX is paid by A and an off-graph address: who paid B is unknown.
    flows = _flows(_nx(aggregate(_graph())))
    edge = flows[btc(A), btc(B), "btc"]
    assert edge["gs:v0:value_determined"] is False
    assert "gs:v0:value" not in edge
    assert edge["gs:v0:txs"] == networks.tx_id("btc", UTXO_TX)


def test_change_and_txs_without_pair_are_dropped_with_a_warning() -> None:
    graph = aggregate(_graph())
    flows = _flows(_nx(graph))
    assert (btc(A), btc(A), "btc") not in flows  # change back to A
    # isolated, unknown, failing, and the trx tx whose sender is not on trx
    assert any(w.startswith("4 tx(s) connect no two") for w in graph.warnings)


def test_conversion_runs_between_the_address_legs() -> None:
    g = _nx(aggregate(_graph()))
    [(u, v, d)] = [e for e in g.edges(data=True) if e[2]["gs:v0:kind"] == "conversion"]
    assert (u, v) == (eth(E1), eth(E2))
    assert d["gs:v0:input_tx"] == networks.tx_id("eth", ETH_TOKEN_TX)


def test_agg_edges_are_kept() -> None:
    g = _nx(aggregate(_graph()))
    assert [(u, v) for u, v, d in g.edges(data=True) if d["gs:v0:kind"] == "agg"] == [
        (btc(A), btc(B))
    ]


def _single_sender_graph():
    t1, t2 = "01" * 32, "02" * 32
    b = GsBuilder(name="single sender")
    for a in (A, B):
        b.add_address(a, network="btc")
    b.add_tx(t1, network="btc")
    b.add_tx(t2, network="btc")
    bodies = {
        # A alone pays B twice in one tx, plus change and an off-graph output.
        ("btc", t1): _utxo(
            t1,
            inputs=[_io([A], 60_000), _io([A], 40_000)],
            outputs=[
                _io([B], 10_000),
                _io([C], 5_000),
                _io([B], 20_000),
                _io([A], 64_000),
            ],
            timestamp=1600000000,
        ),
        ("btc", t2): _utxo(
            t2,
            inputs=[_io([A], 9_000)],
            outputs=[_io([B], 8_000)],
            timestamp=1500000000,
        ),
    }
    return build_graph(graph_from_payload(b.to_payload()), bodies)


def test_single_sender_utxo_values_are_exact_and_summed_over_txs() -> None:
    flows = _flows(_nx(aggregate(_single_sender_graph())))
    edge = flows[btc(A), btc(B), "btc"]
    assert edge["gs:v0:value"] == "38000"  # 10k + 20k from t1, 8k from t2
    assert edge["gs:v0:value_determined"] is True
    assert edge["gs:v0:tx_count"] == 2
    assert edge["gs:v0:first_timestamp"] == "2017-07-14T02:40:00Z"
    assert edge["gs:v0:last_timestamp"] == "2020-09-13T12:26:40Z"


def test_one_inexact_tx_makes_the_pair_value_unknown() -> None:
    graph = _single_sender_graph()
    t2 = next(n for n in graph.nodes if n.id.endswith("02" * 32))
    t2.input_address_count = 2  # pretend a second sender
    edge = _flows(_nx(aggregate(graph)))[btc(A), btc(B), "btc"]
    assert edge["gs:v0:value_determined"] is False
    assert "gs:v0:value" not in edge


def test_aggregate_takes_node_mode_only() -> None:
    with pytest.raises(ValueError, match="node-mode"):
        aggregate(aggregate(_graph()))


def test_eth_base_tx_is_kept_once() -> None:
    flows = _flows(_nx(aggregate(_graph())))
    assert flows[eth(E1), eth(E2), "eth"]["gs:v0:tx_count"] == 1
    assert flows[eth(E1), eth(E2), "eth"]["gs:v0:txs"] == networks.tx_id("eth", ETH_TX)
