"""Tests for the direction-aware layout (``gs_files.layout``).

Most tests assert *rules* — inflows left of their target, no two nodes
collide, same input gives the same output — rather than exact
coordinates, so the layout can keep improving without rewriting them.
They run on hand-written cases, on generated graphs, and on four real
investigation files (``tests/testfiles/gs_files/layout``) whose tx
directions and conversions were fetched once from the REST API into
``tx_sides.json`` and ``conversions.json``.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from graphsenselib.convert.gs_files import (
    apply_hierarchical_layout,
    builder_from_spec,
    decode_gs,
    decode_gs_bytes,
    has_flow_info,
    layout_metrics,
    spec_from_pathfinder,
    structure,
)
from graphsenselib.convert.gs_files.encoder import (
    _HIER_X_STEP,
    _HIER_Y_STEP,
    _LABEL_LINE_HEIGHT,
)

FIXTURES = Path(__file__).parent.parent.parent / "testfiles" / "gs_files" / "layout"


def _xy(out: dict) -> dict[str, tuple[float, float]]:
    return {n["id"]: (n["x"], n["y"]) for n in out["addresses"] + out["txs"]}


def _tx(tid: str, senders: list[str], receivers: list[str]) -> dict:
    return {"id": tid, "senders": senders, "receivers": receivers}


def _edges(txs: list[dict]) -> list[dict]:
    """One agg edge per sender→receiver pair of every tx."""
    return [
        {"a": s, "b": r, "tx_ids": [t["id"]]}
        for t in txs
        for s in t["senders"]
        for r in t["receivers"]
    ]


def _assert_clean(out: dict) -> dict:
    """Rules every direction-aware layout must satisfy on an acyclic flow."""
    m = layout_metrics(out)
    assert m["overlaps"] == 0, m
    assert m["cramped"] == 0, m
    assert m["flow_backwards"] == 0, m
    return m


# ---------------------------------------------------------------- basics --


def test_inflow_to_starting_point_is_drawn_on_its_left() -> None:
    """The reported bug: an address that pays *into* the starting point
    was drawn on its right, like an outflow."""
    txs = [_tx("t_in", ["payer"], ["S"]), _tx("t_out", ["S"], ["payee"])]
    spec = {
        "addresses": [{"id": "S", "starting_point": True}, "payer", "payee"],
        "txs": txs,
        # Edge ends in the "wrong" order on purpose: a/b carry no direction.
        "agg_edges": [
            {"a": "S", "b": "payer", "tx_ids": ["t_in"]},
            {"a": "S", "b": "payee", "tx_ids": ["t_out"]},
        ],
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["S"] == (0.0, 0.0)
    assert xy["payer"][0] == -2 * _HIER_X_STEP
    assert xy["t_in"][0] == -_HIER_X_STEP
    assert xy["t_out"][0] == _HIER_X_STEP
    assert xy["payee"][0] == 2 * _HIER_X_STEP
    # A straight line through the start.
    assert {y for _x, y in xy.values()} == {0.0}


def test_without_flow_info_the_undirected_layout_runs() -> None:
    """Fallback: no senders/receivers anywhere → the old layout, which
    puts every neighbour of the start on its right."""
    spec = {
        "addresses": [{"id": "S", "starting_point": True}, "payer"],
        "txs": [{"id": "t_in", "senders": [], "receivers": []}],
        "agg_edges": [{"a": "S", "b": "payer", "tx_ids": ["t_in"]}],
    }
    assert not has_flow_info(spec)
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["payer"][0] == 2 * _HIER_X_STEP


def test_partial_flow_info_places_unknown_txs_like_the_old_layout() -> None:
    txs = [_tx("t_in", ["payer"], ["S"]), {"id": "t_unknown"}]
    spec = {
        "addresses": [{"id": "S", "starting_point": True}, "payer", "other"],
        "txs": txs,
        "agg_edges": [
            {"a": "S", "b": "payer", "tx_ids": ["t_in"]},
            {"a": "S", "b": "other", "tx_ids": ["t_unknown"]},
        ],
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["payer"][0] < 0
    assert xy["other"][0] == 2 * _HIER_X_STEP


def test_change_address_stays_on_the_spending_side() -> None:
    """An address that is both input and output of a tx (change back to
    the spender) is drawn as a sender."""
    txs = [_tx("t", ["S"], ["S", "payee"])]
    spec = {
        "addresses": [{"id": "S", "starting_point": True}, "payee"],
        "txs": txs,
        "agg_edges": [{"a": "S", "b": "payee", "tx_ids": ["t"]}],
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["S"][0] < xy["t"][0] < xy["payee"][0]


def test_fan_in_and_fan_out_share_rows() -> None:
    """Two inflows on the left and two outflows on the right use the same
    two rows, centred on the start — not four rows stacked in one column."""
    txs = [
        _tx("i1", ["in1"], ["S"]),
        _tx("i2", ["in2"], ["S"]),
        _tx("o", ["S"], ["out1", "out2"]),
    ]
    spec = {
        "addresses": [
            {"id": "S", "starting_point": True},
            "in1",
            "in2",
            "out1",
            "out2",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy["in1"][1] == xy["out1"][1] == -_HIER_Y_STEP / 2
    assert xy["in2"][1] == xy["out2"][1] == _HIER_Y_STEP / 2
    assert xy["S"] == (0.0, 0.0)
    _assert_clean(out)


def test_multi_tx_edge_is_spread_symmetrically_between_its_addresses() -> None:
    txs = [_tx(f"tx{i}", ["src"], ["dst"]) for i in range(5)]
    spec = {
        "addresses": [{"id": "src", "starting_point": True}, "dst"],
        "txs": txs,
        "agg_edges": [{"a": "src", "b": "dst", "tx_ids": [t["id"] for t in txs]}],
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy["src"] == (0.0, 0.0)
    assert xy["dst"] == (2 * _HIER_X_STEP, 0.0)
    ys = sorted(xy[f"tx{i}"][1] for i in range(5))
    assert ys == [-2 * _HIER_Y_STEP, -_HIER_Y_STEP, 0.0, _HIER_Y_STEP, 2 * _HIER_Y_STEP]
    _assert_clean(out)


def test_edge_tx_ids_not_listed_in_txs_take_no_space() -> None:
    """The Pathfinder UI saves an account-model edge with the base tx
    hash and its sub-payment, but draws only the tx listed in ``txs``."""
    base = "ab" * 32
    sub = base + "_I7"
    spec = {
        "addresses": [{"id": "S", "starting_point": True}, "dst"],
        "txs": [_tx(sub, ["S"], ["dst"])],
        "agg_edges": [{"a": "S", "b": "dst", "tx_ids": [base, sub]}],
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy[sub] == (_HIER_X_STEP, 0.0)
    assert xy["dst"] == (2 * _HIER_X_STEP, 0.0)
    assert layout_metrics(out)["links"] == 2


def test_connected_starting_points_get_no_blank_row_between_them() -> None:
    txs = [_tx("t1", ["A"], ["M"]), _tx("t2", ["B"], ["M"])]
    spec = {
        "addresses": [
            {"id": "A", "starting_point": True},
            {"id": "B", "starting_point": True},
            "M",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["B"][1] - xy["A"][1] == _HIER_Y_STEP
    assert xy["M"][1] == _HIER_Y_STEP / 2


def test_separate_graphs_are_stacked_with_a_blank_row() -> None:
    txs = [_tx("t1", ["A"], ["A2"]), _tx("t2", ["B"], ["B2"])]
    spec = {
        "addresses": [
            {"id": "A", "starting_point": True},
            {"id": "B", "starting_point": True},
            "A2",
            "B2",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["B"][1] - xy["A"][1] == 2 * _HIER_Y_STEP


def test_multiline_labels_widen_the_spacing_of_their_neighbours() -> None:
    txs = [_tx("t", ["S"], ["a", "b"])]
    spec = {
        "addresses": [
            {"id": "S", "starting_point": True},
            {"id": "a", "label": "a label that wraps"},
            "b",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["b"][1] - xy["a"][1] == _HIER_Y_STEP + _LABEL_LINE_HEIGHT


def test_caller_coordinates_and_unreachable_nodes() -> None:
    txs = [_tx("t", ["S"], ["pinned"])]
    spec = {
        "addresses": [
            {"id": "S", "starting_point": True},
            {"id": "pinned", "x": 99.0, "y": -3.0},
            "loner",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["pinned"] == (99.0, -3.0)
    # Unreachable from any start: right of the rest, past a blank column.
    assert xy["loner"][0] == 4 * _HIER_X_STEP


def test_links_without_direction_do_not_shortcut_the_flow() -> None:
    """From C3_FullTrail: the victim pays hop1 → hop2 → exchange, and an
    edge without a drawn tx also links the victim straight to the
    exchange. Following that shortcut first put the exchange one hop
    from the victim, and hop2, which pays into it, left of hop1."""
    txs = [
        _tx("t1", ["victim"], ["hop1"]),
        _tx("t2", ["hop1"], ["hop2"]),
        _tx("t3", ["hop2"], ["exchange"]),
    ]
    spec = {
        "addresses": [
            {"id": "victim", "starting_point": True},
            "exchange",
            "hop1",
            "hop2",
        ],
        "txs": txs,
        "agg_edges": [{"a": "victim", "b": "exchange", "tx_ids": []}, *_edges(txs)],
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert [xy[k][0] for k in ("victim", "hop1", "hop2", "exchange")] == [
        0.0,
        2 * _HIER_X_STEP,
        4 * _HIER_X_STEP,
        6 * _HIER_X_STEP,
    ]
    _assert_clean(out)


def test_part_no_start_reaches_is_laid_out_from_where_money_enters() -> None:
    """A cross-chain swap: nothing links the ETH side to the BTC side,
    so no starting point reaches the BTC part. It must still be laid out
    as a flow — not stacked in one column — to the right of the rest."""
    txs = [
        _tx("e1", ["exploiter"], ["bridge"]),
        _tx("b2", ["btc_hop"], ["btc_out"]),
        _tx("b1", ["btc_in"], ["btc_hop"]),
    ]
    spec = {
        "addresses": [
            {"id": "exploiter", "starting_point": True},
            "bridge",
            "btc_out",  # listed first on purpose: the anchor is btc_in
            "btc_hop",
            "btc_in",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy["btc_in"][0] == xy["bridge"][0] + 2 * _HIER_X_STEP
    assert (
        xy["btc_in"][0]
        < xy["b1"][0]
        < xy["btc_hop"][0]
        < xy["b2"][0]
        < xy["btc_out"][0]
    )
    _assert_clean(out)


def test_round_trip_tx_moves_between_its_endpoints() -> None:
    """S → t1 → A and A → t2 → S: BFS reaches t2 from S, as an inflow
    on the left, so A would pay into it from the far right. The tx is
    moved to a column where only one of its links points backwards."""
    txs = [_tx("t1", ["S"], ["A"]), _tx("t2", ["A"], ["S"])]
    spec = {
        "addresses": [{"id": "S", "starting_point": True}, "A"],
        "txs": txs,
        "agg_edges": _edges(txs),
    }
    m = layout_metrics(apply_hierarchical_layout(spec))
    assert m["flow_backwards"] == 1  # a cycle can't be drawn all forwards


# ------------------------------------------------------- generated graphs --


def _random_tree(seed: int, n: int) -> dict:
    """A random investigation-shaped flow without cycles: grows from one
    start, each batch of new addresses pays into or is paid by an
    existing one — sometimes through several txs, sometimes sharing a tx
    with its siblings."""
    r = random.Random(seed)
    addrs = ["a0"]
    txs: list[dict] = []
    while len(addrs) < n:
        anchor = r.choice(addrs)
        new = [f"a{len(addrs) + i}" for i in range(r.choice((1, 1, 1, 2, 3)))]
        addrs.extend(new)
        inflow = r.random() < 0.5
        for _ in range(r.choice((1, 1, 1, 2, 4))):
            tid = f"t{len(txs)}"
            if inflow:
                txs.append(_tx(tid, list(new), [anchor]))
            else:
                txs.append(_tx(tid, [anchor], list(new)))
    return {
        "addresses": [{"id": a, "starting_point": a == "a0"} for a in addrs],
        "txs": txs,
        "agg_edges": _edges(txs),
    }


def _random_graph(seed: int, n_addr: int, n_tx: int) -> dict:
    """Arbitrary flows, cycles included."""
    r = random.Random(seed)
    addrs = [f"a{i}" for i in range(n_addr)]
    txs = []
    for i in range(n_tx):
        senders = r.sample(addrs, r.randint(1, 2))
        receivers = r.sample([a for a in addrs if a not in senders], r.randint(1, 2))
        txs.append(_tx(f"t{i}", senders, receivers))
    return {
        "addresses": [{"id": a, "starting_point": i == 0} for i, a in enumerate(addrs)],
        "txs": txs,
        "agg_edges": _edges(txs),
    }


@pytest.mark.parametrize("seed", range(40))
def test_random_tree_flows_are_drawn_cleanly(seed: int) -> None:
    spec = _random_tree(seed, n=4 + seed % 20)
    out = apply_hierarchical_layout(spec)
    _assert_clean(out)
    # Every tx sits strictly between the columns of its endpoints.
    xy = _xy(out)
    for t in spec["txs"]:
        for s in t["senders"]:
            assert xy[s][0] < xy[t["id"]][0]
        for rcv in t["receivers"]:
            assert xy[t["id"]][0] < xy[rcv][0]


@pytest.mark.parametrize("seed", range(20))
def test_random_graphs_never_overlap_and_are_deterministic(seed: int) -> None:
    spec = _random_graph(seed, n_addr=5 + seed, n_tx=5 + seed)
    out = apply_hierarchical_layout(spec)
    m = layout_metrics(out)
    assert m["overlaps"] == 0 and m["cramped"] == 0, m
    assert m["nodes"] == len(spec["addresses"]) + len(spec["txs"])
    assert apply_hierarchical_layout(spec) == out


def test_laying_out_again_changes_nothing() -> None:
    """Idempotent: a laid-out spec, stripped of coordinates, lays out the same."""
    spec = _random_tree(7, n=15)
    out = apply_hierarchical_layout(spec)
    stripped = {
        **out,
        "addresses": [
            {k: v for k, v in a.items() if k not in "xy"} for a in out["addresses"]
        ],
        "txs": [{k: v for k, v in t.items() if k not in "xy"} for t in out["txs"]],
    }
    assert apply_hierarchical_layout(stripped) == out


# ---------------------------------------------------- real investigations --


def _fixture_spec(name: str, *, with_flows: bool) -> dict:
    data = structure(decode_gs(FIXTURES / name))
    spec = spec_from_pathfinder(data, keep_positions=False)
    if with_flows:
        conversions = json.loads((FIXTURES / "conversions.json").read_text())
        spec["conversions"] = conversions.get(name, [])
        sides = json.loads((FIXTURES / "tx_sides.json").read_text())
        spec["txs"] = [
            {
                **t,
                "senders": sides[t["network"]][t["id"]][0],
                "receivers": sides[t["network"]][t["id"]][1],
            }
            for t in spec["txs"]
        ]
    return spec


@pytest.mark.parametrize(
    "name",
    [
        "C3_HopHistory.gs",
        "C4_A1-A2_PoolingAddress.gs",
        "C5_B5_A6-A8_THORChainToBitcoin.gs",
        "WC_Q5-Q7_Sweep.gs",
    ],
)
def test_fixture_files_are_drawn_cleanly(name: str) -> None:
    new = layout_metrics(
        apply_hierarchical_layout(_fixture_spec(name, with_flows=True))
    )
    # Score the undirected layout against the same flow information.
    old_out = apply_hierarchical_layout(_fixture_spec(name, with_flows=False))
    flows = {t["id"]: t for t in _fixture_spec(name, with_flows=True)["txs"]}
    old_out["txs"] = [{**t, **flows[t["id"]]} for t in old_out["txs"]]
    old = layout_metrics(old_out)

    assert new["flow_links"] > 0
    assert new["flow_backwards"] == 0
    assert new["overlaps"] == new["cramped"] == new["crossings"] == 0
    assert new["crossings"] <= old["crossings"]


def test_fixture_with_inflows_was_drawn_backwards_before() -> None:
    """Pins the regression: the collecting address in the pooling case
    receives from five addresses, and the undirected layout drew those
    inflows on its right."""
    name = "C4_A1-A2_PoolingAddress.gs"
    old_out = apply_hierarchical_layout(_fixture_spec(name, with_flows=False))
    flows = {t["id"]: t for t in _fixture_spec(name, with_flows=True)["txs"]}
    old_out["txs"] = [{**t, **flows[t["id"]]} for t in old_out["txs"]]
    assert layout_metrics(old_out)["flow_backwards"] > 0


# --------------------------------------------------------- spec round trip --


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.gs")), ids=lambda p: p.name)
def test_spec_from_pathfinder_round_trips(path: Path) -> None:
    data = structure(decode_gs(path))
    rebuilt = builder_from_spec(spec_from_pathfinder(data), name=data.name).to_bytes()
    again = structure(decode_gs_bytes(rebuilt))
    assert again.name == data.name
    assert again.addresses == data.addresses
    assert again.txs == data.txs
    assert again.agg_edges == data.agg_edges
    key = lambda n: (n.id.currency, n.id.id)  # noqa: E731
    labelled = [n for n in data.annotations if n.label or n.color is not None]
    assert sorted(again.annotations, key=key) == sorted(labelled, key=key)


# ------------------------------------------------------------------ metrics --


def test_metrics_count_crossings_overlaps_and_backward_links() -> None:
    spec = {
        "addresses": [
            {"id": "a", "x": 0.0, "y": 0.0},
            {"id": "b", "x": 0.0, "y": 1.0},  # cramped + overlapping with a
            {"id": "c", "x": 8.0, "y": 0.0},
            {"id": "d", "x": 8.0, "y": 5.0},
        ],
        "txs": [
            {"id": "t1", "x": 4.0, "y": 5.0, "senders": ["a"], "receivers": ["d"]},
            {"id": "t2", "x": 4.0, "y": 0.0, "senders": ["c"], "receivers": ["b"]},
        ],
        "agg_edges": [
            {"a": "a", "b": "d", "tx_ids": ["t1"]},
            {"a": "b", "b": "c", "tx_ids": ["t2"]},
        ],
    }
    m = layout_metrics(spec)
    assert m["overlaps"] == 1
    assert m["cramped"] == 1
    assert m["crossings"] == 1  # a→t1 crosses b—t2
    assert m["flow_links"] == 4
    assert m["flow_backwards"] == 2  # c pays in from the right, b receives on the left


# ----------------------------------------------------- swaps and bridges --


def _pathfinder_conversion_move(out: dict) -> dict[str, tuple[float, float]]:
    """What the Pathfinder UI does to a loaded file with a conversion
    (graphsense-dashboard, Update/Pathfinder.elm,
    InternalConversionLoopAddressesLoaded): txs stay; the input leg's
    inputs go to tx.x - 4 and its outputs to tx.x + 4, the output leg
    mirrored (outputs to tx.x - 4, then inputs to tx.x + 4), all on the
    tx's row.

    Dashboards since graphsense-dashboard e11add5b (2026-09-23) keep the
    positions a file saves and apply this only to nodes they add, so
    this checks the arrangement matches the UI's, not a hard requirement.
    """
    xy = _xy(out)
    moved = dict(xy)
    txs = {t["id"]: t for t in out["txs"]}
    for c in out.get("conversions", []):
        for tx_id, first, second in (
            (c["input_tx"], "senders", "receivers"),
            (c["output_tx"], "receivers", "senders"),
        ):
            x, y = xy[tx_id]
            for a in txs[tx_id][first]:
                moved[a] = (x - _HIER_X_STEP, y)
            for a in txs[tx_id][second]:
                moved[a] = (x + _HIER_X_STEP, y)
    return moved


def _bridge_spec() -> dict:
    """ETH exploiter → hop → bridge contract; on BTC the vault pays the
    recipient (with change back to itself), who pays on to a next hop."""
    txs = [
        _tx("e1", ["exploiter"], ["hop"]),
        _tx("e2", ["hop"], ["contract"]),
        _tx("b1", ["vault"], ["recipient", "vault"]),
        _tx("b2", ["recipient"], ["next"]),
    ]
    return {
        "addresses": [
            {"id": "exploiter", "starting_point": True},
            "hop",
            "contract",
            "vault",
            "recipient",
            "next",
        ],
        "txs": txs,
        "agg_edges": [
            {"a": "exploiter", "b": "hop", "tx_ids": ["e1"]},
            {"a": "hop", "b": "contract", "tx_ids": ["e2"]},
            {"a": "vault", "b": "recipient", "tx_ids": ["b1"]},
            {"a": "recipient", "b": "next", "tx_ids": ["b2"]},
        ],
        "conversions": [
            {
                "type": "bridge_tx",
                "input_tx": "e2",
                "output_tx": "b1",
                "edge_from": "contract",
                "edge_to": "vault",
            }
        ],
    }


def test_bridge_is_laid_out_as_the_pathfinder_u_turn() -> None:
    out = apply_hierarchical_layout(_bridge_spec())
    xy = _xy(out)
    # The output leg sits under the input leg, the vault under the
    # contract, the recipient under the hop. Only that leg runs left:
    # the recipient's own outflow goes right again.
    assert xy["b1"][0] == xy["e2"][0] and xy["b1"][1] > xy["e2"][1]
    assert xy["vault"] == (xy["contract"][0], xy["b1"][1])
    assert xy["recipient"] == (xy["hop"][0], xy["b1"][1])
    assert xy["recipient"][0] < xy["b2"][0] < xy["next"][0]
    _assert_clean(out)
    # Pathfinder's move on load changes nothing.
    assert _pathfinder_conversion_move(out) == xy


def test_fixture_bridge_survives_pathfinder_load() -> None:
    spec = _fixture_spec("C5_B5_A6-A8_THORChainToBitcoin.gs", with_flows=True)
    out = apply_hierarchical_layout(spec)
    assert _pathfinder_conversion_move(out) == _xy(out)
    _assert_clean(out)


def test_conversion_is_ignored_unless_the_ui_would_draw_it() -> None:
    """No drawn edge end, no bridge: the far side is just an unconnected
    part, laid out left to right."""
    spec = _bridge_spec()
    spec["conversions"][0]["edge_to"] = "not-in-the-graph"
    xy = _xy(apply_hierarchical_layout(spec))
    assert xy["vault"][0] < xy["b1"][0] < xy["recipient"][0]


def test_dex_swap_is_a_loop_right_of_the_swapper() -> None:
    """Same-chain swap: the swapper pays the router on one leg and is paid
    by the pool on the other. Router and pool are stacked (the UI's swap
    edge is a short loop), the two legs are stacked, and the swapper —
    on both legs — sits between them rather than on either row."""
    txs = [
        _tx("in", ["prev"], ["swapper"]),
        _tx("sell", ["swapper"], ["router"]),
        _tx("buy", ["pool"], ["swapper"]),
    ]
    spec = {
        "addresses": [
            {"id": "prev", "starting_point": True},
            "swapper",
            "router",
            "pool",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
        "conversions": [
            {
                "type": "dex_swap",
                "input_tx": "sell",
                "output_tx": "buy",
                "edge_from": "router",
                "edge_to": "pool",
            }
        ],
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy["sell"][0] == xy["buy"][0] == xy["swapper"][0] + _HIER_X_STEP
    assert xy["router"][0] == xy["pool"][0] == xy["sell"][0] + _HIER_X_STEP
    assert xy["router"][1] == xy["sell"][1] < xy["pool"][1] == xy["buy"][1]
    assert xy["sell"][1] < xy["swapper"][1] < xy["buy"][1]
    _assert_clean(out)


def test_bridge_from_utxo_to_account_chain() -> None:
    """BTC → ETH: the input leg is a UTXO tx whose first output is the
    bridge vault (change goes back to the sender)."""
    txs = [
        _tx("b1", ["sender"], ["vault", "sender"]),
        _tx("e1", ["router"], ["recipient"]),
        _tx("e2", ["recipient"], ["next"]),
    ]
    spec = {
        "addresses": [
            {"id": "sender", "starting_point": True},
            "vault",
            "router",
            "recipient",
            "next",
        ],
        "txs": txs,
        "agg_edges": [
            {"a": "sender", "b": "vault", "tx_ids": ["b1"]},
            {"a": "router", "b": "recipient", "tx_ids": ["e1"]},
            {"a": "recipient", "b": "next", "tx_ids": ["e2"]},
        ],
        "conversions": [
            {
                "type": "bridge_tx",
                "input_tx": "b1",
                "output_tx": "e1",
                "edge_from": "vault",
                "edge_to": "router",
            }
        ],
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy["e1"][0] == xy["b1"][0] and xy["e1"][1] > xy["b1"][1]
    assert xy["router"] == (xy["vault"][0], xy["e1"][1])
    assert xy["recipient"][0] < xy["e2"][0] < xy["next"][0]
    _assert_clean(out)
    # No _pathfinder_conversion_move check: the sender gets change back,
    # and the old move would put it on the output side. The layout keeps
    # a change address on the spending side, and current dashboards keep
    # a file's positions.


def test_swap_then_bridge() -> None:
    """Two conversions in a row: swap on ETH, then bridge the proceeds
    to BTC. Both get their shape; nothing overlaps or runs backwards."""
    txs = [
        _tx("sell", ["swapper"], ["router"]),
        _tx("buy", ["pool"], ["swapper"]),
        _tx("out", ["swapper"], ["bridge"]),
        _tx("btc", ["vault"], ["recipient"]),
        _tx("btc2", ["recipient"], ["next"]),
    ]
    spec = {
        "addresses": [
            {"id": "swapper", "starting_point": True},
            "router",
            "pool",
            "bridge",
            "vault",
            "recipient",
            "next",
        ],
        "txs": txs,
        "agg_edges": _edges(txs),
        "conversions": [
            {
                "type": "dex_swap",
                "input_tx": "sell",
                "output_tx": "buy",
                "edge_from": "router",
                "edge_to": "pool",
            },
            {
                "type": "bridge_tx",
                "input_tx": "out",
                "output_tx": "btc",
                "edge_from": "bridge",
                "edge_to": "vault",
            },
        ],
    }
    out = apply_hierarchical_layout(spec)
    xy = _xy(out)
    assert xy["router"][0] == xy["pool"][0]
    assert xy["bridge"][0] == xy["vault"][0]
    assert xy["recipient"][0] < xy["btc2"][0] < xy["next"][0]
    _assert_clean(out)
