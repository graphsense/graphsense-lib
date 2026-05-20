"""Tests for apply_hierarchical_layout — BFS layout used by the MCP
pathfinder_export tool when an agent provides starting-point anchors."""

from __future__ import annotations

from graphsenselib.convert.gs_files import (
    apply_hierarchical_layout,
    builder_from_spec,
    decode_gs_bytes,
    structure,
)
from graphsenselib.convert.gs_files.encoder import _HIER_X_STEP, _HIER_Y_STEP


def _xy_by_id(items: list[dict]) -> dict[str, tuple[float, float]]:
    return {a["id"]: (a["x"], a["y"]) for a in items}


def test_linear_chain_assigns_one_node_per_column() -> None:
    """a (start) — tx1 — b — tx2 — c should land in 5 successive columns."""
    spec = {
        "addresses": [
            {"id": "a", "starting_point": True},
            {"id": "b"},
            {"id": "c"},
        ],
        "txs": ["tx1", "tx2"],
        "agg_edges": [
            {"a": "a", "b": "b", "tx_ids": ["tx1"]},
            {"a": "b", "b": "c", "tx_ids": ["tx2"]},
        ],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])
    txs = _xy_by_id(out["txs"])

    # Hop levels: a=0, tx1=1, b=2, tx2=3, c=4
    assert addrs["a"][0] == 0.0
    assert txs["tx1"][0] == 1.0 * _HIER_X_STEP
    assert addrs["b"][0] == 2.0 * _HIER_X_STEP
    assert txs["tx2"][0] == 3.0 * _HIER_X_STEP
    assert addrs["c"][0] == 4.0 * _HIER_X_STEP
    # Single-node levels are centred on y=0.
    for x, y in [*addrs.values(), *txs.values()]:
        assert y == 0.0


def test_level_is_centred_on_y_zero() -> None:
    """Three siblings at one level get y = -Y_STEP, 0, +Y_STEP, in the
    order the caller listed them in the spec."""
    spec = {
        "addresses": [
            {"id": "root", "starting_point": True},
            {"id": "ax"},
            {"id": "ay"},
            {"id": "az"},
        ],
        "txs": [],
        "agg_edges": [
            {"a": "root", "b": "ax"},
            {"a": "root", "b": "ay"},
            {"a": "root", "b": "az"},
        ],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])

    assert addrs["ax"][0] == _HIER_X_STEP
    assert addrs["ay"][0] == _HIER_X_STEP
    assert addrs["az"][0] == _HIER_X_STEP
    assert addrs["ax"][1] == -_HIER_Y_STEP
    assert addrs["ay"][1] == 0.0
    assert addrs["az"][1] == _HIER_Y_STEP


def test_spec_order_is_the_within_level_tiebreaker() -> None:
    """Siblings come out in the order they appear in the spec, not
    alphabetical. Reordering the spec must reorder the rendered column —
    this is the agent's ordering hint mechanism (no explicit position
    field needed)."""
    # Same nodes, opposite spec order — alphabetical sort would land
    # them the same way, spec-order sort must invert them.
    spec_ascending = {
        "addresses": [
            {"id": "root", "starting_point": True},
            {"id": "z_first"},
            {"id": "a_second"},
        ],
        "txs": [],
        "agg_edges": [
            {"a": "root", "b": "z_first"},
            {"a": "root", "b": "a_second"},
        ],
    }
    out = apply_hierarchical_layout(spec_ascending)
    addrs = _xy_by_id(out["addresses"])
    # z_first listed first -> negative y (top of column); a_second below.
    assert addrs["z_first"][1] < addrs["a_second"][1]


def test_disconnected_nodes_go_to_trailing_column() -> None:
    """Nodes unreachable from any starting point land past the last
    reachable level, so they don't overlap the main graph."""
    spec = {
        "addresses": [
            {"id": "a", "starting_point": True},
            {"id": "b"},
            {"id": "orphan"},
        ],
        "txs": [],
        "agg_edges": [{"a": "a", "b": "b"}],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])

    # Reachable levels: a=0, b=1; orphan goes to level 2.
    assert addrs["a"][0] == 0.0
    assert addrs["b"][0] == _HIER_X_STEP
    assert addrs["orphan"][0] == 2.0 * _HIER_X_STEP


def test_caller_provided_xy_is_preserved() -> None:
    """Explicit x/y on an input item must not be overwritten by BFS."""
    spec = {
        "addresses": [
            {"id": "anchor", "starting_point": True},
            {"id": "pinned", "x": 99.0, "y": -42.0},
        ],
        "txs": [],
        "agg_edges": [{"a": "anchor", "b": "pinned"}],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])
    assert addrs["pinned"] == (99.0, -42.0)


def test_multi_source_bfs_picks_minimum_distance() -> None:
    """With two starts both at level 0, an intermediate node should
    end up at level 1 from whichever start is closer."""
    spec = {
        "addresses": [
            {"id": "s1", "starting_point": True},
            {"id": "s2", "starting_point": True},
            {"id": "shared"},
        ],
        "txs": [],
        "agg_edges": [
            {"a": "s1", "b": "shared"},
            {"a": "s2", "b": "shared"},
        ],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])
    assert addrs["s1"][0] == 0.0
    assert addrs["s2"][0] == 0.0
    assert addrs["shared"][0] == _HIER_X_STEP


def test_no_starting_point_degenerates_to_single_column() -> None:
    """Without anchors, every node is 'disconnected' and lands in one
    column. Callers that want columnar address/tx placement should use
    builder_from_spec directly instead of this layout."""
    spec = {
        "addresses": [{"id": "a"}, {"id": "b"}],
        "txs": ["t"],
        "agg_edges": [{"a": "a", "b": "b", "tx_ids": ["t"]}],
    }
    out = apply_hierarchical_layout(spec)
    xs = {a["x"] for a in out["addresses"]} | {t["x"] for t in out["txs"]}
    assert xs == {0.0}, "without starts, everything collapses to x=0"


def test_layout_round_trips_through_encoder() -> None:
    """The coord-stamped spec must encode and decode through the real
    .gs pipeline so the file is openable by the pathfinder UI."""
    spec = {
        "addresses": [
            {"id": "a", "starting_point": True},
            {"id": "b"},
        ],
        "txs": ["t"],
        "agg_edges": [{"a": "a", "b": "b", "tx_ids": ["t"]}],
    }
    laid = apply_hierarchical_layout(spec)
    builder = builder_from_spec(laid, name="layout-test", default_network="btc")
    data = structure(decode_gs_bytes(builder.to_bytes()))
    assert len(data.addresses) == 2
    assert len(data.txs) == 1
    # Every node has a finite coord.
    for thing in (*data.addresses, *data.txs):
        assert thing.x == thing.x  # not NaN
        assert thing.y == thing.y
