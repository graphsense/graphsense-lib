"""Tests for apply_hierarchical_layout — BFS layout used by the MCP
pathfinder_export tool when an agent provides starting-point anchors."""

from __future__ import annotations

from graphsenselib.convert.gs_files import (
    apply_hierarchical_layout,
    builder_from_spec,
    decode_gs_bytes,
    structure,
)
from graphsenselib.convert.gs_files.encoder import (
    _HIER_X_STEP,
    _HIER_Y_STEP,
    _LABEL_LINE_HEIGHT,
)


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


def test_subtree_stays_clustered_under_parent() -> None:
    """A node's descendants must end up on the same side as the node
    itself — the tree layout's defining property over BFS centring.

    Topology::

        root --- A --- B --- B_a (leaf)
             \\               \\-- B_b (leaf)
              \\-- C (leaf)

    A and C are root's children, A listed first so A's subtree is the
    top branch. With BFS-centred placement, B_a and B_b (level 3) would
    land at the extremes of their column and one of them would cross
    over to C's side. Tidy tree keeps both with A.
    """
    spec = {
        "addresses": [
            {"id": "root", "starting_point": True},
            {"id": "A"},
            {"id": "B"},
            {"id": "B_a"},
            {"id": "B_b"},
            {"id": "C"},
        ],
        "txs": [],
        "agg_edges": [
            {"a": "root", "b": "A"},
            {"a": "A", "b": "B"},
            {"a": "B", "b": "B_a"},
            {"a": "B", "b": "B_b"},
            {"a": "root", "b": "C"},
        ],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])

    # A is listed before C -> A's subtree sits above (negative y).
    assert addrs["A"][1] < addrs["C"][1]
    # Every descendant of A (B, B_a, B_b) stays on A's side of C.
    a_side_negative = addrs["A"][1] < addrs["C"][1]
    for descendant in ("B", "B_a", "B_b"):
        if a_side_negative:
            assert addrs[descendant][1] < addrs["C"][1], (
                f"{descendant} crossed to C's side of the tree"
            )
        else:
            assert addrs[descendant][1] > addrs["C"][1]


def test_tx_y_snaps_to_endpoint_address_midpoint() -> None:
    """A tx between two addresses should sit on the straight line that
    joins them, not at its column's BFS-centred position. With
    asymmetric address y values this is the only way the edge renders
    without a kink at the tx."""
    spec = {
        "addresses": [
            {"id": "root", "starting_point": True},
            # Two children of root land at y = -Y_STEP and +Y_STEP.
            {"id": "top"},
            {"id": "bot"},
            # Grandchild of "top" — only child at level 2, would otherwise
            # be centred at y=0, putting tx_g far off the top→leaf line.
            {"id": "leaf"},
        ],
        "txs": ["tx_top", "tx_bot", "tx_g"],
        "agg_edges": [
            {"a": "root", "b": "top", "tx_ids": ["tx_top"]},
            {"a": "root", "b": "bot", "tx_ids": ["tx_bot"]},
            {"a": "top", "b": "leaf", "tx_ids": ["tx_g"]},
        ],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])
    txs = _xy_by_id(out["txs"])

    # tx_top connects root (y=0) and top (y=-Y_STEP) -> midpoint -Y_STEP/2.
    assert txs["tx_top"][1] == (addrs["root"][1] + addrs["top"][1]) / 2
    assert txs["tx_bot"][1] == (addrs["root"][1] + addrs["bot"][1]) / 2
    # tx_g connects top and leaf — both must be on the same straight line.
    assert txs["tx_g"][1] == (addrs["top"][1] + addrs["leaf"][1]) / 2
    # The straightened tx still keeps its BFS column x.
    assert txs["tx_top"][0] == _HIER_X_STEP


def test_shared_tx_averages_all_endpoints() -> None:
    """A tx referenced by multiple agg_edges (peel that funds two outputs)
    snaps to the mean y of every endpoint address — both edges through
    it still bend, but less than the BFS-centred position."""
    spec = {
        "addresses": [
            {"id": "root", "starting_point": True},
            {"id": "top"},
            {"id": "bot"},
        ],
        "txs": ["peel"],
        "agg_edges": [
            {"a": "root", "b": "top", "tx_ids": ["peel"]},
            {"a": "root", "b": "bot", "tx_ids": ["peel"]},
        ],
    }
    out = apply_hierarchical_layout(spec)
    addrs = _xy_by_id(out["addresses"])
    txs = _xy_by_id(out["txs"])

    expected = (2 * addrs["root"][1] + addrs["top"][1] + addrs["bot"][1]) / 4
    assert txs["peel"][1] == expected


def test_multi_tx_edge_de_overlaps_into_a_vertical_strip() -> None:
    """A single agg edge that carries N transactions used to stack all N
    txs at the same (column-x, midpoint-y) — visually invisible past the
    first. They must now be spread along y around the midpoint, with no
    two ending at the same (x, y).
    """
    spec = {
        "addresses": [
            {"id": "root", "starting_point": True},
            {"id": "dest"},
        ],
        "txs": [{"id": f"tx{i}"} for i in range(8)],
        "agg_edges": [
            {"a": "root", "b": "dest", "tx_ids": [f"tx{i}" for i in range(8)]},
        ],
    }
    out = apply_hierarchical_layout(spec)
    txs = _xy_by_id(out["txs"])
    positions = [txs[f"tx{i}"] for i in range(8)]
    # Same column (the edge spans one BFS hop).
    assert len({x for x, _ in positions}) == 1
    # No two txs share a coordinate.
    assert len(set(positions)) == 8


def test_singleton_tx_landing_on_a_pile_spread_slot_is_pulled_into_the_pile() -> None:
    """The pathological case from the Internet Archive → Coinbase/Kraken
    spec: a multi-tx edge's spread positions extend beyond the original
    snap point, occasionally landing exactly where a singleton edge's
    midpoint would sit. The de-overlap step must run iteratively so the
    singleton joins the pile instead of collapsing onto a spread slot.
    """
    # Three siblings of root: two are leaves on each side (singletons),
    # the middle one is the target of a 5-tx edge. The 5-tx pile spreads
    # wide enough to brush against the singleton midpoints.
    spec = {
        "addresses": [
            {"id": "root", "starting_point": True},
            {"id": "top"},
            {"id": "mid"},
            {"id": "bot"},
        ],
        "txs": [{"id": f"t{i}"} for i in range(7)],
        "agg_edges": [
            {"a": "root", "b": "top", "tx_ids": ["t0"]},
            {"a": "root", "b": "mid", "tx_ids": ["t1", "t2", "t3", "t4", "t5"]},
            {"a": "root", "b": "bot", "tx_ids": ["t6"]},
        ],
    }
    out = apply_hierarchical_layout(spec)
    txs = _xy_by_id(out["txs"])
    positions = [txs[f"t{i}"] for i in range(7)]
    assert len(set(positions)) == 7, f"overlaps: {positions}"


def test_multiline_label_widens_row_spacing() -> None:
    """A node whose label wraps to multiple lines pushes its column
    neighbours apart so the label text does not overlap them."""

    def _spec(mid_label: str | None) -> dict:
        mid: dict = {"id": "mid"}
        if mid_label is not None:
            mid["label"] = mid_label
        return {
            "addresses": [
                {"id": "root", "starting_point": True},
                {"id": "top"},
                mid,
                {"id": "bot"},
            ],
            "txs": [],
            "agg_edges": [
                {"a": "root", "b": "top"},
                {"a": "root", "b": "mid"},
                {"a": "root", "b": "bot"},
            ],
        }

    plain = _xy_by_id(apply_hierarchical_layout(_spec(None))["addresses"])
    # 19 chars -> wraps to 2 lines at ~12 chars per line.
    wrapped = _xy_by_id(
        apply_hierarchical_layout(_spec("victim deposit addr"))["addresses"]
    )

    # No label: two single-line rows -> a 2 * Y_STEP gap top-to-bottom.
    assert plain["bot"][1] - plain["top"][1] == 2 * _HIER_Y_STEP
    # The wrapped label widens the level's uniform row step, applied to
    # every node in the column so it stays aligned (gaps stay equal).
    wide_step = _HIER_Y_STEP + _LABEL_LINE_HEIGHT
    assert wide_step > _HIER_Y_STEP
    assert wrapped["mid"][1] - wrapped["top"][1] == wide_step
    assert wrapped["bot"][1] - wrapped["mid"][1] == wide_step
    # The middle node stays centred on y = 0 either way.
    assert plain["mid"][1] == wrapped["mid"][1] == 0.0
