"""Build GraphSense Pathfinder ``.gs`` save files from Python.

A ``.gs`` file is what the dashboard's "Save graph" button produces. Wire
format::

    payload (Python list)
      -> json.dumps (compact, UTF-8)
      -> base64.b64encode
      -> LZW pack (lzwcompress.js compatible)
      -> uint32 little-endian binary buffer

Pathfinder v1 payload shape::

    ["pathfinder", "1", name,
     [[[net, addr_id], x, y, is_starting], ...],            # addresses
     [[[net, tx_hash], x, y, is_starting, index], ...],     # txs
     [[[net, id], label, [r,g,b,a] | null], ...],           # annotations
     [[[net, a], [net, b], [[net, tx_id], ...]], ...]]      # agg edges

Programmatic usage::

    from graphsenselib.convert.gs_files import GsBuilder
    g = GsBuilder(name="invest", default_network="btc")
    g.add_address("bc1q...", label="A", color=(1, 0.4, 0.2, 1))
    g.add_tx("abcd...")
    g.write("out.gs")

The decoder (`parser.py`) is the inverse: any payload produced here
round-trips through `decode_gs_bytes` -> `structure` to the typed
PathfinderData dataclasses.
"""

from __future__ import annotations

import base64
import json
import struct
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Union

from .parser import lzw_pack

Color = tuple[float, float, float, float]


# ---------------------------------------------------------------------------
# Label-aware vertical spacing
# ---------------------------------------------------------------------------
#
# A node's label is drawn stacked with the node, so a label that wraps to
# several lines makes the node taller. Both layouts keep a column evenly
# spaced and widen that uniform row step to fit the column's tallest label.

# Roughly how many characters fit on one line of a node label in the UI.
_LABEL_CHARS_PER_LINE = 12
# Extra vertical graph units a node needs for each label line beyond the first.
_LABEL_LINE_HEIGHT = 1.5


def _label_line_count(label: object) -> int:
    """How many lines a node label occupies in the UI (always >= 1).

    Text wraps at roughly ``_LABEL_CHARS_PER_LINE`` characters; explicit
    newlines are honoured too. A missing/blank label counts as one line.
    """
    if not isinstance(label, str) or not label.strip():
        return 1
    lines = 0
    for segment in label.strip().split("\n"):
        seg_len = len(segment.strip())
        lines += max(1, -(-seg_len // _LABEL_CHARS_PER_LINE))
    return max(1, lines)


# ---------------------------------------------------------------------------
# Bytes <-> raw JSON payload
# ---------------------------------------------------------------------------


def encode_gs_payload(payload: object) -> bytes:
    """Encode a raw JSON payload into the .gs binary container.

    Inverse of `parser.decode_gs_bytes`. The container is a uint32
    little-endian buffer of LZW codes over the base64-encoded compact
    JSON serialization of `payload`.
    """
    js = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    b64 = base64.b64encode(js.encode("utf-8")).decode("ascii")
    codes = lzw_pack(b64)
    return struct.pack(f"<{len(codes)}I", *(c & 0xFFFFFFFF for c in codes))


# ---------------------------------------------------------------------------
# Internal item types
# ---------------------------------------------------------------------------


@dataclass
class _Item:
    network: str
    id: str
    x: float
    y: float
    is_starting_point: bool
    label: Optional[str] = None
    color: Optional[Color] = None


@dataclass
class _Tx(_Item):
    index: int = 0


@dataclass
class _AggEdge:
    a_network: str
    a_id: str
    b_network: str
    b_id: str
    tx_ids: list[tuple[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# High-level builder
# ---------------------------------------------------------------------------


class GsBuilder:
    """Accumulate addresses, txs, annotations, and agg edges; produce a ``.gs``.

    Coordinates are in the dashboard's "graph units" — small floats
    (e.g. x in -15..6, y in -11..11). Pixel-scale offsets render
    comically zoomed out.
    """

    _ROW = 3.0
    # Default column when side is unspecified.
    _ADDR_COL_X = -5.0
    _TX_COL_X = 0.0
    # Side-aware columns: inputs left of txs, outputs right.
    _INPUT_COL_X = -8.0
    _OUTPUT_COL_X = 8.0

    def __init__(self, name: str = "", default_network: str = "btc") -> None:
        self.name = name
        self.default_network = default_network
        self._addresses: list[_Item] = []
        self._txs: list[_Tx] = []
        self._agg_edges: list[_AggEdge] = []
        # Auto-placed nodes: (item, column name). Their y is assigned in
        # _apply_columnar_layout once every label is known, so each column
        # gets one uniform row step sized to its tallest label.
        self._auto: list[tuple[_Item, str]] = []

    def _addr_column(self, side: Optional[str]) -> tuple[str, float]:
        """Map an address ``side`` to its (column name, column x)."""
        if side in ("input", "left"):
            return "input", self._INPUT_COL_X
        if side in ("output", "right"):
            return "output", self._OUTPUT_COL_X
        return "addr", self._ADDR_COL_X

    def _apply_columnar_layout(self) -> None:
        """Assign y to every auto-placed node.

        Each column is stacked with a single uniform row step, widened so
        the column's tallest (multi-line) label still clears its
        neighbour — the whole column stays aligned rather than going
        ragged row by row.
        """
        by_column: dict[str, list[_Item]] = {}
        for item, column in self._auto:
            by_column.setdefault(column, []).append(item)
        for items in by_column.values():
            max_lines = max(_label_line_count(it.label) for it in items)
            step = self._ROW + (max_lines - 1) * _LABEL_LINE_HEIGHT
            for row, item in enumerate(items):
                item.y = row * step

    def add_address(
        self,
        addr: str,
        *,
        network: Optional[str] = None,
        label: Optional[str] = None,
        color: Optional[Color] = None,
        starting_point: bool = False,
        x: Optional[float] = None,
        y: Optional[float] = None,
        side: Optional[str] = None,
    ) -> "GsBuilder":
        net = network or self.default_network
        column, col_x = self._addr_column(side)
        item = _Item(
            net,
            addr,
            col_x if x is None else x,
            0.0 if y is None else y,
            starting_point,
            label,
            color,
        )
        self._addresses.append(item)
        if y is None:
            # Defer y: assigned by _apply_columnar_layout once all labels
            # are known, so the column shares one uniform row step.
            self._auto.append((item, column))
        return self

    def add_tx(
        self,
        tx_hash: str,
        *,
        network: Optional[str] = None,
        index: int = 0,
        label: Optional[str] = None,
        color: Optional[Color] = None,
        starting_point: bool = False,
        x: Optional[float] = None,
        y: Optional[float] = None,
    ) -> "GsBuilder":
        net = network or self.default_network
        item = _Tx(
            net,
            tx_hash,
            self._TX_COL_X if x is None else x,
            0.0 if y is None else y,
            starting_point,
            label,
            color,
            index=index,
        )
        self._txs.append(item)
        if y is None:
            self._auto.append((item, "tx"))
        return self

    def add_agg_edge(
        self,
        addr_a: str,
        addr_b: str,
        tx_ids: Optional[Iterable[str]] = None,
        *,
        network: Optional[str] = None,
        a_network: Optional[str] = None,
        b_network: Optional[str] = None,
    ) -> "GsBuilder":
        net = network or self.default_network
        edge = _AggEdge(a_network or net, addr_a, b_network or net, addr_b)
        if tx_ids:
            edge.tx_ids = [(net, t) for t in tx_ids]
        self._agg_edges.append(edge)
        return self

    def to_payload(self) -> list:
        """Materialize the raw JSON payload (the inner shape before
        json.dumps + base64 + LZW + uint32 packing)."""
        self._apply_columnar_layout()
        addresses = [
            [[a.network, a.id], a.x, a.y, a.is_starting_point] for a in self._addresses
        ]
        txs = [
            [[t.network, t.id], t.x, t.y, t.is_starting_point, t.index]
            for t in self._txs
        ]
        annotations: list[list] = []
        for item in (*self._addresses, *self._txs):
            if item.label is None and item.color is None:
                continue
            annotations.append(
                [
                    [item.network, item.id],
                    item.label or "",
                    list(item.color) if item.color is not None else None,
                ]
            )
        agg_edges = [
            [
                [e.a_network, e.a_id],
                [e.b_network, e.b_id],
                [[net, tid] for net, tid in e.tx_ids],
            ]
            for e in self._agg_edges
        ]
        return ["pathfinder", "1", self.name, addresses, txs, annotations, agg_edges]

    def to_bytes(self) -> bytes:
        return encode_gs_payload(self.to_payload())

    def write(self, path: Union[str, Path]) -> Path:
        p = Path(path)
        p.write_bytes(self.to_bytes())
        return p


# ---------------------------------------------------------------------------
# Spec helpers (used by the CLI and any external loader)
# ---------------------------------------------------------------------------


def _normalize_color(c: object) -> Optional[Color]:
    if c is None:
        return None
    if not isinstance(c, (list, tuple)) or len(c) != 4:
        raise ValueError(f"color must be [r, g, b, a] floats 0-1, got {c!r}")
    return (float(c[0]), float(c[1]), float(c[2]), float(c[3]))


def builder_from_spec(
    spec: dict, *, name: str = "", default_network: str = "btc"
) -> GsBuilder:
    """Build a `GsBuilder` from a JSON spec.

    Spec schema (everything but ``id`` is optional)::

        {
          "addresses": [
            "bc1q...",
            {"id": "bc1q...", "label": "exchange A",
             "color": [1, 0.4, 0.2, 1],
             "starting_point": true, "x": 0, "y": 0,
             "network": "btc", "side": "input"}
          ],
          "txs": [
            "abcd...",
            {"id": "abcd...", "index": 0, "label": "...", "color": [...]}
          ],
          "agg_edges": [
            {"a": "bc1q...", "b": "bc1q...other", "tx_ids": ["abcd..."]}
          ]
        }
    """
    b = GsBuilder(name=name, default_network=default_network)
    for a in spec.get("addresses", []):
        if isinstance(a, str):
            b.add_address(a)
        else:
            b.add_address(
                a["id"],
                network=a.get("network"),
                label=a.get("label"),
                color=_normalize_color(a.get("color")),
                starting_point=a.get("starting_point", False),
                x=a.get("x"),
                y=a.get("y"),
                side=a.get("side"),
            )
    for t in spec.get("txs", []):
        if isinstance(t, str):
            b.add_tx(t)
        else:
            b.add_tx(
                t["id"],
                network=t.get("network"),
                index=t.get("index", 0),
                label=t.get("label"),
                color=_normalize_color(t.get("color")),
                starting_point=t.get("starting_point", False),
                x=t.get("x"),
                y=t.get("y"),
            )
    for e in spec.get("agg_edges", []):
        b.add_agg_edge(
            e["a"],
            e["b"],
            tx_ids=e.get("tx_ids"),
            network=e.get("network"),
            a_network=e.get("a_network"),
            b_network=e.get("b_network"),
        )
    return b


# ---------------------------------------------------------------------------
# Hierarchical (BFS) layout — for graphs that have starting-point anchors,
# e.g. agent-generated investigation findings. Columns by hop distance,
# rows centred within column.
# ---------------------------------------------------------------------------


_HIER_X_STEP = 4.0
_HIER_Y_STEP = 3.0


def _spec_item_to_dict(item: object) -> dict:
    if isinstance(item, str):
        return {"id": item}
    if isinstance(item, dict):
        return dict(item)
    raise ValueError(f"spec item must be str or dict, got {type(item).__name__}")


def apply_hierarchical_layout(spec: dict) -> dict:
    """Return a copy of ``spec`` with ``x``/``y`` stamped onto every node.

    Layout: multi-source BFS from every node flagged ``starting_point=True``.
    Each level becomes a column at ``x = level * 4.0``; within a level,
    nodes are ordered by the position they were first seen in the spec
    (so listing the most relevant nodes first puts them near the top of
    the column) and centred on ``y = 0``. Rows are ``3.0`` apart; a level
    that holds a label wrapping to multiple lines uses a wider uniform
    spacing so the whole column stays aligned. This makes layout
    order-sensitive: reordering ``addresses`` / ``txs`` /
    ``agg_edges`` in the input reorders the rendered graph. Nodes
    unreachable from any starting point are appended to a trailing
    column so they don't overlap the main graph. If the caller already
    provided ``x`` or ``y`` on a node, that coordinate is preserved.

    Intended for agent-built specs where ``starting_point`` marks the
    addresses or txs that anchored the investigation. Without any
    starting points the function still runs but degenerates to a single
    column at ``x = 0`` — callers that want columnar address/tx/side
    placement should use ``builder_from_spec`` directly.

    Addresses, txs, and the tx ids referenced inside ``agg_edges`` are
    all treated as graph nodes (the tx is connected to both endpoint
    addresses of its agg edge); plain ``a``↔``b`` adjacency is added
    too. Nodes are keyed by ``(kind, id)`` so an address and a tx that
    happen to share a string id don't collide.
    """
    addresses = [_spec_item_to_dict(a) for a in spec.get("addresses", [])]
    txs = [_spec_item_to_dict(t) for t in spec.get("txs", [])]
    edges = list(spec.get("agg_edges", []))

    # Node registry, preserving first-seen order for stable disconnected
    # placement and so that addresses/txs declared but unmentioned in
    # any edge still get coordinates.
    nodes: list[tuple[str, str]] = []
    adj: dict[tuple[str, str], set[tuple[str, str]]] = {}

    def _register(key: tuple[str, str]) -> None:
        if key not in adj:
            adj[key] = set()
            nodes.append(key)

    for a in addresses:
        _register(("addr", a["id"]))
    for t in txs:
        _register(("tx", t["id"]))
    for e in edges:
        a_key = ("addr", e["a"])
        b_key = ("addr", e["b"])
        _register(a_key)
        _register(b_key)
        tx_ids = list(e.get("tx_ids") or [])
        if tx_ids:
            # Route the relationship through the tx nodes so they fall
            # between the endpoint addresses in the BFS columns. A direct
            # a↔b edge here would shortcut the layout and collapse the
            # tx onto the same column as one of its endpoints.
            for tid in tx_ids:
                t_key = ("tx", tid)
                _register(t_key)
                adj[a_key].add(t_key)
                adj[t_key].add(a_key)
                adj[b_key].add(t_key)
                adj[t_key].add(b_key)
        else:
            adj[a_key].add(b_key)
            adj[b_key].add(a_key)

    # Starting points anchor level 0; multi-source BFS assigns hop levels.
    starts: list[tuple[str, str]] = []
    for a in addresses:
        if a.get("starting_point"):
            starts.append(("addr", a["id"]))
    for t in txs:
        if t.get("starting_point"):
            starts.append(("tx", t["id"]))

    level: dict[tuple[str, str], int] = {s: 0 for s in starts}
    queue: deque[tuple[str, str]] = deque(starts)
    while queue:
        n = queue.popleft()
        for m in adj[n]:
            if m not in level:
                level[m] = level[n] + 1
                queue.append(m)

    levels: dict[int, list[tuple[str, str]]] = {}
    for key, lvl in level.items():
        levels.setdefault(lvl, []).append(key)

    disconnected = [n for n in nodes if n not in level]
    if disconnected:
        trailing = (max(levels) + 1) if levels else 0
        levels[trailing] = disconnected

    # Stable spec-order tiebreaker within a level: nodes the caller
    # listed earlier float to the top of their column.
    position = {key: i for i, key in enumerate(nodes)}

    # Label line counts: a level that holds a multi-line label uses a
    # wider uniform row step so the whole column stays aligned. Nodes
    # seen only in edges carry no label.
    label_lines: dict[tuple[str, str], int] = {}
    for a in addresses:
        label_lines[("addr", a["id"])] = _label_line_count(a.get("label"))
    for t in txs:
        label_lines[("tx", t["id"])] = _label_line_count(t.get("label"))

    coords: dict[tuple[str, str], tuple[float, float]] = {}
    for lvl, group in levels.items():
        group_sorted = sorted(group, key=lambda k: position[k])
        n_in_level = len(group_sorted)
        # One uniform row step for the whole level, widened to clear the
        # tallest label in it so the column stays evenly aligned.
        max_lines = max((label_lines.get(key, 1) for key in group_sorted), default=1)
        y_step = _HIER_Y_STEP + (max_lines - 1) * _LABEL_LINE_HEIGHT
        x = float(lvl) * _HIER_X_STEP
        for i, key in enumerate(group_sorted):
            y = (i - (n_in_level - 1) / 2.0) * y_step
            coords[key] = (x, y)

    def _apply(items: list[dict], kind: str) -> list[dict]:
        out: list[dict] = []
        for item in items:
            laid = coords.get((kind, item["id"]), (0.0, 0.0))
            new = dict(item)
            if new.get("x") is None:
                new["x"] = laid[0]
            if new.get("y") is None:
                new["y"] = laid[1]
            out.append(new)
        return out

    return {
        **spec,
        "addresses": _apply(addresses, "addr"),
        "txs": _apply(txs, "tx"),
    }
