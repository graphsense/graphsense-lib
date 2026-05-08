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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Union

from .parser import lzw_pack

Color = tuple[float, float, float, float]


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
        self._addr_n = 0
        self._input_n = 0
        self._output_n = 0
        self._tx_n = 0

    def _next_addr_pos(self, side: Optional[str]) -> tuple[float, float]:
        if side in ("input", "left"):
            y = self._input_n * self._ROW
            self._input_n += 1
            return self._INPUT_COL_X, y
        if side in ("output", "right"):
            y = self._output_n * self._ROW
            self._output_n += 1
            return self._OUTPUT_COL_X, y
        y = self._addr_n * self._ROW
        self._addr_n += 1
        return self._ADDR_COL_X, y

    def _next_tx_pos(self) -> tuple[float, float]:
        y = self._tx_n * self._ROW
        self._tx_n += 1
        return self._TX_COL_X, y

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
        nx, ny = self._next_addr_pos(side)
        self._addresses.append(
            _Item(
                net,
                addr,
                nx if x is None else x,
                ny if y is None else y,
                starting_point,
                label,
                color,
            )
        )
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
        nx, ny = self._next_tx_pos()
        self._txs.append(
            _Tx(
                net,
                tx_hash,
                nx if x is None else x,
                ny if y is None else y,
                starting_point,
                label,
                color,
                index=index,
            )
        )
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
