"""Parse GraphSense `.gs` save files into raw JSON and typed dataclasses.

Wire format (see dashboard src/main.js):
    Uint32Array(LE)  <-  lzwcompress.pack( base64( JSON.stringify(data) ) )

The inner payload's first element is a discriminator used by the dashboard
to pick a decoder (src/Update/Graph.elm, src/Update/Pathfinder.elm):

    Pathfinder :  ["pathfinder", "<version>", name, addrs, txs, annots, aggEdges?]
    Graph 1.0.x:  ["1.0.x", addrs, entities, highlights]
    Graph 0.5.x:  ["0.5.x", tagsPair, layers, highlightsWrap]
    Graph 0.4.x:  ["0.4.x", tagsPair, layers]
"""

from __future__ import annotations

import base64
import json
import re
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Stage 1: bytes -> raw JSON
# ---------------------------------------------------------------------------


def lzw_unpack(codes: list[int]) -> str:
    """Port of lzwcompress' LZWCompress.unpack (npm `lzwcompress`)."""
    if not codes:
        return ""
    dictionary: list[str] = [chr(i) for i in range(256)]
    w = chr(codes[0])
    out: list[str] = [w]
    for k in codes[1:]:
        if k < len(dictionary):
            entry = dictionary[k]
        elif k == len(dictionary):
            entry = w + w[0]
        else:
            raise ValueError(f"invalid LZW code {k} at dict size {len(dictionary)}")
        out.append(entry)
        dictionary.append(w + entry[0])
        w = entry
    return "".join(out)


def lzw_pack(s: str) -> list[int]:
    """Port of lzwcompress' LZWCompress.pack (npm `lzwcompress`).

    Inverse of `lzw_unpack`. The dashboard's encoder pre-populates the
    dictionary with chars 0..255 and always emits ``dictionary[w]`` (no
    charCodeAt fallback) — this mirrors that exactly so a byte-identical
    round-trip is possible.
    """
    if not s:
        return []
    dictionary: dict[str, int] = {chr(i): i for i in range(256)}
    out: list[int] = []
    dict_size = 256
    w = ""
    for c in s:
        wc = w + c
        if wc in dictionary:
            w = wc
        else:
            out.append(dictionary[w])
            dictionary[wc] = dict_size
            dict_size += 1
            w = c
    if w:
        out.append(dictionary[w])
    return out


def decode_gs_bytes(data: bytes) -> Any:
    if len(data) == 0 or len(data) % 4 != 0:
        raise ValueError("not a .gs file (size must be a non-zero multiple of 4)")
    codes = list(struct.unpack(f"<{len(data) // 4}I", data))
    b64 = lzw_unpack(codes)
    return json.loads(base64.b64decode(b64))


def decode_gs(path: str | Path) -> Any:
    return decode_gs_bytes(Path(path).read_bytes())


# ---------------------------------------------------------------------------
# Stage 2: raw JSON -> typed dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Color:
    """RGBA in 0..1 range, matching Elm's `Color` after decoding."""

    r: float
    g: float
    b: float
    a: float

    @classmethod
    def from_rgba_array(cls, v: list[float]) -> Color:
        return cls(float(v[0]), float(v[1]), float(v[2]), float(v[3]))

    @classmethod
    def from_hex(cls, s: str) -> Color:
        m = re.fullmatch(r"#?([0-9a-fA-F]{6})([0-9a-fA-F]{2})?", s)
        if not m:
            raise ValueError(f"bad color hex: {s!r}")
        h = m.group(1)
        r, g, b = (int(h[i : i + 2], 16) / 255.0 for i in (0, 2, 4))
        a = int(m.group(2), 16) / 255.0 if m.group(2) else 1.0
        return cls(r, g, b, a)


# --- Graph (v0.4.x / v0.5.x / v1.0.x) ---------------------------------------


@dataclass
class UserTag:
    label: str
    source: str
    category: str | None
    abuse: str | None


@dataclass
class GraphAddress:
    currency: str
    layer: int
    address: str
    x: float
    y: float
    color: Color | None
    user_tag: UserTag | None


@dataclass
class GraphEntity:
    currency: str
    layer: int
    entity_id: int
    root_address: str | None
    x: float
    y: float
    color: Color | None
    no_addresses: int
    # Not reconstructed for old 0.4/0.5 versions — merged from a separate tag
    # dict upstream and not essential for downstream consumers.
    user_tag: UserTag | None = None


@dataclass
class Highlight:
    title: str
    color: Color


@dataclass
class GraphData:
    kind: str = field(default="graph", init=False)
    version: str = ""
    addresses: list[GraphAddress] = field(default_factory=list)
    entities: list[GraphEntity] = field(default_factory=list)
    highlights: list[Highlight] = field(default_factory=list)


# --- Pathfinder (v1) --------------------------------------------------------


@dataclass
class PathfinderId:
    currency: str
    id: str


@dataclass
class PathfinderThing:
    id: PathfinderId
    x: float
    y: float
    is_starting_point: bool
    index: int


@dataclass
class PathfinderAnnotation:
    id: PathfinderId
    label: str
    color: Color | None


@dataclass
class PathfinderAggEdge:
    a: PathfinderId
    b: PathfinderId
    txs: list[PathfinderId]


@dataclass
class PathfinderData:
    kind: str = field(default="pathfinder", init=False)
    version: str = ""
    name: str = ""
    addresses: list[PathfinderThing] = field(default_factory=list)
    txs: list[PathfinderThing] = field(default_factory=list)
    annotations: list[PathfinderAnnotation] = field(default_factory=list)
    agg_edges: list[PathfinderAggEdge] = field(default_factory=list)


# --- helpers ---------------------------------------------------------------


def _get(seq: list, idx: int, default: Any = None) -> Any:
    return seq[idx] if 0 <= idx < len(seq) else default


def _user_tag_v100(v: list | None) -> UserTag | None:
    # Graph v1.0.x: [label, source, category?, abuse?]
    if not v:
        return None
    return UserTag(
        label=v[0],
        source=v[1],
        category=_get(v, 2),
        abuse=_get(v, 3),
    )


# --- Pathfinder v1 decoder --------------------------------------------------


def _pathfinder_id(v: list) -> PathfinderId:
    return PathfinderId(currency=v[0], id=v[1])


def _pathfinder_thing(v: list) -> PathfinderThing:
    return PathfinderThing(
        id=_pathfinder_id(v[0]),
        x=float(v[1]),
        y=float(v[2]),
        is_starting_point=bool(v[3]),
        index=int(_get(v, 4, 0) or 0),
    )


def _pathfinder_annotation(v: list) -> PathfinderAnnotation:
    color_raw = _get(v, 2)
    return PathfinderAnnotation(
        id=_pathfinder_id(v[0]),
        label=v[1],
        color=Color.from_rgba_array(color_raw) if color_raw else None,
    )


def _pathfinder_agg_edge(v: list) -> PathfinderAggEdge:
    return PathfinderAggEdge(
        a=_pathfinder_id(v[0]),
        b=_pathfinder_id(v[1]),
        txs=[_pathfinder_id(x) for x in v[2]],
    )


def decode_pathfinder_v1(raw: list) -> PathfinderData:
    # ["pathfinder", "1", name, addrs, txs, annotations, aggEdges?]
    return PathfinderData(
        version=raw[1],
        name=raw[2],
        addresses=[_pathfinder_thing(x) for x in raw[3]],
        txs=[_pathfinder_thing(x) for x in raw[4]],
        annotations=[_pathfinder_annotation(x) for x in raw[5]],
        agg_edges=[_pathfinder_agg_edge(x) for x in _get(raw, 6, []) or []],
    )


# --- Graph 1.0.x decoder ----------------------------------------------------


def _graph100_address(v: list) -> GraphAddress:
    # v = [[currency, layer, address], x, y, userTag?, color?]
    cur, layer, addr = v[0]
    color_raw = _get(v, 4)
    return GraphAddress(
        currency=cur,
        layer=int(layer),
        address=str(addr),
        x=float(v[1]),
        y=float(v[2]),
        color=Color.from_rgba_array(color_raw) if color_raw else None,
        user_tag=_user_tag_v100(_get(v, 3)),
    )


def _graph100_entity(v: list) -> GraphEntity:
    # v = [[currency, layer, entityId(int)], rootAddress, x, y, color?, userTag?]
    cur, layer, eid = v[0]
    color_raw = _get(v, 4)
    return GraphEntity(
        currency=cur,
        layer=int(layer),
        entity_id=int(eid),
        root_address=str(v[1]) if v[1] is not None else None,
        x=float(v[2]),
        y=float(v[3]),
        color=Color.from_rgba_array(color_raw) if color_raw else None,
        no_addresses=0,
        user_tag=_user_tag_v100(_get(v, 5)),
    )


def decode_graph_v100(raw: list) -> GraphData:
    # [version, addresses, entities, highlights]
    return GraphData(
        version=raw[0],
        addresses=[_graph100_address(x) for x in raw[1]],
        entities=[_graph100_entity(x) for x in raw[2]],
        highlights=[
            Highlight(title=h[0], color=Color.from_rgba_array(h[1])) for h in raw[3]
        ],
    )


# --- Graph 0.4.x / 0.5.x decoder (best-effort, no tag merge) ---------------
#
# Shape summary from Decode/Graph04x.elm + Decode/Graph050.elm:
#   raw = [version, tagsPair, layers, highlightsWrap?]
#   tagsPair  = [addressTags, entityTags]         (ignored here)
#   layers    = [ ..., addressesAt5, entitiesAt4 ]
#     each addr  = [[address, layer, currency], [x, y, dx, dy, colorHex?, ...], ...]
#     each ent   = [[address, layer, currency], [x, y, dx, dy, memberAddrs, colorHex?, ...]]
#   highlightsWrap (v0.5 only) = [_, _, [[colorHex, title], ...]]


def _old_int(v: Any) -> int:
    return v if isinstance(v, int) else int(v)


def _old_address_id(v: list) -> tuple[str, int, str]:
    # [address, layer, currency]
    return str(v[2]), _old_int(v[1]), str(v[0])


def _old_entity_id(v: list) -> tuple[str, int, int]:
    # [entityId, layer, currency]
    return str(v[2]), _old_int(v[1]), _old_int(v[0])


def _old_coords(meta: list) -> tuple[float, float]:
    return float(meta[0]) + float(meta[2]), float(meta[1]) + float(meta[3])


def _old_color(raw: str | None) -> Color | None:
    return Color.from_hex(raw) if isinstance(raw, str) and raw else None


def _old_address(v: list) -> GraphAddress:
    cur, layer, addr = _old_address_id(v[0])
    meta = v[1]
    x, y = _old_coords(meta)
    return GraphAddress(
        currency=cur,
        layer=layer,
        address=addr,
        x=x,
        y=y,
        color=_old_color(_get(meta, 4)),
        user_tag=None,
    )


def _old_entity(v: list) -> GraphEntity | None:
    try:
        cur, layer, eid = _old_entity_id(v[0])
    except (TypeError, ValueError):
        return None
    meta = v[1]
    x, y = _old_coords(meta)
    members = _get(meta, 4, []) or []
    return GraphEntity(
        currency=cur,
        layer=layer,
        entity_id=eid,
        root_address=None,
        x=x,
        y=y,
        color=_old_color(_get(meta, 5)),
        no_addresses=len(members) if isinstance(members, list) else 0,
    )


def decode_graph_old(raw: list, version: str) -> GraphData:
    layers = raw[2]
    addresses_raw = layers[5] if len(layers) > 5 else []
    entities_raw = layers[4] if len(layers) > 4 else []
    highlights: list[Highlight] = []
    if version.startswith("0.5.") and len(raw) > 3:
        hl_wrap = raw[3]
        if isinstance(hl_wrap, list) and len(hl_wrap) > 2:
            for item in hl_wrap[2]:
                color = _old_color(item[0])
                if color is not None:
                    highlights.append(Highlight(title=item[1], color=color))

    entities = [e for e in (_old_entity(x) for x in entities_raw) if e is not None]
    return GraphData(
        version=version,
        addresses=[_old_address(x) for x in addresses_raw],
        entities=entities,
        highlights=highlights,
    )


# --- top-level dispatch -----------------------------------------------------


def structure(raw: Any) -> PathfinderData | GraphData:
    if not isinstance(raw, list) or not raw:
        raise ValueError("unexpected payload: not a non-empty list")
    head = raw[0]
    if head == "pathfinder":
        version = str(raw[1]) if len(raw) > 1 else ""
        if version == "1":
            return decode_pathfinder_v1(raw)
        raise ValueError(f"unknown pathfinder version: {version!r}")
    if not isinstance(head, str):
        raise ValueError(f"unexpected version marker: {head!r}")
    version = head.split(" ", 1)[0].split("-", 1)[0]
    if version.startswith("1.0."):
        return decode_graph_v100(raw)
    if version.startswith("0.5.") or version.startswith("0.4."):
        return decode_graph_old(raw, version)
    raise ValueError(f"unknown graph version: {head!r}")
