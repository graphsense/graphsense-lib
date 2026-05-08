"""Encode and decode GraphSense `.gs` save files.

Decode:
    decode_gs_bytes / decode_gs  — bytes/path to raw JSON
    structure                    — raw JSON to typed dataclasses
    summarize                    — typed dataclasses to short summary dict
    to_jsonable / write_json     — JSON serialization helpers

Encode:
    GsBuilder                    — high-level fluent API for building graphs
    encode_gs_payload            — raw payload list to .gs bytes
    builder_from_spec            — build a GsBuilder from a JSON spec dict
"""

from .encoder import (
    GsBuilder,
    builder_from_spec,
    encode_gs_payload,
)
from .parser import (
    Color,
    GraphAddress,
    GraphData,
    GraphEntity,
    Highlight,
    PathfinderAggEdge,
    PathfinderAnnotation,
    PathfinderData,
    PathfinderId,
    PathfinderThing,
    UserTag,
    decode_gs,
    decode_gs_bytes,
    lzw_pack,
    lzw_unpack,
    structure,
)
from .summary import summarize
from .writer import to_jsonable, write_decoded, write_json

__all__ = [
    "Color",
    "GraphAddress",
    "GraphData",
    "GraphEntity",
    "GsBuilder",
    "Highlight",
    "PathfinderAggEdge",
    "PathfinderAnnotation",
    "PathfinderData",
    "PathfinderId",
    "PathfinderThing",
    "UserTag",
    "builder_from_spec",
    "decode_gs",
    "decode_gs_bytes",
    "encode_gs_payload",
    "lzw_pack",
    "lzw_unpack",
    "structure",
    "summarize",
    "to_jsonable",
    "write_decoded",
    "write_json",
]
