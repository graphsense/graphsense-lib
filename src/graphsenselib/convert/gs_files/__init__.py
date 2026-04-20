"""Decode GraphSense `.gs` save files.

Public API:
    decode_gs_bytes / decode_gs  — bytes/path to raw JSON
    structure                    — raw JSON to typed dataclasses
    summarize                    — typed dataclasses to short summary dict
    to_jsonable / write_json     — JSON serialization helpers
"""

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
    "Highlight",
    "PathfinderAggEdge",
    "PathfinderAnnotation",
    "PathfinderData",
    "PathfinderId",
    "PathfinderThing",
    "UserTag",
    "decode_gs",
    "decode_gs_bytes",
    "lzw_unpack",
    "structure",
    "summarize",
    "to_jsonable",
    "write_decoded",
    "write_json",
]
