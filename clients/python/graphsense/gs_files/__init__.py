# AUTO-GENERATED — DO NOT EDIT.
# Synced from src/graphsenselib/convert/gs_files/__init__.py via
# clients/python/scripts/sync_gs_files.py. Edit the source and re-run
# `make -C clients/python sync-gs-files`.
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
    spec_from_pathfinder         — decoded pathfinder file back to a spec

Layout:
    apply_hierarchical_layout    — stamp x/y onto a spec (direction-aware
                                   when txs carry senders / receivers)
    layout_metrics               — overlap / crossing / direction numbers
"""

from .encoder import (
    GsBuilder,
    apply_hierarchical_layout,
    builder_from_spec,
    encode_gs_payload,
    normalize_address_id,
    normalize_tx_id,
    spec_from_pathfinder,
)
from .layout import directed_layout, has_flow_info, layout_metrics
from .parser import (
    Color,
    GraphAddress,
    GraphCluster,
    GraphData,
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
    "GraphCluster",
    "GraphData",
    "GsBuilder",
    "Highlight",
    "PathfinderAggEdge",
    "PathfinderAnnotation",
    "PathfinderData",
    "PathfinderId",
    "PathfinderThing",
    "UserTag",
    "apply_hierarchical_layout",
    "builder_from_spec",
    "decode_gs",
    "decode_gs_bytes",
    "directed_layout",
    "encode_gs_payload",
    "has_flow_info",
    "layout_metrics",
    "lzw_pack",
    "lzw_unpack",
    "normalize_address_id",
    "normalize_tx_id",
    "spec_from_pathfinder",
    "structure",
    "summarize",
    "to_jsonable",
    "write_decoded",
    "write_json",
]
