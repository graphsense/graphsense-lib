# AUTO-GENERATED — DO NOT EDIT.
# Synced from src/graphsenselib/convert/gs_files/summary.py via
# clients/python/scripts/sync_gs_files.py. Edit the source and re-run
# `make -C clients/python sync-gs-files`.
"""Short summary statistics for a decoded `.gs` payload."""

from __future__ import annotations

from typing import Any

from .parser import GraphData, PathfinderData


def summarize(data: PathfinderData | GraphData) -> dict[str, Any]:
    """Return a concise summary (kind, version, cluster counts) as a dict."""
    if isinstance(data, PathfinderData):
        return {
            "kind": "pathfinder",
            "version": data.version,
            "name": data.name,
            "n_addresses": len(data.addresses),
            "n_txs": len(data.txs),
            "n_annotations": len(data.annotations),
            "n_agg_edges": len(data.agg_edges),
        }
    if isinstance(data, GraphData):
        return {
            "kind": "graph",
            "version": data.version,
            "n_addresses": len(data.addresses),
            "n_clusters": len(data.clusters),
            "n_highlights": len(data.highlights),
        }
    raise TypeError(f"unknown structured type: {type(data).__name__}")
