"""Convert Pathfinder graphs to other formats, and lay them out.

Pipeline::

    .gs bytes / JSON payload ──parse──▶ PathfinderData
        ──layout=auto (optional, graphsenselib.pathfinder.relayout)──▶ PathfinderData
        ├──to=gs──▶ .gs bytes
        └──to=<graph format>──lookup──▶ tx bodies, conversions
              ──build──▶ Graph (format-neutral, lossless: tx_mode=node)
              ──writer──▶ bytes

A new graph format is one more :class:`Writer` in :data:`WRITERS`; it gets
the same model and therefore the same conventions (see :mod:`.model`).
Give every step one backend: :class:`graphsenselib.pathfinder.RestBackend`
caches tx bodies, so layout and export fetch each tx once.

    graph_from_payload    — .gs bytes or the decoded JSON list -> PathfinderData
    lookup                — fetch tx bodies + conversions from a backend
    build_graph           — PathfinderData + lookup result -> Graph
    render                — Graph -> bytes in a registered graph format
    convert               — all of the above in one call
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from collections.abc import Collection
from typing import Any, Callable, Literal, Optional

from graphsenselib.convert.gs_files import (
    PathfinderData,
    builder_from_spec,
    decode_gs_bytes,
    spec_from_pathfinder,
    structure,
)

from . import graphml
from .aggregate import aggregate
from .builder import TagSummaries, TxBodies, build_graph
from .lookup import (
    ConvertBackend,
    ExportBackend,
    TagSummaryBackend,
    TxBodyBackend,
    lookup,
    lookup_tags,
)
from .model import (
    FORMAT_VERSION,
    AddressNode,
    AggEdge,
    ConversionEdge,
    FlowEdge,
    Graph,
    TxMode,
    TxNode,
)

LayoutMode = Literal["keep", "auto"]

GS_MEDIA_TYPE = "application/octet-stream"


@dataclass(frozen=True)
class Writer:
    name: str
    media_type: str
    extension: str
    write: Callable[[Graph], bytes]


WRITERS: dict[str, Writer] = {
    "graphml": Writer(
        name="graphml",
        media_type=graphml.MEDIA_TYPE,
        extension=graphml.EXTENSION,
        write=graphml.write_graphml,
    ),
}

# Every ``to`` that :func:`convert` accepts.
FORMATS: tuple[str, ...] = ("gs", *WRITERS)

TX_MODES: tuple[str, ...] = ("node", "aggregate")

# Optional enrichment of graph formats; each costs one lookup per address.
#
# ``tags``: the address's own attribution (best label, actor, category,
# confidence, concepts) from the tagstore. Cluster attribution is never
# included.
INCLUDES: tuple[str, ...] = ("tags",)

_FILENAME_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(name: str, extension: str, *, fallback: str = "pathfinder") -> str:
    """A filename for any OS, derived from a user-supplied graph name."""
    base = _FILENAME_UNSAFE.sub("_", name).strip("._-") or fallback
    return f"{base[:80]}.{extension}"


def graph_from_payload(payload: bytes | list[Any]) -> PathfinderData:
    """Parse a ``.gs`` file's bytes or its decoded JSON payload (what the
    dashboard's ``Encode.Pathfinder.encode`` produces before packing).

    Raises ValueError for anything else, including old Graph files."""
    raw = decode_gs_bytes(payload) if isinstance(payload, bytes) else payload
    data = structure(raw)
    if not isinstance(data, PathfinderData):
        raise ValueError("only Pathfinder graphs can be converted")
    return data


def render(graph: Graph, format: str = "graphml", tx_mode: TxMode = "node") -> bytes:
    """Write a node-mode ``graph`` in ``format``, as ``tx_mode`` view."""
    writer = WRITERS.get(format)
    if writer is None:
        raise ValueError(
            f"unknown graph format {format!r}; known: {', '.join(WRITERS)}"
        )
    if tx_mode not in TX_MODES:
        raise ValueError(f"unknown tx_mode {tx_mode!r}; known: {', '.join(TX_MODES)}")
    return writer.write(aggregate(graph) if tx_mode == "aggregate" else graph)


@dataclass
class Converted:
    content: bytes
    media_type: str
    filename: str
    warnings: list[str] = field(default_factory=list)
    graph: Optional[Graph] = None
    """The model the content was written from; None for ``to=gs``."""


def _builder(spec: dict[str, Any], data: PathfinderData):
    # Every item of a spec from a decoded file names its network; the
    # default only matters for an empty one.
    from graphsenselib.pathfinder.relayout import default_network

    return builder_from_spec(
        spec, name=data.name, default_network=default_network(data)
    )


async def convert(
    payload: bytes | list[Any] | PathfinderData,
    backend: Optional[ConvertBackend],
    *,
    to: str = "graphml",
    tx_mode: TxMode = "node",
    layout: LayoutMode = "keep",
    include: Collection[str] = (),
) -> Converted:
    """Convert a Pathfinder graph to ``to`` (one of :data:`FORMATS`).

    ``layout="auto"`` places every node afresh first, as ``gs-files
    layout`` does. ``backend`` gives flow direction for the layout and
    the flows for graph formats; without one the layout is undirected and
    a graph format has no flow edges (every tx unresolved). ``include``
    picks enrichment from :data:`INCLUDES`. Lookup failures become
    warnings, never errors.
    """
    if to not in FORMATS:
        raise ValueError(f"unknown format {to!r}; known: {', '.join(FORMATS)}")
    if tx_mode not in TX_MODES:
        raise ValueError(f"unknown tx_mode {tx_mode!r}; known: {', '.join(TX_MODES)}")
    unknown = set(include) - set(INCLUDES)
    if unknown:
        raise ValueError(
            f"unknown include {', '.join(sorted(unknown))}; known: "
            f"{', '.join(INCLUDES)}"
        )
    if to == "gs" and (tx_mode != "node" or include):
        raise ValueError("tx_mode and include apply to graph formats, not to gs")
    data = (
        payload if isinstance(payload, PathfinderData) else graph_from_payload(payload)
    )
    warnings: list[str] = []
    spec: Optional[dict[str, Any]] = None
    if layout == "auto":
        from graphsenselib.pathfinder import relayout

        spec, more = await relayout(data, backend)
        warnings.extend(more)
        data = graph_from_payload(_builder(spec, data).to_payload())

    if to == "gs":
        content = _builder(spec or spec_from_pathfinder(data), data).to_bytes()
        return Converted(
            content=content,
            media_type=GS_MEDIA_TYPE,
            filename=safe_filename(data.name, "gs"),
            warnings=warnings,
        )

    tags: Optional[TagSummaries] = None
    if backend is not None:
        bodies, conversions, more = await lookup(data, backend)
        warnings.extend(more)
        if "tags" in include:
            tags, more = await lookup_tags(data, backend)
            warnings.extend(more)
    else:
        bodies, conversions = {}, []
        warnings.append("no backend: exported without flow edges or attribution")
    graph = build_graph(
        data, bodies, conversions=conversions, tag_summaries=tags, warnings=warnings
    )
    if tx_mode == "aggregate":
        graph = aggregate(graph)
    writer = WRITERS[to]
    return Converted(
        content=writer.write(graph),
        media_type=writer.media_type,
        filename=safe_filename(data.name, writer.extension),
        warnings=graph.warnings,
        graph=graph,
    )


__all__ = [
    "FORMATS",
    "INCLUDES",
    "TX_MODES",
    "ConvertBackend",
    "TagSummaries",
    "TagSummaryBackend",
    "aggregate",
    "lookup_tags",
    "FORMAT_VERSION",
    "GS_MEDIA_TYPE",
    "WRITERS",
    "AddressNode",
    "AggEdge",
    "ConversionEdge",
    "Converted",
    "ExportBackend",
    "FlowEdge",
    "Graph",
    "LayoutMode",
    "TxBodies",
    "TxBodyBackend",
    "TxMode",
    "TxNode",
    "Writer",
    "build_graph",
    "convert",
    "graph_from_payload",
    "lookup",
    "render",
    "safe_filename",
]
