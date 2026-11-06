"""Lay out a Pathfinder graph afresh, with flow direction from a backend.

The one implementation behind ``gs-files layout``, ``POST /graph/convert``
with ``layout=auto`` and the MCP ``build_pathfinder_file`` tool:

* :func:`annotate_for_layout` asks the backend which addresses send and
  receive each tx and which txs are swap or bridge legs;
* :func:`layout_spec` places every node (columns by hop distance from the
  starting points, senders left of their tx);
* :func:`relayout` does both for a decoded ``.gs`` file.

Give every step the same :class:`RestBackend`: it caches tx bodies, so a
later export of the same graph fetches nothing twice.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol

from graphsenselib.convert.gs_files import (
    PathfinderData,
    apply_hierarchical_layout,
    spec_from_pathfinder,
)
from graphsenselib.pathfinder.tx_flows import (
    ConversionsBackend,
    TxSidesBackend,
    annotate_conversions,
    annotate_tx_flows,
)


class LayoutBackend(TxSidesBackend, ConversionsBackend, Protocol):
    """:class:`graphsenselib.pathfinder.RestBackend` implements it."""


async def annotate_for_layout(
    spec: dict[str, Any],
    *,
    default_network: str,
    backend: LayoutBackend,
    max_concurrency: int = 8,
) -> tuple[dict[str, Any], list[str]]:
    """``spec`` with tx ``senders`` / ``receivers`` and ``conversions``,
    plus the warnings of both lookups. Failed lookups only cost direction."""
    flowed, warnings = await annotate_tx_flows(
        spec,
        default_network=default_network,
        backend=backend,
        max_concurrency=max_concurrency,
    )
    converted, more = await annotate_conversions(
        flowed,
        default_network=default_network,
        backend=backend,
        max_concurrency=max_concurrency,
    )
    return converted, warnings + more


def layout_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Hierarchical layout of ``spec``, also when it has no starting point.

    The layout grows columns out of the starting points. A spec without
    any gets its first address as a stand-in anchor, which is not marked
    as a starting point in the result.
    """
    addresses = spec.get("addresses") or []
    nodes = list(addresses) + list(spec.get("txs") or [])
    needs_anchor = bool(addresses) and not any(
        isinstance(n, dict) and n.get("starting_point") for n in nodes
    )
    if not needs_anchor:
        return apply_hierarchical_layout(spec)
    first = addresses[0]
    first = {"id": first} if isinstance(first, str) else dict(first)
    anchored = {
        **spec,
        "addresses": [{**first, "starting_point": True}, *addresses[1:]],
    }
    laid = apply_hierarchical_layout(anchored)
    laid["addresses"][0]["starting_point"] = False
    return laid


def default_network(data: PathfinderData) -> str:
    """The network of the file's first node; ``btc`` for an empty file."""
    for thing in (*data.addresses, *data.txs):
        return thing.id.currency
    return "btc"


async def relayout(
    data: PathfinderData,
    backend: Optional[LayoutBackend],
) -> tuple[dict[str, Any], list[str]]:
    """A spec of ``data`` with every node placed afresh, plus warnings.

    Labels, colours, starting points and edges are kept. Without a
    backend the layout runs without flow direction, so inflows may be
    drawn right of the address they pay into.
    """
    spec = spec_from_pathfinder(data, keep_positions=False)
    warnings: list[str] = []
    if backend is not None:
        spec, warnings = await annotate_for_layout(
            spec, default_network=default_network(data), backend=backend
        )
    else:
        warnings.append(
            "no backend: laid out without flow direction, so inflows may be "
            "drawn on the right"
        )
    return layout_spec(spec), warnings
