"""Fetch what the builder needs from a GraphSense backend.

One tx body per tx of the file (the flows and values) and the swaps and
bridges among them. Failed lookups are reported as warnings and leave their
tx unresolved; they never fail the export.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional, Protocol, runtime_checkable

from graphsenselib.convert.gs_files import PathfinderData, spec_from_pathfinder
from graphsenselib.pathfinder import (
    ConversionsBackend,
    LayoutBackend,
    annotate_conversions,
)

from . import networks
from .builder import TagSummaries, TxBodies

logger = logging.getLogger(__name__)


@runtime_checkable
class TxBodyBackend(Protocol):
    """:class:`graphsenselib.pathfinder.RestBackend` implements it."""

    async def tx_body(self, network: str, tx_id: str) -> Optional[dict[str, Any]]:
        """The tx response body with inputs/outputs, or None if no such tx."""
        ...


class ExportBackend(TxBodyBackend, ConversionsBackend, Protocol):
    pass


@runtime_checkable
class TagSummaryBackend(Protocol):
    """:class:`graphsenselib.pathfinder.RestBackend` implements it."""

    async def address_tag_summary(
        self, network: str, address: str
    ) -> Optional[dict[str, Any]]:
        """The address's own tag summary, or None if the address is unknown."""
        ...


async def lookup_tags(
    data: PathfinderData,
    backend: TagSummaryBackend,
    *,
    max_concurrency: int = 8,
) -> tuple[TagSummaries, list[str]]:
    """One tag summary per address of the graph (incl. agg edge ends)."""
    ids = [t.id for t in data.addresses]
    ids += [end for e in data.agg_edges for end in (e.a, e.b)]
    keys = list(
        dict.fromkeys((i.currency, networks.normalize_address(i.id)) for i in ids)
    )
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _fetch(key: tuple[str, str]):
        try:
            async with semaphore:
                return key, await backend.address_tag_summary(*key)
        except Exception as exc:  # noqa: BLE001 — reported, never fatal
            logger.warning("tag lookup failed for %s: %s", key[1], exc)
            return key, exc

    summaries: dict[tuple[str, str], dict[str, Any] | None] = {}
    failed: list[str] = []
    for key, result in await asyncio.gather(*(_fetch(k) for k in keys)):
        if isinstance(result, Exception):
            failed.append(key[1])
        else:
            summaries[key] = result
    warnings = []
    if failed:
        warnings.append(
            f"tag lookup failed for {len(failed)} address(es), exported "
            f"without attribution: {', '.join(failed[:10])}"
        )
    return summaries, warnings


async def lookup(
    data: PathfinderData,
    backend: ExportBackend,
    *,
    conversions: bool = True,
    max_concurrency: int = 8,
) -> tuple[TxBodies, list[dict[str, Any]], list[str]]:
    """``(bodies, conversions, warnings)`` for :func:`.builder.build_graph`."""
    semaphore = asyncio.Semaphore(max_concurrency)
    keys = list(dict.fromkeys((t.id.currency, t.id.id) for t in data.txs))

    async def _fetch(key: tuple[str, str]):
        try:
            async with semaphore:
                return key, await backend.tx_body(*key)
        except Exception as exc:  # noqa: BLE001 — reported, never fatal
            logger.warning("tx lookup failed for %s: %s", key[1], exc)
            return key, exc

    bodies: dict[tuple[str, str], dict[str, Any] | None] = {}
    for key, result in await asyncio.gather(*(_fetch(k) for k in keys)):
        if not isinstance(result, Exception):
            bodies[key] = result

    warnings: list[str] = []
    found: list[dict[str, Any]] = []
    if conversions and data.txs:
        network = data.txs[0].id.currency
        spec, more = await annotate_conversions(
            spec_from_pathfinder(data),
            default_network=network,
            backend=backend,
            max_concurrency=max_concurrency,
        )
        found = spec.get("conversions") or []
        warnings.extend(more)
    return bodies, found, warnings


class ConvertBackend(ExportBackend, LayoutBackend, TagSummaryBackend, Protocol):
    """Everything :func:`graphsenselib.convert.graph_convert.convert` may
    ask; :class:`graphsenselib.pathfinder.RestBackend` implements it."""
