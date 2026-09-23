"""Fill in what the layout needs to know from the backend: which
addresses send and which receive each tx, and which txs are the two legs
of a swap or bridge.

The ``.gs`` format has no notion of direction — an agg edge's ``a``/``b``
are just its two ends — so a layout that wants inflows on the left has
to ask the backend. :func:`annotate_tx_flows` does that and writes
``senders`` / ``receivers`` onto each tx entry, which
:func:`graphsenselib.convert.gs_files.apply_hierarchical_layout` picks up.

Lookups that fail leave their tx without direction; the layout then
treats that tx like the undirected layout would. So a partial or failed
lookup degrades the result, never breaks it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional, Protocol, runtime_checkable

from graphsenselib.convert.gs_files import normalize_address_id, normalize_tx_id
from graphsenselib.pathfinder.verify_backend import _as_dicts, _network_of

logger = logging.getLogger(__name__)


@runtime_checkable
class TxSidesBackend(Protocol):
    """What :func:`annotate_tx_flows` needs from a backend.

    :class:`graphsenselib.pathfinder.RestBackend` implements it.
    """

    async def tx_sides(
        self, network: str, tx_id: str
    ) -> Optional[tuple[frozenset[str], frozenset[str]]]:
        """``(senders, receivers)`` of ``tx_id``, or None if it doesn't exist."""
        ...


def _tx_key(tx_id: str) -> str:
    """Compare tx ids across sources: the conversions endpoint writes
    account-model asset transfers with a ``0x`` prefix
    (``0x<hash>_I387``) or without, while a saved file has none."""
    return normalize_tx_id(tx_id).removeprefix("0x")


@runtime_checkable
class ConversionsBackend(Protocol):
    """What :func:`annotate_conversions` needs from a backend.

    :class:`graphsenselib.pathfinder.RestBackend` implements it.
    """

    async def tx_conversions(self, network: str, tx_id: str) -> list[dict[str, Any]]:
        """Conversions (swaps, bridge txs) the tx takes part in."""
        ...

    async def tx_io_order(
        self, network: str, tx_id: str
    ) -> Optional[tuple[list[str], list[str]]]:
        """Input and output addresses in tx order, or None if no such tx."""
        ...


async def annotate_conversions(
    spec: dict[str, Any],
    *,
    default_network: str,
    backend: ConversionsBackend,
    max_concurrency: int = 8,
) -> tuple[dict[str, Any], list[str]]:
    """Return a copy of ``spec`` with a ``conversions`` list, plus warnings.

    The Pathfinder UI draws a swap or bridge as an edge from the first
    output address of the input-leg tx to the first input address of the
    output-leg tx, and on load moves the addresses of both legs into a
    fixed U-turn arrangement. The layout reproduces that arrangement, so
    it needs to know each such pair. An entry is recorded only when the
    UI will draw the edge: both legs are txs of the spec and both edge
    ends are addresses of it::

        {"type": "bridge_tx", "input_tx": "<id>", "output_tx": "<id>",
         "edge_from": "<address>", "edge_to": "<address>"}
    """
    txs = _as_dicts(spec.get("txs") or [])
    tx_ids = {_tx_key(t["id"]): t for t in txs}
    drawn = {
        normalize_address_id(a["id"]) for a in _as_dicts(spec.get("addresses") or [])
    }
    semaphore = asyncio.Semaphore(max_concurrency)
    failed: list[str] = []

    async def _call(fn, *args):
        try:
            async with semaphore:
                return await fn(*args)
        except Exception as exc:  # noqa: BLE001 — reported, never fatal
            logger.warning("conversion lookup failed for %s: %s", args[-1], exc)
            failed.append(args[-1])
            return None

    found = await asyncio.gather(
        *(
            _call(backend.tx_conversions, _network_of(t, default_network), t["id"])
            for t in txs
        )
    )

    conversions: dict[tuple[str, str], dict[str, Any]] = {}
    for listed in found:
        for c in listed or []:
            legs = (c.get("from_asset_transfer"), c.get("to_asset_transfer"))
            if not all(isinstance(leg, str) for leg in legs):
                continue
            key = (_tx_key(legs[0]), _tx_key(legs[1]))
            if key in conversions or not all(k in tx_ids for k in key):
                continue
            conversions[key] = {"type": c.get("conversion_type")}

    out: list[dict[str, Any]] = []
    for (inp, outp), entry in conversions.items():
        in_tx, out_tx = tx_ids[inp], tx_ids[outp]
        in_io, out_io = await asyncio.gather(
            _call(
                backend.tx_io_order, _network_of(in_tx, default_network), in_tx["id"]
            ),
            _call(
                backend.tx_io_order, _network_of(out_tx, default_network), out_tx["id"]
            ),
        )
        if not in_io or not out_io or not in_io[1] or not out_io[0]:
            continue
        edge_from = normalize_address_id(in_io[1][0])
        edge_to = normalize_address_id(out_io[0][0])
        if edge_from not in drawn or edge_to not in drawn:
            continue
        out.append(
            {
                **entry,
                "input_tx": in_tx["id"],
                "output_tx": out_tx["id"],
                "edge_from": edge_from,
                "edge_to": edge_to,
            }
        )

    warnings = []
    if failed:
        warnings.append(
            f"conversion lookup failed for {len(failed)} tx(s); swaps and "
            f"bridges there are laid out as ordinary txs: {', '.join(failed[:10])}"
        )
    return {**spec, "conversions": out}, warnings


async def annotate_tx_flows(
    spec: dict[str, Any],
    *,
    default_network: str,
    backend: TxSidesBackend,
    max_concurrency: int = 8,
) -> tuple[dict[str, Any], list[str]]:
    """Return a copy of ``spec`` whose txs carry ``senders`` / ``receivers``,
    plus a list of warnings for txs whose direction couldn't be found.

    Only the addresses that also appear in the spec are kept — a tx with
    hundreds of outputs doesn't bloat the spec, and the layout only needs
    the drawn ones. A tx that already names its senders or receivers is
    left untouched. Backend errors on one tx are caught and reported, so
    one bad lookup doesn't lose the directions of the others.
    """
    txs = _as_dicts(spec.get("txs") or [])
    drawn: set[str] = set()
    for a in _as_dicts(spec.get("addresses") or []):
        drawn.add(a["id"])
    for e in spec.get("agg_edges") or []:
        drawn.update((e["a"], e["b"]))
    # Compare case-insensitively for EVM addresses, like the rest of the
    # pipeline, but write back the spelling the spec uses.
    spelled = {normalize_address_id(a): a for a in drawn}

    semaphore = asyncio.Semaphore(max_concurrency)

    async def _lookup(tx: dict[str, Any]):
        if tx.get("senders") or tx.get("receivers"):
            return tx, None
        net = _network_of(tx, default_network)
        try:
            async with semaphore:
                return tx, await backend.tx_sides(net, tx["id"])
        except Exception as exc:  # noqa: BLE001 — reported, never fatal
            logger.warning("tx direction lookup failed for %s: %s", tx["id"], exc)
            return tx, exc

    results = await asyncio.gather(*(_lookup(t) for t in txs))

    out_txs: list[dict[str, Any]] = []
    missing: list[str] = []
    failed: list[str] = []
    for tx, result in results:
        new = dict(tx)
        if isinstance(result, tuple):
            senders, receivers = result
            new["senders"] = sorted(
                {
                    spelled[n]
                    for a in senders
                    if (n := normalize_address_id(a)) in spelled
                }
            )
            new["receivers"] = sorted(
                {
                    spelled[n]
                    for a in receivers
                    if (n := normalize_address_id(a)) in spelled
                }
            )
        elif isinstance(result, Exception):
            failed.append(tx["id"])
        elif result is None and not (tx.get("senders") or tx.get("receivers")):
            missing.append(tx["id"])
        out_txs.append(new)

    warnings: list[str] = []
    if missing:
        warnings.append(
            f"{len(missing)} tx(s) not found on the backend; laid out without "
            f"direction: {', '.join(missing[:10])}"
        )
    if failed:
        warnings.append(
            f"direction lookup failed for {len(failed)} tx(s); laid out "
            f"without direction: {', '.join(failed[:10])}"
        )
    return {**spec, "txs": out_txs}, warnings
