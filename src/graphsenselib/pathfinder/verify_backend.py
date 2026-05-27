"""Backend-aware sanity checks for a pathfinder spec.

The structural checks in
:func:`graphsenselib.pathfinder.verify_structural` only catch in-spec
inconsistencies (orphan tx, dangling endpoint, …). A spec that is
internally consistent can still be wrong on the chain — the tx hash
doesn't exist, the address is a typo, or the agg_edge claims an a↔b
relationship that the tx itself doesn't mediate. Catching those needs
to ask the backend.

This module is decoupled from any specific transport by way of a tiny
:class:`GraphsenseBackend` Protocol. The verifier only knows two
operations — ``address_exists`` and ``tx_addresses`` — so any backend
(REST, generated python client, fake-for-tests) is a small adapter
away.

Concurrency is bounded by ``max_concurrency`` so a large spec can't
fan-out hundreds of simultaneous backend requests; the pool-exhaustion
incident from 2026-05-04 is the relevant precedent.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Iterable, Optional, Protocol, runtime_checkable

import httpx

logger = logging.getLogger(__name__)

# Cap the per-warning identifier list so a pathological spec can't bloat
# the response. Mirrors the cap used by the structural warnings.
_WARNING_REF_LIMIT = 10


@runtime_checkable
class GraphsenseBackend(Protocol):
    """Minimal async surface the verifier needs.

    Implementations adapt to whatever transport the caller has — the
    shipped :class:`RestBackend` wraps httpx for REST callers; a
    python-client caller can write a ~15-line adapter wrapping
    ``graphsense.ext.GraphSense`` with ``asyncio.to_thread``. Keep this
    surface tiny — every method here is a network call the verifier
    may issue once per spec entry, so adding ops costs latency on every
    verify call.
    """

    async def address_exists(self, network: str, address: str) -> bool:
        """Return True iff ``address`` is known on ``network``."""
        ...

    async def tx_addresses(self, network: str, tx_id: str) -> Optional[frozenset[str]]:
        """Return the set of addresses that participated in ``tx_id``.

        Returns ``None`` when the tx doesn't exist on ``network`` (the
        signal the verifier uses to flag a non-existent tx). For UTXO
        txs this is ``inputs ∪ outputs``; for account-model it's at
        least ``{from_address, to_address}``. Implementations may
        include more (token sub-payments etc.) — the verifier only
        uses set membership, so a superset is correct.
        """
        ...


def _truncate(items: Iterable[str]) -> str:
    seq = list(items)
    shown = seq[:_WARNING_REF_LIMIT]
    more = len(seq) - len(shown)
    suffix = f" (+{more} more)" if more > 0 else ""
    return ", ".join(shown) + suffix


def _network_of(item: dict[str, Any], default: str) -> str:
    """Resolve the network ticker for a spec entry, falling back to the
    spec-level default. Mirrors what the encoder does so the verifier
    looks at the same network the .gs file will commit to."""
    net = item.get("network")
    return net if isinstance(net, str) and net else default


async def verify_against_backend(
    spec: dict[str, Any],
    *,
    default_network: str,
    backend: GraphsenseBackend,
    max_concurrency: int = 8,
) -> list[str]:
    """Run backend-aware checks on a pathfinder spec.

    Returns a list of human-readable warning strings; empty when the
    spec checks out. Callers typically merge this with the output of
    :func:`graphsenselib.pathfinder.verify_structural` before emitting.

    The verifier issues at most ``max_concurrency`` backend requests in
    flight at once. Tune up for fast LANs; the default is conservative
    so a stray spec with a thousand entries doesn't drown the backend
    or trip a connection-pool cap (see the 2026-05-04 incident).

    Backend errors propagate — the verifier does NOT swallow them so
    the caller can decide whether a 5xx aborts the build or merely
    warns. The MCP build tool catches and downgrades to a single
    "verifier unavailable" warning so a flaky backend doesn't sink a
    structurally-valid .gs.
    """
    addresses: list[dict[str, Any]] = spec.get("addresses") or []
    txs: list[dict[str, Any]] = spec.get("txs") or []
    agg_edges: list[dict[str, Any]] = spec.get("agg_edges") or []

    semaphore = asyncio.Semaphore(max_concurrency)

    async def _check_address(addr: dict[str, Any]) -> tuple[str, bool]:
        net = _network_of(addr, default_network)
        async with semaphore:
            return addr["id"], await backend.address_exists(net, addr["id"])

    async def _fetch_tx(tx: dict[str, Any]) -> tuple[str, Optional[frozenset[str]]]:
        net = _network_of(tx, default_network)
        async with semaphore:
            return tx["id"], await backend.tx_addresses(net, tx["id"])

    addr_results, tx_results = await asyncio.gather(
        asyncio.gather(*(_check_address(a) for a in addresses)),
        asyncio.gather(*(_fetch_tx(t) for t in txs)),
    )

    warnings: list[str] = []

    missing_addresses = [a for a, exists in addr_results if not exists]
    if missing_addresses:
        warnings.append(
            "backend says these address(es) do not exist on their declared "
            f"network: {_truncate(missing_addresses)}. Either you have a "
            "typo, the network is wrong, or the address is unindexed."
        )

    tx_addresses_by_id: dict[str, frozenset[str]] = {}
    missing_txs: list[str] = []
    for tx_id, found in tx_results:
        if found is None:
            missing_txs.append(tx_id)
        else:
            tx_addresses_by_id[tx_id] = found
    if missing_txs:
        warnings.append(
            "backend says these tx hash(es) do not exist on their declared "
            f"network: {_truncate(missing_txs)}. Either you have a typo or "
            "the network is wrong."
        )

    # Endpoint-mediation check: for each (edge, tx) pair, both endpoints
    # of the edge should be in the set of addresses that participated in
    # the tx. If they're not, the edge claims a relationship the tx
    # itself doesn't mediate — the most damaging silent error mode.
    mediation_misses: list[str] = []
    for edge in agg_edges:
        a = edge.get("a")
        b = edge.get("b")
        for tid in edge.get("tx_ids") or []:
            participants = tx_addresses_by_id.get(tid)
            if participants is None:
                # tx not in our lookups: either user didn't list it (the
                # structural orphan-tx warning covers that) or the tx
                # doesn't exist on chain (the missing-tx warning above).
                # Either way we don't know what addresses it mediated, so
                # skip mediation check for it.
                continue
            offenders = [e for e in (a, b) if e not in participants]
            if offenders:
                mediation_misses.append(
                    f"{tid}: claims {a}↔{b} but tx involves "
                    f"{{{', '.join(sorted(participants))[:120]}}}"
                )
    if mediation_misses:
        warnings.append(
            "backend says these tx(s) don't actually mediate the a↔b "
            f"edge they're attached to: {_truncate(mediation_misses)}. "
            "Either the edge endpoints are wrong or the tx_id was attached "
            "to the wrong edge."
        )

    return warnings


# ----------------------------------------------------------------- adapters --


class RestBackend:
    """Adapter that exposes a graphsense REST endpoint as a verifier
    backend. Shares an httpx client lifecycle with the caller — i.e.
    create one inside the same ``async with httpx.AsyncClient(...)``
    block that the caller already manages.

    The endpoints used are the public graphsense REST paths
    (``/{network}/addresses/{address}``, ``/{network}/txs/{tx_id}``)
    so this adapter works against any deployment, in-process or
    out-of-process. ``tx_id`` is passed verbatim: the endpoint resolves
    account-model sub-payment identifiers natively, so no parsing into
    a separate ``token_tx_id`` query param is needed.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def address_exists(self, network: str, address: str) -> bool:
        path = f"/{network}/addresses/{address}"
        response = await self._client.get(path)
        if response.status_code == 404:
            return False
        if response.status_code >= 400:
            # Hand back to caller as an exception; verify_against_backend
            # lets backend errors propagate so the caller decides whether
            # to abort or downgrade.
            response.raise_for_status()
        return True

    async def tx_addresses(self, network: str, tx_id: str) -> Optional[frozenset[str]]:
        path = f"/{network}/txs/{tx_id}"
        # `include_io` / `include_nonstandard_io` are load-bearing for
        # UTXO chains: the response model has `inputs` / `outputs`
        # declared `Optional[...] = None` and they are EXCLUDED from
        # the response body when not requested. Without these flags,
        # _addresses_from_tx_body returns an empty set for every UTXO
        # tx, which downstream produces the misleading "tx involves
        # {}" mediation warning. Account-model bodies carry
        # from_address / to_address unconditionally, so the flags
        # are no-ops there.
        params = {"include_io": "true", "include_nonstandard_io": "true"}
        response = await self._client.get(path, params=params)
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            response.raise_for_status()
        body = response.json()
        return _addresses_from_tx_body(body)


def _addresses_from_tx_body(body: dict[str, Any]) -> frozenset[str]:
    """Extract every address that participated in a tx response body.

    UTXO bodies expose ``inputs`` / ``outputs`` with each ``TxValue``
    carrying an ``address`` list (multiple in non-standard scripts).
    Account-model bodies expose flat ``from_address`` / ``to_address``
    strings. We accept both shapes; any superset of addresses is fine
    — the verifier only does set-membership checks.
    """
    addrs: set[str] = set()
    for side in ("inputs", "outputs"):
        for entry in body.get(side) or []:
            value = entry.get("address") if isinstance(entry, dict) else None
            if isinstance(value, list):
                addrs.update(str(v) for v in value if v)
            elif isinstance(value, str):
                addrs.add(value)
    for key in ("from_address", "to_address", "sender", "receiver"):
        value = body.get(key)
        if isinstance(value, str) and value:
            addrs.add(value)
    return frozenset(addrs)
