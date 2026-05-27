"""Structural sanity checks for a pathfinder spec — pure stdlib, sync,
no backend required.

These checks catch in-spec inconsistencies that produce a syntactically
valid ``.gs`` file but a visually wrong / empty pathfinder graph. Run
this on any spec you're about to encode, regardless of whether you can
reach a backend — the warnings here are cheap and require no I/O.

For backend-aware checks (tx exists on chain, edge endpoints really did
participate in the tx, address really exists on the declared network),
see :func:`graphsenselib.pathfinder.verify_against_backend`.

Spec shape: a dict matching what
:func:`graphsenselib.convert.gs_files.builder_from_spec` accepts (see
the docstring there). Keys read by this module: ``addresses`` (list of
dicts each with at least ``id`` and optionally ``starting_point``),
``txs`` (list of dicts with at least ``id``), and ``agg_edges`` (list
of dicts with ``a``, ``b`` and optionally ``tx_ids``).
"""

from __future__ import annotations

from typing import Any, Iterable

# Cap how many unknown-ref ids we list in a single warning so a
# malformed spec can't bloat the response. The truncation suffix tells
# the agent how many more there were.
_WARNING_REF_LIMIT = 10


def _truncate(items: Iterable[str]) -> str:
    """Render up to _WARNING_REF_LIMIT identifiers with a `(+N more)`
    suffix when the list overflows. Used by every warning that lists
    offending ids so a malformed spec can't bloat the response."""
    seq = list(items)
    shown = seq[:_WARNING_REF_LIMIT]
    more = len(seq) - len(shown)
    suffix = f" (+{more} more)" if more > 0 else ""
    return ", ".join(shown) + suffix


def verify_structural(spec: dict[str, Any]) -> list[str]:
    """Return human-readable warnings about in-spec inconsistencies.

    The checks are warnings, not errors: sometimes the caller really
    does want an abstract relationship graph with no txs. But silently
    shipping a spec whose graph won't render (the historical failure
    mode) is worse than telling the caller up front.

    Specifically:

    * ``agg_edges`` present but ``txs`` empty — abstract edges only.
    * ``agg_edges`` present but some have no ``tx_ids`` — those edges
      won't be tx-mediated.
    * ``agg_edges.tx_ids`` references tx ids not in ``txs``.
    * ``agg_edges.a`` / ``b`` references addresses not in ``addresses``.
    * Orphan tx: listed in ``txs`` but never referenced from any
      ``agg_edges.tx_ids`` — would render as a floating node.
    * Stray address: listed in ``addresses`` but never used as an edge
      endpoint AND not declared ``starting_point=true``.
    """
    warnings: list[str] = []
    addresses: list[dict[str, Any]] = spec.get("addresses") or []
    txs: list[dict[str, Any]] = spec.get("txs") or []
    agg_edges: list[dict[str, Any]] = spec.get("agg_edges") or []

    address_ids = {a["id"] for a in addresses if "id" in a}
    tx_ids = {t["id"] for t in txs if "id" in t}

    if agg_edges and not txs:
        warnings.append(
            f"spec has {len(agg_edges)} agg_edge(s) but no txs were "
            "provided; pathfinder will show abstract address-to-address "
            "links only (no transactions render). Populate `txs` and "
            "reference the hashes from `agg_edges.tx_ids` to make "
            "transactions appear."
        )

    edges_without_tx_ids = sum(1 for e in agg_edges if not e.get("tx_ids"))
    if edges_without_tx_ids and txs:
        warnings.append(
            f"{edges_without_tx_ids} of {len(agg_edges)} agg_edge(s) "
            "have no tx_ids; those edges will render as abstract a↔b lines "
            "and the txs you provided will not be tied to them."
        )

    unknown_tx: list[str] = []
    seen_tx: set[str] = set()
    for e in agg_edges:
        for tid in e.get("tx_ids") or []:
            if tid not in tx_ids and tid not in seen_tx:
                unknown_tx.append(tid)
                seen_tx.add(tid)
    if unknown_tx:
        warnings.append(
            "agg_edge.tx_ids references tx hash(es) not in `txs`: "
            f"{_truncate(unknown_tx)}. Add them to `txs` or remove the "
            "references."
        )

    unknown_addr: list[str] = []
    seen_addr: set[str] = set()
    for e in agg_edges:
        for endpoint in (e.get("a"), e.get("b")):
            if (
                endpoint is not None
                and endpoint not in address_ids
                and endpoint not in seen_addr
            ):
                unknown_addr.append(endpoint)
                seen_addr.add(endpoint)
    if unknown_addr:
        warnings.append(
            "agg_edge endpoints reference address(es) not in `addresses`: "
            f"{_truncate(unknown_addr)}. Add them to `addresses` or fix the "
            "typo."
        )

    referenced_tx_ids: set[str] = {
        tid for e in agg_edges for tid in (e.get("tx_ids") or [])
    }
    orphan_txs = [
        t["id"] for t in txs if "id" in t and t["id"] not in referenced_tx_ids
    ]
    if orphan_txs:
        warnings.append(
            f"{len(orphan_txs)} tx(s) are not referenced from any agg_edge.tx_ids: "
            f"{_truncate(orphan_txs)}. These render as floating nodes with "
            "no source or destination address. For proper visualisation, add an "
            "`agg_edge` with the tx's `from`-address as `a` and `to`-address as "
            "`b` (at least one source and one destination per tx is strongly "
            "recommended; on ETH, edges with off-line tx positions can be "
            "silently dropped by the renderer)."
        )

    # Stray addresses: listed in `addresses` but not used as an edge
    # endpoint anywhere. They render as floating address nodes that no
    # tx and no relationship can ever reach — usually a leftover from
    # an exploratory step the caller forgot to wire up, or a typo in
    # an edge endpoint (which the unknown-address warning above flags
    # from the other side).
    #
    # Exception: a starting_point address with no edges yet is a
    # legitimate "anchor only" graph (used for very early drafts and
    # for the address-only example in the docstring), so don't flag
    # those — the caller meant to put them there.
    referenced_addrs: set[str] = {
        ep for e in agg_edges for ep in (e.get("a"), e.get("b")) if ep is not None
    }
    orphan_addrs = [
        a["id"]
        for a in addresses
        if "id" in a and a["id"] not in referenced_addrs and not a.get("starting_point")
    ]
    if orphan_addrs:
        warnings.append(
            f"{len(orphan_addrs)} address(es) are not referenced from any "
            f"agg_edge: {_truncate(orphan_addrs)}. These render as floating "
            "address nodes with no relationship to anything else in the graph. "
            "Either add an `agg_edge` that involves them, mark them with "
            "`starting_point=true` if they really are isolated anchors, or "
            "remove them from `addresses`."
        )

    return warnings
