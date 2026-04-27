from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Literal, Optional

import httpx
from fastmcp.exceptions import ToolError

logger = logging.getLogger(__name__)

# Regex guards on user-controlled path segments. httpx does not URL-encode
# path components, so an unvalidated segment with '/' or '..' could escape
# the intended endpoint — even though FastAPI routing would likely reject
# it downstream. These patterns are deliberately conservative.
_CURRENCY_PATTERN = re.compile(r"^[a-z0-9]{2,10}$")
_ID_PATTERN = re.compile(r"^[a-zA-Z0-9]{1,150}$")


def _validate_currency(currency: str) -> None:
    if not _CURRENCY_PATTERN.match(currency):
        raise ToolError(f"Invalid currency identifier: {currency!r}")


def _validate_id(name: str, value: str) -> None:
    """Guard addresses, tx hashes, and opaque ids passed into URL segments."""
    if not _ID_PATTERN.match(value):
        raise ToolError(f"Invalid {name}: {value!r}")


def _make_client(app) -> httpx.AsyncClient:
    """Build an httpx client that dispatches to the FastAPI app in-process."""
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://graphsense-mcp",
    )


async def _get_json(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    response = await client.get(path, params=params)
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = response.text
        raise ToolError(
            f"Graphsense API returned HTTP {response.status_code} for {path}: {detail}"
        )
    return response.json()


async def _get_json_optional(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    """Like _get_json, but returns None on 404 instead of raising.

    Use when the absence of a resource is a valid, expected outcome —
    e.g. an address that has no cluster association shouldn't fail the
    whole lookup_address call.
    """
    response = await client.get(path, params=params)
    if response.status_code == 404:
        return None
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = response.text
        raise ToolError(
            f"Graphsense API returned HTTP {response.status_code} for {path}: {detail}"
        )
    return response.json()


def _slim(obj: Any) -> Any:
    """Recursively flatten graphsense's `{fiat_values: [{code, value}...], value}`
    money objects into `{native: N, eur: V, usd: V, ...}`.

    Saves roughly 40% of tokens on responses dominated by fiat conversions
    (balance, totals, neighbor edges, tx values) by eliminating the repeated
    `code` / `value` keys and the array wrapper.
    """
    if isinstance(obj, dict):
        # Money object detection: has both fiat_values (list) and value (native)
        fv = obj.get("fiat_values")
        if (
            isinstance(fv, list)
            and "value" in obj
            and all(isinstance(e, dict) and "code" in e and "value" in e for e in fv)
        ):
            flat: dict[str, Any] = {"native": obj["value"]}
            for entry in fv:
                flat[entry["code"]] = entry["value"]
            # Preserve any extra keys alongside (rare but possible)
            for k, v in obj.items():
                if k not in {"fiat_values", "value"}:
                    flat[k] = _slim(v)
            return flat
        return {k: _slim(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_slim(x) for x in obj]
    return obj


# Legacy quick-aggregate fields the underlying REST surface still emits but
# that we deliberately hide from the MCP shape. They predate `tag_summary`
# and overlap with it (and with each other), which is exactly the kind of
# thing the LLM gets confused by. The single source of truth for tag-derived
# data is `tag_summary` (high-level) and `list_tags_by_address` (raw list).
_LEGACY_ADDRESS_FIELDS = frozenset({"actors"})
_LEGACY_CLUSTER_FIELDS = frozenset({"actors", "best_address_tag"})
_LEGACY_NEIGHBOR_FIELDS = frozenset({"labels"})


def _strip_address_legacy(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if k not in _LEGACY_ADDRESS_FIELDS}


def _strip_cluster_legacy(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if k not in _LEGACY_CLUSTER_FIELDS}


def _strip_neighbor_legacy(neighbor: dict[str, Any]) -> dict[str, Any]:
    cleaned = {k: v for k, v in neighbor.items() if k not in _LEGACY_NEIGHBOR_FIELDS}
    nested = cleaned.get("address")
    if isinstance(nested, dict):
        cleaned["address"] = _strip_address_legacy(nested)
    return cleaned


def _compact_neighbor(neighbor: dict[str, Any]) -> dict[str, Any]:
    """Flatten a neighbor row for counterparty scans: replace the nested
    `address` dict with the address string itself, dropping the
    balance/totals/activity-counts block (heavy, rarely useful when
    scanning counterparties). The `tag_summary`, `value`, `no_txs`,
    and `token_values` fields stay at top level.
    """
    nested = neighbor.get("address")
    a = nested.get("address") if isinstance(nested, dict) else nested
    cleaned = {k: v for k, v in neighbor.items() if k != "address"}
    if isinstance(a, str):
        cleaned["address"] = a
    return cleaned


def _matches_tag_filter(tag_summary: Optional[dict[str, Any]], needle: str) -> bool:
    """Case-insensitive substring match against the LLM-relevant fields of
    a slim tag_summary: best_actor, best_label, broad_category, every
    label-dict key, and every concept-dict key. Used to pre-filter
    `list_neighbors` results so the LLM doesn't have to iterate.
    """
    if not isinstance(tag_summary, dict):
        return False
    needle_lower = needle.lower()
    for field in ("best_actor", "best_label", "broad_category"):
        v = tag_summary.get(field)
        if isinstance(v, str) and needle_lower in v.lower():
            return True
    labels = tag_summary.get("labels")
    if isinstance(labels, dict):
        for label in labels:
            if isinstance(label, str) and needle_lower in label.lower():
                return True
    concepts = tag_summary.get("concepts")
    if isinstance(concepts, dict):
        for concept in concepts:
            if isinstance(concept, str) and needle_lower in concept.lower():
                return True
    return False


# Per-label fields kept in the slim tag_summary. Provenance/audit fields
# (creators, lastmod, inherited_from) and per-label `concepts` (already
# aggregated in the top-level concept cloud) are dropped. `sources` stays —
# it's the LLM's only path to attribute claims to a tagpack.
_LABEL_KEEP_FIELDS = frozenset({"count", "confidence", "relevance", "sources"})


def _slim_tag_summary(ts: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Reshape graphsense's TagSummary into a leaner LLM-friendly form.

    Keeps the analysis-relevant fields (counts, best label/actor, broad
    category, per-label confidence/relevance/sources, concept weights)
    and drops the provenance/audit ones (creators, lastmod,
    inherited_from). The concept_tag_cloud's `{cnt, weighted}` pair is
    flattened to a label→weight map. Renames `label_summary`→`labels`
    and `concept_tag_cloud`→`concepts` for compactness.

    Returns None when the input isn't a dict (404 on the tag_summary
    endpoint surfaces here as None).
    """
    if not isinstance(ts, dict):
        return None
    out: dict[str, Any] = {
        "tag_count": ts.get("tag_count"),
        "broad_category": ts.get("broad_category"),
        "best_label": ts.get("best_label"),
        "best_actor": ts.get("best_actor"),
    }
    if ts.get("tag_count_indirect") is not None:
        out["tag_count_indirect"] = ts["tag_count_indirect"]

    label_summary = ts.get("label_summary")
    if isinstance(label_summary, dict) and label_summary:
        out["labels"] = {
            label: {k: v for k, v in entry.items() if k in _LABEL_KEEP_FIELDS}
            for label, entry in label_summary.items()
            if isinstance(entry, dict)
        }

    cloud = ts.get("concept_tag_cloud")
    if isinstance(cloud, dict) and cloud:
        out["concepts"] = {
            concept: (entry.get("weighted") if isinstance(entry, dict) else entry)
            for concept, entry in cloud.items()
        }

    return out


def register_lookup_address(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_address-level", "gs_lookup"})
    async def lookup_address(
        currency: str,
        address: str,
        include_cluster: bool = True,
        include_tag_summary: bool = True,
        include_cross_chain_addresses: bool = False,
    ) -> dict[str, Any]:
        """Look up a cryptocurrency address and return a consolidated view:
        base details (balance, activity range), cluster membership, and the
        aggregated tag summary. One call replaces several separate endpoint
        hits.

        For raw, lower-confidence per-tag detail (the long list), use the
        separate `list_tags_by_address` tool — `tag_summary` carries the
        confidence-weighted aggregate that should drive any conclusion
        about identity, category, or actor.

        Args:
            currency: Network identifier (e.g. "btc", "eth").
            address: Address to look up.
            include_cluster: Include the cluster the address belongs to.
            include_tag_summary: Include the aggregated tag_summary.
            include_cross_chain_addresses: Include addresses derived from the
                same public key on OTHER chains (e.g. same pubkey exposes a
                BTC address and its BCH/LTC/... counterparts).

        Returns:
            A dict with keys: "address" (always), "cluster", "tag_summary",
            and "cross_chain_addresses" — populated when their corresponding
            include_* flag is True (cluster only present when one exists).
        """
        _validate_currency(currency)
        _validate_id("address", address)
        client = _make_client(app)
        async with client:
            base = await _get_json(client, f"/{currency}/addresses/{address}")
            result: dict[str, Any] = {
                "address": _slim(_strip_address_legacy(base)),
            }
            if include_cluster:
                # Some addresses have no cluster association (freshly indexed,
                # some non-UTXO cases) — a 404 here must not fail the whole
                # call, so we use the 404-tolerant variant.
                cluster_body = await _get_json_optional(
                    client, f"/{currency}/addresses/{address}/entity"
                )
                if cluster_body is not None:
                    result["cluster"] = _slim(_strip_cluster_legacy(cluster_body))
            if include_tag_summary:
                # `include_best_cluster_tag=true` matches what the UI sends:
                # when the address itself has no direct tag, the cluster's
                # best tag is folded into the digest, surfacing useful
                # cluster-derived attribution. The MCP server's `instructions`
                # remind the LLM to qualify cluster-derived claims.
                ts = await _get_json(
                    client,
                    f"/{currency}/addresses/{address}/tag_summary",
                    params={"include_best_cluster_tag": "true"},
                )
                result["tag_summary"] = _slim_tag_summary(ts)
            if include_cross_chain_addresses:
                related = await _get_json(
                    client,
                    f"/{currency}/addresses/{address}/related_addresses",
                    params={"address_relation_type": "pubkey"},
                )
                result["cross_chain_addresses"] = _slim(related)
        return result


def register_lookup_cluster(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_cluster-level", "gs_lookup"})
    async def lookup_cluster(
        currency: str,
        cluster: int,
    ) -> dict[str, Any]:
        """Look up an address cluster: balances, activity range, root
        address, and member counts.

        Tag context is intentionally NOT included on cluster lookups —
        tag attribution at cluster scale is a heuristic stack on top of
        the address-clustering heuristic, which compounds the error rate.
        Use `lookup_address` (or `list_tags_by_address`) on a member
        address for tag context, and present any cluster-level
        attribution as a hint, not a conclusion.

        Args:
            currency: Network identifier (e.g. "btc").
            cluster: Numeric cluster id.

        Returns:
            A dict with a single "cluster" key carrying the slimmed body.
        """
        _validate_currency(currency)
        # `cluster` is typed as int by pydantic, so no path-injection risk.
        client = _make_client(app)
        async with client:
            base = await _get_json(client, f"/{currency}/clusters/{cluster}")
        return {"cluster": _slim(_strip_cluster_legacy(base))}


def _params_from(
    direction: Optional[str],
    pagesize: Optional[int],
    page: Optional[str],
    **extra: Any,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if direction is not None:
        params["direction"] = direction
    if pagesize is not None:
        params["pagesize"] = pagesize
    if page is not None:
        params["page"] = page
    for k, v in extra.items():
        if v is not None:
            params[k] = v
    return params


def register_list_neighbors(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_neighbors"})
    async def list_neighbors(
        currency: str,
        address: str,
        direction: Literal["in", "out"] = "out",
        pagesize: Optional[int] = None,
        page: Optional[str] = None,
        only_ids: Optional[list[str]] = None,
        include_tag_summary: bool = True,
        compact: bool = True,
        tag_filter: Optional[str] = None,
    ) -> dict[str, Any]:
        """List neighbors of an address in a network.

        Address-level only: cluster-level neighbors are deliberately not
        exposed. Follow counterparty graphs at the address level — that's
        on-chain fact, whereas cluster edges are inference stacked on top.

        Default shape (compact=True): each row is `{address, value,
        no_txs, tag_summary}` — the address is a bare string and the
        nested balance / totals / activity-counts block is dropped. Set
        `compact=False` when you actually need that block (rare for a
        counterparty scan).

        When `include_tag_summary=True` (default), each row is enriched
        with the address-level `tag_summary` via a per-neighbor lookup.
        That's one extra in-process call per neighbor — pair with a
        modest `pagesize` (start at 20–30 when shape is unknown).

        `tag_filter` runs a case-insensitive substring match against
        `best_actor`, `best_label`, `broad_category`, every key in the
        per-neighbor `labels` dict, and every key in `concepts`. Passing
        it forces `include_tag_summary=True` regardless of the flag, then
        filters in-memory before returning. Pagination still happens at
        the upstream layer, so a filtered page may contain fewer rows
        than `pagesize` (or zero) — keep walking `next_page`.

        For raw per-tag detail (provenance, tagpack info, lower-confidence
        leads) on a specific address, call `list_tags_by_address`.

        Args:
            currency: Network identifier (e.g. "btc").
            address: The address to list neighbors for.
            direction: "in" (incoming) or "out" (outgoing).
            pagesize: Results per page.
            page: Pagination token from a previous response.
            only_ids: Limit to specific neighbor ids.
            include_tag_summary: Enrich each neighbor with its `tag_summary`.
            compact: Return a flattened, lean row shape (default True).
            tag_filter: Case-insensitive substring; keep only rows whose
                tag_summary names/categories match.

        Returns:
            A dict with the neighbor list and pagination cursor. In
            compact mode each row is `{address, value, no_txs,
            tag_summary?, token_values?}`; in non-compact mode `address`
            is the full nested dict.
        """
        _validate_currency(currency)
        _validate_id("address", address)
        # Suppress the legacy quick-aggregate fields at source. The MCP
        # contract exposes tag context only via `tag_summary`.
        params = _params_from(
            direction,
            pagesize,
            page,
            only_ids=only_ids,
            include_actors=False,
            include_labels=False,
        )
        # tag_filter implies tag_summary — we need the data to match against.
        if tag_filter is not None:
            include_tag_summary = True
        client = _make_client(app)
        async with client:
            body = await _get_json(
                client,
                f"/{currency}/addresses/{address}/neighbors",
                params=params,
            )
            neighbors = body.get("neighbors") or []
            if include_tag_summary and neighbors:

                async def _ts_for(n: dict[str, Any]) -> Optional[dict[str, Any]]:
                    nested = n.get("address")
                    if not isinstance(nested, dict):
                        return None
                    a = nested.get("address")
                    if not isinstance(a, str) or not _ID_PATTERN.match(a):
                        return None
                    raw = await _get_json_optional(
                        client,
                        f"/{currency}/addresses/{a}/tag_summary",
                        params={"include_best_cluster_tag": "true"},
                    )
                    return _slim_tag_summary(raw)

                summaries = await asyncio.gather(*[_ts_for(n) for n in neighbors])
                for n, ts in zip(neighbors, summaries):
                    if ts is not None:
                        n["tag_summary"] = ts
            if tag_filter is not None:
                neighbors = [
                    n
                    for n in neighbors
                    if _matches_tag_filter(n.get("tag_summary"), tag_filter)
                ]
            cleaned = [_strip_neighbor_legacy(n) for n in neighbors]
            if compact:
                cleaned = [_compact_neighbor(n) for n in cleaned]
            body["neighbors"] = cleaned
        return _slim(body)


def register_lookup_tx_details(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_transaction-level"})
    async def lookup_tx_details(
        currency: str,
        tx_hash: str,
        include_upstream: bool = False,
        include_downstream: bool = False,
        include_heuristics: bool = False,
        include_conversions: bool = False,
    ) -> dict[str, Any]:
        """Fetch a transaction with full IO detail and optional trace context.

        One call replaces get_tx, get_tx_io, get_spending_txs,
        get_spent_in_txs, and get_tx_conversions. The tx body is retrieved
        with io + nonstandard io + io indices always enabled, so UTXO txs
        come back with a complete inputs/outputs list (including
        non-standard scripts and their positional indices) and
        account-model txs come back with the usual sender/receiver/value
        fields.

        Optional add-ons:

        - `include_upstream=True` → appends an `upstream` list: for each
          INPUT of this tx, the earlier tx whose output funded it. Backward
          tracing ("where did the money come from?"). Each entry
          `{"tx_hash": str, "input_index": int, "output_index": int}` reads
          as "our input [input_index] was produced by [tx_hash]'s output
          [output_index]".
        - `include_downstream=True` → appends a `downstream` list: for each
          OUTPUT of this tx, the later tx that consumed it. Forward tracing
          ("where did the money go next?"). Outputs that haven't been spent
          yet simply don't appear.
        - `include_heuristics=True` → asks graphsense to compute all
          supported UTXO heuristics (change-address detection, CoinJoin
          identification — wasabi/whirlpool/joinmarket variants). The
          results are embedded in the returned tx body under
          implementation-specific fields. UTXO-chain only; a no-op on
          account-model chains.
        - `include_conversions=True` → appends a `conversions` list:
          "conversion" is graphsense's internal term for any cross-asset
          movement within a single tx. It covers BOTH DEX swaps (token A
          → token B on the same chain) and bridge transactions (asset X
          on chain A → asset Y on chain B). The returned entries share one
          schema (`conversion_type: "dex_swap" | "bridge_tx"`, `from_address`,
          `to_address`, `from_asset`, `to_asset`, `from_amount`, …) so the
          LLM doesn't need to branch on the subtype to reason about the
          cross-asset edge.

        Note on `spending` vs `spent_in`: the underlying graphsense
        endpoints `/spending` and `/spent_in` are NAMED counter-intuitively
        (/spending is backward, /spent_in is forward). This consolidation
        hides that and uses `upstream` / `downstream` to mean what they say.

        Args:
            currency: Network identifier (e.g. "btc", "bch", "ltc", "eth").
            tx_hash: Transaction hash.
            include_upstream: Backward trace — where our inputs came from.
            include_downstream: Forward trace — where our outputs went next.
            include_heuristics: Compute all supported UTXO heuristics.
            include_conversions: DEX swaps and bridge transactions in this
                tx, unified under one schema.

        Returns:
            The full tx body (always includes io, nonstandard io, and io
            indices) with optional top-level `upstream`, `downstream`, and
            `conversions` keys.
        """
        _validate_currency(currency)
        _validate_id("tx_hash", tx_hash)
        params: dict[str, Any] = {
            "include_io": True,
            "include_nonstandard_io": True,
            "include_io_index": True,
        }
        if include_heuristics:
            params["include_heuristics"] = ["all"]

        client = _make_client(app)
        async with client:
            result: dict[str, Any] = _slim(
                await _get_json(client, f"/{currency}/txs/{tx_hash}", params=params)
            )
            if include_upstream:
                # graphsense endpoint /spending returns the BACKWARD trace
                # (the txs that produced our inputs), despite its name.
                result["upstream"] = await _get_json(
                    client, f"/{currency}/txs/{tx_hash}/spending"
                )
            if include_downstream:
                # graphsense endpoint /spent_in returns the FORWARD trace
                # (the txs that consumed our outputs).
                result["downstream"] = await _get_json(
                    client, f"/{currency}/txs/{tx_hash}/spent_in"
                )
            if include_conversions:
                result["conversions"] = _slim(
                    await _get_json(client, f"/{currency}/txs/{tx_hash}/conversions")
                )
        return result


def register_list_txs_for(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_transaction-level"})
    async def list_txs_for(
        currency: str,
        address: str,
        neighbor: Optional[str] = None,
        direction: Optional[Literal["in", "out"]] = None,
        pagesize: Optional[int] = None,
        page: Optional[str] = None,
        min_height: Optional[int] = None,
        max_height: Optional[int] = None,
        order: Optional[Literal["asc", "desc"]] = None,
        token_currency: Optional[str] = None,
    ) -> dict[str, Any]:
        """List transactions involving an address.

        When `neighbor` is set, lists transactions between `address` and
        `neighbor` (any direction — both "sent to" and "received from");
        otherwise lists all transactions of `address`.

        Args:
            currency: Network identifier.
            address: The address to list transactions for.
            neighbor: Optional counterparty address. When set, narrows the
                response to transactions between `address` and this
                counterparty. The underlying links endpoint does not
                accept a `direction` filter, so the two cannot be
                combined — inspect each item's flow to tell inbound from
                outbound.
            direction: "in" / "out" / None to include both. Ignored when
                `neighbor` is set (raises on combination).
            pagesize: Results per page.
            page: Pagination token from a previous response.
            min_height: Only include transactions at or above this block height.
            max_height: Only include transactions at or below this block height.
            order: "asc" or "desc" by block height.
            token_currency: Filter to a specific token (e.g. "usdt").

        Returns:
            A dict with `address_txs` (or `links` when `neighbor` is set)
            and a `next_page` pagination cursor.
        """
        _validate_currency(currency)
        _validate_id("address", address)

        if neighbor is not None:
            _validate_id("neighbor", neighbor)
            if direction is not None:
                raise ToolError(
                    "direction cannot be combined with neighbor: the links "
                    "endpoint has no direction filter."
                )
            params = _params_from(
                None,
                pagesize,
                page,
                neighbor=neighbor,
                min_height=min_height,
                max_height=max_height,
                order=order,
                token_currency=token_currency,
            )
            path = f"/{currency}/addresses/{address}/links"
        else:
            params = _params_from(
                direction,
                pagesize,
                page,
                min_height=min_height,
                max_height=max_height,
                order=order,
                token_currency=token_currency,
            )
            path = f"/{currency}/addresses/{address}/txs"

        client = _make_client(app)
        async with client:
            return _slim(await _get_json(client, path, params=params))


def register_list_tags_by_address(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_address-level", "gs_tags"})
    async def list_tags_by_address(
        currency: str,
        address: str,
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
    ) -> dict[str, Any]:
        """Paginated raw attribution tags for a single address.

        Use this for surfacing lower-confidence leads or per-tag
        provenance (tagpack, sources). The aggregated, confidence-weighted
        view is `tag_summary` (returned by `lookup_address`); reach for
        the raw list only when you need per-tag detail.

        The wrapper always sends `include_best_cluster_tag=true` to the
        upstream (UI parity): when the address has no direct tag, the
        cluster's best-confidence tag is appended to the last page so
        cluster-derived attribution still surfaces. Qualify any claim
        drawn from a cluster-derived tag (see server `instructions`).

        Args:
            currency: Network identifier (e.g. "btc", "eth").
            address: Address to list tags for.
            page: Pagination token from a previous response.
            pagesize: Results per page.

        Returns:
            A dict with `address_tags` and a `next_page` cursor.
        """
        _validate_currency(currency)
        _validate_id("address", address)
        params = _params_from(
            None,
            pagesize,
            page,
            include_best_cluster_tag="true",
        )
        client = _make_client(app)
        async with client:
            return _slim(
                await _get_json(
                    client,
                    f"/{currency}/addresses/{address}/tags",
                    params=params,
                )
            )
