from __future__ import annotations

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


def _best_cluster_tag(
    cluster_body: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Extract the graphsense `best_address_tag` field from a cluster response.

    Graphsense's cluster endpoint returns this field by default when the
    cluster has any tag attribution. We surface it at the top level of
    lookup_address / lookup_cluster responses (renamed to `best_cluster_tag`
    for clarity) regardless of which include flags the caller set, because
    it's the single most useful datum when orienting on an unknown address.
    """
    if not isinstance(cluster_body, dict):
        return None
    tag = cluster_body.get("best_address_tag")
    return tag if isinstance(tag, dict) else None


def register_lookup_address(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_address-level", "gs_lookup"})
    async def lookup_address(
        currency: str,
        address: str,
        include_tags: bool = True,
        include_cluster: bool = True,
        include_tag_summary: bool = True,
        include_cross_chain_addresses: bool = False,
    ) -> dict[str, Any]:
        """Look up a cryptocurrency address and return a consolidated view:
        base details (balance, activity range), cluster membership, the
        cluster's best tag, tag summary, and the address's own tags. One
        call replaces four or more separate endpoint hits that LLMs would
        otherwise chain.

        The `best_cluster_tag` field at the top level is always populated
        when the cluster has any tag attribution — it's the single most
        useful datum for orienting on an unknown address, so we surface it
        unconditionally rather than burying it under an include flag.

        Args:
            currency: Network identifier (e.g. "btc", "eth").
            address: Address to look up.
            include_tags: Include the list of tags attached to the address.
            include_cluster: Include the cluster the address belongs to.
            include_tag_summary: Include the aggregated tag_summary.
            include_cross_chain_addresses: Include addresses derived from the
                same public key on OTHER chains (e.g. same pubkey exposes a
                BTC address and its BCH/LTC/... counterparts).

        Returns:
            A dict with keys: "address" (always), "best_cluster_tag" (always —
            may be null if no tag exists), "cluster", "tag_summary", "tags",
            and "cross_chain_addresses" — each populated when the
            corresponding include_* flag is True.
        """
        _validate_currency(currency)
        _validate_id("address", address)
        client = _make_client(app)
        async with client:
            base = await _get_json(client, f"/{currency}/addresses/{address}")
            # We always fetch the cluster body so best_cluster_tag can be
            # surfaced at top level regardless of include_cluster. However,
            # some addresses have no cluster association (freshly indexed,
            # some non-UTXO cases) — a 404 there must not fail the whole
            # lookup_address call, so we use the 404-tolerant variant.
            cluster_body = await _get_json_optional(
                client, f"/{currency}/addresses/{address}/entity"
            )
            result: dict[str, Any] = {
                "address": _slim(base),
                "best_cluster_tag": _slim(_best_cluster_tag(cluster_body)),
            }
            if include_cluster and cluster_body is not None:
                result["cluster"] = _slim(cluster_body)
            if include_tag_summary:
                result["tag_summary"] = _slim(
                    await _get_json(
                        client, f"/{currency}/addresses/{address}/tag_summary"
                    )
                )
            if include_tags:
                result["tags"] = _slim(
                    await _get_json(client, f"/{currency}/addresses/{address}/tags")
                )
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
        include_tags: bool = True,
    ) -> dict[str, Any]:
        """Look up an address cluster and return a consolidated view: base
        details (balance, activity range, root address, best tag) plus the
        aggregated address tags attached to any of its addresses.

        The `best_cluster_tag` field at the top level mirrors the
        `cluster.best_address_tag` field for convenience — it's the single
        most useful datum for orienting on an unknown cluster.

        Args:
            currency: Network identifier (e.g. "btc").
            cluster: Numeric cluster id.
            include_tags: Include aggregated address tags for the cluster.

        Returns:
            A dict with keys: "cluster" (always), "best_cluster_tag" (always —
            may be null if no tag exists), "tags" (when requested).
        """
        _validate_currency(currency)
        # `cluster` is typed as int by pydantic, so no path-injection risk.
        client = _make_client(app)
        async with client:
            base = await _get_json(client, f"/{currency}/clusters/{cluster}")
            result: dict[str, Any] = {
                "cluster": _slim(base),
                "best_cluster_tag": _slim(_best_cluster_tag(base)),
            }
            if include_tags:
                result["tags"] = _slim(
                    await _get_json(client, f"/{currency}/clusters/{cluster}/tags")
                )
        return result


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
        include_labels: Optional[bool] = None,
        include_actors: Optional[bool] = None,
    ) -> dict[str, Any]:
        """List neighbors of an address in a network.

        Address-level only: cluster-level neighbors are deliberately not
        exposed. Follow counterparty graphs at the address level — that's
        on-chain fact, whereas cluster edges are inference stacked on top.

        Args:
            currency: Network identifier (e.g. "btc").
            address: The address to list neighbors for.
            direction: "in" (incoming) or "out" (outgoing).
            pagesize: Results per page.
            page: Pagination token from a previous response.
            only_ids: Limit to specific neighbor ids.
            include_labels: Include labels on each neighbor.
            include_actors: Include actor metadata on each neighbor.

        Returns:
            A dict with the neighbor list and pagination cursor.
        """
        _validate_currency(currency)
        _validate_id("address", address)
        params = _params_from(
            direction,
            pagesize,
            page,
            only_ids=only_ids,
            include_labels=include_labels,
            include_actors=include_actors,
        )
        client = _make_client(app)
        async with client:
            return _slim(
                await _get_json(
                    client,
                    f"/{currency}/addresses/{address}/neighbors",
                    params=params,
                )
            )


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
