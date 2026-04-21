from __future__ import annotations

import logging
from typing import Any, Literal, Optional

import httpx
from fastmcp.exceptions import ToolError

logger = logging.getLogger(__name__)

EntityKind = Literal["address", "entity", "cluster"]
_KIND_TO_PATH = {
    "address": "addresses",
    "entity": "entities",
    "cluster": "clusters",
}


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


def register_lookup_address(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_address-level", "gs_lookup"})
    async def lookup_address(
        currency: str,
        address: str,
        include_tags: bool = True,
        include_entity: bool = True,
        include_tag_summary: bool = True,
    ) -> dict[str, Any]:
        """Look up a cryptocurrency address and return a consolidated view:
        base details (balance, activity range), entity membership, tag summary,
        and the list of tags attached to the address. One call replaces four
        separate endpoint hits that LLMs otherwise chain.

        Args:
            currency: Network identifier (e.g. "btc", "eth").
            address: Address to look up.
            include_tags: Include the list of tags attached to the address.
            include_entity: Include the entity/cluster the address belongs to.
            include_tag_summary: Include the aggregated tag_summary.

        Returns:
            A dict with keys: "address" (always), "entity" (if requested),
            "tag_summary" (if requested), "tags" (if requested).
        """
        client = _make_client(app)
        async with client:
            base = await _get_json(client, f"/{currency}/addresses/{address}")
            result: dict[str, Any] = {"address": base}
            if include_entity:
                result["entity"] = await _get_json(
                    client, f"/{currency}/addresses/{address}/entity"
                )
            if include_tag_summary:
                result["tag_summary"] = await _get_json(
                    client, f"/{currency}/addresses/{address}/tag_summary"
                )
            if include_tags:
                result["tags"] = await _get_json(
                    client, f"/{currency}/addresses/{address}/tags"
                )
        return result


def register_lookup_entity(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_entity-level", "gs_lookup"})
    async def lookup_entity(
        currency: str,
        entity: int,
        include_tags: bool = True,
    ) -> dict[str, Any]:
        """Look up an entity (address cluster) and return a consolidated view:
        base details plus the tags attached to any of its addresses.

        Args:
            currency: Network identifier (e.g. "btc").
            entity: Numeric entity id.
            include_tags: Include aggregated address tags for the entity.

        Returns:
            A dict with keys: "entity" (always), "tags" (if requested).
        """
        client = _make_client(app)
        async with client:
            base = await _get_json(client, f"/{currency}/entities/{entity}")
            result: dict[str, Any] = {"entity": base}
            if include_tags:
                result["tags"] = await _get_json(
                    client, f"/{currency}/entities/{entity}/tags"
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
        kind: EntityKind,
        id: str,
        direction: Literal["in", "out"] = "out",
        pagesize: Optional[int] = None,
        page: Optional[str] = None,
        only_ids: Optional[list[str]] = None,
        include_labels: Optional[bool] = None,
        include_actors: Optional[bool] = None,
    ) -> dict[str, Any]:
        """List neighbors of an address, entity, or cluster in a network.

        A single tool replaces three near-identical endpoints. Use `kind` to
        choose the level of aggregation.

        Args:
            currency: Network identifier (e.g. "btc").
            kind: "address", "entity", or "cluster".
            id: The address string, entity id, or cluster id.
            direction: "in" (incoming) or "out" (outgoing).
            pagesize: Results per page.
            page: Pagination token from a previous response.
            only_ids: Limit to specific neighbor ids.
            include_labels: Include labels on each neighbor.
            include_actors: Include actor metadata on each neighbor.

        Returns:
            A dict with the neighbor list and pagination cursor.
        """
        kind_segment = _KIND_TO_PATH[kind]
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
            return await _get_json(
                client, f"/{currency}/{kind_segment}/{id}/neighbors", params=params
            )


def register_lookup_tx_io(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_transaction-level", "gs_utxo"})
    async def lookup_tx_io(
        currency: str,
        tx_hash: str,
        include_upstream: bool = False,
        include_downstream: bool = False,
    ) -> dict[str, Any]:
        """Inspect a UTXO transaction with optional upstream/downstream context.

        Always returns the transaction's inputs and outputs. Optionally
        returns:

        - `upstream`: for each INPUT of this tx, the earlier tx whose output
          funded it. Useful for **backward** tracing ("where did the money
          come from?"). Each entry is
          `{"tx_hash": str, "input_index": int, "output_index": int}` and
          reads literally as "our input [input_index] was produced by
          [tx_hash]'s output [output_index]".
        - `downstream`: for each OUTPUT of this tx, the later tx that
          consumed it. Useful for **forward** tracing ("where did the money
          go next?"). Each entry reads as "our output [output_index] was
          consumed by [tx_hash]'s input [input_index]". An output that
          hasn't been spent yet simply doesn't appear in the list.

        For account-model chains (ETH etc.) use list_tx_flows instead — this
        tool is for UTXO networks.

        Note: this consolidation hides the underlying graphsense endpoint
        names (/spending = upstream, /spent_in = downstream), whose naming
        is counter-intuitive. The kwarg names here use the
        source-of-the-direction semantics to avoid that pitfall.

        Args:
            currency: Network identifier (e.g. "btc", "bch", "ltc").
            tx_hash: Transaction hash.
            include_upstream: Backward trace — where our inputs came from.
            include_downstream: Forward trace — where our outputs went next.

        Returns:
            A dict with keys: "inputs", "outputs", plus "upstream" and/or
            "downstream" when the corresponding include_* flag is true.
        """
        client = _make_client(app)
        async with client:
            result: dict[str, Any] = {
                "inputs": await _get_json(client, f"/{currency}/txs/{tx_hash}/inputs"),
                "outputs": await _get_json(
                    client, f"/{currency}/txs/{tx_hash}/outputs"
                ),
            }
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
        return result


def register_list_txs_for(mcp, app, stack) -> None:
    @mcp.tool(tags={"gs_transaction-level"})
    async def list_txs_for(
        currency: str,
        kind: EntityKind,
        id: str,
        direction: Optional[Literal["in", "out"]] = None,
        pagesize: Optional[int] = None,
        page: Optional[str] = None,
        min_height: Optional[int] = None,
        max_height: Optional[int] = None,
        order: Optional[Literal["asc", "desc"]] = None,
        token_currency: Optional[str] = None,
    ) -> dict[str, Any]:
        """List transactions involving an address, entity, or cluster.

        A single tool replaces three near-identical endpoints. Use `kind` to
        choose the level of aggregation.

        Args:
            currency: Network identifier.
            kind: "address", "entity", or "cluster".
            id: The address string, entity id, or cluster id.
            direction: "in" / "out" / None to include both.
            pagesize: Results per page.
            page: Pagination token from a previous response.
            min_height: Only include transactions at or above this block height.
            max_height: Only include transactions at or below this block height.
            order: "asc" or "desc" by block height.
            token_currency: Filter to a specific token (e.g. "usdt").

        Returns:
            A dict with the tx list and pagination cursor.
        """
        kind_segment = _KIND_TO_PATH[kind]
        params = _params_from(
            direction,
            pagesize,
            page,
            min_height=min_height,
            max_height=max_height,
            order=order,
            token_currency=token_currency,
        )
        client = _make_client(app)
        async with client:
            return await _get_json(
                client, f"/{currency}/{kind_segment}/{id}/txs", params=params
            )
