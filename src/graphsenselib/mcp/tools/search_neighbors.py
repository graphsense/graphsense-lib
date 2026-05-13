from __future__ import annotations

import asyncio
import logging
import os
import re
from contextlib import AsyncExitStack
from typing import Any, Literal, Optional

import httpx
from fastmcp.exceptions import ToolError

from graphsenselib.mcp.config import SearchNeighborsConfig

logger = logging.getLogger(__name__)

TERMINAL_STATES = {"done", "timeout", "error"}
NETWORK_PATTERN = re.compile(r"^[a-z]{2,10}$")
TASK_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_network(network: str) -> None:
    if not NETWORK_PATTERN.match(network):
        raise ToolError(f"Invalid network identifier: {network!r}")


def _validate_task_id(task_id: str) -> None:
    if not TASK_ID_PATTERN.match(task_id):
        raise ToolError(f"Invalid task ID format: {task_id!r}")


class SearchNeighborsClient:
    def __init__(self, config: SearchNeighborsConfig) -> None:
        headers = {"Content-Type": "application/json"}
        if config.api_key_env:
            api_key = os.environ.get(config.api_key_env)
            if api_key:
                headers[config.auth_header] = api_key
            else:
                logger.warning(
                    "SEARCH_NEIGHBORS api_key_env=%r is set but the variable "
                    "is empty — talking to upstream without auth.",
                    config.api_key_env,
                )
        self._config = config
        self._client = httpx.AsyncClient(
            base_url=config.base_url,
            headers=headers,
            timeout=config.timeout_s,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def start_search(self, network: str, params: dict[str, Any]) -> str:
        _validate_network(network)
        try:
            response = await self._client.get(
                f"/find_neighbors/{network}", params=params
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "search_neighbors upstream returned %s for network=%s",
                exc.response.status_code,
                network,
            )
            logger.debug("upstream body: %s", exc.response.text)
            raise ToolError(
                f"Upstream search service returned HTTP {exc.response.status_code}"
            ) from exc
        payload = response.json()
        task_id = payload.get("task_id")
        if not task_id:
            raise ToolError("Upstream search service did not return a task_id")
        return task_id

    async def poll(
        self, task_id: str, include_path_details: bool = True
    ) -> dict[str, Any]:
        _validate_task_id(task_id)
        elapsed = 0.0
        interval = self._config.poll_interval_s
        max_time = self._config.max_poll_time_s
        while elapsed < max_time:
            try:
                response = await self._client.get(
                    f"/get_task_state/{task_id}",
                    params={"include_path_details": include_path_details},
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "poll upstream returned %s for task_id=%s",
                    exc.response.status_code,
                    task_id,
                )
                logger.debug("upstream body: %s", exc.response.text)
                raise ToolError(
                    f"Upstream search service returned HTTP {exc.response.status_code} while polling"
                ) from exc
            state_data = response.json()
            task_state = state_data.get("state")
            logger.debug("task %s state=%s", task_id, task_state)
            if task_state in TERMINAL_STATES:
                return state_data
            await asyncio.sleep(interval)
            elapsed += interval
        raise ToolError(
            f"Search task {task_id} did not reach a terminal state within "
            f"{max_time:.0f} seconds"
        )


def register(mcp, config: SearchNeighborsConfig, stack: AsyncExitStack) -> None:
    """Attach the search_neighbors tool to the FastMCP server.

    The httpx client's lifecycle is bound to the provided AsyncExitStack so
    it is cleanly closed on server shutdown.
    """
    client = SearchNeighborsClient(config)
    stack.push_async_callback(client.aclose)

    @mcp.tool(tags={"gs_address-level", "gs_neighbors", "gs_tracing"})
    async def search_neighbors(
        network: str,
        start_address: str,
        direction: Literal["in", "out"] = "out",
        search_type: Literal[
            "quicklock",
            "addr_only",
            "utxo_links_only",
            "chronological_links_only",
            "last_links_only",
        ] = "addr_only",
        match_keywords: Optional[list[str]] = None,
        prune_keywords: Optional[list[str]] = None,
        max_search_depth: int = 5,
        max_search_breadth: int = 200,
        search_time_seconds: int = 5,
        max_nr_results: Optional[int] = None,
    ) -> dict[str, Any]:
        """Search the transaction graph for neighbors of an address that match
        specific labels or categories (e.g. "exchange", "mixer"). The call
        starts an asynchronous search upstream and polls until completion,
        returning the full result.

        Args:
            network: Network identifier, lowercase (e.g. "btc", "eth", "trx").
            start_address: Address to start the search from.
            direction: "out" = outgoing funds, "in" = incoming funds.
            search_type: Search strategy. "addr_only" traces the address graph;
                "quicklock" is optimised for exchange tracing.
            match_keywords: Categories/labels/addresses to find.
            prune_keywords: Categories/labels/addresses to stop exploring at.
            max_search_depth: Maximum hops from the start address (1-30).
            max_search_breadth: Max neighbours explored per hop (1-10000).
            search_time_seconds: Upstream-side timeout per search (1-600).
            max_nr_results: Stop after finding N results (optional).

        Returns:
            The terminal task state, including any discovered paths.
        """
        params: dict[str, Any] = {
            "start_address": start_address,
            "direction": direction,
            "search_type": search_type,
            "max_search_depth": max_search_depth,
            "max_search_breadth": max_search_breadth,
            "search_time_seconds": search_time_seconds,
        }
        if match_keywords:
            params["match_keywords"] = match_keywords
        if prune_keywords:
            params["prune_keywords"] = prune_keywords
        if max_nr_results:
            params["max_nr_results"] = max_nr_results

        task_id = await client.start_search(network, params)
        logger.info("started upstream search task %s", task_id)
        return await client.poll(task_id)
