"""Page-size policy: the default/ceiling that keeps one list call from
returning megabytes, and the middleware that applies it to the
auto-generated tools (which have no chokepoint of their own).
"""

from types import SimpleNamespace

import pytest

from graphsenselib.mcp.pagesize import (
    DEFAULT_PAGESIZE,
    MAX_PAGESIZE,
    PagesizeCapMiddleware,
    capped,
)


def test_capped_defaults_and_clamps():
    # Omission is the failure mode: None reaches the route as "no limit".
    assert capped(None) == DEFAULT_PAGESIZE
    assert capped(40) == 40
    assert capped(5000) == MAX_PAGESIZE
    # Middleware runs before schema validation, so junk can reach us.
    assert capped("50") == DEFAULT_PAGESIZE
    assert capped(0) == DEFAULT_PAGESIZE
    assert capped(-1) == DEFAULT_PAGESIZE


async def _call(middleware, name, arguments):
    seen = {}

    async def call_next(ctx):
        seen["arguments"] = ctx.message.arguments
        return "ok"

    ctx = SimpleNamespace(message=SimpleNamespace(name=name, arguments=arguments))
    assert await middleware.on_call_tool(ctx, call_next) == "ok"
    return seen["arguments"]


@pytest.mark.asyncio
async def test_middleware_caps_only_listed_tools():
    mw = PagesizeCapMiddleware({"list_tx_flows"})

    assert (await _call(mw, "list_tx_flows", {}))["pagesize"] == DEFAULT_PAGESIZE
    assert (await _call(mw, "list_tx_flows", {"pagesize": 9999}))[
        "pagesize"
    ] == MAX_PAGESIZE
    assert (await _call(mw, "list_tx_flows", {"pagesize": 10}))["pagesize"] == 10

    # list_neighbors reads pagesize as a filter target with its own default;
    # injecting one here would silently change that.
    assert await _call(mw, "list_neighbors", {}) == {}
    assert await _call(mw, "get_statistics", None) is None


@pytest.fixture
def paged_stub_app():
    """Stub standing in for the auto-generated `list_tx_flows` route: same
    param types as the real one (`page` is a string, `pagesize` an int) and
    the same "no pagesize means no pagination" behaviour.
    """
    from typing import Optional

    from fastapi import FastAPI, Query

    app = FastAPI()
    items = list(range(60))

    @app.get("/{currency}/txs/{tx_hash}/flows", operation_id="list_tx_flows")
    async def _flows(
        currency: str,
        tx_hash: str,
        page: Optional[str] = None,
        pagesize: Optional[int] = Query(None, ge=1, le=5000),
    ):
        if pagesize is None:
            return {"txs": items, "next_page": None, "seen_pagesize": None}
        number = int(page) if page else 1
        start = (number - 1) * pagesize
        end = start + pagesize
        return {
            "txs": items[start:end],
            "next_page": number + 1 if end < len(items) else None,
            "seen_pagesize": pagesize,
        }

    return app


async def _flows_call(client, **kwargs):
    result = await client.call_tool(
        "list_tx_flows", {"currency": "eth", "tx_hash": "0xdead", **kwargs}
    )
    return result.data


@pytest.mark.asyncio
async def test_paging_onward_works_through_the_cap(paged_stub_app):
    """The cap is only safe if the caller can still reach the rest of the
    data: page 1 must come back capped and carrying a cursor, and that
    cursor must fetch a distinct page 2.
    """
    from fastmcp import Client, FastMCP

    mcp = FastMCP.from_fastapi(app=paged_stub_app)
    mcp.add_middleware(PagesizeCapMiddleware({"list_tx_flows"}))

    async with Client(mcp) as client:
        first = await _flows_call(client)
        assert first["seen_pagesize"] == DEFAULT_PAGESIZE  # injected, not omitted
        assert first["txs"] == list(range(25))
        assert first["next_page"] == 2

        # The cursor comes back as an int while `page` is typed string
        # upstream; FastMCP coerces, so either form reaches the route.
        second = await _flows_call(client, page=first["next_page"])
        assert second["txs"] == list(range(25, 50))
        assert second["next_page"] == 3

        third = await _flows_call(client, page=str(second["next_page"]))
        assert third["txs"] == list(range(50, 60))
        assert third["next_page"] is None

        # An explicit oversized pagesize is clamped, and still pages onward.
        wide = await _flows_call(client, pagesize=5000)
        assert wide["seen_pagesize"] == MAX_PAGESIZE
        assert wide["next_page"] is None  # 60 items < 100, one page covers it
