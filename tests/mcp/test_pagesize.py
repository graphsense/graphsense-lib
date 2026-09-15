"""Page-size defaults for consolidated and auto-generated MCP tools."""

from types import SimpleNamespace

import pytest
from fastmcp.exceptions import ToolError

from graphsenselib.mcp.pagesize import (
    DEFAULT_PAGESIZE,
    PagesizeDefaultMiddleware,
    resolve_pagesize,
)


def test_resolve_pagesize_defaults_only_none():
    # Omission is the failure mode: None reaches the route as "no limit".
    assert resolve_pagesize(None) == DEFAULT_PAGESIZE
    # Explicit values pass through. The route validates them.
    assert resolve_pagesize(40) == 40
    assert resolve_pagesize(5000) == 5000
    assert resolve_pagesize(0) == 0
    assert resolve_pagesize(-1) == -1


async def _call(middleware, name, arguments):
    seen = {}

    async def call_next(ctx):
        seen["arguments"] = ctx.message.arguments
        return "ok"

    ctx = SimpleNamespace(message=SimpleNamespace(name=name, arguments=arguments))
    assert await middleware.on_call_tool(ctx, call_next) == "ok"
    return seen["arguments"]


@pytest.mark.asyncio
async def test_middleware_defaults_only_explicit_null_on_listed_tools():
    mw = PagesizeDefaultMiddleware({"list_tx_flows"})

    assert await _call(mw, "list_tx_flows", {}) == {}
    assert (await _call(mw, "list_tx_flows", {"pagesize": None}))[
        "pagesize"
    ] == DEFAULT_PAGESIZE
    assert (await _call(mw, "list_tx_flows", {"pagesize": 9999}))["pagesize"] == 9999
    assert (await _call(mw, "list_tx_flows", {"pagesize": 10}))["pagesize"] == 10
    assert (await _call(mw, "list_tx_flows", {"pagesize": 0}))["pagesize"] == 0
    assert (await _call(mw, "list_tx_flows", {"pagesize": "50"}))["pagesize"] == "50"

    for pagesize in ([], {}):
        with pytest.raises(ToolError, match="pagesize must be an integer or null"):
            await _call(mw, "list_tx_flows", {"pagesize": pagesize})

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
    items = list(range(2 * DEFAULT_PAGESIZE + 10))

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
async def test_paging_onward_works_with_the_default(paged_stub_app):
    """The default is only safe if the caller can still reach the rest of the
    data: page 1 must come back defaulted and carrying a cursor, and that
    cursor must fetch a distinct page 2.
    """
    from fastmcp import Client, FastMCP
    from fastmcp.server.transforms import ToolTransform
    from fastmcp.tools.tool_transform import ArgTransformConfig, ToolTransformConfig

    mcp = FastMCP.from_fastapi(app=paged_stub_app)
    mcp.add_transform(
        ToolTransform(
            {
                "list_tx_flows": ToolTransformConfig(
                    arguments={"pagesize": ArgTransformConfig(default=DEFAULT_PAGESIZE)}
                )
            }
        )
    )
    mcp.add_middleware(PagesizeDefaultMiddleware({"list_tx_flows"}))

    async with Client(mcp) as client:
        tool = next(t for t in await client.list_tools() if t.name == "list_tx_flows")
        assert tool.inputSchema["properties"]["pagesize"]["default"] == DEFAULT_PAGESIZE

        first = await _flows_call(client)
        assert first["seen_pagesize"] == DEFAULT_PAGESIZE
        assert first["txs"] == list(range(DEFAULT_PAGESIZE))
        assert first["next_page"] == 2

        explicit_null = await _flows_call(client, pagesize=None)
        assert explicit_null["seen_pagesize"] == DEFAULT_PAGESIZE

        # The cursor comes back as an int while `page` is typed string
        # upstream; FastMCP coerces, so either form reaches the route.
        second = await _flows_call(client, page=first["next_page"])
        assert second["txs"] == list(range(DEFAULT_PAGESIZE, 2 * DEFAULT_PAGESIZE))
        assert second["next_page"] == 3

        third = await _flows_call(client, page=str(second["next_page"]))
        assert third["txs"] == list(
            range(2 * DEFAULT_PAGESIZE, 2 * DEFAULT_PAGESIZE + 10)
        )
        assert third["next_page"] is None

        # An explicit large pagesize is the caller's call and passes through.
        wide = await _flows_call(client, pagesize=5000)
        assert wide["seen_pagesize"] == 5000
        assert wide["next_page"] is None  # one 5000-row page covers them all


@pytest.mark.asyncio
@pytest.mark.parametrize("pagesize", [0, -1, 5001])
async def test_invalid_explicit_pagesize_reaches_route_validation(
    paged_stub_app, pagesize
):
    from fastmcp import Client, FastMCP
    from fastmcp.server.transforms import ToolTransform
    from fastmcp.tools.tool_transform import ArgTransformConfig, ToolTransformConfig

    mcp = FastMCP.from_fastapi(app=paged_stub_app)
    mcp.add_transform(
        ToolTransform(
            {
                "list_tx_flows": ToolTransformConfig(
                    arguments={"pagesize": ArgTransformConfig(default=DEFAULT_PAGESIZE)}
                )
            }
        )
    )
    mcp.add_middleware(PagesizeDefaultMiddleware({"list_tx_flows"}))

    async with Client(mcp) as client:
        with pytest.raises(ToolError, match="HTTP error 422"):
            await _flows_call(client, pagesize=pagesize)


@pytest.mark.asyncio
@pytest.mark.parametrize("pagesize", [[], {}], ids=["array", "object"])
async def test_container_pagesize_is_rejected_before_http_serialization(
    paged_stub_app, pagesize
):
    from fastmcp import Client, FastMCP
    from fastmcp.server.transforms import ToolTransform
    from fastmcp.tools.tool_transform import ArgTransformConfig, ToolTransformConfig

    mcp = FastMCP.from_fastapi(app=paged_stub_app)
    mcp.add_transform(
        ToolTransform(
            {
                "list_tx_flows": ToolTransformConfig(
                    arguments={"pagesize": ArgTransformConfig(default=DEFAULT_PAGESIZE)}
                )
            }
        )
    )
    mcp.add_middleware(PagesizeDefaultMiddleware({"list_tx_flows"}))

    async with Client(mcp) as client:
        with pytest.raises(ToolError, match="pagesize must be an integer or null"):
            await _flows_call(client, pagesize=pagesize)
