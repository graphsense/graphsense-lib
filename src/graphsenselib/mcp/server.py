from __future__ import annotations

import logging
from contextlib import AsyncExitStack, asynccontextmanager

from fastmcp import FastMCP

from graphsenselib.mcp import curation as curation_mod
from graphsenselib.mcp.config import GSMCPConfig
from graphsenselib.mcp.routes import make_component_fn, make_route_map_fn
from graphsenselib.mcp.tools import register_custom_tools

logger = logging.getLogger(__name__)


class MCPBootstrapError(Exception):
    pass


def build_mcp(app, config: GSMCPConfig) -> tuple[FastMCP, AsyncExitStack]:
    """Build a FastMCP server from the given FastAPI app.

    Returns (mcp, stack). The stack owns async resources (notably the httpx
    client used by external tools) and must be entered before serving and
    exited on shutdown. When used via `attach_to_fastapi`, lifespan handles
    that automatically.
    """
    curation = curation_mod.load(config.resolved_curation_path())
    if config.strict_validation:
        curation_mod.validate_against_app(
            curation, curation_mod.collect_operation_ids(app)
        )

    route_map_fn = make_route_map_fn(curation)
    component_fn = make_component_fn(curation)

    mcp = FastMCP.from_fastapi(
        app=app,
        name="graphsense-mcp",
        route_map_fn=route_map_fn,
        mcp_component_fn=component_fn,
    )

    stack = AsyncExitStack()
    register_custom_tools(mcp, app, curation, config, stack)
    return mcp, stack


def attach_to_fastapi(app, config: GSMCPConfig) -> None:
    """Mount the MCP endpoint into the given FastAPI app at `config.path`.

    This is the supported way to deploy the MCP: one uvicorn process serves
    both REST and MCP. The MCP's own session-manager lifespan is composed
    with the app's existing lifespan, and the AsyncExitStack holding the
    external-tool httpx client is entered on startup / exited on shutdown.

    Calling this twice on the same app raises MCPBootstrapError.
    """
    if getattr(app.state, "_graphsense_mcp_attached", False):
        raise MCPBootstrapError("MCP is already attached to this FastAPI app")

    mcp, stack = build_mcp(app, config)
    mcp_asgi = mcp.http_app(path="/", stateless_http=config.stateless_http)

    existing_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def composed_lifespan(app_):
        async with stack:
            async with mcp_asgi.lifespan(mcp_asgi):
                async with existing_lifespan(app_):
                    yield

    app.router.lifespan_context = composed_lifespan
    app.mount(config.path, mcp_asgi)
    app.state._graphsense_mcp_attached = True
    logger.info("graphsense MCP mounted at %s", config.path)


def validate_curation(app, config: GSMCPConfig) -> list[str]:
    """Check the curation YAML against the given FastAPI app. Returns the
    list of problems (empty if valid). Does not raise — the CLI layer decides
    how to surface issues.
    """
    try:
        curation = curation_mod.load(config.resolved_curation_path())
    except curation_mod.CurationError as exc:
        return [str(exc)]

    try:
        curation_mod.validate_against_app(
            curation, curation_mod.collect_operation_ids(app)
        )
    except curation_mod.CurationError as exc:
        return [str(exc)]

    return []
