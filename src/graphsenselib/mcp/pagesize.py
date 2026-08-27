"""Page-size policy for the MCP tool surface.

Without an explicit pagesize the upstream returns its own default page, which
for list endpoints on high-volume objects is effectively unbounded (a single
`list_txs_for` on an exchange hot wallet came back at ~2.7 MB / ~677k LLM
tokens). Omission, not a large explicit value, is the failure mode: `None`
reaches the route as "no limit", not as "server default".

The hand-written tools in `tools/consolidated.py` build their own query dict,
so `_params_from` is the chokepoint that applies the policy there. The
auto-generated tools have none: FastMCP's `OpenAPITool.run` hands the model's
arguments straight to the `RequestDirector` with no gslib code in between.
`PagesizeCapMiddleware` is that missing chokepoint.
"""

from __future__ import annotations

from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

DEFAULT_PAGESIZE = 25
MAX_PAGESIZE = 100


def capped(pagesize: Any) -> int:
    """Clamp a caller-supplied pagesize, treating unset / junk as the default.

    Middleware runs before FastMCP validates arguments against the tool
    schema, so `pagesize` here is whatever the model sent, possibly a
    string or a negative number. Anything that isn't a positive int is
    treated the same as omission.
    """
    if not isinstance(pagesize, int) or isinstance(pagesize, bool) or pagesize < 1:
        pagesize = DEFAULT_PAGESIZE
    return min(pagesize, MAX_PAGESIZE)


class PagesizeCapMiddleware(Middleware):
    """Apply the pagesize default and ceiling to auto-generated tools.

    `tool_names` is the set of auto-generated tools that actually take a
    `pagesize` query param, collected at build time in `routes.py`. Scoping
    to that set matters: the consolidated tools already cap themselves in
    `_params_from`, and `list_neighbors` reads `pagesize` as a target match
    count when filtering, with its own default; injecting one here would
    silently change that.
    """

    def __init__(self, tool_names: set[str]) -> None:
        self.tool_names = tool_names

    async def on_call_tool(
        self, context: MiddlewareContext, call_next: CallNext
    ) -> Any:
        message = context.message
        if getattr(message, "name", None) in self.tool_names:
            arguments = dict(message.arguments or {})
            arguments["pagesize"] = capped(arguments.get("pagesize"))
            message.arguments = arguments
        return await call_next(context)
