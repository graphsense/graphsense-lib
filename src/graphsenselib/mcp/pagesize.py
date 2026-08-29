"""Page-size policy for the MCP tool surface.

Omission is the failure mode. When an LLM leaves `pagesize` out, the argument
reaches the REST route as `None`, and `None` does not select a server default.
For `list_txs_for` it selects BIG_PAGE_SIZE (5000 rows, measured at ~2.7 MB /
~677k LLM tokens on an exchange hot wallet), and for `list_tx_flows` it selects
nothing at all: the service only slices when both a page and a pagesize are
set, so every flow event of the transaction comes back with `next_page: null`.

An explicit pagesize needs no policy here. The route already bounds it at
`web/routes/params.py:MAX_PAGESIZE` (5000), and the Cassandra layer clamps
again to BIG_PAGE_SIZE / SMALL_PAGE_SIZE. A caller that asks for a large page
gets one.

The hand-written tools in `tools/consolidated.py` build their own query dict,
so `_params_from` applies the default there. The auto-generated tools have
none: FastMCP's `OpenAPITool.run` hands the model's arguments straight to the
`RequestDirector` with no gslib code in between. `PagesizeDefaultMiddleware`
fills the gap.
"""

from __future__ import annotations

from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

DEFAULT_PAGESIZE = 25


def resolve_pagesize(pagesize: Any) -> int:
    """Return the caller's pagesize, or the default when there isn't one.

    Middleware runs before FastMCP validates arguments against the tool
    schema, so `pagesize` here is whatever the model sent, possibly a string
    or a negative number. Anything that isn't a positive int is treated the
    same as omission. Positive values pass through untouched; the route
    bounds them.
    """
    if not isinstance(pagesize, int) or isinstance(pagesize, bool) or pagesize < 1:
        return DEFAULT_PAGESIZE
    return pagesize


class PagesizeDefaultMiddleware(Middleware):
    """Supply the pagesize default to auto-generated tools.

    `tool_names` is the set of auto-generated tools that actually take a
    `pagesize` query param, collected at build time in `routes.py`. Scoping
    to that set matters: the consolidated tools already default themselves in
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
            arguments["pagesize"] = resolve_pagesize(arguments.get("pagesize"))
            message.arguments = arguments
        return await call_next(context)
