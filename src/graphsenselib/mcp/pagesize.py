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
so `_params_from` applies the default there. For auto-generated tools, a
FastMCP `ToolTransform` advertises the default and supplies it when the caller
omits `pagesize`. `PagesizeDefaultMiddleware` handles explicit JSON null,
which the transformed optional argument otherwise passes through unchanged.
"""

from __future__ import annotations

from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

DEFAULT_PAGESIZE = 25


def resolve_pagesize(pagesize: int | None) -> int:
    """Return the caller's pagesize, or 25 when it is omitted."""
    return DEFAULT_PAGESIZE if pagesize is None else pagesize


class PagesizeDefaultMiddleware(Middleware):
    """Default an explicit null pagesize on auto-generated tools.

    `tool_names` is the set of auto-generated tools that actually take a
    `pagesize` query param, collected at build time in `routes.py`. Scoping
    to that set matters: the consolidated tools already default themselves in
    `_params_from`, and `list_neighbors` reads `pagesize` as a target match
    count when filtering, with its own default. The `ToolTransform` handles an
    omitted argument. This middleware only prevents explicit null from
    bypassing that default and leaves invalid values to normal validation.
    """

    def __init__(self, tool_names: set[str]) -> None:
        self.tool_names = tool_names

    async def on_call_tool(
        self, context: MiddlewareContext, call_next: CallNext
    ) -> Any:
        message = context.message
        if getattr(message, "name", None) in self.tool_names:
            arguments = dict(message.arguments or {})
            if "pagesize" in arguments and arguments["pagesize"] is None:
                arguments["pagesize"] = DEFAULT_PAGESIZE
            message.arguments = arguments
        return await call_next(context)
