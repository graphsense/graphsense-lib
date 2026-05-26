"""FastMCP middleware that surfaces unexpected tool failures to the app logger.

Without this, an unhandled exception inside an MCP tool turns into an MCP
protocol error and is swallowed silently — ops never see it. The middleware
re-raises every exception (so FastMCP still produces its protocol response)
but first calls ``logger.exception`` on the ``graphsenselib.mcp`` logger, so
whatever handlers are attached to that tree (Slack, SMTP, file handlers) fire
just as they would for an unhandled REST exception.

``ToolError`` / ``ResourceError`` / ``PromptError`` are user-visible expected
errors (e.g. "file too large", "invalid spec") and are passed through without
logging at ERROR — they're contract, not incidents.
"""

from __future__ import annotations

import logging
from typing import Any

from fastmcp.exceptions import PromptError, ResourceError, ToolError
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

logger = logging.getLogger(__name__)

# Expected, user-visible MCP errors — emitted by tools to communicate a bad
# request or precondition failure. Surfacing these on Slack would be noise.
_EXPECTED_MCP_ERRORS: tuple[type[BaseException], ...] = (
    ToolError,
    ResourceError,
    PromptError,
)


class ErrorLoggingMiddleware(Middleware):
    """Log unexpected exceptions from MCP tool / resource / prompt handlers."""

    async def on_call_tool(
        self, context: MiddlewareContext, call_next: CallNext
    ) -> Any:
        return await self._guard(context, call_next, kind="tool")

    async def on_read_resource(
        self, context: MiddlewareContext, call_next: CallNext
    ) -> Any:
        return await self._guard(context, call_next, kind="resource")

    async def on_get_prompt(
        self, context: MiddlewareContext, call_next: CallNext
    ) -> Any:
        return await self._guard(context, call_next, kind="prompt")

    async def _guard(
        self, context: MiddlewareContext, call_next: CallNext, *, kind: str
    ) -> Any:
        try:
            return await call_next(context)
        except _EXPECTED_MCP_ERRORS:
            # Tools raise these to communicate a bad request to the model.
            # They are not incidents; let them through silently.
            raise
        except Exception:
            name = _identify(context)
            logger.exception("MCP %s %r raised an unhandled exception", kind, name)
            raise


def _identify(context: MiddlewareContext) -> str:
    """Best-effort identifier for the MCP item being invoked."""
    msg = getattr(context, "message", None)
    for attr in ("name", "uri"):
        value = getattr(msg, attr, None)
        if value:
            return str(value)
    return context.method or "<unknown>"
