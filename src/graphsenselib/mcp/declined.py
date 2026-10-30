"""Tell the model when a tool result leaves out the externally served networks.

A merged REST answer (/stats, /search, /capabilities, a twin list) whose
external backend failed or declined is still a 200: the core networks' part,
with the backend named in ``X-Ikn-Backends-Declined``
(web/middleware/external_backends.py). The dashboard reads that header. A
model only reads the tool result, so without a note a search for a bnb
address during an outage of the lite service reads as "not found on any
network" -- "not checked" turned into "does not exist".

Two parts. ``record_declined`` is an httpx response hook on every client the
MCP tools use for their REST calls (the generated tools' client in
server.py, the consolidated wrappers' in tools/consolidated.py); it collects
the declined backends of the current tool call in a context variable.
``DeclinedBackendsNoteMiddleware`` opens that collection for each tool call
and, when it is not empty, puts a note naming the missing networks in front
of the tool's result.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Optional

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.base import ToolResult
from mcp.types import TextContent

from graphsenselib.web.middleware.external_backends import DECLINED_HEADER

# The backends the REST answers of the current tool call named as missing.
# None outside a tool call: a REST call made elsewhere records nothing.
_declined: ContextVar[Optional[set[str]]] = ContextVar(
    "graphsense_mcp_declined_backends", default=None
)


async def record_declined(response: Any) -> None:
    """httpx response hook: remember the backends this answer is missing."""
    seen = _declined.get()
    value = response.headers.get(DECLINED_HEADER)
    if seen is None or not value:
        return
    seen.update(url.strip() for url in value.split(",") if url.strip())


def networks_by_backend(app: Any) -> dict[str, list[str]]:
    """Backend URL -> the networks it serves, from the REST app's config;
    empty when the app has no external backends."""
    config = getattr(app.state, "config", None)
    backends = getattr(config, "external_backends", None)
    if backends is None or not backends.enabled:
        return {}
    grouped: dict[str, list[str]] = {}
    for network, backend in backends.networks.items():
        grouped.setdefault(backend.url, []).append(network)
    return {url: sorted(networks) for url, networks in grouped.items()}


def declined_note(backends: set[str], networks: dict[str, list[str]]) -> str:
    """The note for a result whose REST answers were missing `backends`. It
    names networks, never backend URLs (internal hostnames)."""
    missing = sorted({n for url in backends for n in networks.get(url, [])})
    which = ", ".join(missing) if missing else "the externally served networks"
    return (
        f"Partial result: the service for {which} did not answer, so this "
        "result does not cover them. For those networks a missing hit or "
        "statistic means not checked, not not found. Retry in a few minutes."
    )


class DeclinedBackendsNoteMiddleware(Middleware):
    """Prefix a tool result with `declined_note` when one of its REST answers
    named a declined backend. `networks` maps backend URL -> networks
    (`networks_by_backend`)."""

    def __init__(self, networks: dict[str, list[str]]) -> None:
        self.networks = networks

    async def on_call_tool(
        self, context: MiddlewareContext, call_next: CallNext
    ) -> Any:
        seen: set[str] = set()
        _declined.set(seen)
        result = await call_next(context)
        if not seen:
            return result
        note = TextContent(type="text", text=declined_note(seen, self.networks))
        return ToolResult(
            content=[note, *result.content],
            structured_content=result.structured_content,
            meta=result.meta,
            is_error=result.is_error,
        )
