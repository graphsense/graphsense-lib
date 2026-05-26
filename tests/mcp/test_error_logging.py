"""Verify the FastMCP error-logging middleware surfaces incidents the same way
the REST app's unhandled-exception handler does — via logger.exception on the
``graphsenselib.mcp`` logger tree, where the Slack handler is attached
(``web/app.py:setup_logging``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext

from graphsenselib.mcp.error_logging import ErrorLoggingMiddleware


@dataclass
class _FakeMessage:
    """Stand-in for a tools/call request payload — the middleware only reads
    the ``name`` attribute, so we don't need the full FastMCP request type."""

    name: str


def _ctx(name: str, method: str = "tools/call") -> MiddlewareContext:
    return MiddlewareContext(message=_FakeMessage(name=name), method=method)


@pytest.mark.asyncio
async def test_unexpected_exception_is_logged_and_reraised(caplog) -> None:
    middleware = ErrorLoggingMiddleware()

    async def boom(context: MiddlewareContext) -> None:
        raise RuntimeError("upstream blew up")

    with caplog.at_level(logging.ERROR, logger="graphsenselib.mcp.error_logging"):
        with pytest.raises(RuntimeError, match="upstream blew up"):
            await middleware.on_call_tool(_ctx("build_pathfinder_file"), boom)

    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert "build_pathfinder_file" in record.getMessage()
    # logger.exception attaches the traceback so the Slack handler can quote it.
    assert record.exc_info is not None


@pytest.mark.asyncio
async def test_tool_error_passes_through_silently(caplog) -> None:
    """ToolError is the expected user-visible error contract (e.g. 'file too
    large'); surfacing it on Slack would be noise. The middleware must pass it
    through without logging at ERROR."""
    middleware = ErrorLoggingMiddleware()

    async def expected(context: MiddlewareContext) -> None:
        raise ToolError("file is too large")

    with caplog.at_level(logging.ERROR, logger="graphsenselib.mcp"):
        with pytest.raises(ToolError, match="file is too large"):
            await middleware.on_call_tool(_ctx("build_pathfinder_file"), expected)

    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


@pytest.mark.asyncio
async def test_successful_call_is_not_logged(caplog) -> None:
    middleware = ErrorLoggingMiddleware()

    async def ok(context: MiddlewareContext) -> str:
        return "done"

    with caplog.at_level(logging.ERROR, logger="graphsenselib.mcp"):
        assert await middleware.on_call_tool(_ctx("ok_tool"), ok) == "done"

    assert caplog.records == []


@pytest.mark.asyncio
async def test_validation_error_passes_through_silently(caplog) -> None:
    """Pydantic / FastMCP call-argument validation errors are caller-side
    (the model passed bad input — same flavour as a 4xx, model-fixable).
    They must not log at ERROR — routing them to Slack would just be
    noise every time the LLM mistypes a field."""
    import logging as _logging

    from pydantic import BaseModel, ValidationError

    middleware = ErrorLoggingMiddleware()

    class _Schema(BaseModel):
        n: int

    async def bad_args(context: MiddlewareContext) -> None:
        # Triggering a real Pydantic ValidationError so we know the
        # middleware actually catches the same class FastMCP raises
        # when call-arg validation fails.
        _Schema.model_validate({"n": "not-a-number"})

    with caplog.at_level(_logging.ERROR, logger="graphsenselib.mcp"):
        with pytest.raises(ValidationError):
            await middleware.on_call_tool(_ctx("build_pathfinder_file"), bad_args)

    assert [r for r in caplog.records if r.levelno >= _logging.ERROR] == []


@pytest.mark.asyncio
async def test_resource_and_prompt_handlers_are_guarded(caplog) -> None:
    """on_read_resource and on_get_prompt share the same _guard, so a tool-only
    test isn't enough — exercise both so a future refactor that drops one of
    the hooks is caught."""
    middleware = ErrorLoggingMiddleware()

    async def boom(context: MiddlewareContext) -> None:
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.ERROR, logger="graphsenselib.mcp.error_logging"):
        with pytest.raises(RuntimeError):
            await middleware.on_read_resource(_ctx("r"), boom)
        with pytest.raises(RuntimeError):
            await middleware.on_get_prompt(_ctx("p"), boom)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 2
    assert "resource" in errors[0].getMessage()
    assert "prompt" in errors[1].getMessage()
