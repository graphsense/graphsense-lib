"""RespondAfterDisconnect: a client hanging up mid MCP call must not surface as
"RuntimeError: No response returned." in the BaseHTTPMiddlewares above the
mount (see graphsenselib/mcp/disconnect.py)."""

from __future__ import annotations

import asyncio
import json
import logging

import pytest
from fastapi import FastAPI
from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import PlainTextResponse

from graphsenselib.mcp.disconnect import CLIENT_CLOSED_REQUEST, RespondAfterDisconnect


class _PassThrough(BaseHTTPMiddleware):
    """Stands in for CurrencyRoleMiddleware / ExternalBackendMiddleware."""

    async def dispatch(self, request, call_next):
        return await call_next(request)


def _scope(body: bytes, extra_headers=()) -> dict:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/mcp/",
        "raw_path": b"/mcp/",
        "query_string": b"",
        "root_path": "",
        "server": ("localhost", 80),
        "client": ("client", 1),
        "headers": [
            (b"host", b"localhost"),
            (b"content-type", b"application/json"),
            (b"accept", b"application/json, text/event-stream"),
            (b"content-length", str(len(body)).encode()),
            *extra_headers,
        ],
    }


async def _call(app, body: bytes = b"", disconnect_after: float = 0.0, scope=None):
    """Drive one request; the client disconnects `disconnect_after` seconds
    after the body was read. Returns the messages the server sent."""
    sent = []
    first = True

    async def receive():
        nonlocal first
        if first:
            first = False
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.sleep(disconnect_after)
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    await asyncio.wait_for(app(scope or _scope(body), receive, send), 10)
    return sent


def _statuses(sent):
    return [m["status"] for m in sent if m["type"] == "http.response.start"]


async def _silent_after_disconnect(scope, receive, send):
    """What the mcp SDK's modern handler does when the client hangs up."""
    while (await receive())["type"] != "http.disconnect":
        pass


async def _silent(scope, receive, send):
    """An app bug: returns without a response, client still connected."""
    await receive()


def _behind_middleware(inner):
    app = FastAPI()
    app.mount("/mcp", inner)
    app.add_middleware(_PassThrough)
    app.add_middleware(_PassThrough)
    return app


async def test_answers_after_disconnect(caplog):
    caplog.set_level(logging.WARNING, logger="graphsenselib.mcp.disconnect")
    sent = await _call(
        RespondAfterDisconnect(_silent_after_disconnect), disconnect_after=0.1
    )
    assert _statuses(sent) == [CLIENT_CLOSED_REQUEST]
    assert sent[-1] == {"type": "http.response.body", "body": b"", "more_body": False}
    # One WARNING (visible at the root level; not ERROR, so no Slack page)
    # with method, path and duration.
    [record] = [r for r in caplog.records if r.name == "graphsenselib.mcp.disconnect"]
    assert record.levelno == logging.WARNING
    assert "POST /mcp/ after 0.1s" in record.getMessage()


async def test_base_http_middleware_does_not_raise_after_disconnect():
    app = _behind_middleware(RespondAfterDisconnect(_silent_after_disconnect))
    sent = await _call(app)
    assert _statuses(sent) == [CLIENT_CLOSED_REQUEST]


async def test_stays_loud_while_client_is_connected():
    """No disconnect seen: a missing response is a bug and must still raise."""
    app = _behind_middleware(RespondAfterDisconnect(_silent))
    with pytest.raises(RuntimeError, match="No response returned"):
        await _call(app, disconnect_after=60)


async def test_passes_a_response_through_untouched():
    inner = PlainTextResponse("hi", status_code=201)
    sent = await _call(RespondAfterDisconnect(inner), disconnect_after=60)
    assert _statuses(sent) == [201]
    assert b"".join(m.get("body", b"") for m in sent) == b"hi"


async def test_no_second_response_when_one_started_before_disconnect():
    async def respond_then_see_disconnect(scope, receive, send):
        await PlainTextResponse("x")(scope, receive, send)
        await receive()  # the disconnect

    sent = await _call(RespondAfterDisconnect(respond_then_see_disconnect))
    assert _statuses(sent) == [200]


async def test_non_http_scope_is_passed_through():
    seen = []

    async def inner(scope, receive, send):
        seen.append(scope["type"])

    async def receive():
        return {"type": "lifespan.startup"}

    async def send(message):
        pass

    await RespondAfterDisconnect(inner)({"type": "lifespan"}, receive, send)
    assert seen == ["lifespan"]


MODERN = "2026-07-28"


async def test_mcp_modern_protocol_tool_call_cancelled_by_disconnect():
    """End to end, as in prod: a 2026-07-28 client hangs up while a tool runs.
    The SDK cancels the call and sends nothing; the wrapper answers instead,
    so the BaseHTTPMiddlewares above raise nothing."""
    mcp = FastMCP("t")

    @mcp.tool
    async def slow(sec: float) -> str:
        await asyncio.sleep(sec)
        return "done"

    mcp_asgi = mcp.http_app(path="/", stateless_http=True)
    app = FastAPI(lifespan=mcp_asgi.lifespan)
    app.mount("/mcp", RespondAfterDisconnect(mcp_asgi))
    app.add_middleware(_PassThrough)
    app.add_middleware(_PassThrough)

    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "slow",
                "arguments": {"sec": 5},
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": MODERN,
                    "io.modelcontextprotocol/clientInfo": {"name": "t", "version": "1"},
                    "io.modelcontextprotocol/clientCapabilities": {},
                },
            },
        }
    ).encode()
    scope = _scope(
        body,
        [
            (b"mcp-protocol-version", MODERN.encode()),
            (b"mcp-method", b"tools/call"),
            (b"mcp-name", b"slow"),
        ],
    )
    async with app.router.lifespan_context(app):
        sent = await _call(app, body, disconnect_after=0.2, scope=scope)
    assert _statuses(sent) == [CLIENT_CLOSED_REQUEST]
