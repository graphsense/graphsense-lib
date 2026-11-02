"""Answer an MCP request whose client hung up before any response started.

The mcp SDK's handler for the 2026-07-28 protocol (``mcp/server/
_streamable_http_modern.py``) watches for ``http.disconnect`` while a tool call
runs. When the client goes away before the result is ready, it cancels the call
and returns without sending a response. That is legal ASGI: nobody is left to
answer. But the MCP is mounted below the REST app's ``BaseHTTPMiddleware``s
(``CurrencyRoleMiddleware``, ``ExternalBackendMiddleware``), and Starlette's
``call_next`` turns "the app returned without a response" into
``RuntimeError: No response returned.``. The REST app's catch-all handler then
logs it at ERROR, which pages Slack for a client that merely hung up.

``RespondAfterDisconnect`` closes that gap: if the wrapped app returns without
starting a response *and* it saw the client disconnect, it sends a bodiless
499 (nginx's "client closed request"). The client never receives it, and the
middlewares above get a response instead of raising. Uvicorn writes no access
log line for a disconnected client, so the wrapper logs one WARNING line
instead, with the request's duration: a user cancelling a call is short, a
proxy cutting off a slow tool call (APISIX ``proxy_read_timeout``) shows the
timeout. WARNING, not INFO: the ``graphsenselib.mcp`` logger tree inherits the
root level (WARNING) because ``setup_logging`` only sets the level of the web
app's logger, so INFO would never reach the logs. Not ERROR: that pages Slack
for what is usually a client's own choice.

An app that returns without a response while the client is still connected is
a real bug, so in that case the wrapper sends nothing and the error stays loud.
"""

from __future__ import annotations

import logging
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

CLIENT_CLOSED_REQUEST = 499


class RespondAfterDisconnect:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        disconnected = False
        started = False
        began = time.monotonic()

        async def watching_receive() -> Message:
            nonlocal disconnected
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected = True
            return message

        async def watching_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        await self.app(scope, watching_receive, watching_send)

        if started or not disconnected:
            return
        logger.warning(
            "Client disconnected from %s %s after %.1fs, before a response "
            "started; answered %d",
            scope.get("method"),
            scope.get("path"),
            time.monotonic() - began,
            CLIENT_CLOSED_REQUEST,
        )
        await send(
            {
                "type": "http.response.start",
                "status": CLIENT_CLOSED_REQUEST,
                "headers": [(b"content-length", b"0")],
            }
        )
        await send({"type": "http.response.body", "body": b"", "more_body": False})
