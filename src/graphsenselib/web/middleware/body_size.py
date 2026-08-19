"""Middleware bounding the size of request bodies.

The bulk endpoints accept an arbitrary JSON object (``Dict[str, Any]``), which
FastAPI reads and ``json.loads``-es in full *before* the route handler — and
therefore before any application-level item cap — can look at it. A multi-
megabyte list of integers costs far more as parsed Python objects than as
wire bytes, so a body limit is the only thing that bounds memory on that path
Deployments normally also cap this at the reverse
proxy; this middleware makes the stock container safe on its own.

Pure ASGI (like EmptyQueryParamsMiddleware) so it neither buffers responses
nor consumes the request stream.
"""

import json
import logging

from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)


def _declared_content_length(scope: Scope) -> "int | None":
    """Content-Length as an int, or None when absent/unparseable."""
    for name, value in scope.get("headers", []):
        if name == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


class RequestBodySizeLimitMiddleware:
    """Reject request bodies larger than ``max_body_bytes`` with 413.

    A body that declares an oversized ``Content-Length`` is refused before the
    application is called at all. A chunked body (no ``Content-Length``) is
    counted as it streams and cut off once it exceeds the limit.
    """

    def __init__(self, app: ASGIApp, max_body_bytes: int):
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or self.max_body_bytes <= 0:
            await self.app(scope, receive, send)
            return

        declared = _declared_content_length(scope)
        if declared is not None:
            if declared > self.max_body_bytes:
                logger.warning(
                    "Rejecting %s %s: Content-Length %d exceeds limit %d",
                    scope.get("method"),
                    scope.get("path"),
                    declared,
                    self.max_body_bytes,
                )
                await self._send_too_large(send)
                return
            # A declared length within the limit is enough: the server never
            # hands us more than Content-Length bytes, so counting would be
            # pure overhead on every request.
            await self.app(scope, receive, send)
            return

        # No declared length (chunked): count the body as it streams. Cutting
        # the stream short makes the application fail its own body parse, so
        # its response is swapped for the 413 the client should have seen.
        seen = 0
        exceeded = False
        replaced = False

        async def counting_receive() -> Message:
            nonlocal seen, exceeded
            message = await receive()
            if message["type"] == "http.request":
                seen += len(message.get("body", b""))
                if seen > self.max_body_bytes:
                    exceeded = True
                    logger.warning(
                        "Aborting %s %s: streamed body exceeds limit %d",
                        scope.get("method"),
                        scope.get("path"),
                        self.max_body_bytes,
                    )
                    return {"type": "http.disconnect"}
            return message

        async def replacing_send(message: Message):
            nonlocal replaced
            if replaced:
                # Our 413 is already on the wire; drop whatever the
                # application made of the truncated body.
                return
            if exceeded and message["type"] == "http.response.start":
                replaced = True
                await self._send_too_large(send)
                return
            await send(message)

        await self.app(scope, counting_receive, replacing_send)

    async def _send_too_large(self, send: Send):
        body = json.dumps(
            {
                "detail": (
                    f"Request body too large (limit: {self.max_body_bytes} bytes)."
                )
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
