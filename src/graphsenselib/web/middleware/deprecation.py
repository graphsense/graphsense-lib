"""Middleware that advertises deprecation on responses from deprecated routes.

Implements RFC 9745 (`Deprecation` header) and a companion `Link` header with
`rel="deprecation"` pointing to the human-readable deprecation policy, for any
route whose FastAPI `deprecated=True` flag is set. Clients and gateways can
detect the header and warn/log without having to inspect the OpenAPI schema.

A `Sunset` header (RFC 8594) is intentionally NOT emitted here: we don't yet
commit to specific removal dates per endpoint. When a removal date is chosen,
set it on the individual route via `responses={...}` or extend this middleware
to pull a per-route sunset from a config map.
"""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

DEPRECATION_HEADER_NAME = b"deprecation"
LINK_HEADER_NAME = b"link"
DEPRECATION_POLICY_LINK = (
    b'</docs#section/Deprecation-policy>; rel="deprecation"; type="text/html"'
)


class DeprecationHeaderMiddleware:
    """Attach RFC 9745 `Deprecation` + `Link` headers to deprecated routes.

    Pure ASGI so we can mutate the outgoing `http.response.start` message
    without buffering the response body.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message: Message):
            if message["type"] == "http.response.start":
                route = scope.get("route")
                if route is not None and getattr(route, "deprecated", False):
                    headers = list(message.get("headers", []))
                    headers.append((DEPRECATION_HEADER_NAME, b"true"))
                    headers.append((LINK_HEADER_NAME, DEPRECATION_POLICY_LINK))
                    message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)
