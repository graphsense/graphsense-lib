"""Middleware that advertises deprecation on responses from deprecated routes.

Implements RFC 9745 (`Deprecation` header), RFC 8594 (`Sunset` header), and a
companion `Link` header with `rel="deprecation"` pointing to the human-readable
deprecation policy, for any route whose FastAPI `deprecated=True` flag is set.
Clients and gateways can detect the headers and warn/log without having to
inspect the OpenAPI schema.

Per-route sunset dates live next to the route definition via
`openapi_extra={"x-sunset": "YYYY-MM-DD"}`. The middleware reads that value,
converts it to an RFC 7231 HTTP-date once (cached), and emits the `Sunset`
header. Routes that are `deprecated=True` without an `x-sunset` still get the
`Deprecation` and `Link` headers but no `Sunset`.
"""

from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Optional

from starlette.types import ASGIApp, Message, Receive, Scope, Send

DEPRECATION_HEADER_NAME = b"deprecation"
LINK_HEADER_NAME = b"link"
SUNSET_HEADER_NAME = b"sunset"
DEPRECATION_POLICY_LINK = (
    b'</docs#section/Deprecation-policy>; rel="deprecation"; type="text/html"'
)

# OpenAPI vendor-extension key used by deprecated routes to carry their sunset
# date. Value format: ISO-8601 calendar date (e.g. "2026-10-31"), interpreted
# as UTC midnight.
SUNSET_OPENAPI_EXTRA_KEY = "x-sunset"

# Cache of formatted HTTP-date headers, keyed on the ISO date string from
# `openapi_extra["x-sunset"]`, so we only parse/format each distinct date once.
_sunset_header_cache: dict[str, bytes] = {}


def _sunset_header_value(iso_date: str) -> Optional[bytes]:
    """Return the cached HTTP-date bytes for an ISO-8601 date, or None if invalid."""
    cached = _sunset_header_cache.get(iso_date)
    if cached is not None:
        return cached
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    header = format_datetime(dt, usegmt=True).encode("ascii")
    _sunset_header_cache[iso_date] = header
    return header


class DeprecationHeaderMiddleware:
    """Attach RFC 9745 `Deprecation`, RFC 8594 `Sunset`, and `Link` headers to
    responses from deprecated routes.

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
                    extra = getattr(route, "openapi_extra", None) or {}
                    iso_sunset = extra.get(SUNSET_OPENAPI_EXTRA_KEY)
                    if iso_sunset:
                        sunset_value = _sunset_header_value(iso_sunset)
                        if sunset_value is not None:
                            headers.append((SUNSET_HEADER_NAME, sunset_value))
                    message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)
