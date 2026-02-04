"""Middleware to normalize empty query parameters.

FastAPI/Pydantic will fail when parsing empty string values for typed parameters.
This middleware converts empty query string values to None before FastAPI processes them.
"""

from urllib.parse import parse_qsl, urlencode

from starlette.types import ASGIApp, Receive, Scope, Send


class EmptyQueryParamsMiddleware:
    """Convert empty query string values to omitted parameters.

    This ensures compatibility with the old Connexion/OpenAPI behavior where
    empty string query parameters were treated as not provided.

    Uses pure ASGI for better performance (avoids response buffering).
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] == "http":
            query_string = scope.get("query_string", b"").decode("utf-8")

            if query_string:
                # Parse query params, keeping only non-empty values
                params = parse_qsl(query_string, keep_blank_values=True)
                filtered_params = [(k, v) for k, v in params if v.strip() != ""]

                # Reconstruct query string
                new_query_string = urlencode(filtered_params)

                # Update scope with new query string
                scope["query_string"] = new_query_string.encode("utf-8")

        await self.app(scope, receive, send)
