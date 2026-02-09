from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send


class PluginMiddleware:
    """Middleware that invokes plugin before_request hooks.

    Note: before_response hooks are handled at the route level via decorators
    since middleware only sees serialized bytes, not the response objects.

    Uses pure ASGI for better performance (avoids response buffering).
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] == "http":
            # Initialize state dict if not present
            if "state" not in scope:
                scope["state"] = {}

            # Initialize request state for plugin data
            scope["state"]["show_private_tags"] = False
            scope["state"]["header_modifications"] = {}

            # Create a Request object to access app.state
            request = Request(scope)

            # Get plugins from app state (may not be set during startup)
            plugins = getattr(request.app.state, "plugins", [])
            plugin_contexts = getattr(request.app.state, "plugin_contexts", {})

            # Execute before_request hooks
            for plugin in plugins:
                if hasattr(plugin, "before_request"):
                    ctx = plugin_contexts.get(plugin.__module__, {})
                    header_mods = plugin.before_request(ctx, request)
                    if header_mods:
                        scope["state"]["header_modifications"].update(header_mods)

        await self.app(scope, receive, send)
