"""Install a one-shot stderr warning whenever the server reports that a
called endpoint is deprecated (via RFC 8594 `Deprecation` / `Sunset` headers).

Hook strategy: monkey-patch the bound `call_api` method on a given ApiClient
instance so we can inspect the RESTResponse headers. This is non-invasive —
it does not touch generated code and is opt-in per ApiClient.
"""

from __future__ import annotations

import sys
import threading
from typing import TYPE_CHECKING, Any, Callable, Union

if TYPE_CHECKING:
    from graphsense.api_client import ApiClient


_SENTINEL_ATTR = "_graphsense_ext_deprecation_installed"

StreamOrGetter = Union[Any, Callable[[], Any], None]


def install(
    api_client: "ApiClient",
    *,
    stream: StreamOrGetter = None,
    quiet: bool = False,
) -> None:
    """Wrap api_client.call_api to emit stderr warnings on Deprecation headers.

    `stream` may be:
      * None — write to `sys.stderr` at call time
      * a callable — invoked each write to resolve the current stream (useful
        when a test harness captures stderr with a stream the caller can only
        obtain lazily, e.g. click.get_text_stream('stderr'))
      * a file-like object — used directly

    Safe to call twice on the same client — the second call is a no-op.
    """
    if getattr(api_client, _SENTINEL_ATTR, False):
        return

    seen: set[str] = set()
    lock = threading.Lock()
    original: Callable = api_client.call_api

    def _resolve_stream() -> Any:
        if stream is None:
            return sys.stderr
        if callable(stream):
            return stream()
        return stream

    def wrapped(method, url, *args, **kwargs):
        response = original(method, url, *args, **kwargs)
        if quiet:
            return response
        try:
            headers = response.headers or {}
            deprecated = headers.get("Deprecation")
            if deprecated:
                sunset = headers.get("Sunset", "unknown")
                key = f"{method} {url.split('?')[0]}"
                with lock:
                    if key not in seen:
                        seen.add(key)
                        print(
                            f"warning: {key} is deprecated (sunset: {sunset})",
                            file=_resolve_stream(),
                        )
        except Exception:
            # never let the warning machinery break a real call
            pass
        return response

    # Replace the bound method. Type checkers (ty, mypy) flag the structural
    # mismatch between our *args/**kwargs wrapper and the generated
    # ApiClient.call_api's fixed signature; the substitution is intentional.
    api_client.call_api = wrapped  # type: ignore[method-assign,assignment]  # ty: ignore[invalid-assignment]
    setattr(api_client, _SENTINEL_ATTR, True)
