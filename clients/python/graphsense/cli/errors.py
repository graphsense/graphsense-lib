"""Friendly handling of REST errors raised from inside CLI commands.

The generated client raises subclasses of `graphsense.exceptions.ApiException`
on non-2xx responses (404 → NotFoundException, 401 → UnauthorizedException,
etc.). Without intervention these surface as raw tracebacks. Wrapping the
root command's `invoke` lets us print a one-line stderr message with the
relevant detail and exit with a non-zero code.
"""

from __future__ import annotations

import json
from typing import Any

import rich_click as click

from graphsense.exceptions import ApiException

# 4xx are user/input errors (exit 1); 5xx are server-side (exit 2).
_EXIT_CLIENT_ERROR = 1
_EXIT_SERVER_ERROR = 2
_EXIT_OTHER_ERROR = 1


def _detail_from_body(body: Any) -> str | None:
    """Extract a `detail` field from a JSON error body if present."""
    if not body:
        return None
    if isinstance(body, (bytes, bytearray)):
        try:
            body = body.decode("utf-8")
        except Exception:
            return None
    if isinstance(body, str):
        try:
            parsed = json.loads(body)
        except Exception:
            return body.strip() or None
        body = parsed
    if isinstance(body, dict):
        detail = body.get("detail") or body.get("message") or body.get("error")
        if isinstance(detail, str):
            return detail
    return None


def format_api_error(exc: ApiException) -> str:
    status = getattr(exc, "status", None) or "?"
    reason = getattr(exc, "reason", None) or "API error"
    detail = _detail_from_body(getattr(exc, "body", None))
    if detail:
        return f"Error {status} ({reason}): {detail}"
    return f"Error {status} ({reason})"


def exit_code_for(exc: ApiException) -> int:
    status = getattr(exc, "status", None)
    if isinstance(status, int):
        if 400 <= status < 500:
            return _EXIT_CLIENT_ERROR
        if 500 <= status < 600:
            return _EXIT_SERVER_ERROR
    return _EXIT_OTHER_ERROR


class FriendlyErrorGroup(click.RichGroup):
    """Top-level group that converts `ApiException` into a friendly error."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except ApiException as exc:
            click.echo(format_api_error(exc), err=True)
            ctx.exit(exit_code_for(exc))
