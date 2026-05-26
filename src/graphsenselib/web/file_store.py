"""Reusable Redis-backed file store for serving downloadable artifacts.

A web handler that produces a file (e.g. the MCP ``build_pathfinder_file``
tool) can stash the bytes here and hand the user a short-lived download link
instead of streaming the whole payload back inline. Files live in Redis with a
TTL, so the store works correctly across multiple REST workers and cleans up
after itself.

The download URL carries an unguessable random token as its only credential
(256 bits of CSPRNG entropy). It is therefore safe to expose the ``/download``
route without API-key auth — which is required, since a plain browser click
cannot send an API key.
"""

from __future__ import annotations

import logging
import re
import secrets
from typing import Optional, Protocol, runtime_checkable

from pydantic import BaseModel
from starlette.requests import Request

logger = logging.getLogger(__name__)


class FileTooLargeError(Exception):
    """Raised by ``FileStore.put`` when the payload exceeds the size cap."""


class StoredFile(BaseModel):
    """A file retrieved from the store, ready to be served."""

    filename: str
    content_type: str
    data: bytes


@runtime_checkable
class FileStore(Protocol):
    """Minimal interface a download-capable file store must provide.

    Kept as a Protocol so tests can substitute an in-memory fake and other web
    features can depend on the interface rather than the concrete Redis class.
    """

    async def put(self, data: bytes, *, filename: str, content_type: str) -> str:
        """Store ``data`` and return its opaque token. Raises FileTooLargeError."""
        ...

    async def get(self, token: str) -> Optional[StoredFile]:
        """Return the stored file, or None if the token is unknown/expired."""
        ...

    def url_for(self, request: Request, token: str) -> str:
        """Build the absolute, publicly reachable download URL for ``token``."""
        ...


# secrets.token_urlsafe(32) yields a 43-char base64url string carrying 256
# bits of entropy. Validate the shape before touching Redis so a malformed
# path segment is rejected cheaply (and uniformly — same 404 as not-found).
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{16,86}$")


def _first_header_value(raw: Optional[str]) -> Optional[str]:
    """X-Forwarded-* headers may carry a comma-separated list; take the first."""
    if not raw:
        return None
    first = raw.split(",")[0].strip()
    return first or None


def _decode(value: Optional[bytes]) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


class RedisFileStore:
    """Redis-backed :class:`FileStore`. Each file is one Redis hash with a TTL.

    The hash holds the filename, content type and raw bytes; ``EXPIRE`` gives
    the TTL. Keys are namespaced by ``key_prefix`` so the store can share a
    Redis instance with other features (locking, tag-access logging).
    """

    def __init__(
        self,
        redis_client,
        *,
        key_prefix: str,
        ttl_s: int,
        max_bytes: int,
        download_path: str,
        base_url: Optional[str] = None,
    ) -> None:
        self._redis = redis_client
        self._key_prefix = key_prefix
        self._ttl_s = ttl_s
        self._max_bytes = max_bytes
        self._download_path = "/" + download_path.strip("/")
        self._base_url = base_url.rstrip("/") if base_url else None

    def _key(self, token: str) -> str:
        return f"{self._key_prefix}{token}"

    async def put(self, data: bytes, *, filename: str, content_type: str) -> str:
        if len(data) > self._max_bytes:
            raise FileTooLargeError(
                f"file is {len(data)} bytes, exceeds the {self._max_bytes}-byte limit"
            )
        # 256-bit CSPRNG token — unguessable, so the URL alone is the credential.
        token = secrets.token_urlsafe(32)
        key = self._key(token)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                key,
                mapping={
                    "filename": filename,
                    "content_type": content_type,
                    "data": data,
                },
            )
            pipe.expire(key, self._ttl_s)
            await pipe.execute()
        return token

    async def get(self, token: str) -> Optional[StoredFile]:
        if not _TOKEN_RE.match(token):
            return None
        raw = await self._redis.hgetall(self._key(token))
        if not raw:
            return None
        return StoredFile(
            filename=_decode(raw.get(b"filename")),
            content_type=_decode(raw.get(b"content_type")),
            data=raw.get(b"data") or b"",
        )

    def url_for(self, request: Request, token: str) -> str:
        path = f"{self._download_path}/{token}"
        if self._base_url:
            return f"{self._base_url}{path}"
        host = (
            _first_header_value(request.headers.get("x-forwarded-host"))
            or request.headers.get("host")
            or request.url.netloc
        )
        scheme = (
            _first_header_value(request.headers.get("x-forwarded-proto"))
            or request.url.scheme
        )
        # TEMPORARY DIAGNOSTIC — remove once we've confirmed which input
        # (X-Forwarded-Proto vs request.url.scheme) is producing the wrong
        # http:// scheme on api.test.iknaio.com.
        logger.info(
            "file_store url_for: xf-proto=%r xf-host=%r host=%r "
            "request.url.scheme=%r netloc=%r -> %s://%s%s",
            request.headers.get("x-forwarded-proto"),
            request.headers.get("x-forwarded-host"),
            request.headers.get("host"),
            request.url.scheme,
            request.url.netloc,
            scheme,
            host,
            path,
        )
        return f"{scheme}://{host}{path}"

    async def aclose(self) -> None:
        await self._redis.aclose()
