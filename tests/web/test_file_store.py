"""Tests for the reusable Redis-backed download file store.

Three groups:
  * RedisFileStore put/get/TTL/size-cap against a real Redis (testcontainer);
  * url_for derivation (pure — X-Forwarded-* / Host / base_url override);
  * the /download route wired by _register_download_route.
"""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from redis import asyncio as aioredis
from starlette.requests import Request

from graphsenselib.web.app import _register_download_route
from graphsenselib.web.config import FileStoreConfig
from graphsenselib.web.file_store import FileTooLargeError, RedisFileStore
from tests.web.conftest import RedisContainer


# --------------------------------------------------------------------------
# Redis-backed store (needs Docker)
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _file_store_redis_url():
    """A dedicated Redis container for this module (no Cassandra/Postgres)."""
    container = RedisContainer("redis:7-alpine")
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}"
    finally:
        container.stop()


@pytest_asyncio.fixture
async def redis_store(_file_store_redis_url):
    """A RedisFileStore (max 1 KiB, 60 s TTL) plus its raw client."""
    client = await aioredis.from_url(_file_store_redis_url)
    store = RedisFileStore(
        client,
        key_prefix="test:file:",
        ttl_s=60,
        max_bytes=1024,
        download_path="/download",
    )
    yield store, client
    await client.flushdb()
    await store.aclose()


async def test_put_get_roundtrip(redis_store):
    store, _ = redis_store
    token = await store.put(
        b"some-gs-bytes",
        filename="graph.gs",
        content_type="application/octet-stream",
    )
    stored = await store.get(token)
    assert stored is not None
    assert stored.data == b"some-gs-bytes"
    assert stored.filename == "graph.gs"
    assert stored.content_type == "application/octet-stream"


async def test_get_unknown_token_returns_none(redis_store):
    store, _ = redis_store
    assert await store.get("a" * 43) is None


async def test_get_malformed_token_returns_none(redis_store):
    """Wrong-charset / too-short tokens are rejected before touching Redis."""
    store, _ = redis_store
    assert await store.get("../etc/passwd") is None
    assert await store.get("short") is None


async def test_put_rejects_oversize_payload(redis_store):
    store, _ = redis_store  # max_bytes == 1024
    with pytest.raises(FileTooLargeError):
        await store.put(
            b"x" * 1025,
            filename="big.gs",
            content_type="application/octet-stream",
        )


async def test_stored_file_has_ttl(redis_store):
    store, client = redis_store
    token = await store.put(
        b"ttl-test", filename="t.gs", content_type="application/octet-stream"
    )
    ttl = await client.ttl("test:file:" + token)
    assert 0 < ttl <= 60


async def test_tokens_are_unguessable(redis_store):
    """Tokens carry 256 bits of CSPRNG entropy — long, urlsafe, unique."""
    store, _ = redis_store
    tokens = {
        await store.put(b"x", filename="f.gs", content_type="application/octet-stream")
        for _ in range(20)
    }
    assert len(tokens) == 20
    for tok in tokens:
        assert len(tok) >= 40
        assert all(c.isalnum() or c in "-_" for c in tok)


# --------------------------------------------------------------------------
# url_for derivation (pure — no Redis)
# --------------------------------------------------------------------------


def _request(headers: dict[str, str], *, scheme: str = "http") -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {
        "type": "http",
        "method": "GET",
        "scheme": scheme,
        "server": ("internal-host", 80),
        "path": "/x",
        "query_string": b"",
        "headers": raw,
    }
    return Request(scope)


def _url_store(base_url: str | None = None) -> RedisFileStore:
    return RedisFileStore(
        None,
        key_prefix="t:",
        ttl_s=60,
        max_bytes=1024,
        download_path="/download",
        base_url=base_url,
    )


def test_url_for_uses_base_url_override():
    store = _url_store(base_url="https://cdn.example.com/")
    req = _request({"host": "ignored.example"})
    assert store.url_for(req, "TOK") == "https://cdn.example.com/download/TOK"


def test_url_for_derives_from_x_forwarded_headers():
    store = _url_store()
    req = _request(
        {
            "host": "internal:9000",
            "x-forwarded-host": "public.example.com",
            "x-forwarded-proto": "https",
        }
    )
    assert store.url_for(req, "TOK") == "https://public.example.com/download/TOK"


def test_url_for_falls_back_to_host_header():
    store = _url_store()
    req = _request({"host": "direct.example.com"}, scheme="http")
    assert store.url_for(req, "TOK") == "http://direct.example.com/download/TOK"


def test_url_for_takes_first_of_forwarded_list():
    store = _url_store()
    req = _request(
        {
            "host": "internal",
            "x-forwarded-host": "first.example.com, second.example.com",
            "x-forwarded-proto": "https, http",
        }
    )
    assert store.url_for(req, "TOK") == "https://first.example.com/download/TOK"


# --------------------------------------------------------------------------
# /download route
# --------------------------------------------------------------------------


def _app_with_route(store) -> FastAPI:
    app = FastAPI()
    app.state.file_store = store
    _register_download_route(app, FileStoreConfig(enabled=True))
    return app


async def test_download_route_serves_file(redis_store):
    store, _ = redis_store
    token = await store.put(
        b"hello-gs-bytes",
        filename="graph.gs",
        content_type="application/octet-stream",
    )
    transport = httpx.ASGITransport(app=_app_with_route(store))
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        resp = await client.get(f"/download/{token}")
    assert resp.status_code == 200
    assert resp.content == b"hello-gs-bytes"
    cd = resp.headers["content-disposition"]
    assert "attachment" in cd and "graph.gs" in cd


async def test_download_route_404_for_unknown_and_malformed_token(redis_store):
    store, _ = redis_store
    transport = httpx.ASGITransport(app=_app_with_route(store))
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        unknown = await client.get("/download/" + "a" * 43)
        malformed = await client.get("/download/bad..token")
    assert unknown.status_code == 404
    assert malformed.status_code == 404


def test_download_route_absent_from_openapi():
    app = FastAPI()
    _register_download_route(app, FileStoreConfig(enabled=True))
    paths = app.openapi().get("paths", {})
    assert not any("download" in p for p in paths)


def test_download_route_not_registered_when_disabled():
    app = FastAPI()
    n_before = len(app.router.routes)
    _register_download_route(app, None)
    _register_download_route(app, FileStoreConfig(enabled=False))
    assert len(app.router.routes) == n_before
