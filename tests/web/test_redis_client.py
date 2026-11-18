"""make_async_redis: clients that survive a Redis restart (2026-11-18)."""

import pytest
from redis import asyncio as aioredis
from redis.exceptions import ConnectionError

from graphsenselib.web.redis_client import (
    HEALTH_CHECK_INTERVAL_S,
    RETRIES,
    make_async_redis,
    redact_redis_url,
)


def test_from_url_alone_does_not_retry():
    """The trap the helper exists for: from_url() pools retry zero times."""
    conn = aioredis.from_url("redis://127.0.0.1:1").connection_pool.make_connection()
    assert conn.retry.get_retries() == 0


def test_client_retries_and_health_checks():
    conn = make_async_redis("redis://127.0.0.1:1").connection_pool.make_connection()
    assert conn.retry.get_retries() == RETRIES
    assert conn.health_check_interval == HEALTH_CHECK_INTERVAL_S


@pytest.mark.parametrize(
    "url,expected",
    [
        ("redis://localhost:6379/0", "redis://localhost:6379/0"),
        ("redis://:pw@redis:6379/1", "redis://***@redis:6379/1"),
        ("rediss://user:pw@redis:6380", "rediss://***@redis:6380"),
        ("redis://u:p@ss@redis", "redis://***@redis"),
    ],
)
def test_redact_redis_url(url, expected):
    assert redact_redis_url(url) == expected


def _url():
    from tests.web.conftest import redis_container

    redis = redis_container()
    return f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}"


@pytest.mark.asyncio
async def test_survives_killed_connections(redis_client):
    """Server-side kill of the pooled connection, as a Redis restart does."""
    url = _url()
    plain = aioredis.from_url(url)
    robust = make_async_redis(url)
    try:
        await plain.set("rc:plain", 1)
        await robust.set("rc:robust", 1)

        killed = await redis_client.client_kill_filter(_type="normal", skipme=True)
        assert killed >= 2

        with pytest.raises(ConnectionError):
            await plain.incr("rc:plain")
        assert await robust.incr("rc:robust") == 2
    finally:
        await plain.aclose()
        await robust.aclose()
