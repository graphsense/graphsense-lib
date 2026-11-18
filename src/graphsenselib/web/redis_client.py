"""Async Redis clients for the REST API (tag access logger, file store).

``redis.asyncio.from_url()`` builds its connection pool without a ``Retry``:
unlike ``Redis(host=...)`` (10 retries since redis-py 6), a client made from a
URL retries **zero** times. After a Redis restart every pooled connection is
dead, and each one fails exactly one request with ``ConnectionError:
Connection closed by server`` before it reconnects, so the API answers 500s
until all pooled connections have been used once. Build the clients here
instead: a short retry with backoff on connection errors and timeouts, and a
health check that pings a connection idle for longer than
``HEALTH_CHECK_INTERVAL_S`` before using it.
"""

from urllib.parse import urlsplit, urlunsplit

from redis import asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import ConnectionError, TimeoutError

HEALTH_CHECK_INTERVAL_S = 30
RETRIES = 3
BACKOFF_BASE_S = 0.1
BACKOFF_CAP_S = 2.0


def make_async_redis(url: str) -> aioredis.Redis:
    """An async Redis client for ``url`` that survives a Redis restart."""
    return aioredis.from_url(
        url,
        health_check_interval=HEALTH_CHECK_INTERVAL_S,
        retry=Retry(
            ExponentialWithJitterBackoff(base=BACKOFF_BASE_S, cap=BACKOFF_CAP_S),
            RETRIES,
        ),
        retry_on_error=[ConnectionError, TimeoutError],
    )


def redact_redis_url(url: str) -> str:
    """``url`` without user and password, for log lines."""
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit(parts._replace(netloc=f"***@{host}"))
