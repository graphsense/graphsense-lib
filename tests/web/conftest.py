import logging
from os import environ

import pytest
import pytest_asyncio
from docker.errors import NotFound
from starlette.testclient import TestClient
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from graphsenselib.web.app import create_app
from graphsenselib.web.config import GSRestConfig
from tests.web.cassandra.insert import load_test_data as cas_load_test_data
from tests.web.tagstore.insert import load_test_data as tags_load_test_data

from tests.conftest import DANGEROUSLY_ACCELERATE_TESTS, create_web_schemas

# Web-specific containers (not shared with root tests)
postgres = PostgresContainer("postgres:16-alpine")
redis = RedisContainer("redis:7-alpine")


def _stop_container(container, name: str) -> None:
    try:
        container.stop()
    except NotFound:
        return
    except Exception:
        wrapped = None
        try:
            wrapped = container.get_wrapped_container()
        except Exception:
            wrapped = None

        container_id = getattr(wrapped, "id", None)
        logging.getLogger(__name__).exception(
            "Failed to stop %s test container%s",
            name,
            f" ({container_id[:12]})" if container_id else "",
        )
        raise


@pytest.fixture(scope="session")
def gs_rest_db_setup(gs_db_setup):
    """Set up all web test infrastructure. Depends on gs_db_setup for Cassandra."""
    SKIP_REST_CONTAINER_SETUP = environ.get("SKIP_REST_CONTAINER_SETUP", False)
    if SKIP_REST_CONTAINER_SETUP:
        return

    cas_host, cas_port = gs_db_setup

    postgres.start()
    redis.start()

    postgres_sync_url = postgres.get_connection_url()
    portgres_async_url = postgres_sync_url.replace("psycopg2", "asyncpg")

    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = f"redis://{redis_host}:{redis_port}"

    config = {
        "logging": {"level": "DEBUG"},
        "database": {
            "driver": "cassandra",
            "port": cas_port,
            "nodes": [cas_host],
            "strict_data_validation": False,
            "currencies": {
                "btc": {
                    "raw": "resttest_btc_raw",
                    "transformed": "resttest_btc_transformed",
                },
                "ltc": {
                    "raw": "resttest_ltc_raw",
                    "transformed": "resttest_ltc_transformed",
                },
                "eth": {
                    "raw": "resttest_eth_raw",
                    "transformed": "resttest_eth_transformed",
                },
                "trx": {
                    "raw": "resttest_trx_raw",
                    "transformed": "resttest_trx_transformed",
                },
            },
        },
        "gs-tagstore": {"url": portgres_async_url},
        "show_private_tags": {"on_header": {"Authorization": "x"}},
        "tag_access_logger": {"redis_url": redis_url},
    }

    if not DANGEROUSLY_ACCELERATE_TESTS:
        create_web_schemas(cas_host, cas_port)

    cas_load_test_data(cas_host, cas_port)
    tags_load_test_data(postgres_sync_url.replace("+psycopg2", ""))

    try:
        yield config
    finally:
        _stop_container(redis, "redis")
        _stop_container(postgres, "postgres")


@pytest.fixture(scope="session")
def client(gs_rest_db_setup):
    """Session-scoped sync test client. Created once, reused across all web tests."""
    config = gs_rest_db_setup
    logging.getLogger("uvicorn.error").setLevel("ERROR")
    logging.getLogger("uvicorn.access").setLevel("ERROR")
    fastapi_app = create_app(
        config=GSRestConfig.from_dict(config),
        validate_responses=True,
    )
    with TestClient(fastapi_app) as c:
        c.app_state = fastapi_app.state
        yield c


# NOTE: Regression test fixtures (baseline_server_url) moved to iknaio-tests-nightly


@pytest_asyncio.fixture
async def redis_client(gs_rest_db_setup):
    """Provide an async Redis client for tests."""
    from redis import asyncio as aioredis

    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = f"redis://{redis_host}:{redis_port}"

    client = await aioredis.from_url(redis_url)
    yield client
    await client.flushdb()
    await client.aclose()
