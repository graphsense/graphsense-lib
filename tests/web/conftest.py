from os import environ

import pytest
from docker.errors import ImageNotFound, NotFound
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from tests.web import BaseTestCase
from tests.web.cassandra.insert import load_test_data as cas_load_test_data
from tests.web.tagstore.insert import load_test_data as tags_load_test_data

# Import shared Cassandra container and utilities from root conftest
# NOTE: postgres is NOT shared because web tests use different tagstore data
from tests.conftest import (
    cassandra,
    USE_FAST_CASSANDRA,
    FAST_CASSANDRA_IMAGE,
    create_web_schemas,
)

# Web-specific containers (not shared with root tests)
postgres = PostgresContainer("postgres:16-alpine")
redis = RedisContainer("redis:7-alpine")


@pytest.fixture(scope="session", autouse=True)
def gs_rest_db_setup(request):
    SKIP_REST_CONTAINER_SETUP = environ.get("SKIP_REST_CONTAINER_SETUP", False)
    if SKIP_REST_CONTAINER_SETUP:
        return

    # Track if we started cassandra (it may already be running from root conftest)
    started_cassandra = False

    # Start Cassandra (shared with root tests, may already be running)
    if not cassandra._container:
        try:
            cassandra.start()
            started_cassandra = True
        except ImageNotFound as e:
            if USE_FAST_CASSANDRA and "graphsense/cassandra-test" in str(e):
                raise RuntimeError(
                    f"Fast Cassandra image not found: {FAST_CASSANDRA_IMAGE}\n"
                    "You need to build it first with: make build-fast-cassandra\n"
                    "Or run tests with vanilla Cassandra: make test-web (slower)"
                ) from e
            raise

    # Start web-specific containers
    postgres.start()
    redis.start()

    def remove_containers():
        # Always stop web-specific containers
        try:
            redis.stop()
        except NotFound:
            pass
        try:
            postgres.stop()
        except NotFound:
            pass
        # Only stop cassandra if we started it
        if started_cassandra and cassandra._container:
            try:
                cassandra.stop()
            except NotFound:
                pass

    request.addfinalizer(remove_containers)

    cas_host = cassandra.get_container_host_ip()
    cas_port = cassandra.get_exposed_port(9042)

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

    # Ugly hack to pass parameters
    BaseTestCase.config = config

    # Create schemas if using vanilla Cassandra (slow mode)
    if not USE_FAST_CASSANDRA:
        create_web_schemas(cas_host, cas_port)

    cas_load_test_data(cas_host, cas_port)

    tags_load_test_data(postgres_sync_url.replace("+psycopg2", ""))

    return config


# NOTE: Regression test fixtures (baseline_server_url) moved to iknaio-tests-nightly


@pytest.fixture
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
