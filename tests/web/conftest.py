import subprocess
from os import environ
from pathlib import Path

import docker
import pytest
from testcontainers.cassandra import CassandraContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from tests.web import BaseTestCase
from tests.web.cassandra.insert import load_test_data as cas_load_test_data
from tests.web.tagstore.insert import load_test_data as tags_load_test_data

postgres = PostgresContainer("postgres:16-alpine")
redis = RedisContainer("redis:7-alpine")

# Pre-baked Cassandra image with schemas and fast startup settings already configured
# Build with: make build-test-cassandra
# Baked-in optimizations: NUM_TOKENS=1, ring_delay_ms=100, skip_wait_for_gossip=0
CASSANDRA_TEST_IMAGE = environ.get(
    "CASSANDRA_TEST_IMAGE", "graphsense/cassandra-test:4.1.4"
)


def ensure_cassandra_image_exists():
    """Build Cassandra test image if it doesn't exist locally."""
    client = docker.from_env()
    try:
        client.images.get(CASSANDRA_TEST_IMAGE)
    except docker.errors.ImageNotFound:
        dockerfile_path = Path(__file__).parent / "cassandra"
        subprocess.run(
            ["docker", "build", "-t", CASSANDRA_TEST_IMAGE, str(dockerfile_path)],
            check=True,
        )


ensure_cassandra_image_exists()
cassandra = CassandraContainer(CASSANDRA_TEST_IMAGE)


@pytest.fixture(scope="session", autouse=True)
def gs_rest_db_setup(request):
    SKIP_REST_CONTAINER_SETUP = environ.get("SKIP_REST_CONTAINER_SETUP", False)
    if SKIP_REST_CONTAINER_SETUP:
        return

    postgres.start()
    cassandra.start()
    redis.start()

    def remove_container():
        postgres.stop()
        cassandra.stop()
        redis.stop()

    request.addfinalizer(remove_container)

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
