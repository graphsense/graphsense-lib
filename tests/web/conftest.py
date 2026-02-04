import os
import subprocess
import tempfile
from os import environ
from pathlib import Path

import docker
import pytest
import yaml
from testcontainers.cassandra import CassandraContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from tests.web import BaseTestCase
from tests.web.cassandra.insert import load_test_data as cas_load_test_data
from tests.web.tagstore.insert import load_test_data as tags_load_test_data
from tests.web.version_utils import get_baseline_image

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


@pytest.fixture(scope="session")
def baseline_server_url(gs_rest_db_setup):
    """Start baseline server container connected to test databases.

    This fixture starts a Docker container running the previous stable version
    of the REST API, configured to use the same test databases as the current
    code. This enables regression testing between versions.

    The baseline version is determined by:
    1. BASELINE_VERSION env var (explicit override)
    2. Previous stable git tag (auto-detected)

    Set SKIP_BASELINE_CONTAINER=1 to skip this fixture.
    """
    if environ.get("SKIP_BASELINE_CONTAINER"):
        pytest.skip("Baseline container disabled via SKIP_BASELINE_CONTAINER")

    if environ.get("SKIP_REST_CONTAINER_SETUP"):
        pytest.skip("Container setup disabled via SKIP_REST_CONTAINER_SETUP")

    # Generate config pointing to test containers
    config = BaseTestCase.config
    config_content = yaml.dump(config)

    # Write config to temp file
    config_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, prefix="gs_baseline_config_"
    )
    config_file.write(config_content)
    config_file.close()

    # Start baseline container with host networking
    image = get_baseline_image()
    baseline_port = environ.get("GS_REST_BASELINE_PORT", "9001")

    container = (
        DockerContainer(image)
        .with_network_mode("host")  # Access test containers via localhost
        .with_volume_mapping(config_file.name, "/config.yaml", "ro")
        .with_env("CONFIG_FILE", "/config.yaml")
        .with_env("GS_REST_PORT", baseline_port)
        .with_env("NUM_WORKERS", "1")
        .with_env("NUM_THREADS", "1")
    )
    container.start()
    wait_for_logs(container, "Application startup complete", timeout=120)

    yield f"http://localhost:{baseline_port}"

    container.stop()
    os.unlink(config_file.name)


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
