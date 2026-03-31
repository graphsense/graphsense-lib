"""Shared fixture factories and pytest hook helpers for regression tests.

These are plain functions (not fixtures). Each module's conftest.py
imports them and wraps them in thin @pytest.fixture definitions.
"""

import time
import urllib.request

import boto3
import pytest
from testcontainers.cassandra import CassandraContainer
from testcontainers.core.container import DockerContainer

from tests.lib.config import get_minio_storage_options
from tests.lib.constants import (
    MINIO_ACCESS_KEY,
    MINIO_CONTAINER_PORT,
    MINIO_HEALTH_TIMEOUT_S,
    MINIO_IMAGE,
    MINIO_SECRET_KEY,
    VANILLA_CASSANDRA_IMAGE,
)


# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------

def start_minio_container() -> DockerContainer:
    """Start a MinIO container and wait for it to become healthy."""
    container = (
        DockerContainer(MINIO_IMAGE)
        .with_exposed_ports(MINIO_CONTAINER_PORT)
        .with_env("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .with_command("server /data")
    )
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(MINIO_CONTAINER_PORT)
    health_url = f"http://{host}:{port}/minio/health/live"
    for _ in range(MINIO_HEALTH_TIMEOUT_S):
        time.sleep(1)
        try:
            urllib.request.urlopen(health_url, timeout=2)
            return container
        except Exception:
            continue

    container.stop()
    raise RuntimeError(f"MinIO did not become ready in {MINIO_HEALTH_TIMEOUT_S}s")


def make_minio_config(container: DockerContainer, bucket: str) -> dict[str, str]:
    """Extract connection details from a running MinIO container and create the bucket."""
    host = container.get_container_host_ip()
    port = container.get_exposed_port(MINIO_CONTAINER_PORT)
    endpoint = f"http://{host}:{port}"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=bucket)

    return {
        "endpoint": endpoint,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "bucket": bucket,
    }


def make_storage_options(minio_config: dict[str, str]) -> dict[str, str]:
    """Build deltalake/pyarrow storage options from a minio_config dict."""
    return get_minio_storage_options(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )


# ---------------------------------------------------------------------------
# Cassandra
# ---------------------------------------------------------------------------

def start_cassandra_container(
    image: str = VANILLA_CASSANDRA_IMAGE,
) -> CassandraContainer:
    """Start a Cassandra container."""
    container = CassandraContainer(image)
    container.start()
    return container


def get_cassandra_coords(container: CassandraContainer) -> tuple[str, int]:
    """Extract (host, port) from a running Cassandra container."""
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(9042))
    return host, port


# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------

def skip_if_no_configs(items, keyword: str, build_fn):
    """Skip tests matching *keyword* when build_fn() returns no configs.

    Use in ``pytest_collection_modifyitems``:

        def pytest_collection_modifyitems(config, items):
            skip_if_no_configs(items, "cassandra", build_cassandra_configs)
    """
    if build_fn():
        return

    skip_marker = pytest.mark.skip(
        reason="No currencies with node URLs found in .graphsense.yaml"
    )
    for item in items:
        if keyword in item.keywords or keyword in str(item.path):
            item.add_marker(skip_marker)


def parametrize_configs(metafunc, fixture_name: str, build_fn):
    """Parametrize *fixture_name* over configs from build_fn().

    Use in ``pytest_generate_tests``:

        def pytest_generate_tests(metafunc):
            parametrize_configs(metafunc, "cassandra_config", build_cassandra_configs)
    """
    if fixture_name not in metafunc.fixturenames:
        return
    configs = build_fn()
    metafunc.parametrize(
        fixture_name, configs, ids=[c.test_id for c in configs]
    )
