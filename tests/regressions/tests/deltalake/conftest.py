"""Fixtures for Delta Lake cross-version compatibility tests.

Provides:
- MinIO testcontainer (session-scoped)
- S3 storage options
- Reference and current virtual environments
- Graphsense config generation
"""

import os
import time
import urllib.request

import boto3
import pytest
from testcontainers.core.container import DockerContainer

from tests.deltalake.config import DeltaTestConfig, get_minio_storage_options
from tests.deltalake.venv_manager import (
    get_or_create_current_venv,
    get_or_create_reference_venv,
)


MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "test"


def pytest_collection_modifyitems(config, items):
    """Skip all deltalake-marked tests early when NODE_URL is not set."""
    node_url = os.environ.get("NODE_URL", "")
    if node_url:
        return

    skip_marker = pytest.mark.skip(
        reason="NODE_URL not set — required for Delta Lake ingestion test"
    )
    for item in items:
        if "deltalake" in item.keywords:
            item.add_marker(skip_marker)


@pytest.fixture(scope="session")
def delta_config() -> DeltaTestConfig:
    """Load test configuration from environment variables."""
    return DeltaTestConfig()


@pytest.fixture(scope="session")
def minio_container():
    """Start a MinIO container for the test session."""
    container = (
        DockerContainer("minio/minio:latest")
        .with_exposed_ports(9000)
        .with_env("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .with_command("server /data")
    )
    container.start()

    # Poll health endpoint until MinIO is ready
    host = container.get_container_host_ip()
    port = container.get_exposed_port(9000)
    health_url = f"http://{host}:{port}/minio/health/live"
    for _ in range(30):
        time.sleep(1)
        try:
            urllib.request.urlopen(health_url, timeout=2)
            break
        except Exception:
            continue
    else:
        raise RuntimeError("MinIO did not become ready in 30s")

    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container) -> dict[str, str]:
    """Return MinIO connection details as a dict."""
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)
    endpoint = f"http://{host}:{port}"

    config = {
        "endpoint": endpoint,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "bucket": MINIO_BUCKET,
    }

    # Create the test bucket via boto3 (no mc CLI dependency)
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=MINIO_BUCKET)

    return config


@pytest.fixture(scope="session")
def storage_options(minio_config) -> dict[str, str]:
    """AWS-compatible storage options for deltalake / pyarrow."""
    return get_minio_storage_options(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )


@pytest.fixture(scope="session")
def reference_venv(delta_config):
    """Create (or reuse cached) virtual environment for the reference version."""
    return get_or_create_reference_venv(delta_config.ref_version)


@pytest.fixture(scope="session")
def current_venv(delta_config):
    """Create (or reuse cached) virtual environment for the current version."""
    return get_or_create_current_venv(delta_config.gslib_path)
