"""Fixtures for Delta Lake cross-version compatibility tests.

Provides:
- MinIO testcontainer (session-scoped)
- S3 storage options
- Reference and current virtual environments
- Per-currency test parametrization via .graphsense.yaml
"""

import time
import urllib.request

import boto3
import pytest
from testcontainers.core.container import DockerContainer

from tests.deltalake.config import build_delta_configs, get_minio_storage_options
from tests.deltalake.venv_manager import (
    get_or_create_current_venv,
    get_or_create_reference_venv,
    get_venv_package_versions,
)


MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "test"


def pytest_collection_modifyitems(config, items):
    """Skip all deltalake tests when no currencies have node URLs configured."""
    configs = build_delta_configs()
    if configs:
        return

    skip_marker = pytest.mark.skip(
        reason="No currencies with node URLs found in .graphsense.yaml"
    )
    for item in items:
        if "deltalake" in item.keywords:
            item.add_marker(skip_marker)


def pytest_generate_tests(metafunc):
    """Parametrize delta_config over all configured currencies."""
    if "delta_config" not in metafunc.fixturenames:
        return
    configs = build_delta_configs()
    metafunc.parametrize(
        "delta_config", configs, ids=[c.currency for c in configs]
    )


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
def reference_venv():
    """Create (or reuse cached) virtual environment for the reference version."""
    configs = build_delta_configs()
    ref_version = configs[0].ref_version if configs else "v25.11.18"
    return get_or_create_reference_venv(ref_version)


@pytest.fixture(scope="session")
def current_venv():
    """Create (or reuse cached) virtual environment for the current version."""
    configs = build_delta_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No delta configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def ref_package_versions(reference_venv) -> dict[str, str]:
    """Package versions from the reference venv (shared across currencies)."""
    return get_venv_package_versions(reference_venv)


@pytest.fixture(scope="session")
def current_package_versions(current_venv) -> dict[str, str]:
    """Package versions from the current venv (shared across currencies)."""
    return get_venv_package_versions(current_venv)
