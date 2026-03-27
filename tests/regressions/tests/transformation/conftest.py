"""Fixtures for transformation regression tests."""

import subprocess

import pytest

from tests.deltalake.venv_manager import get_or_create_current_venv
from tests.lib.conftest_helpers import (
    get_cassandra_coords as _get_cassandra_coords,
    make_minio_config,
    make_storage_options,
    parametrize_configs,
    skip_if_no_configs,
    start_cassandra_container,
    start_minio_container,
)
from tests.transformation.config import build_transformation_configs

TRANSFORMATION_IMAGE_NAME = "gslib-transformation-test:latest"


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "transformation", build_transformation_configs)


def pytest_generate_tests(metafunc):
    parametrize_configs(
        metafunc, "transformation_config", build_transformation_configs
    )


@pytest.fixture(scope="session")
def minio_container():
    container = start_minio_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container):
    return make_minio_config(minio_container, bucket="transformation-test")


@pytest.fixture(scope="session")
def storage_options(minio_config):
    return make_storage_options(minio_config)


@pytest.fixture(scope="session")
def cassandra_container():
    container = start_cassandra_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def cassandra_coords(cassandra_container):
    return _get_cassandra_coords(cassandra_container)


@pytest.fixture(scope="session")
def current_venv():
    configs = build_transformation_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No transformation configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def transformation_image():
    """Build the Docker image for PySpark transformation (once per session)."""
    configs = build_transformation_configs()
    if not configs:
        pytest.skip("No transformation configs available")

    gslib_path = configs[0].gslib_path
    dockerfile = gslib_path / "Dockerfile"
    if not dockerfile.exists():
        pytest.fail(f"Dockerfile not found at {dockerfile}")

    print(f"\nBuilding transformation Docker image from {gslib_path}...")
    result = subprocess.run(
        [
            "docker", "build",
            "-f", str(dockerfile),
            "-t", TRANSFORMATION_IMAGE_NAME,
            str(gslib_path),
        ],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Docker build failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout[-3000:]}\n"
            f"stderr: {result.stderr[-3000:]}"
        )
    print(f"Transformation image built: {TRANSFORMATION_IMAGE_NAME}")
    return TRANSFORMATION_IMAGE_NAME
