"""Fixtures for clustering regression tests."""

import os

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
from tests.clustering.config import build_clustering_configs


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "clustering", build_clustering_configs)


def pytest_generate_tests(metafunc):
    parametrize_configs(
        metafunc, "clustering_config", build_clustering_configs
    )


@pytest.fixture(scope="session")
def minio_container():
    container = start_minio_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container):
    return make_minio_config(minio_container, bucket="clustering-test")


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
    configs = build_clustering_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No clustering configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def scala_transformation_image():
    """Return the Scala transformation Docker image name from env var.

    If CLUSTERING_SCALA_IMAGE is not set, skip the test with a clear message.
    This image runs the full Scala/Spark transformation (including clustering).
    """
    image = os.environ.get("CLUSTERING_SCALA_IMAGE")
    if not image:
        pytest.skip(
            "CLUSTERING_SCALA_IMAGE env var not set -- "
            "set it to the Docker image that runs the full Scala/Spark transformation "
            "(e.g. graphsense/graphsense-spark:latest)"
        )
    return image
