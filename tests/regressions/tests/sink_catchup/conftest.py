"""Fixtures for sink catch-up tests."""

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
from tests.sink_catchup.config import build_sink_catchup_configs


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "sink_catchup", build_sink_catchup_configs)


def pytest_generate_tests(metafunc):
    parametrize_configs(metafunc, "catchup_config", build_sink_catchup_configs)


@pytest.fixture(scope="session")
def minio_container():
    container = start_minio_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container):
    return make_minio_config(minio_container, bucket="catchup-test")


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
    configs = build_sink_catchup_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No sink catchup configs available")
    return get_or_create_current_venv(gslib_path)
