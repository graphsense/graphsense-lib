"""Fixtures for continuation (split-ingest) tests."""

import pytest

from tests.continuation.config import build_continuation_configs
from tests.deltalake.venv_manager import get_or_create_current_venv
from tests.lib.conftest_helpers import (
    make_minio_config,
    make_storage_options,
    parametrize_configs,
    skip_if_no_configs,
    start_minio_container,
)


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "continuation", build_continuation_configs)


def pytest_generate_tests(metafunc):
    parametrize_configs(metafunc, "continuation_config", build_continuation_configs)


@pytest.fixture(scope="session")
def minio_container():
    container = start_minio_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container):
    return make_minio_config(minio_container, bucket="continuation-test")


@pytest.fixture(scope="session")
def storage_options(minio_config):
    return make_storage_options(minio_config)


@pytest.fixture(scope="session")
def current_venv():
    configs = build_continuation_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No continuation configs available")
    return get_or_create_current_venv(gslib_path)
