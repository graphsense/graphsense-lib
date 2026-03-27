"""Fixtures for Cassandra ingest regression tests."""

import pytest

from tests.cassandra.config import (
    FAST_CASSANDRA_IMAGE,
    build_cassandra_configs,
)
from tests.deltalake.venv_manager import (
    get_or_create_current_venv,
    get_or_create_reference_venv,
    get_venv_package_versions,
)
from tests.lib.conftest_helpers import (
    get_cassandra_coords as _get_cassandra_coords,
    parametrize_configs,
    skip_if_no_configs,
    start_cassandra_container,
)
from tests.lib.constants import DEFAULT_REF_VERSION, VANILLA_CASSANDRA_IMAGE


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "cassandra", build_cassandra_configs)


def pytest_generate_tests(metafunc):
    parametrize_configs(metafunc, "cassandra_config", build_cassandra_configs)


@pytest.fixture(scope="session")
def cassandra_container():
    """Start a Cassandra container, preferring the fast pre-baked image."""
    image = VANILLA_CASSANDRA_IMAGE
    try:
        import docker
        client = docker.from_env()
        client.images.get(FAST_CASSANDRA_IMAGE)
        image = FAST_CASSANDRA_IMAGE
    except Exception:
        pass

    container = start_cassandra_container(image)
    yield container
    container.stop()


@pytest.fixture(scope="session")
def cassandra_coords(cassandra_container):
    return _get_cassandra_coords(cassandra_container)


@pytest.fixture(scope="session")
def reference_venv():
    configs = build_cassandra_configs()
    ref_version = configs[0].ref_version if configs else DEFAULT_REF_VERSION
    return get_or_create_reference_venv(ref_version)


@pytest.fixture(scope="session")
def current_venv():
    configs = build_cassandra_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No cassandra configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def ref_package_versions(reference_venv):
    return get_venv_package_versions(reference_venv)


@pytest.fixture(scope="session")
def current_package_versions(current_venv):
    return get_venv_package_versions(current_venv)
