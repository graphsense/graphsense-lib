"""Fixtures for Cassandra ingest regression tests.

Provides:
- Cassandra testcontainer (session-scoped)
- Per-currency test parametrization via .graphsense.yaml
- Virtual environments for reference and current versions
"""

import pytest
from testcontainers.cassandra import CassandraContainer

from tests.cassandra.config import (
    DEFAULT_REF_VERSION,
    FAST_CASSANDRA_IMAGE,
    VANILLA_CASSANDRA_IMAGE,
    build_cassandra_configs,
)
from tests.deltalake.venv_manager import (
    get_or_create_current_venv,
    get_or_create_reference_venv,
    get_venv_package_versions,
)


def pytest_collection_modifyitems(config, items):
    """Skip all cassandra tests when no currencies have node URLs configured."""
    configs = build_cassandra_configs()
    if configs:
        return

    skip_marker = pytest.mark.skip(
        reason="No currencies with node URLs found in .graphsense.yaml"
    )
    for item in items:
        if "cassandra" in str(item.fspath):
            item.add_marker(skip_marker)


def pytest_generate_tests(metafunc):
    """Parametrize cassandra_config over all configured currencies."""
    if "cassandra_config" not in metafunc.fixturenames:
        return
    configs = build_cassandra_configs()
    metafunc.parametrize(
        "cassandra_config", configs, ids=[c.test_id for c in configs]
    )


@pytest.fixture(scope="session")
def cassandra_container():
    """Start a Cassandra container for the test session."""
    # Try the fast pre-baked image first, fall back to vanilla
    image = VANILLA_CASSANDRA_IMAGE
    try:
        import docker

        client = docker.from_env()
        client.images.get(FAST_CASSANDRA_IMAGE)
        image = FAST_CASSANDRA_IMAGE
    except Exception:
        pass

    container = CassandraContainer(image)
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def cassandra_coords(cassandra_container) -> tuple[str, int]:
    """Return (host, port) for the running Cassandra container."""
    host = cassandra_container.get_container_host_ip()
    port = int(cassandra_container.get_exposed_port(9042))
    return host, port


@pytest.fixture(scope="session")
def reference_venv():
    """Create (or reuse cached) virtual environment for the reference version."""
    configs = build_cassandra_configs()
    ref_version = configs[0].ref_version if configs else DEFAULT_REF_VERSION
    return get_or_create_reference_venv(ref_version)


@pytest.fixture(scope="session")
def current_venv():
    """Create (or reuse cached) virtual environment for the current version."""
    configs = build_cassandra_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No cassandra configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def ref_package_versions(reference_venv) -> dict[str, str]:
    """Package versions from the reference venv."""
    return get_venv_package_versions(reference_venv)


@pytest.fixture(scope="session")
def current_package_versions(current_venv) -> dict[str, str]:
    """Package versions from the current venv."""
    return get_venv_package_versions(current_venv)
