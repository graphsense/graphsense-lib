"""Fixtures for delta-update regression tests.

Reuses the deltalake suite's venv_manager so the cached venvs are shared
between regression suites that need a current + reference graphsense-lib
install.
"""

import os
import subprocess

import pytest

from tests.deltalake.venv_manager import (
    get_or_create_current_venv,
    get_or_create_reference_venv,
)
from tests.lib.conftest_helpers import (
    get_cassandra_coords as _get_cassandra_coords,
    make_minio_config,
    make_storage_options,
    parametrize_configs,
    skip_if_no_configs,
    start_cassandra_container,
    start_minio_container,
)
from tests.delta_update.config import build_delta_update_configs


# Reuses the transformation suite's image. The Docker image only depends on
# the local checkout, so a single build is fine.
TRANSFORMATION_IMAGE_NAME = "gslib-transformation-test:latest"

# Default reference release of graphsense-lib for the regression comparison.
DEFAULT_REF_VERSION = "v2.12.3"


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "delta_update", build_delta_update_configs)


def pytest_generate_tests(metafunc):
    parametrize_configs(
        metafunc, "delta_update_config", build_delta_update_configs
    )


@pytest.fixture(scope="session")
def minio_container():
    container = start_minio_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container):
    return make_minio_config(minio_container, bucket="delta-update-test")


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
    configs = build_delta_update_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No delta_update configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def baseline_venv():
    """Reference graphsense-lib release used for the regression comparison.

    Override the version via ``DELTA_UPDATE_REF_VERSION`` env var (default
    ``v2.12.3``).
    """
    ref_version = os.environ.get("DELTA_UPDATE_REF_VERSION", DEFAULT_REF_VERSION)
    return get_or_create_reference_venv(ref_version)


@pytest.fixture(scope="session")
def baseline_version() -> str:
    return os.environ.get("DELTA_UPDATE_REF_VERSION", DEFAULT_REF_VERSION)


@pytest.fixture(scope="session")
def transformation_image():
    """Build the graphsense-lib Docker image once per session.

    Uses the repo Dockerfile so the image carries the local PySpark
    transformation code -- the same image is shared by current + baseline
    runs because the spark step itself is *not* the regression target.
    """
    configs = build_delta_update_configs()
    if not configs:
        pytest.skip("No delta_update configs available")

    gslib_path = configs[0].gslib_path
    dockerfile = gslib_path / "Dockerfile"
    if not dockerfile.exists():
        pytest.fail(f"Dockerfile not found at {dockerfile}")

    # setuptools_scm fails inside the docker build when the working tree has
    # uncommitted changes (e.g. mid-merge). Resolve the version on the host
    # and pass it through as a build-arg, matching the top-level Makefile's
    # build-docker target.
    scm_proc = subprocess.run(
        ["uv", "run", "--frozen", "python", "-m", "setuptools_scm"],
        cwd=str(gslib_path),
        capture_output=True, text=True, timeout=120,
    )
    if scm_proc.returncode != 0:
        raise RuntimeError(
            f"setuptools_scm failed (exit {scm_proc.returncode}):\n"
            f"stderr: {scm_proc.stderr[-1000:]}"
        )
    scm_version = scm_proc.stdout.strip()

    print(
        f"\nBuilding transformation Docker image from {gslib_path} "
        f"(version {scm_version})..."
    )
    result = subprocess.run(
        [
            "docker", "build",
            "-f", str(dockerfile),
            "--build-arg",
            f"SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB={scm_version}",
            "-t", TRANSFORMATION_IMAGE_NAME,
            str(gslib_path),
        ],
        capture_output=True, text=True, timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Docker build failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout[-3000:]}\n"
            f"stderr: {result.stderr[-3000:]}"
        )
    print(f"Transformation image built: {TRANSFORMATION_IMAGE_NAME}")
    return TRANSFORMATION_IMAGE_NAME
