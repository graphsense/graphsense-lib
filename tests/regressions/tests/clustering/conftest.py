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


SCALA_IMAGE_NAME = "graphsense-spark-test:latest"

# Known locations for the Scala transformation repo (sibling of graphsense-lib).
_SCALA_REPO_CANDIDATES = [
    "graphsense-ethereum-transformation",
    "graphsense-spark",
]


@pytest.fixture(scope="session")
def scala_transformation_image():
    """Build or locate the Scala/Spark transformation Docker image.

    Checks CLUSTERING_SCALA_IMAGE env var first.  If unset, tries to build
    from a sibling repo (graphsense-ethereum-transformation or graphsense-spark).
    """
    import subprocess

    image = os.environ.get("CLUSTERING_SCALA_IMAGE")
    if image:
        return image

    # Try to build from sibling repo
    configs = build_clustering_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No clustering configs available")

    parent = gslib_path.parent  # e.g. .../graphsense/
    for candidate in _SCALA_REPO_CANDIDATES:
        repo_path = parent / candidate
        dockerfile = repo_path / "Dockerfile"
        if dockerfile.exists():
            print(f"\nBuilding Scala transformation image from {repo_path} ...")
            result = subprocess.run(
                [
                    "docker", "build",
                    "-f", str(dockerfile),
                    "-t", SCALA_IMAGE_NAME,
                    str(repo_path),
                ],
                capture_output=True, text=True, timeout=900,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Docker build failed for {repo_path} "
                    f"(exit {result.returncode}):\n"
                    f"stderr: {result.stderr[-3000:]}"
                )
            print(f"Scala image built: {SCALA_IMAGE_NAME}")
            return SCALA_IMAGE_NAME

    pytest.skip(
        "No Scala transformation image found. Either set CLUSTERING_SCALA_IMAGE "
        "env var or place graphsense-ethereum-transformation repo next to "
        "graphsense-lib."
    )
