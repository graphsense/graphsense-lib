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
    """Current venv for clustering tests.

    In addition to the editable graphsense-lib install, this ensures that the
    Rust ``gs_clustering`` extension is built and installed into the same venv,
    so that the production ``run_incremental_clustering`` function (which
    imports ``gs_clustering``) can be invoked as a subprocess from here.
    """
    import subprocess

    configs = build_clustering_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No clustering configs available")

    venv_dir = get_or_create_current_venv(gslib_path)

    # Check if gs_clustering is already installed in this venv
    python_bin = venv_dir / "bin" / "python"
    check = subprocess.run(
        [str(python_bin), "-c", "import gs_clustering"],
        capture_output=True,
    )
    if check.returncode != 0:
        # Build and install gs_clustering into the current_venv.
        # `uv pip install <rust_path>` uses maturin as the build backend and
        # compiles for the target Python version automatically.
        rust_dir = gslib_path / "rust" / "gs_clustering"
        if not rust_dir.exists():
            pytest.skip(f"rust/gs_clustering not found at {rust_dir}")
        print(f"\nBuilding gs_clustering into {venv_dir} ...", flush=True)
        result = subprocess.run(
            ["uv", "pip", "install", str(rust_dir), "--python", str(python_bin)],
            capture_output=True,
            text=True,
            timeout=900,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to build gs_clustering (exit {result.returncode}):\n"
                f"stdout: {result.stdout[-3000:]}\n"
                f"stderr: {result.stderr[-3000:]}"
            )
        # Verify install
        verify = subprocess.run(
            [str(python_bin), "-c", "import gs_clustering"],
            capture_output=True,
            text=True,
        )
        if verify.returncode != 0:
            raise RuntimeError(
                f"gs_clustering still not importable after build:\n"
                f"stderr: {verify.stderr}"
            )
        print(f"gs_clustering built and installed into {venv_dir}", flush=True)

    return venv_dir


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
