"""Fixtures for Delta Lake cross-version compatibility tests."""

import boto3
import pytest

from tests.deltalake.config import build_delta_configs
from tests.deltalake.perf_report import format_perf_report, save_perf_report
from tests.deltalake.venv_manager import (
    get_or_create_current_venv,
    get_or_create_reference_venv,
    get_venv_package_versions,
)
from tests.lib.conftest_helpers import (
    make_minio_config,
    make_storage_options,
    skip_if_no_configs,
    start_minio_container,
)
from tests.lib.constants import DEFAULT_REF_VERSION


def pytest_collection_modifyitems(config, items):
    skip_if_no_configs(items, "deltalake", build_delta_configs)


def pytest_generate_tests(metafunc):
    """Parametrize delta_config, with optional table_name cross-product."""
    if "delta_config" not in metafunc.fixturenames:
        return
    configs = build_delta_configs()
    if "table_name" in metafunc.fixturenames:
        argvalues = []
        ids = []
        for c in configs:
            for table in c.tables:
                argvalues.append((c, table))
                ids.append(f"{c.test_id}-{table}")
        metafunc.parametrize(["delta_config", "table_name"], argvalues, ids=ids)
    else:
        metafunc.parametrize(
            "delta_config", configs, ids=[c.test_id for c in configs]
        )


@pytest.fixture(scope="session")
def minio_container():
    container = start_minio_container()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_config(minio_container):
    return make_minio_config(minio_container, bucket="test")


@pytest.fixture(scope="session")
def storage_options(minio_config):
    return make_storage_options(minio_config)


@pytest.fixture(scope="session")
def reference_venv():
    configs = build_delta_configs()
    ref_version = configs[0].ref_version if configs else DEFAULT_REF_VERSION
    return get_or_create_reference_venv(ref_version)


@pytest.fixture(scope="session")
def current_venv():
    configs = build_delta_configs()
    gslib_path = configs[0].gslib_path if configs else None
    if gslib_path is None:
        pytest.skip("No delta configs available")
    return get_or_create_current_venv(gslib_path)


@pytest.fixture(scope="session")
def s3_client(minio_config):
    return boto3.client(
        "s3",
        endpoint_url=minio_config["endpoint"],
        aws_access_key_id=minio_config["access_key"],
        aws_secret_access_key=minio_config["secret_key"],
        region_name="us-east-1",
    )


@pytest.fixture(scope="session")
def ref_package_versions(reference_venv):
    return get_venv_package_versions(reference_venv)


@pytest.fixture(scope="session")
def current_package_versions(current_venv):
    return get_venv_package_versions(current_venv)


@pytest.fixture(scope="session")
def perf_report_collector():
    return []


@pytest.fixture(scope="session", autouse=True)
def perf_report_finalizer(request, perf_report_collector):
    yield
    if not perf_report_collector:
        return
    terminalreporter = request.config.pluginmanager.get_plugin("terminalreporter")
    write = terminalreporter.write_line if terminalreporter else print
    report_text = format_perf_report(perf_report_collector)
    for line in report_text.split("\n"):
        write(line)
    report_path = save_perf_report(perf_report_collector)
    write(f"Performance report saved to: {report_path}")
