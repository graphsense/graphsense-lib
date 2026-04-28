"""Shared fixtures for transformation regression tests.

Tests run in pure local Spark: no Cassandra connector, no Delta JARs. We
monkey-patch the transformation classes' read/write methods so we can feed
hand-built DataFrames in and capture the post-transform DataFrame out.
"""

import os

import pytest

# The dev shell sets SPARK_SUBMIT_OPTS with JDWP debugging on a wait-for-attach
# socket and points SPARK_HOME at an older system Spark. Both must go before
# pyspark imports, otherwise the test JVM hangs waiting for a debugger.
os.environ.pop("SPARK_SUBMIT_OPTS", None)
os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYSPARK_SUBMIT_ARGS", None)

pyspark = pytest.importorskip("pyspark")


# Pyspark's py4j leaks a TCP socket between JVM and driver that gets
# garbage-collected mid-test, surfacing as a ResourceWarning the unraisable-
# exception machinery promotes to PytestUnraisableExceptionWarning. Harmless;
# mute via per-item marker (markers override the CLI -W error used by the
# pre-commit hook, which `filterwarnings` in pyproject.toml does not).
def pytest_collection_modifyitems(config, items):
    marker = pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnraisableExceptionWarning"
    )
    for item in items:
        if item.module.__name__.startswith("tests.transformation."):
            item.add_marker(marker)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.appName("transformation-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def install_harness():
    """Patch a transformer's IO so tests can drive it with in-memory DataFrames.

    Sets `_read_stub[table] = df` (the test fills this in), and captures every
    `_write_cassandra` call into `_captured[table]` for assertions.
    """

    def _install(transformer):
        transformer._captured = {}
        transformer._read_stub = {}

        def fake_read(table, start_block=None, end_block=None):
            return transformer._read_stub[table]

        def fake_write(df, table, **kwargs):
            transformer._captured[table] = df

        transformer._read_delta = fake_read
        transformer._write_cassandra = fake_write
        return transformer

    return _install
