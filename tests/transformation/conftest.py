"""Shared fixtures for transformation regression tests.

Tests run in pure local Spark: no Cassandra connector, no Delta JARs. We
monkey-patch the transformation classes' read/write methods so we can feed
hand-built DataFrames in and capture the post-transform DataFrame out.
"""

import os
import shutil
import subprocess

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
#
# `toPandas()` additionally makes pyspark version-check pandas through
# distutils.LooseVersion, which deprecation-warns on 3.12+. Also pyspark's, not
# ours.
PYSPARK_WARNING_FILTERS = (
    "ignore::pytest.PytestUnraisableExceptionWarning",
    "ignore:distutils Version classes are deprecated:DeprecationWarning",
)


def pytest_collection_modifyitems(config, items):
    markers = [pytest.mark.filterwarnings(f) for f in PYSPARK_WARNING_FILTERS]
    for item in items:
        if item.module.__name__.startswith("tests.transformation."):
            for marker in markers:
                item.add_marker(marker)


def _java_binary() -> str | None:
    """Mirror pyspark's JVM lookup: $JAVA_HOME/bin/java, else `java` on PATH."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = os.path.join(java_home, "bin", "java")
        if os.path.isfile(candidate):
            return candidate
    return shutil.which("java")


def _java_major(java: str) -> int | None:
    """Major version of `java` (8, 17, 25, ...), or None if undeterminable."""
    try:
        proc = subprocess.run(
            [java, "-XshowSettings:properties", "-version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in proc.stderr.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == "java.specification.version":
            # Pre-9 JDKs report "1.8"; 9+ report the major on its own.
            value = value.strip().removeprefix("1.")
            return int(value) if value.isdigit() else None
    return None


@pytest.fixture(scope="session")
def spark():
    # pyspark being importable does not mean a usable JVM exists. Both gates
    # below otherwise surface as opaque py4j crashes at session build time.
    # Only the tests that actually need a SparkSession are affected -- the rest
    # of this package still runs.
    java = _java_binary()
    if java is None:
        pytest.skip("no JVM found (pyspark needs a JDK); install e.g. openjdk-17")

    # JEP 486 (JDK 24) removed the Security Manager for good, so Hadoop's
    # UserGroupInformation.getCurrentUser() -> Subject.getSubject() dies with
    # `UnsupportedOperationException: getSubject is not supported` while the
    # SparkContext is being constructed. No flag brings it back: passing
    # -Djava.security.manager=allow is itself a hard error on 25. pyspark 3.5
    # supports Java 8/11/17.
    major = _java_major(java)
    if major is not None and major >= 24:
        pytest.skip(f"JDK {major} too new for pyspark 3.5, which needs Java 8/11/17")

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
