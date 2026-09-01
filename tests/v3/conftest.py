"""Spark fixtures for the v3 tests.

Deliberately a near-copy of ``tests/transformation/conftest.py`` rather than an
import: the test tree is not a package, so there is nothing to import from, and
the v3 work is meant not to modify existing code. The reasoning behind each gate
is documented there; the short version is in the comments below.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

# The dev shell points SPARK_HOME at an older Spark and sets a wait-for-attach
# JDWP option; both hang the test JVM. Must happen before pyspark is imported.
os.environ.pop("SPARK_SUBMIT_OPTS", None)
os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYSPARK_SUBMIT_ARGS", None)

# py4j leaks a JVM<->driver socket that is collected mid-test, surfacing as a
# ResourceWarning which the unraisable-exception machinery promotes to an error
# under this repo's `filterwarnings = ["error"]`. Harmless, and pyspark's, not
# ours. Markers are used because they override the CLI `-W error` that a plain
# `filterwarnings` entry does not.
PYSPARK_WARNING_FILTERS = (
    "ignore::pytest.PytestUnraisableExceptionWarning",
    "ignore:distutils Version classes are deprecated:DeprecationWarning",
    "ignore::DeprecationWarning:pyspark",
)


#: This hook is handed every item in the session, not just ours, so the filters
#: are scoped by path. Matching on ``item.module.__name__`` does not work here:
#: the test tree has no ``__init__.py``, so modules are named ``test_v3_codec``
#: rather than ``tests.v3.test_v3_codec``.
_HERE = Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    markers = [pytest.mark.filterwarnings(f) for f in PYSPARK_WARNING_FILTERS]
    for item in items:
        if _HERE in Path(str(item.path)).parents:
            for marker in markers:
                item.add_marker(marker)


def _java_binary() -> str | None:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = os.path.join(java_home, "bin", "java")
        if os.path.isfile(candidate):
            return candidate
    return shutil.which("java")


def _java_major(java: str) -> int | None:
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
            value = value.strip().removeprefix("1.")
            return int(value) if value.isdigit() else None
    return None


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession, or a skip if this machine cannot run one."""
    java = _java_binary()
    if java is None:
        pytest.skip("no JVM found (pyspark needs a JDK); install e.g. openjdk-17")

    # JDK 24 removed the Security Manager (JEP 486), so Hadoop's
    # UserGroupInformation dies while the SparkContext is built. No flag brings
    # it back. pyspark 3.5 supports Java 8/11/17.
    major = _java_major(java)
    if major is not None and major >= 24:
        pytest.skip(f"JDK {major} too new for pyspark 3.5, which needs Java 8/11/17")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("v3-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
