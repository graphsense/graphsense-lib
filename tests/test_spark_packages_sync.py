"""The Scala job's dependencies live in spark/build.sbt; config.py mirrors them.

Nothing enforced that mirroring before, and it drifted. `make check-spark-packages`
is the gate; these tests cover the parser behind it, including the two shapes
that made the drift silent in the first place.
"""

import importlib.util
from pathlib import Path

import pytest

from graphsenselib.config.config import (
    DEFAULT_SCALA_JOB_EXCLUDES,
    DEFAULT_SCALA_JOB_PACKAGES,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_spark_packages.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_spark_packages", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def checker():
    return _load()


def test_config_matches_build_sbt(checker):
    packages, excludes = checker.parse_build_sbt()
    assert packages == DEFAULT_SCALA_JOB_PACKAGES
    assert excludes == DEFAULT_SCALA_JOB_EXCLUDES


def test_provided_and_test_scopes_are_not_shipped(checker):
    packages, _ = checker.parse_build_sbt()
    joined = " ".join(packages)
    # Provided: the cluster supplies Spark and cassandra-analytics.
    assert "spark-sql" not in joined
    assert "spark-graphx" not in joined
    assert "cassandra-analytics" not in joined
    # Test scope never reaches a submitted job.
    assert "scalatest" not in joined
    assert "spark-fast-tests" not in joined


def test_scala_versioned_artifacts_get_the_binary_suffix(checker):
    packages, _ = checker.parse_build_sbt()
    # `%%` in build.sbt must become _2.12 in a Maven coordinate...
    assert "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1" in packages
    # ...while `%` must not be given a suffix.
    assert "joda-time:joda-time:2.10.10" in packages


def test_excludes_are_captured_despite_the_comma(checker):
    """The exclude call contains a comma; a comma-terminated match loses it."""
    _, excludes = checker.parse_build_sbt()
    assert "org.bouncycastle:bcprov-jdk15on" in excludes


def test_drift_is_detected(checker, tmp_path, monkeypatch):
    """A dependency added to build.sbt but not config.py must fail the check."""
    fake_sbt = tmp_path / "build.sbt"
    fake_sbt.write_text(
        'ThisBuild / scalaVersion := "2.12.17"\n'
        "libraryDependencies ++= Seq(\n"
        '      "org.rogach" %% "scallop" % "4.1.0",\n'
        '      "com.example" % "brand-new" % "1.0.0",\n'
        '      "org.apache.spark" %% "spark-sql" % "3.5.8" % Provided)\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(checker, "BUILD_SBT", fake_sbt)
    packages, _ = checker.parse_build_sbt()
    assert packages == ["org.rogach:scallop_2.12:4.1.0", "com.example:brand-new:1.0.0"]
    assert packages != DEFAULT_SCALA_JOB_PACKAGES


def test_declaration_shape_change_fails_loudly(checker, tmp_path, monkeypatch):
    """Silence is the one unacceptable outcome if build.sbt is restructured."""
    fake_sbt = tmp_path / "build.sbt"
    fake_sbt.write_text('ThisBuild / scalaVersion := "2.12.17"\n', encoding="utf-8")
    monkeypatch.setattr(checker, "BUILD_SBT", fake_sbt)
    with pytest.raises(SystemExit):
        checker.parse_build_sbt()
