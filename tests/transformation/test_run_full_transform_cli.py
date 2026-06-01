"""Dry-run wiring test for `transformation run-full-transform`.

Exercises the command end-to-end with the jar download stubbed out and no
Cassandra/Spark contact (--dry-run prints the spark-submit command and creates
no keyspace).
"""

from click.testing import CliRunner

from graphsenselib.config import get_config
from graphsenselib.config.config import FullTransformArgs
from graphsenselib.transformation import spark_jar
from graphsenselib.transformation.cli import transformation_cli


def test_run_full_transform_dry_run(monkeypatch):
    cfg = get_config()  # rebuilt per-test by the autouse patch_config fixture
    cfg.full_transform_args = FullTransformArgs(
        version="v26.06.0",
        spark_profile={"btc": "utxo"},
        jar_args={"btc": ["--bech32-prefix", "bc", "--bucket-size", "5000"]},
    )
    cfg.spark_config = {
        "baseline": {"spark.master": "spark://m:7077"},
        "utxo": {},
    }
    monkeypatch.setattr(
        spark_jar, "fetch_release_jar", lambda *a, **k: "/cache/spark-jars/x.jar"
    )

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "run-full-transform",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    out = result.output
    assert "--class org.graphsense.TransformationJob" in out
    assert "--conf spark.master=spark://m:7077" in out
    assert "--conf spark.cassandra.connection.host=localhost" in out
    assert "--network btc" in out
    assert "--raw-keyspace pytest_btc_raw" in out
    assert "--target-keyspace btc_transformed_" in out  # fresh dated keyspace
    assert "--bech32-prefix bc" in out
    assert "/cache/spark-jars/x.jar" in out


def test_run_full_transform_resolves_latest_by_default(monkeypatch):
    """With no version pinned, the runner resolves the latest stable release."""
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(spark_profile={"btc": "utxo"})
    cfg.spark_config = {
        "baseline": {"spark.master": "spark://m:7077"},
        "utxo": {},
    }
    monkeypatch.setattr(spark_jar, "resolve_latest_release", lambda repo: "v99.9.9")
    seen = {}

    def fake_fetch(repo, version, artifact, cache_dir):
        seen["version"] = version
        return "/cache/spark-jars/x.jar"

    monkeypatch.setattr(spark_jar, "fetch_release_jar", fake_fetch)

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "run-full-transform",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["version"] == "v99.9.9"


def test_run_full_transform_version_latest_keyword(monkeypatch):
    """`--version latest` triggers the same resolution path."""
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(version="v1.0.0")
    cfg.spark_config = {"baseline": {"spark.master": "spark://m:7077"}}
    monkeypatch.setattr(spark_jar, "resolve_latest_release", lambda repo: "v99.9.9")
    seen = {}

    def fake_fetch(repo, version, artifact, cache_dir):
        seen["version"] = version
        return "/x.jar"

    monkeypatch.setattr(spark_jar, "fetch_release_jar", fake_fetch)

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "run-full-transform",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--version",
            "latest",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["version"] == "v99.9.9"


def test_run_full_transform_requires_master(monkeypatch):
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(version="v26.06.0")
    cfg.spark_config = {}
    monkeypatch.setattr(spark_jar, "fetch_release_jar", lambda *a, **k: "/x.jar")

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "run-full-transform",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--dry-run",
        ],
    )

    assert result.exit_code != 0
    assert "spark.master" in result.output


def test_run_full_transform_local_flag_sets_master(monkeypatch):
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(version="v26.06.0")
    cfg.spark_config = {}
    monkeypatch.setattr(spark_jar, "fetch_release_jar", lambda *a, **k: "/x.jar")

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "run-full-transform",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--local",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "spark.master=local[*]" in result.output
