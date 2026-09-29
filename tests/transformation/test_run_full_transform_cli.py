"""Dry-run wiring test for `transformation raw-to-transformed`.

Exercises the command end-to-end with the jar download stubbed out and no
Cassandra/Spark contact (--dry-run prints the spark-submit command and creates
no keyspace).
"""

import click
import pytest
from click.testing import CliRunner

from graphsenselib.config import get_config
from graphsenselib.config.config import (
    FullTransformArgs,
    get_default_data_configuration,
)
from graphsenselib.transformation import spark_jar
from graphsenselib.transformation.cli import (
    _data_config_jar_args,
    transformation_cli,
)

# Production layout: bucket size 5000, bech32 prefix "bc", address prefix
# length 4, coinjoin filtering on.
BTC_DEFAULTS = get_default_data_configuration("btc", "transformed")


def _set_btc_data_configuration(data_configuration):
    ks = get_config().get_keyspace_config("pytest", "btc")
    ks.keyspace_setup_config["transformed"].data_configuration = data_configuration


@pytest.fixture(autouse=True)
def btc_default_data_configuration():
    """The pytest config deviates from the defaults (address_prefix_length 3);
    reset it so the wiring tests don't trip the deviation check."""
    _set_btc_data_configuration(BTC_DEFAULTS)


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
            "raw-to-transformed",
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
    monkeypatch.setattr(
        spark_jar, "resolve_latest_release", lambda repo, prefix=None: "v99.9.9"
    )
    seen = {}

    def fake_fetch(repo, version, artifact, cache_dir):
        seen["version"] = version
        return "/cache/spark-jars/x.jar"

    monkeypatch.setattr(spark_jar, "fetch_release_jar", fake_fetch)

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "raw-to-transformed",
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
    monkeypatch.setattr(
        spark_jar, "resolve_latest_release", lambda repo, prefix=None: "v99.9.9"
    )
    seen = {}

    def fake_fetch(repo, version, artifact, cache_dir):
        seen["version"] = version
        return "/x.jar"

    monkeypatch.setattr(spark_jar, "fetch_release_jar", fake_fetch)

    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "raw-to-transformed",
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
            "raw-to-transformed",
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
            "raw-to-transformed",
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


def _dry_run_local(monkeypatch, spark_config):
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(version="v26.06.0")
    cfg.spark_config = spark_config
    monkeypatch.setattr(spark_jar, "fetch_release_jar", lambda *a, **k: "/x.jar")
    return CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "raw-to-transformed",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--local",
            "--dry-run",
        ],
    )


def test_run_full_transform_local_flag_keeps_configured_local_master(monkeypatch):
    """--local must not widen a configured local[N] to local[*]."""
    result = _dry_run_local(monkeypatch, {"baseline": {"spark.master": "local[1]"}})

    assert result.exit_code == 0, result.output
    assert "--conf spark.master=local[1]" in result.output
    assert "local[*]" not in result.output


def test_run_full_transform_local_flag_overrides_cluster_master(monkeypatch):
    result = _dry_run_local(
        monkeypatch, {"baseline": {"spark.master": "spark://m:7077"}}
    )

    assert result.exit_code == 0, result.output
    assert "--conf spark.master=local[*]" in result.output
    assert "spark://m:7077" not in result.output


def _dry_run_jar_args(monkeypatch, *extra):
    cfg = get_config()
    cfg.spark_config = {"baseline": {"spark.master": "spark://m:7077"}}
    monkeypatch.setattr(spark_jar, "fetch_release_jar", lambda *a, **k: "/x.jar")
    result = CliRunner().invoke(
        transformation_cli,
        [
            "transformation",
            "raw-to-transformed",
            "-e",
            "pytest",
            "-c",
            "btc",
            "--dry-run",
            *extra,
        ],
    )
    assert result.exit_code == 0, result.output
    cmd = [line for line in result.output.splitlines() if "spark-submit" in line][-1]
    return cmd.split("/x.jar", 1)[1].split()


def test_run_full_transform_passes_default_data_configuration(monkeypatch):
    """A stock config passes graphsense-lib's defaults (the production layout)
    explicitly, instead of leaving the job on its own bucket size 25000 and
    empty bech32 prefix."""
    get_config().full_transform_args = FullTransformArgs(version="v26.06.0")

    args = _dry_run_jar_args(monkeypatch)

    assert args[args.index("--bucket-size") + 1] == "5000"
    assert args[args.index("--address-prefix-length") + 1] == "4"
    assert args[args.index("--bech32-prefix") + 1] == "bc"
    assert "--coinjoin-filtering" in args
    assert not any("fiat" in a for a in args)


def test_run_full_transform_fills_partial_data_configuration(monkeypatch):
    """Keys missing from data_configuration get graphsense-lib's defaults,
    not the job's."""
    get_config().full_transform_args = FullTransformArgs(version="v26.06.0")
    _set_btc_data_configuration({"fiat_currencies": ["EUR", "USD"]})

    args = _dry_run_jar_args(monkeypatch)

    assert args[args.index("--bucket-size") + 1] == "5000"
    assert args[args.index("--bech32-prefix") + 1] == "bc"


def test_run_full_transform_fails_on_non_default_data_configuration(monkeypatch):
    """Without --override-defaults a deviation from the defaults fails before
    any keyspace is created."""
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(version="v26.06.0")
    cfg.spark_config = {"baseline": {"spark.master": "spark://m:7077"}}
    _set_btc_data_configuration(
        {**BTC_DEFAULTS, "bucket_size": 25000, "coinjoin_filtering": False}
    )
    monkeypatch.setattr(spark_jar, "fetch_release_jar", lambda *a, **k: "/x.jar")

    result = CliRunner().invoke(
        transformation_cli,
        ["transformation", "raw-to-transformed", "-e", "pytest", "-c", "btc"],
    )

    assert result.exit_code != 0
    assert "bucket_size: 25000 (default 5000)" in result.output
    assert "coinjoin_filtering: False (default True)" in result.output
    assert "bech_32_prefix" not in result.output
    assert "--override-defaults" in result.output


def test_run_full_transform_override_defaults(monkeypatch):
    get_config().full_transform_args = FullTransformArgs(version="v26.06.0")
    _set_btc_data_configuration(
        {**BTC_DEFAULTS, "bucket_size": 25000, "coinjoin_filtering": False}
    )

    args = _dry_run_jar_args(monkeypatch, "--override-defaults")

    assert args[args.index("--bucket-size") + 1] == "25000"
    assert "--no-coinjoin-filtering" in args
    assert "--coinjoin-filtering" not in args


def test_run_full_transform_explicit_jar_args_override_data_configuration(
    monkeypatch,
):
    """jar_args and CLI passthrough win and are not repeated (Scallop rejects
    an option given twice). Overridden values are not checked against the
    defaults: they were always passed to the job."""
    cfg = get_config()
    cfg.full_transform_args = FullTransformArgs(
        version="v26.06.0",
        jar_args={"btc": ["--bucket-size", "100"]},
    )

    args = _dry_run_jar_args(
        monkeypatch, "--", "--bech32-prefix=bc1", "--no-coinjoin-filtering"
    )

    assert args.count("--bucket-size") == 1
    assert args[args.index("--bucket-size") + 1] == "100"
    assert "--bech32-prefix" not in args
    assert "--bech32-prefix=bc1" in args
    assert "--coinjoin-filtering" not in args
    assert "--no-coinjoin-filtering" in args


def test_data_config_jar_args_account_skips_utxo_only_options():
    """trx passes its production layout, including the block bucket size the
    job would otherwise default to 150000; UTXO-only keys are ignored."""
    args, deviations = _data_config_jar_args(
        "trx",
        "account_trx",
        {
            "bucket_size": 25000,
            "address_prefix_length": 5,
            "tx_prefix_length": 5,
            "bech_32_prefix": "bc",
            "coinjoin_filtering": True,
        },
        [],
    )

    assert args == [
        "--bucket-size",
        "25000",
        "--address-prefix-length",
        "5",
        "--tx-prefix-length",
        "5",
        "--block-bucket-size-address-txs",
        "50000",
        "--addressrelations-ids-nbuckets",
        "100",
    ]
    assert deviations == []


def test_data_config_jar_args_account_deviation():
    _, deviations = _data_config_jar_args(
        "trx", "account_trx", {"block_bucket_size_address_txs": 450000}, []
    )

    assert deviations == [("block_bucket_size_address_txs", 450000, 50000)]


def test_data_config_jar_args_empty_bech32_prefix_is_not_passed():
    """bch has no bech32 prefix; "" is the job default, so nothing is passed."""
    args, deviations = _data_config_jar_args(
        "bch", "utxo", get_default_data_configuration("bch", "transformed"), []
    )

    assert "--bech32-prefix" not in args
    assert deviations == []


def test_data_config_jar_args_option_value_is_not_an_option():
    """A value following a value-taking option is not read as an option name."""
    args, _ = _data_config_jar_args(
        "btc", "utxo", BTC_DEFAULTS, ["--bech32-prefix", "--bucket-size"]
    )

    assert args[args.index("--bucket-size") + 1] == "5000"


@pytest.mark.parametrize(
    "key, value",
    [
        ("coinjoin_filtering", "false"),
        ("coinjoin_filtering", 0),
        ("bucket_size", "5000"),
        ("bucket_size", True),
        ("bech_32_prefix", 1),
    ],
)
def test_data_config_jar_args_rejects_wrong_types(key, value):
    with pytest.raises(click.ClickException, match=key):
        _data_config_jar_args("btc", "utxo", {key: value}, [])
