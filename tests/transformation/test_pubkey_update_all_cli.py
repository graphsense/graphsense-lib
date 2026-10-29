"""Wiring tests for `transformation pubkey-update-all`.

The Spark factories are stubbed: the tests check which steps run, in which
order and with which arguments, not the pubkey job itself.
"""

import contextlib

import pytest
from click.testing import CliRunner

from graphsenselib.config import get_config
from graphsenselib.config.config import FileSink
from graphsenselib.transformation.cli import transformation_cli


@pytest.fixture
def steps(monkeypatch, tmp_path):
    """Give pytest/btc a delta sink and record every factory call."""
    ks = get_config().get_keyspace_config("pytest", "btc")
    ks.ingest_config.raw_keyspace_file_sinks = {
        "delta": FileSink(directory=str(tmp_path / "btc"))
    }
    calls = []

    @contextlib.contextmanager
    def fake_create_lock(name, **kwargs):
        yield

    def record(step):
        def _f(**kwargs):
            calls.append((step, kwargs))

        return _f

    monkeypatch.setattr("graphsenselib.utils.locking.create_lock", fake_create_lock)
    monkeypatch.setattr(
        "graphsenselib.ingest.delta.sink.delta_lake_highest_block",
        lambda *a, **k: 1000,
    )
    for name, step in [
        ("run_pubkey", "update"),
        ("run_pubkey_detect", "detect"),
        ("run_pubkey_compact", "compact"),
        ("run_pubkey_load", "load"),
    ]:
        monkeypatch.setattr(f"graphsenselib.pubkey.factory.{name}", record(step))
    return calls


def _invoke(args):
    return CliRunner().invoke(
        transformation_cli,
        ["transformation", "pubkey-update-all", "-e", "pytest", *args],
        catch_exceptions=False,
    )


def test_defaults_to_networks_with_a_delta_sink(steps, tmp_path):
    result = _invoke(["--sink-path", str(tmp_path / "pk"), "--sink-type", "delta"])

    assert result.exit_code == 0, result.output
    assert [s for s, _ in steps] == ["update", "detect", "compact", "load"]
    update = steps[0][1]
    assert update["currency"] == "btc"
    assert update["skip_detect"] is True
    assert update["end_block"] == 1000
    assert update["source_path"] == str(tmp_path / "btc")
    assert update["sink_type"] == "delta"


def test_networks_run_in_the_given_order(steps, tmp_path):
    result = _invoke(
        [
            "--sink-path",
            str(tmp_path / "pk"),
            "--sink-type",
            "delta",
            "-n",
            "btc",
            "-n",
            "btc",
            "--no-compact",
            "--no-load",
        ]
    )

    assert result.exit_code == 0, result.output
    assert [s for s, _ in steps] == ["update", "update", "detect"]


def test_cassandra_sink_has_no_load_step(steps, tmp_path):
    result = _invoke(["--sink-path", str(tmp_path / "pk"), "--sink-type", "cassandra"])

    assert result.exit_code == 0, result.output
    assert [s for s, _ in steps] == ["update", "detect", "compact"]
    assert steps[1][1]["sink_type"] == "cassandra"


def test_stops_at_the_first_failing_step(steps, monkeypatch, tmp_path):
    def boom(**kwargs):
        raise RuntimeError("spark died")

    monkeypatch.setattr("graphsenselib.pubkey.factory.run_pubkey_detect", boom)

    with pytest.raises(RuntimeError, match="spark died"):
        _invoke(["--sink-path", str(tmp_path / "pk"), "--sink-type", "delta"])
    assert [s for s, _ in steps] == ["update"]


def test_no_readable_network_is_a_usage_error(steps, tmp_path):
    get_config().get_keyspace_config(
        "pytest", "btc"
    ).ingest_config.raw_keyspace_file_sinks = {}

    result = CliRunner().invoke(
        transformation_cli,
        ["transformation", "pubkey-update-all", "-e", "pytest", "--sink-path", "x"],
    )

    assert result.exit_code == 2
    assert "No network in environment pytest" in result.output
