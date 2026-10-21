"""`ingest from-node` and `ingest delta-lake auto-compact` take sinks and the
compaction schedule from ingest_config, with flags overriding."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from graphsenselib.config import IngestConfig
from graphsenselib.utils.locking import LockAcquisitionError


def _config(**ingest):
    ic = IngestConfig(
        node_reference="http://node:8332",
        raw_keyspace_file_sinks={"delta": {"directory": "/data/btc"}},
        **ingest,
    )
    config = MagicMock()
    config.legacy_ingest = False
    config.get_keyspace_config.return_value = MagicMock(
        ingest_config=ic, schema_type="utxo"
    )
    return config


def _from_node(config, *args):
    from graphsenselib.ingest.cli import ingest

    with (
        patch("graphsenselib.ingest.cli.get_config", return_value=config),
        patch("graphsenselib.ingest.cli.GraphsenseSchemas"),
        patch("graphsenselib.ingest.cli.DbFactory"),
        patch("graphsenselib.ingest.cli._run_new_ingest") as run_ingest,
        patch("graphsenselib.ingest.cli._run_auto_compact") as compact,
    ):
        result = CliRunner().invoke(
            ingest, ["-e", "test", "-c", "btc", "--no-exchange-rates", *args]
        )
    return result, run_ingest, compact


def _sinks(run_ingest):
    return run_ingest.call_args.args[5]


def test_sinks_from_config_and_flag_override():
    config = _config(sinks=["cassandra", "delta"])
    result, run_ingest, _ = _from_node(config)
    assert result.exit_code == 0, result.output
    assert _sinks(run_ingest) == ["cassandra", "delta"]

    result, run_ingest, _ = _from_node(config, "--sinks", "cassandra")
    assert _sinks(run_ingest) == ["cassandra"]

    result, run_ingest, _ = _from_node(_config())
    assert _sinks(run_ingest) == ["cassandra"]


def test_auto_compact_from_config():
    config = _config(sinks=["cassandra", "delta"], auto_compact="1w;sunday")
    result, _, compact = _from_node(config)
    assert result.exit_code == 0, result.output
    assert compact.call_args.kwargs["auto_compact"] == "1w;sunday"
    assert compact.call_args.kwargs["auto_compact_last_n"] == 10

    # what the sync daemon passes
    result, _, compact = _from_node(config, "--no-auto-compact")
    compact.assert_not_called()


def test_configured_auto_compact_skipped_without_delta_sink():
    # a manual cassandra-only run must not fail over the configured schedule
    config = _config(auto_compact="1w;sunday")
    result, _, compact = _from_node(config, "--sinks", "cassandra")
    assert result.exit_code == 0, result.output
    compact.assert_not_called()

    # ...but asking for it explicitly still is an error
    result, _, _ = _from_node(config, "--sinks", "cassandra", "--auto-compact", "1d")
    assert result.exit_code == 11


def _auto_compact_cmd(config, *args, side_effect=None):
    from graphsenselib.ingest.cli import auto_compact_deltalake

    with (
        patch("graphsenselib.ingest.cli.get_config", return_value=config),
        patch(
            "graphsenselib.ingest.cli._run_auto_compact", side_effect=side_effect
        ) as compact,
    ):
        result = CliRunner().invoke(
            auto_compact_deltalake, ["-e", "test", "-c", "btc", *args]
        )
    return result, compact


def test_auto_compact_command_uses_config_schedule():
    result, compact = _auto_compact_cmd(
        _config(auto_compact="1w;sat", auto_compact_last_n=16)
    )
    assert result.exit_code == 0, result.output
    assert compact.call_args.kwargs["auto_compact"] == "1w;sat"
    assert compact.call_args.kwargs["auto_compact_last_n"] == 16

    result, compact = _auto_compact_cmd(
        _config(auto_compact="1w;sat"), "--auto-compact", "1d"
    )
    assert compact.call_args.kwargs["auto_compact"] == "1d"


@pytest.mark.parametrize(
    "side_effect, code", [(None, 11), (LockAcquisitionError("held"), 911)]
)
def test_auto_compact_command_exit_codes(side_effect, code):
    config = _config() if side_effect is None else _config(auto_compact="1d")
    result, _ = _auto_compact_cmd(config, side_effect=side_effect)
    assert result.exit_code == code
