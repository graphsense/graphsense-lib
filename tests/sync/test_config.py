import pytest
from pydantic import ValidationError

from graphsenselib.config import (
    DeltaUpdateConfig,
    IngestConfig,
    SyncConfig,
    SyncCurrencyConfig,
    get_reorg_backoff_blocks,
)
from graphsenselib.sync.cli import _parse_duration
from graphsenselib.sync.scheduler import default_max_blocks


@pytest.mark.parametrize(
    "args", [["--end-block", "5"], ["--start-block=1"], ["-c", "btc"]]
)
def test_scheduler_owned_args_are_rejected(args):
    with pytest.raises(ValidationError):
        SyncConfig(currencies={"zec": {"ingest_args": args}})


def test_reorg_backoff_override():
    assert get_reorg_backoff_blocks("zec") == 150
    assert get_reorg_backoff_blocks("zec", override=24) == 24
    with pytest.raises(ValidationError):
        IngestConfig(reorg_backoff_blocks=0)


def test_default_slice_is_about_two_hours_of_blocks():
    assert default_max_blocks("btc") == 12
    assert default_max_blocks("zec") == 96


def test_parse_duration():
    assert _parse_duration("90m") == 5400
    assert _parse_duration("2d") == 172800
    with pytest.raises(Exception):
        _parse_duration("0h")


@pytest.mark.parametrize(
    "arg", ["--auto-compact", "--auto-compact=1d", "--no-auto-compact"]
)
def test_compaction_flags_point_to_ingest_config(arg):
    with pytest.raises(ValueError, match="ingest_config.auto_compact"):
        SyncCurrencyConfig(ingest_args=[arg, "1d"])


def test_ingest_and_delta_update_defaults_validated_at_load():
    ic = IngestConfig(sinks=["cassandra", "delta"], auto_compact="1w;sunday")
    assert ic.sinks == ["cassandra", "delta"]
    for bad in (
        {"sinks": ["kafka"]},
        {"sinks": []},
        {"auto_compact": "weekly"},
        {"auto_compact": "1w;funday"},
        {"auto_compact_last_n": 0},
    ):
        with pytest.raises(ValueError):
            IngestConfig(**bad)
    with pytest.raises(ValueError):
        DeltaUpdateConfig(write_batch_size=0)
