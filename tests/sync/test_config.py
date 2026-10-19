import pytest
from pydantic import ValidationError

from graphsenselib.config import IngestConfig, SyncConfig, get_reorg_backoff_blocks
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
