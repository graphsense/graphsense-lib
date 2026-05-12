"""Smoke tests for `graphsense gs ...` commands.

Builds a minimal Pathfinder `.gs` file in-memory via the vendored encoder
and runs the CLI against it through `CliRunner`. No network, no fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from graphsense.cli.main import cli
from graphsense.gs_files import GsBuilder


def _make_gs(tmp_path: Path) -> Path:
    g = (
        GsBuilder(name="t", default_network="btc")
        .add_address("1A1z")
        .add_address("1B2y")
        .add_tx("dead")
        .add_tx("beef")
    )
    out = tmp_path / "t.gs"
    g.write(out)
    return out


def test_summary(tmp_path: Path) -> None:
    res = CliRunner().invoke(cli, ["gs", "summary", str(_make_gs(tmp_path))])
    assert res.exit_code == 0, res.output
    payload = json.loads(res.output)
    assert payload["kind"] == "pathfinder"
    assert payload["n_addresses"] == 2
    assert payload["n_txs"] == 2


def test_txs_emits_network_id_records(tmp_path: Path) -> None:
    res = CliRunner().invoke(cli, ["-f", "json", "gs", "txs", str(_make_gs(tmp_path))])
    assert res.exit_code == 0, res.output
    assert json.loads(res.output) == [
        {"network": "btc", "id": "dead"},
        {"network": "btc", "id": "beef"},
    ]


def test_addresses_emits_network_id_records(tmp_path: Path) -> None:
    res = CliRunner().invoke(
        cli, ["-f", "json", "gs", "addresses", str(_make_gs(tmp_path))]
    )
    assert res.exit_code == 0, res.output
    assert json.loads(res.output) == [
        {"network": "btc", "id": "1A1z"},
        {"network": "btc", "id": "1B2y"},
    ]


def test_jsonl_is_pipe_friendly(tmp_path: Path) -> None:
    """Default list output is JSONL — each line is a standalone JSON record."""
    res = CliRunner().invoke(cli, ["gs", "txs", str(_make_gs(tmp_path))])
    assert res.exit_code == 0, res.output
    lines = [line for line in res.output.splitlines() if line.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"network": "btc", "id": "dead"}


def test_dedupe_drops_repeats(tmp_path: Path) -> None:
    g = (
        GsBuilder(name="t", default_network="btc")
        .add_address("1A1z")
        .add_tx("dup")
        .add_tx("dup")
    )
    gs_path = tmp_path / "dup.gs"
    g.write(gs_path)
    res = CliRunner().invoke(cli, ["-f", "json", "gs", "txs", str(gs_path)])
    assert res.exit_code == 0, res.output
    assert json.loads(res.output) == [{"network": "btc", "id": "dup"}]
