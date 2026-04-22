"""End-to-end tests for the `gs-files` CLI (decode / summary).

Fixture .gs files expected under tests/testfiles/gs_files/ — once example
files land there, `test_decode_and_summary_on_fixtures` exercises them.
"""

from __future__ import annotations

import base64
import json
import struct
from pathlib import Path

import pytest
from click.testing import CliRunner

from graphsenselib.convert.gs_files.cli import gs_files_cli

FIXTURES_DIR = Path(__file__).parent.parent.parent / "testfiles" / "gs_files"


def _lzw_pack(text: str) -> list[int]:
    if not text:
        return []
    dictionary = {chr(i): i for i in range(256)}
    out: list[int] = []
    w = ""
    for c in text:
        wc = w + c
        if wc in dictionary:
            w = wc
        else:
            out.append(dictionary[w])
            dictionary[wc] = len(dictionary)
            w = c
    if w:
        out.append(dictionary[w])
    return out


def _make_gs(obj) -> bytes:
    b64 = base64.b64encode(json.dumps(obj).encode()).decode("ascii")
    codes = _lzw_pack(b64)
    return struct.pack(f"<{len(codes)}I", *codes)


@pytest.fixture
def sample_graph_gs(tmp_path: Path) -> Path:
    payload = [
        "1.0.0",
        [[["btc", 0, "addr1"], 1.0, 2.0, None, None]],
        [[["btc", 0, 10], None, 3.0, 4.0, None, None]],
        [["title", [0.1, 0.2, 0.3, 1.0]]],
    ]
    path = tmp_path / "sample.gs"
    path.write_bytes(_make_gs(payload))
    return path


@pytest.fixture
def sample_pathfinder_gs(tmp_path: Path) -> Path:
    payload = ["pathfinder", "1", "case-42", [], [], []]
    path = tmp_path / "pf.gs"
    path.write_bytes(_make_gs(payload))
    return path


class TestDecodeCommand:
    def test_structured_to_stdout(self, sample_graph_gs: Path):
        runner = CliRunner()
        result = runner.invoke(gs_files_cli, ["decode", str(sample_graph_gs)])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["kind"] == "graph"
        assert data["version"] == "1.0.0"
        assert len(data["addresses"]) == 1

    def test_raw_to_stdout(self, sample_graph_gs: Path):
        runner = CliRunner()
        result = runner.invoke(
            gs_files_cli, ["decode", "--format", "raw", str(sample_graph_gs)]
        )
        assert result.exit_code == 0
        raw = json.loads(result.output)
        assert raw[0] == "1.0.0"

    def test_both_writes_two_files(self, sample_graph_gs: Path, tmp_path: Path):
        runner = CliRunner()
        out = tmp_path / "result.json"
        result = runner.invoke(
            gs_files_cli,
            ["decode", "--format", "both", "-o", str(out), str(sample_graph_gs)],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "result.raw.json").exists()
        assert (tmp_path / "result.structured.json").exists()

    def test_missing_file_errors(self, tmp_path: Path):
        runner = CliRunner()
        result = runner.invoke(gs_files_cli, ["decode", str(tmp_path / "nope.gs")])
        assert result.exit_code != 0


class TestSummaryCommand:
    def test_graph_summary(self, sample_graph_gs: Path):
        runner = CliRunner()
        result = runner.invoke(gs_files_cli, ["summary", str(sample_graph_gs)])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {
            "kind": "graph",
            "version": "1.0.0",
            "n_addresses": 1,
            "n_entities": 1,
            "n_highlights": 1,
        }

    def test_pathfinder_summary(self, sample_pathfinder_gs: Path):
        runner = CliRunner()
        result = runner.invoke(gs_files_cli, ["summary", str(sample_pathfinder_gs)])
        assert result.exit_code == 0, result.output
        out = json.loads(result.output)
        assert out["kind"] == "pathfinder"
        assert out["version"] == "1"
        assert out["name"] == "case-42"
        assert out["n_addresses"] == out["n_txs"] == 0

    def test_summary_to_file(self, sample_graph_gs: Path, tmp_path: Path):
        runner = CliRunner()
        target = tmp_path / "s.json"
        result = runner.invoke(
            gs_files_cli,
            ["summary", "-o", str(target), str(sample_graph_gs)],
        )
        assert result.exit_code == 0
        assert json.loads(target.read_text())["kind"] == "graph"


# ---------------------------------------------------------------------------
# Fixture-file sweep (activates once example .gs files are dropped in).
# ---------------------------------------------------------------------------


def _fixture_files() -> list[Path]:
    if not FIXTURES_DIR.exists():
        return []
    return sorted(FIXTURES_DIR.glob("*.gs"))


@pytest.mark.parametrize("gs_file", _fixture_files(), ids=lambda p: p.name)
def test_decode_and_summary_on_fixtures(gs_file: Path):
    """Smoke-decode each example `.gs` file under tests/testfiles/gs_files/."""
    runner = CliRunner()
    decode = runner.invoke(gs_files_cli, ["decode", str(gs_file)])
    assert decode.exit_code == 0, decode.output
    json.loads(decode.output)  # valid JSON

    summary = runner.invoke(gs_files_cli, ["summary", str(gs_file)])
    assert summary.exit_code == 0, summary.output
    out = json.loads(summary.output)
    assert out["kind"] in ("graph", "pathfinder")
    assert "version" in out
