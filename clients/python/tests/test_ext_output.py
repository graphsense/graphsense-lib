"""Output writers: JSON / JSONL / CSV to stdout, file, or directory."""

from __future__ import annotations

import json
import os


from graphsense.ext import output as out_mod


def test_resolve_format_default_single():
    assert (
        out_mod.resolve_format(explicit=None, output_path=None, is_list=False) == "json"
    )


def test_resolve_format_default_list():
    assert (
        out_mod.resolve_format(explicit=None, output_path=None, is_list=True) == "jsonl"
    )


def test_resolve_format_from_extension():
    assert (
        out_mod.resolve_format(explicit=None, output_path="x.csv", is_list=True)
        == "csv"
    )
    assert (
        out_mod.resolve_format(explicit=None, output_path="x.jsonl", is_list=False)
        == "jsonl"
    )
    assert (
        out_mod.resolve_format(explicit=None, output_path="x.json", is_list=True)
        == "json"
    )


def test_resolve_format_explicit_overrides_extension():
    assert (
        out_mod.resolve_format(explicit="jsonl", output_path="x.csv", is_list=True)
        == "jsonl"
    )


def test_write_single_json(tmp_path):
    p = tmp_path / "out.json"
    out_mod.write({"address": "1A", "cluster": 1}, output=str(p), format="json")
    assert json.loads(p.read_text()) == {"address": "1A", "cluster": 1}


def test_write_list_jsonl(tmp_path):
    p = tmp_path / "out.jsonl"
    out_mod.write([{"address": "1A"}, {"address": "1B"}], output=str(p), format="jsonl")
    lines = p.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["address"] == "1A"


def test_write_list_csv_flattens_nested(tmp_path):
    p = tmp_path / "out.csv"
    out_mod.write(
        [
            {"address": "1A", "balance": {"value": 100}},
            {"address": "1B", "balance": {"value": 200}},
        ],
        output=str(p),
        format="csv",
    )
    rows = p.read_text().splitlines()
    assert rows[0] == "address,balance.value"
    assert rows[1] == "1A,100"


def test_write_directory_one_file_per_record(tmp_path):
    out_dir = tmp_path / "per-addr"
    out_mod.write(
        [{"address": "1A", "balance": 1}, {"address": "1B", "balance": 2}],
        directory=str(out_dir),
        id_key="address",
    )
    files = sorted(os.listdir(out_dir))
    assert files == ["1A.json", "1B.json"]
    assert json.loads((out_dir / "1A.json").read_text())["balance"] == 1


def test_write_uses_extension_when_format_missing(tmp_path):
    p = tmp_path / "out.csv"
    out_mod.write([{"x": 1}, {"x": 2}], output=str(p))
    rows = p.read_text().splitlines()
    assert rows == ["x", "1", "2"]


def test_pydantic_model_single_record_not_iterated(tmp_path):
    """A pydantic model exposes __iter__ (yields (name, value) pairs) but
    represents a single record. Ensure we write it as one JSON object, not
    as a jsonl stream of [key, value] lines.
    """
    from graphsense.models.stats import Stats

    stats = Stats(currencies=[], version="2.10.0", request_timestamp="2026-01-01")
    p = tmp_path / "stats.json"
    out_mod.write(stats, output=str(p))
    data = json.loads(p.read_text())
    assert isinstance(data, dict)
    assert data["version"] == "2.10.0"
    assert data["currencies"] == []


# ---------------------------------------------------------------- color


class _TTY:
    """Fake stream that reports isatty()=True."""

    def isatty(self) -> bool:
        return True


class _NotTTY:
    def isatty(self) -> bool:
        return False


def test_should_colorize_auto_on_tty(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("CLICK_COLOR", raising=False)
    assert out_mod.should_colorize(_TTY(), override="auto") is True
    assert out_mod.should_colorize(_NotTTY(), override="auto") is False


def test_should_colorize_no_color_env(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.delenv("CLICK_COLOR", raising=False)
    assert out_mod.should_colorize(_TTY(), override="auto") is False


def test_should_colorize_always_overrides_no_color(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    assert out_mod.should_colorize(_NotTTY(), override="always") is True


def test_should_colorize_never_beats_tty(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    assert out_mod.should_colorize(_TTY(), override="never") is False


def test_file_output_never_colored(tmp_path, monkeypatch):
    """Writing to -o FILE must not emit ANSI even with color=always."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    p = tmp_path / "out.json"
    out_mod.write({"x": 1}, output=str(p), format="json", color="always")
    text = p.read_text()
    assert "\x1b[" not in text


def test_csv_never_colored(tmp_path):
    p = tmp_path / "out.csv"
    out_mod.write([{"x": 1}], output=str(p), format="csv", color="always")
    assert "\x1b[" not in p.read_text()


def test_colorize_json_produces_ansi_when_available():
    """Smoke-test: colorize_json either returns ANSI or (no pygments) plain."""
    result = out_mod.colorize_json('{"x": 1}')
    # Either pygments is installed and we see at least one ANSI escape,
    # or pygments is missing and we see the input unchanged.
    assert "\x1b[" in result or result == '{"x": 1}'
