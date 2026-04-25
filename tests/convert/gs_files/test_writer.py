"""Tests for JSON serialization and file writing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from graphsenselib.convert.gs_files.parser import Color, PathfinderData
from graphsenselib.convert.gs_files.writer import (
    _resolve_paths,
    to_jsonable,
    write_decoded,
    write_json,
)


class TestToJsonable:
    def test_dataclass_recursion(self):
        data = PathfinderData(version="1", name="x")
        out = to_jsonable(data)
        assert out["version"] == "1"
        assert out["name"] == "x"
        assert out["addresses"] == []
        # kind is populated (non-init field)
        assert out["kind"] == "pathfinder"

    def test_nested_dataclass(self):
        c = Color(0.1, 0.2, 0.3, 1.0)
        assert to_jsonable({"c": c, "xs": [c]})["c"] == {
            "r": 0.1,
            "g": 0.2,
            "b": 0.3,
            "a": 1.0,
        }

    def test_passthrough_primitives(self):
        assert to_jsonable(42) == 42
        assert to_jsonable("s") == "s"
        assert to_jsonable(None) is None


class TestWriteJson:
    def test_to_file(self, tmp_path: Path):
        target = tmp_path / "out.json"
        write_json({"a": 1}, target, indent=2)
        assert json.loads(target.read_text()) == {"a": 1}

    def test_to_stdout(self, capsys):
        write_json({"a": 1}, None, indent=None)
        captured = capsys.readouterr()
        assert json.loads(captured.out) == {"a": 1}


class TestResolvePaths:
    def test_no_output(self):
        assert _resolve_paths(None, "both") == (None, None)
        assert _resolve_paths(None, "raw") == (None, None)

    def test_raw_only(self, tmp_path: Path):
        p = tmp_path / "x.json"
        assert _resolve_paths(p, "raw") == (p, None)

    def test_structured_only(self, tmp_path: Path):
        p = tmp_path / "x.json"
        assert _resolve_paths(p, "structured") == (None, p)

    def test_both_splits_suffix(self, tmp_path: Path):
        p = tmp_path / "x.json"
        raw, structured = _resolve_paths(p, "both")
        assert raw == tmp_path / "x.raw.json"
        assert structured == tmp_path / "x.structured.json"


class TestWriteDecoded:
    def test_both_writes_two_files(self, tmp_path: Path):
        base = tmp_path / "out.json"
        structured = PathfinderData(version="1", name="g")
        write_decoded(
            raw=["pathfinder", "1", "g", [], [], []],
            structured=structured,
            fmt="both",
            output=base,
            indent=None,
        )
        assert (tmp_path / "out.raw.json").exists()
        assert (tmp_path / "out.structured.json").exists()

    def test_structured_requires_payload(self, tmp_path: Path):
        with pytest.raises(ValueError, match="structured payload required"):
            write_decoded(
                raw={"x": 1},
                structured=None,
                fmt="structured",
                output=tmp_path / "o.json",
                indent=None,
            )
