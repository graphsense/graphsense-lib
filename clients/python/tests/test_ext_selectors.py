"""Thin selector re-exports."""

from __future__ import annotations

from graphsense.ext.selectors import select_csv, select_json, select_lines


def test_select_json():
    assert select_json('["a","b"]') == ["a", "b"]


def test_select_json_jq():
    assert select_json('[{"x":1},{"x":2}]', "[].x") == ["1", "2"]


def test_select_csv_single_col():
    assert select_csv("address\n1A\n1B\n") == ["1A", "1B"]


def test_select_csv_named_col():
    assert select_csv("address,bal\n1A,10\n1B,20\n", "address") == ["1A", "1B"]


def test_select_lines_skips_comments():
    assert select_lines("# c\n1A\n\n1B\n") == ["1A", "1B"]
