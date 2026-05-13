"""Regression tests for input detection and parsing."""

from __future__ import annotations

import pytest

from graphsense.ext import io as io_mod


def test_detect_json_object():
    assert io_mod.detect_format('  {"x":1}') == "json"


def test_detect_json_array():
    assert io_mod.detect_format("[1,2,3]") == "json"


def test_detect_csv():
    assert io_mod.detect_format("address,balance\n1A1z,100") == "csv"


def test_detect_lines():
    assert io_mod.detect_format("1A1z\n1B2y\n") == "lines"


def test_detect_empty():
    assert io_mod.detect_format("") == "lines"


def test_parse_lines_strips_blanks_and_comments():
    text = "# comment\n1A1z\n\n1B2y\n  # indented comment is a line\n1C3x\n"
    ids = io_mod.parse_input(text, input_format="lines")
    # Non-strict: only `#` at start is a comment; indented `#` lines are ids.
    assert ids[0] == "1A1z"
    assert "1B2y" in ids
    assert "1C3x" in ids


def test_parse_json_array_of_strings():
    ids = io_mod.parse_input('["1A1z","1B2y"]', input_format="json")
    assert ids == ["1A1z", "1B2y"]


def test_parse_json_with_jq_projection():
    doc = '[{"address":"1A1z"},{"address":"1B2y"}]'
    ids = io_mod.parse_input(doc, input_format="json", jq="[].address")
    assert ids == ["1A1z", "1B2y"]


def test_parse_json_with_jq_nested():
    doc = '{"result":{"rows":[{"id":"A"},{"id":"B"}]}}'
    ids = io_mod.parse_input(doc, input_format="json", jq="result.rows[].id")
    assert ids == ["A", "B"]


def test_parse_json_dict_without_jq_errors():
    with pytest.raises(ValueError):
        io_mod.parse_input('{"a":1}', input_format="json")


def test_parse_csv_by_name():
    text = "address,balance\n1A1z,100\n1B2y,200\n"
    ids = io_mod.parse_input(text, input_format="csv", col="address")
    assert ids == ["1A1z", "1B2y"]


def test_parse_csv_by_index():
    text = "address,balance\n1A1z,100\n1B2y,200\n"
    ids = io_mod.parse_input(text, input_format="csv", col="0")
    assert ids == ["1A1z", "1B2y"]


def test_parse_csv_single_column_auto():
    text = "address\n1A1z\n1B2y\n"
    ids = io_mod.parse_input(text, input_format="csv")
    assert ids == ["1A1z", "1B2y"]


def test_parse_csv_missing_column_name():
    text = "address,balance\n1A1z,100\n"
    with pytest.raises(ValueError):
        io_mod.parse_input(text, input_format="csv", col="nope")


def test_parse_csv_named_col_with_underscore_header():
    # Header columns with underscores must not defeat header detection;
    # `_looks_like_header` used to require `.isalpha()` and would reject
    # `btc_address`, breaking `--col btc_address`.
    text = (
        "btc_address,internal_id,seen_at\n"
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa,A-1001,2026-05-04\n"
        "bc1qgdjqv0av3rfu4qf8q5sjxqj5cu4r4qrlu0t0xt,A-1002,2026-05-05\n"
    )
    ids = io_mod.parse_input(text, input_format="csv", col="btc_address")
    assert ids == [
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        "bc1qgdjqv0av3rfu4qf8q5sjxqj5cu4r4qrlu0t0xt",
    ]


def test_parse_csv_named_col_trusts_row_one_as_header():
    # Even if the heuristic would say "this doesn't look like a header",
    # a named --col implies row 1 is the header — just look it up there.
    text = "0xfeed,balance\n0xdead,1\n0xbeef,2\n"
    ids = io_mod.parse_input(text, input_format="csv", col="0xfeed")
    assert ids == ["0xdead", "0xbeef"]


def test_parse_csv_requires_col_for_multi_columns():
    text = "address,balance\n1A1z,100\n"
    with pytest.raises(ValueError):
        io_mod.parse_input(text, input_format="csv")


def test_explicit_format_overrides_auto():
    # content looks like json but we force lines
    ids = io_mod.parse_input('{"x":1}', input_format="lines")
    assert ids == ['{"x":1}']


# --------------------------------------------------------- per-row network


def test_parse_input_with_network_default_when_no_selector():
    pairs = io_mod.parse_input_with_network(
        '["1A","1B"]', input_format="json", default_network="btc"
    )
    assert pairs == [("btc", "1A"), ("btc", "1B")]


def test_parse_input_with_network_jq_aligned():
    doc = '[{"net":"btc","a":"1A"},{"net":"eth","a":"0x1"}]'
    pairs = io_mod.parse_input_with_network(
        doc,
        input_format="json",
        jq="[].a",
        network_jq="[].net",
        default_network="btc",
    )
    assert pairs == [("btc", "1A"), ("eth", "0x1")]


def test_parse_input_with_network_col():
    text = "network,address\nbtc,1A\neth,0x1\n"
    pairs = io_mod.parse_input_with_network(
        text,
        input_format="csv",
        col="address",
        network_col="network",
        default_network="btc",
    )
    assert pairs == [("btc", "1A"), ("eth", "0x1")]


def test_parse_input_with_network_empty_row_uses_default():
    text = "network,address\n,1A\neth,0x1\n"
    pairs = io_mod.parse_input_with_network(
        text,
        input_format="csv",
        col="address",
        network_col="network",
        default_network="btc",
    )
    assert pairs == [("btc", "1A"), ("eth", "0x1")]


def test_parse_input_with_network_misaligned_raises():
    # --network-jq yields fewer values than --jq
    doc = '{"ids":["1A","1B"],"nets":["btc"]}'
    try:
        io_mod.parse_input_with_network(
            doc,
            input_format="json",
            jq="ids",
            network_jq="nets",
            default_network="btc",
        )
    except ValueError as e:
        assert "aligned" in str(e)
    else:
        raise AssertionError("expected ValueError")
