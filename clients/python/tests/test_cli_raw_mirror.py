"""Regression-proof the auto-mirrored `gs raw` tree.

These tests pin the CLI to the *current* shape of `graphsense.api`. If the
generator renames a method or an API class, one of these tests will fail and
point precisely at the mismatch.

They are deliberately tolerant about additions — every generated `*Api` class
except the deprecated `EntitiesApi` must become a group; a small set of
stable endpoints must exist; method invocation must route to the right
HTTP call.
"""

from __future__ import annotations

import inspect

from click.testing import CliRunner

import graphsense
from graphsense.cli.main import cli


def _all_api_classes():
    return sorted(
        name
        for name in dir(graphsense)
        if name.endswith("Api") and inspect.isclass(getattr(graphsense, name))
    )


def test_raw_help_lists_all_non_deprecated_apis():
    runner = CliRunner()
    res = runner.invoke(cli, ["raw", "--help"])
    assert res.exit_code == 0, res.output
    output = res.output
    for cls_name in _all_api_classes():
        key = cls_name[: -len("Api")].lower()
        if cls_name == "EntitiesApi":
            assert key not in output, f"deprecated group {key} leaked"
        else:
            assert key in output, f"missing raw group {key} in help"


def test_raw_addresses_has_stable_methods():
    runner = CliRunner()
    res = runner.invoke(cli, ["raw", "addresses", "--help"])
    assert res.exit_code == 0, res.output
    for expected in (
        "get-address",
        "list-tags-by-address",
        "get-tag-summary-by-address",
        "list-address-txs",
    ):
        assert expected in res.output


def test_raw_entities_hidden_unless_flag_set(monkeypatch):
    monkeypatch.delenv("GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS", raising=False)
    # Re-import the built group to respect env at build time.
    from graphsense.cli.raw import build_raw_group

    group = build_raw_group()
    assert "entities" not in group.commands

    monkeypatch.setenv("GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS", "1")
    group2 = build_raw_group()
    assert "entities" in group2.commands


def test_raw_get_address_round_trips(http_mock, sample_address):
    http_mock.add("GET", "/btc/addresses/1A1z", json_body=sample_address)
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "raw",
            "addresses",
            "get-address",
            "btc",
            "1A1z",
        ],
    )
    assert res.exit_code == 0, res.output
    assert sample_address["address"] in res.output
