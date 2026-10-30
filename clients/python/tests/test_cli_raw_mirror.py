"""Regression-proof the auto-mirrored `gs raw` tree.

These tests pin the CLI to the *current* shape of `graphsense.api`. If the
generator renames a method or an API class, one of these tests will fail and
point precisely at the mismatch.

They are deliberately tolerant about additions — every generated `*Api` class
must become a group; a small set of
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


def test_raw_entities_removed_points_to_clusters():
    runner = CliRunner()
    res = runner.invoke(cli, ["raw", "--help"])
    assert "entities" not in res.output

    res = runner.invoke(cli, ["raw", "entities", "get-entity", "btc", "1"])
    assert res.exit_code != 0
    assert "ClustersApi.get_cluster" in res.output


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
