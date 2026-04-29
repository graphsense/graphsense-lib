"""CLI tests for `gs bulk`."""

from __future__ import annotations

from click.testing import CliRunner

from graphsense.cli.main import cli


def test_gs_bulk_json(http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.json/get_address",
        json_body=[{"address": "1A"}, {"address": "1B"}],
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "bulk",
            "get_address",
            "btc",
            "1A",
            "1B",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "1A" in res.output


def test_gs_bulk_reads_csv_from_stdin(http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.json/get_address",
        json_body=[{"address": "1A"}, {"address": "1B"}],
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--address-col",
            "address",
            "bulk",
            "get_address",
            "btc",
        ],
        input="address\n1A\n1B\n",
    )
    assert res.exit_code == 0, res.output


def test_gs_bulk_csv_format(http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.csv/get_address",
        body="address,balance\n1A,1\n1B,2\n",
        headers={"content-type": "text/csv"},
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--format",
            "csv",
            "bulk",
            "get_address",
            "btc",
            "1A",
            "1B",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "address,balance" in res.output
