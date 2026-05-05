"""stdin piping + deprecation warnings."""

from __future__ import annotations

import inspect
import json

from click.testing import CliRunner

from graphsense.cli.main import cli


def _runner_with_split_stderr() -> CliRunner:
    """Return a CliRunner that exposes stderr separately on Result.

    Click 8.1 defaults to merging stderr into stdout and needs
    mix_stderr=False to split them; click 8.2+ removes that kwarg and
    always splits. Support both.
    """
    params = inspect.signature(CliRunner.__init__).parameters
    if "mix_stderr" in params:
        return CliRunner(mix_stderr=False)
    return CliRunner()


def test_pipe_json_selector(http_mock, sample_address):
    http_mock.add("GET", "/btc/addresses/1A1z", json_body=sample_address)
    http_mock.add("GET", "/btc/addresses/1B2y", json_body=sample_address)

    runner = CliRunner()
    payload = json.dumps([{"address": "1A1z"}, {"address": "1B2y"}])
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--address-jq",
            "[].address",
            "--format",
            "jsonl",
            "lookup-address",
            "btc",
        ],
        input=payload,
    )
    assert res.exit_code == 0, res.output
    assert res.output.count("\n") == 2


def test_pipe_csv_selector(http_mock, sample_address):
    http_mock.add("GET", "/btc/addresses/1A1z", json_body=sample_address)
    http_mock.add("GET", "/btc/addresses/1B2y", json_body=sample_address)

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
            "--format",
            "jsonl",
            "lookup-address",
            "btc",
        ],
        input="address,balance\n1A1z,100\n1B2y,200\n",
    )
    assert res.exit_code == 0, res.output
    assert res.output.count("\n") == 2


def test_deprecation_header_triggers_stderr_warning(http_mock, sample_address):
    http_mock.add(
        "GET",
        "/btc/addresses/1A1z",
        json_body=sample_address,
        headers={
            "content-type": "application/json",
            "Deprecation": "true",
            "Sunset": "Sat, 31 Oct 2026 00:00:00 GMT",
        },
    )
    runner = _runner_with_split_stderr()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "lookup-address",
            "btc",
            "1A1z",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "deprecated" in res.stderr.lower()
    assert "2026" in res.stderr


def test_quiet_suppresses_deprecation_warning(http_mock, sample_address):
    http_mock.add(
        "GET",
        "/btc/addresses/1A1z",
        json_body=sample_address,
        headers={
            "content-type": "application/json",
            "Deprecation": "true",
            "Sunset": "Sat, 31 Oct 2026 00:00:00 GMT",
        },
    )
    runner = _runner_with_split_stderr()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--quiet",
            "lookup-address",
            "btc",
            "1A1z",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "deprecated" not in (res.stderr or "").lower()
