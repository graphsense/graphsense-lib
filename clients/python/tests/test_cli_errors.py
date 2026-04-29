"""Friendly handling of REST 4xx/5xx errors raised inside CLI commands."""

from __future__ import annotations

import inspect
import json

from click.testing import CliRunner

from graphsense.cli.main import cli


def _runner_with_split_stderr() -> CliRunner:
    params = inspect.signature(CliRunner.__init__).parameters
    if "mix_stderr" in params:
        return CliRunner(mix_stderr=False)
    return CliRunner()


def test_404_prints_friendly_error_and_exits_nonzero(http_mock):
    http_mock.add(
        "GET",
        r"/btc/txs/missing",
        status=404,
        body=json.dumps({"detail": "tx missing not found"}),
        headers={"content-type": "application/json"},
    )
    runner = _runner_with_split_stderr()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "lookup-tx",
            "btc",
            "missing",
        ],
    )
    assert res.exit_code == 1
    err = res.stderr.lower() if hasattr(res, "stderr") else res.output.lower()
    assert "404" in err
    assert "tx missing not found" in err
    # No traceback in user-facing output.
    assert "traceback" not in err


def test_400_bad_request_friendly_error(http_mock):
    http_mock.add(
        "GET",
        r"/btc/txs/oops",
        status=400,
        body=json.dumps({"detail": "bad input"}),
        headers={"content-type": "application/json"},
    )
    runner = _runner_with_split_stderr()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "lookup-tx",
            "btc",
            "oops",
        ],
    )
    assert res.exit_code == 1
    err = res.stderr if hasattr(res, "stderr") else res.output
    assert "400" in err
    assert "bad input" in err


def test_500_server_error_distinct_exit_code(http_mock):
    http_mock.add(
        "GET",
        r"/btc/txs/boom",
        status=500,
        body=json.dumps({"detail": "internal"}),
        headers={"content-type": "application/json"},
    )
    runner = _runner_with_split_stderr()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "lookup-tx",
            "btc",
            "boom",
        ],
    )
    assert res.exit_code == 2
    err = res.stderr if hasattr(res, "stderr") else res.output
    assert "500" in err


def test_404_with_non_json_body_still_friendly(http_mock):
    http_mock.add(
        "GET",
        r"/btc/txs/x",
        status=404,
        body="not found, plain text",
        headers={"content-type": "text/plain"},
    )
    runner = _runner_with_split_stderr()
    res = runner.invoke(
        cli,
        ["--host", "http://testserver", "--api-key", "t", "lookup-tx", "btc", "x"],
    )
    assert res.exit_code == 1
    err = res.stderr if hasattr(res, "stderr") else res.output
    assert "404" in err
    assert "traceback" not in err.lower()
