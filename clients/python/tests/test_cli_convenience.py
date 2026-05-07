"""End-to-end tests for `gs` flat convenience commands via CliRunner."""

from __future__ import annotations

import json

from click.testing import CliRunner

from graphsense.cli.main import cli


def _invoke(runner, http_mock, args, input=None):
    # Inject our http_mock via env: the CliRunner builds a fresh GraphSense
    # which gets the ambient RESTClientObject monkeypatch. No extra wiring needed.
    return runner.invoke(cli, args, input=input, catch_exceptions=False)


def test_lookup_address_single(http_mock, sample_address):
    http_mock.add("GET", "/btc/addresses/1A1z", json_body=sample_address)
    runner = CliRunner()
    res = _invoke(
        runner,
        http_mock,
        [
            "--api-key",
            "t",
            "--host",
            "http://testserver",
            "lookup-address",
            "btc",
            "1A1z",
        ],
    )
    assert res.exit_code == 0, res.output
    parsed = json.loads(res.output)
    assert parsed["address"] == sample_address["address"]


def test_lookup_address_list_of_two_uses_per_item(http_mock, sample_address):
    """Two ids with the default threshold=10 stays per-item."""
    http_mock.add("GET", "/btc/addresses/1A", json_body=sample_address)
    http_mock.add("GET", "/btc/addresses/1B", json_body=sample_address)
    runner = CliRunner()
    res = _invoke(
        runner,
        http_mock,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--format",
            "jsonl",
            "lookup-address",
            "btc",
            "1A",
            "1B",
        ],
    )
    assert res.exit_code == 0, res.output
    lines = [line for line in res.output.strip().splitlines() if line]
    assert len(lines) == 2
    # both calls went to per-item endpoints
    per_item = [c for c in http_mock.calls if "/bulk." not in c.url]
    assert len(per_item) == 2


def test_lookup_address_auto_bulk_above_threshold(http_mock):
    http_mock.add(
        "POST",
        "/btc/bulk.json/get_address",
        json_body=[{"address": f"addr{i}"} for i in range(11)],
    )
    runner = CliRunner()
    ids = [f"addr{i}" for i in range(11)]
    res = _invoke(
        runner,
        http_mock,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--quiet",
            "lookup-address",
            "btc",
            *ids,
        ],
    )
    assert res.exit_code == 0, res.output
    assert any("/btc/bulk.json/get_address" in c.url for c in http_mock.calls)


def test_lookup_address_force_no_bulk(http_mock, sample_address):
    for i in range(11):
        http_mock.add("GET", f"/btc/addresses/addr{i}", json_body=sample_address)
    runner = CliRunner()
    ids = [f"addr{i}" for i in range(11)]
    res = _invoke(
        runner,
        http_mock,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--no-bulk",
            "--format",
            "jsonl",
            "lookup-address",
            "btc",
            *ids,
        ],
    )
    assert res.exit_code == 0, res.output
    assert not any("/bulk." in c.url for c in http_mock.calls)


def test_statistics(http_mock):
    http_mock.add("GET", "/stats", json_body={"currencies": []})
    runner = CliRunner()
    res = _invoke(
        runner,
        http_mock,
        ["--host", "http://testserver", "--api-key", "t", "statistics"],
    )
    assert res.exit_code == 0, res.output
    assert "currencies" in res.output


def test_lookup_address_needs_input(http_mock):
    runner = CliRunner()
    res = runner.invoke(
        cli,
        ["--host", "http://testserver", "--api-key", "t", "lookup-address", "btc"],
    )
    assert res.exit_code != 0
    assert "no addresses" in res.output.lower() or "usage" in res.output.lower()


def test_statistics_no_color_by_default_in_cli_runner(http_mock):
    """CliRunner stdout is not a TTY, so color auto should be off."""
    http_mock.add(
        "GET",
        "/stats",
        json_body={"currencies": [], "version": "2.10.0", "request_timestamp": "x"},
    )
    runner = CliRunner()
    res = runner.invoke(
        cli, ["--host", "http://testserver", "--api-key", "t", "statistics"]
    )
    assert res.exit_code == 0, res.output
    assert "\x1b[" not in res.output  # no ANSI codes


def test_statistics_color_always_emits_ansi(http_mock):
    http_mock.add(
        "GET",
        "/stats",
        json_body={"currencies": [], "version": "2.10.0", "request_timestamp": "x"},
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--color",
            "always",
            "statistics",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "\x1b[" in res.output  # pygments ANSI present


def test_no_color_flag_forces_off(http_mock):
    """Even with NO_COLOR unset and --color=always would have colored, --no-color wins."""
    http_mock.add(
        "GET",
        "/stats",
        json_body={"currencies": [], "version": "2.10.0", "request_timestamp": "x"},
    )
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--color",
            "always",
            "--no-color",
            "statistics",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "\x1b[" not in res.output


def test_lookup_address_per_row_network_csv(http_mock, sample_address):
    """CSV with `network` column → dispatches per-network in parallel."""
    http_mock.add("GET", r"/btc/addresses/1A(\?|$)", json_body=sample_address)
    http_mock.add("GET", r"/eth/addresses/0x1(\?|$)", json_body=sample_address)
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
            "--network-col",
            "network",
            "--format",
            "jsonl",
            "--no-bulk",
            "lookup-address",
            "btc",  # positional = fallback
        ],
        input="network,address\nbtc,1A\neth,0x1\n",
    )
    assert res.exit_code == 0, res.output
    # One line per record
    assert res.output.count("\n") == 2
    # Both networks hit
    urls = [c.url for c in http_mock.calls]
    assert any("/btc/addresses/" in u for u in urls)
    assert any("/eth/addresses/" in u for u in urls)


def test_lookup_address_per_row_network_jq(http_mock, sample_address):
    http_mock.add("GET", r"/btc/addresses/1A(\?|$)", json_body=sample_address)
    http_mock.add("GET", r"/eth/addresses/0x1(\?|$)", json_body=sample_address)
    runner = CliRunner()
    payload = '[{"net":"btc","a":"1A"},{"net":"eth","a":"0x1"}]'
    res = runner.invoke(
        cli,
        [
            "--host",
            "http://testserver",
            "--api-key",
            "t",
            "--address-jq",
            "[].a",
            "--network-jq",
            "[].net",
            "--format",
            "jsonl",
            "--no-bulk",
            "lookup-address",
            "btc",
        ],
        input=payload,
    )
    assert res.exit_code == 0, res.output
    urls = [c.url for c in http_mock.calls]
    assert any("/btc/addresses/" in u for u in urls)
    assert any("/eth/addresses/" in u for u in urls)


def test_lookup_address_empty_network_cell_falls_back(http_mock, sample_address):
    """Empty network cell → falls back to positional CURRENCY."""
    http_mock.add("GET", r"/btc/addresses/1A(\?|$)", json_body=sample_address)
    http_mock.add("GET", r"/eth/addresses/0x1(\?|$)", json_body=sample_address)
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
            "--network-col",
            "network",
            "--format",
            "jsonl",
            "--no-bulk",
            "lookup-address",
            "btc",  # fallback network
        ],
        input="network,address\n,1A\neth,0x1\n",
    )
    assert res.exit_code == 0, res.output
    urls = [c.url for c in http_mock.calls]
    # empty first row → btc (fallback); second row → eth
    assert any("/btc/addresses/1A" in u for u in urls)
    assert any("/eth/addresses/0x1" in u for u in urls)
