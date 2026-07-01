"""Smoke tests for `graphsense scan-for-addresses` / `graphsense file ...`.

Uses the vendored, standalone `graphsense.address_scan` (no graphsenselib).
No network, no fixtures.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

from click.testing import CliRunner

from graphsense.cli.main import cli

BTC = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_file_scan_json(tmp_path: Path) -> None:
    p = tmp_path / "dump.txt"
    p.write_text(f"pay to {BTC}")
    res = CliRunner().invoke(cli, ["file", "scan-for-addresses", "--json", str(p)])
    assert res.exit_code == 0, res.output
    report = json.loads(res.stdout)
    assert report["summary"]["unique_valid_addresses"] == 1


def test_file_subcommand_unwraps_gzip(tmp_path: Path) -> None:
    p = tmp_path / "dump.sql.gz"
    p.write_bytes(gzip.compress(f"addr {BTC}".encode()))
    res = CliRunner().invoke(cli, ["file", "scan-for-addresses", "--json", str(p)])
    assert res.exit_code == 0, res.output
    report = json.loads(res.stdout)
    assert report["summary"]["unique_valid_addresses"] == 1


def test_tx_hashes_warns_and_json_parses(tmp_path: Path) -> None:
    p = tmp_path / "dump.txt"
    p.write_text(f"{BTC} {SHA}")
    res = CliRunner().invoke(
        cli, ["file", "scan-for-addresses", "--tx-hashes", "--json", str(p)]
    )
    assert res.exit_code == 0, res.output

    # The warning is emitted to stderr (so it never corrupts the JSON on stdout
    # in real use). Older click versions merge the streams in CliRunner, so
    # check the combined output rather than depending on separation.
    combined = res.stdout
    try:
        combined += res.stderr
    except ValueError:  # streams not separately captured on this click version
        pass
    assert "WARNING" in combined

    # JSON must still be parseable; slice from the first '{' to be robust to
    # test-runners that prepend the merged stderr warning.
    report = json.loads(res.stdout[res.stdout.index("{") :])
    assert report["summary"]["unique_tx_hash_candidates"] == 1
