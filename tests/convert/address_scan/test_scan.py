"""Tests for the address scanner (detectors, decompression, CLI)."""

from __future__ import annotations

import base64
import gzip
import json
import struct

import pytest
from click.testing import CliRunner

from graphsenselib.convert.address_scan.cli import scan_for_addresses_cmd
from graphsenselib.convert.address_scan.detectors import decodes_to_text
from graphsenselib.convert.address_scan.scanner import build_report, scan
from graphsenselib.convert.gs_files.parser import lzw_pack

# Real / valid fixtures per detector.
VALID = {
    "BTC legacy/P2SH": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    "BTC bech32": "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
    "LTC legacy": "LZmTCnfQ9rmGWxaEJ4SKQKXmrL8girNk19",
    "ETH": "0x52908400098527886E0F7030069857D2E4169EE7",
    "TRX": "TMacq4TDUw5q8NFBwmbY4RLXvzvG5JTkvi",
    "ZEC transparent": "t1YR6wumi3XJornvy4MG9G7ZvtmxVJenFnj",
    "XRP": "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh",
}


def test_scan_validates_every_currency():
    text = " ".join(VALID.values())
    found, _rejected = scan(text)
    for label, addr in VALID.items():
        assert label in found, f"{label} not detected"
        assert addr in found[label], f"{addr} not validated for {label}"


def test_scan_rejects_bad_checksums():
    # Mangled last char -> checksum fails; address-shaped but invalid.
    text = (
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNb 0x52908400098527886e0f7030069857d2e4169zz7"
    )
    found, rejected = scan(text)
    assert not found
    assert "BTC legacy/P2SH" in rejected


def test_eth_all_lower_accepted_mixed_checksum_enforced():
    lower = "0x52908400098527886e0f7030069857d2e4169ee7"
    # flip case of one letter to break EIP-55
    broken = "0x52908400098527886E0f7030069857D2E4169EE7"
    found, rejected = scan(f"{lower} {broken}")
    assert lower in found["ETH"]
    assert broken in rejected["ETH"]


def test_decodes_to_text_filters_hex_blobs():
    assert decodes_to_text(b"hello world this is text".hex())
    assert not decodes_to_text(b"\x00\x11\x22\xfe\xdd\xac\x90".hex())


def test_tx_hashes_opt_in():
    sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    without, _ = scan(sha, tx_hashes=False)
    assert not without
    with_tx, _ = scan(sha, tx_hashes=True)
    assert sha in with_tx["TX-hash candidate (64hex)"]


def test_build_report_unwraps_gzip(tmp_path):
    payload = f"addr {VALID['BTC legacy/P2SH']}".encode()
    p = tmp_path / "dump.sql.gz"
    p.write_bytes(gzip.compress(payload))
    report = build_report([str(p)])
    assert report["summary"]["unique_valid_addresses"] == 1
    # two streams: the raw .gz (no hits) and the inflated content
    labels = [s["stream"] for f in report["files"] for s in f["streams"]]
    assert any("gzip" in label for label in labels)


def test_build_report_unwraps_gs(tmp_path):
    # Minimal .gs: Uint32Array(LE) <- lzwcompress.pack(base64(JSON))
    inner = json.dumps(["pathfinder", "1", VALID["ETH"]])
    b64 = base64.b64encode(inner.encode()).decode()
    codes = lzw_pack(b64)
    p = tmp_path / "graph.gs"
    p.write_bytes(struct.pack(f"<{len(codes)}I", *codes))
    report = build_report([str(p)])
    labels = [s["stream"] for f in report["files"] for s in f["streams"]]
    assert any("gs/lzw" in label for label in labels)
    assert report["summary"]["unique_valid_addresses"] == 1


def test_no_decompress_skips_containers(tmp_path):
    p = tmp_path / "dump.sql.gz"
    p.write_bytes(gzip.compress(VALID["ETH"].encode()))
    report = build_report([str(p)], decompress=False)
    assert report["summary"]["unique_valid_addresses"] == 0


def test_cli_json_output_and_tx_warning(tmp_path):
    sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    p = tmp_path / "dump.txt"
    p.write_text(f"{VALID['BTC legacy/P2SH']} {sha}")
    runner = CliRunner()
    result = runner.invoke(scan_for_addresses_cmd, ["--tx-hashes", "--json", str(p)])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["summary"]["unique_valid_addresses"] == 1
    assert report["summary"]["unique_tx_hash_candidates"] == 1
    # warning must go to stderr, not pollute JSON on stdout
    assert "WARNING" in result.stderr


def test_cli_missing_file_errors():
    runner = CliRunner()
    result = runner.invoke(scan_for_addresses_cmd, ["/no/such/file.sql"])
    assert result.exit_code != 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
