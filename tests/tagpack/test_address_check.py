"""Address validation of tagpacks: hints, the report, severity and opt-out.
Public example addresses and synthetic variants only."""

import logging

import base58
import pytest
from click.testing import CliRunner

from graphsenselib.tagpack.address_check import (
    address_hint,
    address_line_numbers,
    autofill_hints,
    validate_token_address,
)
from graphsenselib.tagpack.constants import NETWORK_SUGGESTIONS
from graphsenselib.tagpack.tagpack import check_text_field
from graphsenselib.utils.address import is_verifiable_network

pytest.importorskip("yaml_include")
pytest.importorskip("rapidfuzz")

from graphsenselib.tagpack.cli import tagpacktool_cli  # noqa: E402

BTC = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"  # genesis coinbase
TRX = "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu"
ETH = "0xaAA9402664f1a41F40EBbC52c9993eb66AeB3666"  # EIP-55 mixed case
TAPROOT = "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0"
WIF = base58.b58encode_check(b"\x80" + bytes(range(1, 33)) + b"\x01").decode()


@pytest.mark.parametrize(
    "network, address, context, expected",
    [
        ("btc", TRX, None, "a valid TRX address; wrong network?"),
        ("trx", BTC, None, "a valid BTC address; wrong network?"),
        ("btc", ETH, None, "an EVM address, not a valid BTC address"),
        ("btc", ETH, '{"chain": "BSC"}', "Set network to BSC"),
        ("btc", ETH, {"chain": "BSC"}, "Set network to BSC"),
        (
            "btc",
            "0x" + BTC.encode().hex(),
            None,
            f"hex-encoded text of the BTC address {BTC}",
        ),
        (
            "trx",
            "0x" + base58.b58decode_check(TRX)[1:].hex(),
            None,
            f"base58check is {TRX}",
        ),
        ("btc", TAPROOT[:42], None, "truncated? bc1p… addresses have 62 characters"),
        (
            "eth",
            "0xAAA9402664f1a41F40EBbC52c9993eb66AeB3666",
            None,
            "EIP-55 checksum case",
        ),
        (
            "bsc",
            "0xAAA9402664f1a41F40EBbC52c9993eb66AeB3666",
            None,
            "EIP-55 checksum case",
        ),
        ("trx", "583812503", None, "not an address"),
        ("trx", "7707056547/771901001", None, "not an address"),
        ("trx", "6038735(IMO-number)", None, "not an address"),
        ("trx", TRX[:30] + "Н" + TRX[31:], None, "U+041D) at position 31"),
        ("trx", "T\\u041d" + TRX[2:], None, "undecoded escape sequence"),
        (
            "btc",
            "xpub661MyMwAqRbcEYS8w7XLSVeEsBXy79zSzH1J8vCdxAZningWLdN3",
            None,
            "extended key",
        ),
        ("btc", WIF, None, "private key (WIF)"),
        ("btc", BTC[:-1] + "b", None, "checksum mismatch"),
        (
            "btc",
            "Bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            None,
            "upper and lower case mixed",
        ),
        ("trx", TRX[:5] + "x" + TRX[6:], None, "checksum mismatch"),
        ("btc", "definitely-not-an-address", None, "not a valid BTC address"),
        # ZIP-320 test vector: TEX encoding of a t1 P2PKH hash
        (
            "zec",
            "tex1s2rt77ggv6q989lr49rkgzmh5slsksa9khdgte",
            None,
            "the chain has it as t1VmmGiyjVNeCjxDZzg7vZmd99WyzVby9yC",
        ),
    ],
)
def test_address_hint(network, address, context, expected):
    assert expected in address_hint(network, address, context)


@pytest.mark.parametrize(
    "network, address, expected",
    [
        ("usdt", ETH, True),
        ("usdt", TRX, True),
        ("usdt", "tethertether", True),  # EOSIO account
        ("usdt", BTC, True),  # Omni USDT lives on BTC addresses
        ("usdt", "LiM295pXpydyt4ahLxEYJeevpLtm2e6LSv", False),  # LTC
        ("usdt", "EQCD39VS5jcptHL8vMjEXrzGaRcCVYto7HUn4bpAOg8xqB2N", True),  # TON
        ("dai", ETH, True),
        ("weth", TRX, False),
        ("usdc", "0xAAA9402664f1a41F40EBbC52c9993eb66AeB3666", False),
        ("kcs", ETH, True),
        ("wbtc", ETH, None),  # no known chains
        ("btc", BTC, None),  # a chain, not a token
    ],
)
def test_validate_token_address(network, address, expected):
    assert validate_token_address(network, address) is expected


def test_every_chain_of_a_token_has_a_validator():
    # otherwise no address of the token can be called invalid
    for token, chains in NETWORK_SUGGESTIONS.items():
        missing = [c for c in chains if not is_verifiable_network(c)]
        assert not missing, f"{token}: no validator for {missing}"


def test_token_hint_names_the_likely_chain():
    hint = address_hint("usdt", TRX[:5] + "x" + TRX[6:])
    assert hint.startswith("USDT is a token; not valid on any of its chains (ETH,")
    assert hint.endswith("as TRX: checksum mismatch, probably a typo")


def test_autofill_run_names_the_valid_address():
    stem = BTC[:-2]
    run = [f"{stem}{i}" for i in (11, 12, 13)]
    hints = autofill_hints(run + ["unrelated"], {f"{stem}10", BTC})
    assert set(hints) == set(run)
    assert f"the valid one in this pack is {stem}10" in hints[run[0]]
    # two are no run
    assert autofill_hints(run[:2], set()) == {}


def test_address_line_numbers(tmp_path):
    p = tmp_path / "p.yaml"
    p.write_text(f"tags:\n- address: {BTC}\n  label: x\n-   address: '{TRX}'\n")
    assert address_line_numbers(str(p), {BTC, TRX, "missing"}) == {BTC: 2, TRX: 4}


@pytest.mark.parametrize("value", ["see \\u041d", "a\\x41b", "line\\nbreak"])
def test_undecoded_escapes_are_text_problems(value):
    warnings = []
    check_text_field("label", value, "tag", warnings)
    assert "undecoded escape sequence" in warnings[0]


def test_escapes_of_a_json_context_are_not_text_problems():
    warnings = []
    check_text_field("context", '{"note": "\\u738b \\n"}', "tag", warnings)
    check_text_field("label", "王", "tag", warnings)
    assert warnings == []


HEADER = """title: Test pack
creator: Test
source: http://example.com
currency: BTC
lastmod: 2024-01-01
"""


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "packs"
    root.mkdir()
    (root / "header.yaml").write_text(HEADER)
    (root / "good.yaml").write_text(
        f"header: !include header.yaml\ntags:\n- address: {BTC}\n  label: A\n"
    )
    (root / "bad.yaml").write_text(
        "header: !include header.yaml\n"
        "tags:\n"
        f"- address: {BTC}\n"
        "  label: A\n"
        f"- address: {TRX}\n"
        "  label: B\n"
        "- address: '583812503'\n"
        "  label: C\n"
        "  address_validation: skip\n"
        '  context: \'{"note": "registration number, kept verbatim"}\'\n'
    )
    return root


def _validate(root, *args):
    return CliRunner().invoke(
        tagpacktool_cli,
        [
            "tagpack-tool",
            "tagpack",
            "validate",
            str(root),
            "--no-strict-actor-references",
            *args,
        ],
    )


@pytest.mark.parametrize("jobs", ["1", "2"])
def test_warn_is_the_default_and_report_lists_every_invalid_address(
    root, tmp_path, jobs
):
    report = tmp_path / "invalid.tsv"
    result = _validate(root, "-j", jobs, "--invalid-addresses-report", str(report))
    assert result.exit_code == 0, result.output
    rows = report.read_text().splitlines()
    assert rows[0] == "file\tline\tnetwork\taddress\thint"
    # the skipped tag is not reported
    assert len(rows) == 2
    file, line, network, address, hint = rows[1].split("\t")
    assert file.endswith("bad.yaml")
    assert (line, network, address) == ("5", "BTC", TRX)
    assert hint == "a valid TRX address; wrong network?"


@pytest.mark.parametrize("jobs", ["1", "2"])
def test_error_fails_packs_with_invalid_addresses(root, jobs, caplog):
    with caplog.at_level(logging.INFO):
        result = _validate(root, "-j", jobs, "--address-validation", "error")
    assert result.exit_code == 1
    failed = [r.getMessage() for r in caplog.records if "FAILED" in r.getMessage()]
    assert len(failed) == 1
    assert "bad.yaml - 1 invalid address(es)" in failed[0]
    assert any("1/2 TagPacks passed" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(
    "args",
    [
        ["--address-validation", "off"],
        ["--address-validation", "error", "--no-address-validation"],
    ],
)
def test_off_skips_address_validation(root, args):
    result = _validate(root, *args)
    assert result.exit_code == 0, result.output


def test_address_validation_field_only_takes_skip(root, caplog):
    (root / "good.yaml").write_text(
        "header: !include header.yaml\n"
        f"tags:\n- address: {BTC}\n  label: A\n  address_validation: 'no'\n"
    )
    with caplog.at_level(logging.INFO):
        result = _validate(root)
    assert result.exit_code == 1
    assert any(
        "address_validation must be 'skip'" in r.getMessage() for r in caplog.records
    )
