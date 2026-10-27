"""address_checksum_invalid: telling a typo from an address that is not indexed."""

import hashlib

import base58
import pytest

from graphsenselib.utils.address import address_checksum_invalid, bech32_mixed_case

BTC_LEGACY = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"  # genesis
BTC_BECH32 = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
LTC_BECH32 = "ltc1q000hma9ysnh7ref6crn45448vnw8vlwawlmxqe"
# base58: starts with "LTC1" by chance (always "L", then random characters)
LTC_BASE58_LOOKALIKE = "LTC1A4GhnLkKbpfkaDyXHTeShP2YksUVgY"


def _b58check(payload: bytes) -> str:
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    return base58.b58encode(payload + checksum).decode()


def _flip_last(s: str) -> str:
    """Change the last character to another one of the same alphabet."""
    return s[:-1] + ("b" if s[-1] != "b" else "c")


@pytest.mark.parametrize(
    "network, address",
    [
        ("btc", BTC_LEGACY),
        ("btc", BTC_BECH32),
        ("btc", BTC_BECH32.upper()),
        # BIP350 vectors: Taproot-style and the odd witness versions/lengths
        # that exist on chain must never look like typos.
        ("btc", "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0"),
        ("btc", "BC1SW50QGDZ25J"),
        ("btc", "bc1zw508d6qejxtdg4y5r3zarvaryvaxxpcs"),
        # Lenient: a v1 program with the old bech32 constant (BIP173 vector,
        # invalid under BIP350) is not called a typo either.
        (
            "btc",
            "bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7k7grplx",
        ),
        ("btc", "tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx"),
        ("bch", BTC_LEGACY),
        ("ltc", _b58check(bytes([0x30]) + bytes(20))),
        ("ltc", LTC_BASE58_LOOKALIKE),
        ("zec", _b58check(bytes([0x1C, 0xB8]) + bytes(20))),
    ],
)
def test_valid_checksums_are_not_typos(network, address):
    assert not address_checksum_invalid(network, address)


@pytest.mark.parametrize(
    "network, address",
    [
        ("btc", _flip_last(BTC_LEGACY)),
        ("btc", BTC_BECH32[:-1] + "j"),  # valid charset, wrong checksum
        ("btc", BTC_BECH32[:-1]),  # truncated
        ("btc", BTC_BECH32 + "q"),  # extended
        ("btc", "Bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"),  # mixed case
        ("btc", "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlb"),  # 'b' not bech32
        ("bch", _flip_last(BTC_LEGACY)),
        ("ltc", _flip_last(_b58check(bytes([0x30]) + bytes(20)))),
        ("ltc", "ltc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"),  # wrong hrp checksum
        ("zec", _flip_last(_b58check(bytes([0x1C, 0xB8]) + bytes(20)))),
    ],
)
def test_wrong_checksums_are_typos(network, address):
    assert address_checksum_invalid(network, address)


@pytest.mark.parametrize(
    "network, address",
    [
        # Not a checksummed format we recognise: stays "not found".
        ("btc", "nonstandard0123456789abcdef"),
        ("btc", "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfN0"),  # '0' is not base58
        ("btc", ""),
        ("btc", None),
        ("eth", "0x" + "ab" * 20),
        ("trx", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6u"),  # checked elsewhere
        ("bnb", BTC_LEGACY),
    ],
)
def test_unrecognised_input_is_left_alone(network, address):
    assert not address_checksum_invalid(network, address)


def test_network_is_case_insensitive():
    assert address_checksum_invalid("BTC", _flip_last(BTC_LEGACY))


@pytest.mark.parametrize(
    "network, address, expected",
    [
        ("btc", "Bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh", True),
        ("ltc", "Ltc1q000hma9ysnh7ref6crn45448vnw8vlwawlmxqe", True),
        ("btc", BTC_BECH32, False),
        ("btc", BTC_BECH32.upper(), False),
        ("btc", "Bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlj", False),  # also a typo
        ("ltc", LTC_BASE58_LOOKALIKE, False),
        ("ltc", "Bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh", False),  # not ltc
    ],
)
def test_bech32_mixed_case(network, address, expected):
    """Only the case is wrong: in lowercase it is a valid bech32 address."""
    assert bech32_mixed_case(network, address) is expected
