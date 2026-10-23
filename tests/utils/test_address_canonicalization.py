"""User-input canonicalization: typos must fail, not decode to another address."""

import pytest

from graphsenselib.utils.address import (
    cannonicalize_address,
    lowercase_uppercase_bech32,
    validate_bch_address,
)
from graphsenselib.utils.bch import (
    _CHARSET,
    _PREFIX_VALUES,
    InvalidAddress,
    _polymod,
    bch_address_to_legacy,
    checked_bch_address_to_legacy,
)

BCH_CASHADDR = "bitcoincash:qp3wjpa3tjlj042z2wv7hahsldgwhwy0rq9sywjpyy"
BCH_LEGACY = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
BTC_BECH32 = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
TRX_BASE58 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRX_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"


def _cashaddr(raw: bytes, pad: int = 0) -> str:
    """Encode version byte + hash as a cashaddr with a valid checksum; ``pad``
    sets the padding bits so malformed payloads can still pass the checksum."""
    nbits = len(raw) * 8
    nchars = -(-nbits // 5)
    acc = (int.from_bytes(raw, "big") << (nchars * 5 - nbits)) | pad
    data = [(acc >> 5 * (nchars - 1 - i)) & 31 for i in range(nchars)]
    mod = _polymod(_PREFIX_VALUES + data + [0] * 8)
    data += [(mod >> 5 * (7 - i)) & 31 for i in range(8)]
    return "bitcoincash:" + "".join(_CHARSET[d] for d in data)


@pytest.mark.parametrize(
    "spelling",
    [
        BCH_CASHADDR,
        BCH_CASHADDR.upper(),
        "BITCOINCASH:" + BCH_CASHADDR.split(":")[1],
        BCH_CASHADDR.split(":")[1],
        BCH_CASHADDR.split(":")[1].upper(),
        BCH_LEGACY,
    ],
)
def test_bch_spellings_canonicalize_to_legacy(spelling):
    assert cannonicalize_address("bch", spelling) == BCH_LEGACY


@pytest.mark.parametrize(
    "typo",
    [
        BCH_CASHADDR[:-1] + "z",  # last char changed
        BCH_CASHADDR[:-8] + "qqqqqqqq",  # checksum zeroed
        BCH_CASHADDR[:-2],  # truncated
        BCH_CASHADDR + "qq",  # extended
        "bitcoincash:Q" + BCH_CASHADDR.split(":")[1][1:],  # mixed case
        "bchtest:" + BCH_CASHADDR.split(":")[1],  # other prefix
    ],
)
def test_bch_typos_are_rejected(typo):
    with pytest.raises(ValueError):
        cannonicalize_address("bch", typo)
    assert not validate_bch_address(typo)


def test_bch_checksummed_but_malformed_payloads_are_rejected():
    h20 = bytes(range(20))
    assert checked_bch_address_to_legacy(_cashaddr(b"\x00" + h20)) == (
        bch_address_to_legacy(_cashaddr(b"\x00" + h20))
    )
    with pytest.raises(InvalidAddress, match="padding"):
        checked_bch_address_to_legacy(_cashaddr(b"\x00" + h20, pad=1))
    with pytest.raises(InvalidAddress, match="hash length"):
        checked_bch_address_to_legacy(_cashaddr(b"\x00" + h20 + b"\x00"))
    with pytest.raises(InvalidAddress, match="hash length"):
        checked_bch_address_to_legacy(_cashaddr(b"\x0b" + h20))
    with pytest.raises(InvalidAddress, match="version"):
        checked_bch_address_to_legacy(_cashaddr(b"\x01" + h20))


def test_ingest_fast_path_stays_unchecked():
    # ingest/utxo.py converts trusted node data with the fast path, which
    # skips the checksum; only user input goes through the checked variant.
    typo = BCH_CASHADDR[:-1] + "z"
    assert bch_address_to_legacy(typo) == BCH_LEGACY
    from graphsenselib.ingest import utxo

    assert utxo.bch_address_to_legacy is bch_address_to_legacy


def test_bch_legacy_and_nonstandard_pass_through():
    assert cannonicalize_address("bch", BCH_LEGACY) == BCH_LEGACY
    assert validate_bch_address(BCH_LEGACY)
    assert validate_bch_address(BCH_CASHADDR)
    assert cannonicalize_address("bch", "nonstandard0123abc") == "nonstandard0123abc"


def test_trx_checksum_and_length_are_verified():
    assert cannonicalize_address("trx", TRX_BASE58).hex() == TRX_HEX[2:]
    with pytest.raises(ValueError):
        cannonicalize_address("trx", TRX_BASE58[:-1] + "u")
    with pytest.raises(ValueError):
        cannonicalize_address("trx", TRX_BASE58[:-3])
    with pytest.raises(ValueError):
        cannonicalize_address("trx", BCH_LEGACY)  # valid base58check, not TRX


@pytest.mark.parametrize("spelling", [TRX_HEX, TRX_HEX.upper().replace("X", "x")])
def test_trx_hex_form_is_accepted(spelling):
    assert cannonicalize_address("trx", spelling).hex() == TRX_HEX[2:]


def test_trx_0x_evm_form_is_not_accepted():
    with pytest.raises(ValueError):
        cannonicalize_address("trx", "0x" + TRX_HEX[2:])


def test_uppercase_bech32_is_lowercased():
    assert cannonicalize_address("btc", BTC_BECH32.upper()) == BTC_BECH32
    assert cannonicalize_address("btc", BTC_BECH32) == BTC_BECH32
    ltc = "ltc1qg82tdk8ufl6k0g5a3hqkqrr9ma9p9yn8hzw7an"
    assert cannonicalize_address("ltc", ltc.upper()) == ltc


@pytest.mark.parametrize(
    "currency,typed,expected",
    [
        ("btc", BTC_BECH32.upper(), BTC_BECH32),
        # search input is often only the start of an address
        ("btc", BTC_BECH32.upper()[:10], BTC_BECH32[:10]),
        ("ltc", "LTC1QG82TDK8U", "ltc1qg82tdk8u"),
        ("btc", "Bc1qxy2kgdyg", "Bc1qxy2kgdyg"),  # mixed: untouched
        ("ltc", BTC_BECH32.upper(), BTC_BECH32.upper()),  # not an ltc prefix
        ("zec", "T1HSC1LR8YKN", "T1HSC1LR8YKN"),  # base58 never folded
    ],
)
def test_lowercase_uppercase_bech32(currency, typed, expected):
    assert lowercase_uppercase_bech32(currency, typed) == expected


@pytest.mark.parametrize(
    "currency,address",
    [
        ("btc", "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"),
        ("btc", "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"),
        ("btc", "Bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"),  # mixed: untouched
        ("ltc", "LMHEXuJGoYUmRuVnx4p1UzhDQV3sUafjDk"),
        # starts with "LTC1" by chance, but mixed case: never folded
        ("ltc", "LTC1A4GhnLkKbpfkaDyXHTeShP2YksUVgY"),
        ("zec", "t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs"),
        ("zec", "T1HSC1LR8YKNBBE3TWRP88P6VFFC5T7DLBS"),  # base58 never folded
    ],
)
def test_base58_and_mixed_case_unchanged(currency, address):
    assert cannonicalize_address(currency, address) == address
