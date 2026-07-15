# -*- coding: utf-8 -*-
"""Wrong-network base58 P2PKH normalization.

Pre-c103323c ingests derived fallback addresses with BTC's version byte on
every UTXO network. On LTC this produced BTC-form (``1…``) P2PK addresses
which are still stored in the raw keyspace, the raw delta lake, and — via
input resolution against those stored rows — keep leaking into newly
ingested spends. The normalizer re-encodes such addresses to the network's
own P2PKH version byte and is a no-op on everything else.
"""

from base58 import b58decode_check

from graphsenselib.ingest.utxo import (
    OutputResolverBase,
    enrich_txs,
    normalize_base58_p2pkh,
    normalize_tx_addresses_inplace,
)
from graphsenselib.utils.pubkey_to_address import base58check_encode

# Prod-proven pair: LTC block-157 coinbase P2PK output, BTC-form as stored
# by pre-fix ingests vs the correct LTC encoding of the same hash160.
BTC_FORM = "1417xKRL9Lx146CsswrwqZUBq6PDsU6dWq"
LTC_FORM = "LNE5DXjAE1C4Jtu345rF7aXx3JkVyMevrL"
HASH160 = bytes.fromhex("20ee2e9ade9fd8a499f17b69962edf07524ab5b7")


def test_btc_form_is_reencoded_for_ltc():
    assert normalize_base58_p2pkh(BTC_FORM, "ltc") == LTC_FORM


def test_correct_ltc_forms_unchanged():
    for address in [
        LTC_FORM,  # p2pkh (0x30)
        base58check_encode(b"\x32", HASH160),  # p2sh, M… (0x32)
        base58check_encode(b"\x05", HASH160),  # deprecated ltc p2sh, 3… (0x05)
        "ltc1ql8dxppm0ge7g3nx4x9l4lssazf9j2wxz2umstw",  # bech32
    ]:
        assert normalize_base58_p2pkh(address, "ltc") == address


def test_noop_on_networks_sharing_btc_version_byte():
    assert normalize_base58_p2pkh(BTC_FORM, "btc") == BTC_FORM
    assert normalize_base58_p2pkh(BTC_FORM, "bch") == BTC_FORM


def test_noop_on_unknown_network():
    assert normalize_base58_p2pkh(BTC_FORM, "nosuchnet") == BTC_FORM


def test_doge_rewrites_btc_form():
    fixed = normalize_base58_p2pkh(BTC_FORM, "doge")
    assert fixed.startswith("D")
    assert b58decode_check(fixed)[1:] == HASH160


def test_invalid_or_synthetic_addresses_unchanged():
    # bad checksum (last char flipped)
    broken = BTC_FORM[:-1] + ("q" if BTC_FORM[-1] != "q" else "r")
    assert normalize_base58_p2pkh(broken, "ltc") == broken
    # synthetic nonstandard id
    synthetic = "nonstandard" + "0" * 40
    assert normalize_base58_p2pkh(synthetic, "ltc") == synthetic
    # non-base58 characters
    assert normalize_base58_p2pkh("1nv@lid!!", "ltc") == "1nv@lid!!"


def test_none_and_empty_unchanged():
    assert normalize_base58_p2pkh(None, "ltc") is None
    assert normalize_base58_p2pkh("", "ltc") == ""


def test_zec_btc_form_reencodes_to_t1():
    wrong = base58check_encode(b"\x00", HASH160)
    expected = base58check_encode(b"\x1c\xb8", HASH160)
    assert normalize_base58_p2pkh(wrong, "zec") == expected


def test_leading_zero_hash160_keeps_all_digits():
    # hash160 with a leading zero byte encodes with an extra leading "1";
    # the swap must preserve the payload exactly.
    h160 = b"\x00" + HASH160[1:]
    wrong = base58check_encode(b"\x00", h160)
    assert wrong.startswith("11")
    assert normalize_base58_p2pkh(wrong, "ltc") == base58check_encode(b"\x30", h160)


def test_normalize_tx_addresses_inplace():
    txs = [
        {
            "inputs": [
                {"addresses": [BTC_FORM], "type": "p2pk", "value": 1},
                {"addresses": [], "type": None, "value": None},
            ],
            "outputs": [
                {"addresses": [LTC_FORM, BTC_FORM], "type": "multisig", "value": 2},
                {"addresses": [None], "type": "nonstandard", "value": 3},
            ],
        }
    ]
    normalize_tx_addresses_inplace(txs, "ltc")
    assert txs[0]["inputs"][0]["addresses"] == [LTC_FORM]
    assert txs[0]["inputs"][1]["addresses"] == []
    assert txs[0]["outputs"][0]["addresses"] == [LTC_FORM, LTC_FORM]
    assert txs[0]["outputs"][1]["addresses"] == [None]


class _FakeResolver(OutputResolverBase):
    """Returns the stored (corrupted) form, like CassandraOutputResolver
    does for pre-fix raw rows."""

    def __init__(self, outputs):
        self._outputs = outputs

    def get_output(self, tx_hash):
        return self._outputs.get(tx_hash)

    def add_output(self, tx_hash, output):
        pass


def test_enrich_txs_normalizes_resolver_resolved_inputs():
    resolver = _FakeResolver(
        {"beef": {0: {"addresses": [BTC_FORM], "type": "p2pk", "value": 5000000000}}}
    )
    txs = [
        {
            "hash": "aa",
            "is_coinbase": False,
            "inputs": [
                {
                    "spent_transaction_hash": "beef",
                    "spent_output_index": 0,
                    "addresses": [],
                    "type": None,
                    "value": None,
                }
            ],
            "outputs": [],
        }
    ]
    enrich_txs(
        txs,
        resolver,
        ignore_missing_outputs=False,
        input_reference_only=False,
        network="ltc",
    )
    assert txs[0]["inputs"][0]["addresses"] == [LTC_FORM]
    assert txs[0]["inputs"][0]["value"] == 5000000000


def test_enrich_txs_normalizes_output_addresses():
    txs = [
        {
            "hash": "bb",
            "is_coinbase": True,
            "inputs": [],
            "outputs": [
                {
                    "addresses": [BTC_FORM],
                    "type": "p2pk",
                    "script_hex": "",
                    "value": 5000000000,
                }
            ],
        }
    ]
    enrich_txs(
        txs,
        resolver=None,
        ignore_missing_outputs=True,
        input_reference_only=True,
        network="ltc",
    )
    assert txs[0]["outputs"][0]["addresses"] == [LTC_FORM]


def test_enrich_txs_leaves_btc_untouched():
    txs = [
        {
            "hash": "cc",
            "is_coinbase": True,
            "inputs": [],
            "outputs": [
                {
                    "addresses": [BTC_FORM],
                    "type": "p2pk",
                    "script_hex": "",
                    "value": 1,
                }
            ],
        }
    ]
    enrich_txs(
        txs,
        resolver=None,
        ignore_missing_outputs=True,
        input_reference_only=True,
        network="btc",
    )
    assert txs[0]["outputs"][0]["addresses"] == [BTC_FORM]
