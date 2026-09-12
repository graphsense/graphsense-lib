"""Tests for rpc_utxo.py — direct JSON-RPC UTXO block/tx exporter."""

import copy
import gzip
import importlib.resources
import json
from unittest.mock import patch

import pytest

from graphsenselib.ingest import rpc_utxo
from graphsenselib.ingest.rpc_utxo import (
    BtcBlockExporter,
    _btc_to_satoshi,
    _make_cache_entry,
    _nonce_to_hex,
    _parse_btc_block_and_txs,
    _parse_input,
    _parse_output,
    _script_hex_to_non_standard_address,
)

from .ingest import resources as ingest_resources


def load_zcash_block(height):
    """Return the verbatim ``getblock(<height>, 2)`` response stored for a height.

    The fixtures are captured from a Zcash mainnet node and stored unmodified,
    only gzipped; the shielded bundles carry kilobytes of proof and ciphertext
    that no assertion reads but that the field validation must still accept.
    """
    path = importlib.resources.files(ingest_resources).joinpath(
        f"zcash_block_{height}.json.gz"
    )
    with path.open("rb") as fh:
        return json.loads(gzip.decompress(fh.read()))


def tx_by_hash(txs, tx_hash):
    return next(tx for tx in txs if tx["hash"] == tx_hash)


def shielded(inoutputs):
    return [x for x in inoutputs if x["type"] == "shielded"]


class TestBtcToSatoshi:
    def test_none(self):
        assert _btc_to_satoshi(None) is None

    def test_zero(self):
        assert _btc_to_satoshi(0.0) == 0

    def test_one_btc(self):
        assert _btc_to_satoshi(1.0) == 100_000_000

    def test_one_satoshi(self):
        assert _btc_to_satoshi(0.00000001) == 1

    def test_typical_value(self):
        assert _btc_to_satoshi(0.12345678) == 12_345_678

    def test_large_value(self):
        assert _btc_to_satoshi(21_000_000.0) == 2_100_000_000_000_000

    def test_precision_edge_case(self):
        # 0.1 BTC — float 0.1 * 1e8 = 10000000.000000002, round handles it
        assert _btc_to_satoshi(0.1) == 10_000_000

    def test_19_99999999(self):
        assert _btc_to_satoshi(19.99999999) == 1_999_999_999


class TestNonceToHex:
    def test_none(self):
        assert _nonce_to_hex(None) is None

    def test_int(self):
        assert _nonce_to_hex(12345) == "3039"

    def test_hex_string(self):
        # ZCash returns hex strings
        assert _nonce_to_hex("abc123") == "abc123"


class TestNonStandardAddress:
    def test_empty_script(self):
        result = _script_hex_to_non_standard_address("")
        assert result.startswith("nonstandard")
        assert len(result) == len("nonstandard") + 40

    def test_none_script(self):
        result = _script_hex_to_non_standard_address(None)
        assert result.startswith("nonstandard")

    def test_known_script(self):
        # Should match bitcoin-etl's output
        import hashlib

        script_hex = "76a914abc12300000000000000000000000000000000000088ac"
        expected_hash = hashlib.sha256(bytearray.fromhex(script_hex)).hexdigest()[:40]
        result = _script_hex_to_non_standard_address(script_hex)
        assert result == "nonstandard" + expected_hash


class TestParseInput:
    def test_regular_input(self):
        vin = {
            "txid": "abc123",
            "vout": 0,
            "scriptSig": {"asm": "OP_DUP OP_HASH160", "hex": "76a914"},
            "sequence": 4294967295,
            "txinwitness": ["304402"],
        }
        result = _parse_input(vin, 0)
        assert result["index"] == 0
        assert result["spent_transaction_hash"] == "abc123"
        assert result["spent_output_index"] == 0
        assert result["script_asm"] == "OP_DUP OP_HASH160"
        assert result["script_hex"] == "76a914"
        assert result["sequence"] == 4294967295
        assert result["txinwitness"] == ["304402"]
        assert result["addresses"] == []
        assert result["value"] is None
        assert result["type"] is None

    def test_input_without_witness(self):
        vin = {
            "txid": "def456",
            "vout": 1,
            "scriptSig": {"asm": "sig", "hex": "00"},
            "sequence": 0,
        }
        result = _parse_input(vin, 3)
        assert result["index"] == 3
        assert result["txinwitness"] is None

    def test_input_with_prevout(self):
        """Verbosity 3: prevout present → value/addresses/type filled."""
        vin = {
            "txid": "abc123",
            "vout": 0,
            "scriptSig": {"asm": "sig", "hex": "00"},
            "sequence": 4294967295,
            "prevout": {
                "value": 0.5,
                "scriptPubKey": {
                    "type": "witness_v0_keyhash",
                    "address": "bc1qexample",
                },
            },
        }
        result = _parse_input(vin, 0)
        assert result["value"] == 50_000_000
        assert result["addresses"] == ["bc1qexample"]
        assert result["type"] == "witness_v0_keyhash"

    def test_input_with_prevout_addresses_list(self):
        """Verbosity 3: prevout with 'addresses' list (older nodes)."""
        vin = {
            "txid": "abc123",
            "vout": 1,
            "scriptSig": {"asm": "sig", "hex": "00"},
            "sequence": 0,
            "prevout": {
                "value": 1.0,
                "scriptPubKey": {
                    "type": "pubkeyhash",
                    "addresses": ["1Address1", "1Address2"],
                },
            },
        }
        result = _parse_input(vin, 0)
        assert result["value"] == 100_000_000
        assert result["addresses"] == ["1Address1", "1Address2"]
        assert result["type"] == "pubkeyhash"

    def test_input_with_prevout_p2pk_malformed_script(self):
        """Verbosity 3: prevout typed 'pubkey' but the script is not a parseable
        P2PK (truncated) → fall back to the synthetic nonstandard id."""
        vin = {
            "txid": "abc123",
            "vout": 0,
            "scriptSig": {"asm": "sig", "hex": "00"},
            "sequence": 0,
            "prevout": {
                "value": 50.0,
                "scriptPubKey": {
                    "type": "pubkey",
                    "asm": "04678... OP_CHECKSIG",
                    "hex": "410467",
                },
            },
        }
        result = _parse_input(vin, 0)
        assert result["value"] == 5_000_000_000
        assert result["type"] == "nonstandard"
        assert len(result["addresses"]) == 1
        assert result["addresses"][0].startswith("nonstandard")

    def test_input_with_prevout_p2pk_derives_address(self):
        """Verbosity 3: a well-formed P2PK prevout without an address gets the
        address derived (keeping the node's 'pubkey' type)."""
        p2pk_hex = (
            "4104ea0d6650c8305f1213a89c65fc8f4343a5dac8e985c869e51d3aa02879b57c60"
            "cff49fcb99314d02dfc612d654e4333150ef61fa569c1c66415602cae387baf7ac"
        )
        vin = {
            "txid": "abc123",
            "vout": 0,
            "scriptSig": {"asm": "sig", "hex": "00"},
            "sequence": 0,
            "prevout": {
                "value": 50.0,
                "scriptPubKey": {"type": "pubkey", "hex": p2pk_hex},
            },
        }
        result = _parse_input(vin, 0, network="btc")
        assert result["type"] == "pubkey"
        assert result["addresses"] == ["1BDvQZjaAJH4ecZ8aL3fYgTi7rnn3o2thE"]
        assert "prevout_script_hex" not in result

    def test_input_without_prevout(self):
        """Verbosity 2: no prevout → value=None, addresses=[], type=None."""
        vin = {
            "txid": "def456",
            "vout": 0,
            "scriptSig": {"asm": "sig", "hex": "00"},
            "sequence": 0,
        }
        result = _parse_input(vin, 0)
        assert result["value"] is None
        assert result["addresses"] == []
        assert result["type"] is None


class TestParseOutput:
    def test_p2pkh_output(self):
        vout = {
            "value": 0.5,
            "n": 0,
            "scriptPubKey": {
                "asm": "OP_DUP OP_HASH160 abc OP_EQUALVERIFY OP_CHECKSIG",
                "hex": "76a914abc88ac",
                "reqSigs": 1,
                "type": "pubkeyhash",
                "addresses": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
            },
        }
        result = _parse_output(vout)
        assert result["index"] == 0
        assert result["value"] == 50_000_000
        assert result["type"] == "pubkeyhash"
        assert result["addresses"] == ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]
        assert result["required_signatures"] == 1

    def test_witness_v0_single_address(self):
        """Modern Bitcoin Core returns 'address' (singular) not 'addresses'."""
        vout = {
            "value": 1.0,
            "n": 1,
            "scriptPubKey": {
                "asm": "0 abc",
                "hex": "0014abc",
                "type": "witness_v0_keyhash",
                "address": "bc1qabc",
            },
        }
        result = _parse_output(vout)
        assert result["addresses"] == ["bc1qabc"]
        assert result["type"] == "witness_v0_keyhash"

    def test_nulldata_output(self):
        vout = {
            "value": 0.0,
            "n": 0,
            "scriptPubKey": {
                "asm": "OP_RETURN deadbeef",
                "hex": "6a04deadbeef",
                "type": "nulldata",
            },
        }
        result = _parse_output(vout)
        assert result["type"] == "nulldata"
        assert result["addresses"] == []  # nulldata stays empty, not nonstandard

    def test_nonstandard_output(self):
        """Empty addresses + non-nulldata type → nonstandard."""
        vout = {
            "value": 0.001,
            "n": 0,
            "scriptPubKey": {
                "asm": "unknown",
                "hex": "aa",
                "type": "nonstandard",
            },
        }
        result = _parse_output(vout)
        assert result["type"] == "nonstandard"
        assert len(result["addresses"]) == 1
        assert result["addresses"][0].startswith("nonstandard")

    # Uncompressed-pubkey P2PK script (real Zcash height-0 coinbase output);
    # zcashd reports the t-address + reqSigs, Zebra omits both.
    _P2PK_HEX = (
        "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb6"
        "49f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac"
    )

    def test_p2pk_output_address_present_unchanged(self):
        """If the node already gives the address (zcashd), use it verbatim."""
        vout = {
            "value": 50.0,
            "n": 0,
            "scriptPubKey": {
                "type": "pubkey",
                "hex": self._P2PK_HEX,
                "reqSigs": 1,
                "addresses": ["t1StbPM4X3j4FGM57HpGnb9BMbS7C1nFW1r"],
            },
        }
        result = _parse_output(vout, network="zec")
        assert result["type"] == "pubkey"
        assert result["addresses"] == ["t1StbPM4X3j4FGM57HpGnb9BMbS7C1nFW1r"]
        assert result["required_signatures"] == 1

    def test_p2pk_output_derived_when_address_omitted(self):
        """If the node omits the address (Zebra), derive the same t-address and
        reqSigs=1 so parsing matches zcashd byte-for-byte."""
        vout = {
            "value": 50.0,
            "n": 0,
            "scriptPubKey": {"type": "pubkey", "hex": self._P2PK_HEX},
        }
        result = _parse_output(vout, network="zec")
        assert result["type"] == "pubkey"
        assert result["addresses"] == ["t1StbPM4X3j4FGM57HpGnb9BMbS7C1nFW1r"]
        assert result["required_signatures"] == 1

    def test_p2pk_output_network_aware(self):
        """The derived address uses the network's version byte (BTC vs ZEC)."""
        btc_hex = (
            "4104ea0d6650c8305f1213a89c65fc8f4343a5dac8e985c869e51d3aa02879b57c60"
            "cff49fcb99314d02dfc612d654e4333150ef61fa569c1c66415602cae387baf7ac"
        )
        vout = {
            "value": 1.0,
            "n": 0,
            "scriptPubKey": {"type": "pubkey", "hex": btc_hex},
        }
        assert _parse_output(vout, network="btc")["addresses"] == [
            "1BDvQZjaAJH4ecZ8aL3fYgTi7rnn3o2thE"
        ]


class TestParseBlockAndTxs:
    SAMPLE_BLOCK = {
        "hash": "000000000000000000block",
        "height": 600000,
        "time": 1573040400,
        "size": 1234,
        "strippedsize": 1000,
        "weight": 4000,
        "version": 536870912,
        "merkleroot": "merkle123",
        "nonce": 12345,
        "bits": "17148edf",
        "tx": [
            {
                "txid": "coinbase_tx_hash",
                "size": 200,
                "vsize": 180,
                "version": 2,
                "locktime": 0,
                "vin": [
                    {
                        "coinbase": "03e09209042f4d696e696e67",
                        "sequence": 4294967295,
                    }
                ],
                "vout": [
                    {
                        "value": 6.25,
                        "n": 0,
                        "scriptPubKey": {
                            "asm": "OP_DUP",
                            "hex": "76a914",
                            "type": "pubkeyhash",
                            "addresses": ["1MinerAddress"],
                        },
                    }
                ],
            },
            {
                "txid": "regular_tx_hash",
                "size": 300,
                "vsize": 250,
                "version": 2,
                "locktime": 0,
                "vin": [
                    {
                        "txid": "prev_tx_hash",
                        "vout": 0,
                        "scriptSig": {"asm": "sig", "hex": "00"},
                        "sequence": 4294967295,
                    }
                ],
                "vout": [
                    {
                        "value": 0.5,
                        "n": 0,
                        "scriptPubKey": {
                            "asm": "OP_DUP",
                            "hex": "76a914",
                            "type": "pubkeyhash",
                            "addresses": ["1RecipientAddress"],
                        },
                    },
                    {
                        "value": 0.49,
                        "n": 1,
                        "scriptPubKey": {
                            "asm": "OP_DUP",
                            "hex": "76a914",
                            "type": "pubkeyhash",
                            "addresses": ["1ChangeAddress"],
                        },
                    },
                ],
            },
        ],
    }

    def test_block_fields(self):
        block, txs = _parse_btc_block_and_txs(self.SAMPLE_BLOCK)
        assert block["number"] == 600000
        assert block["hash"] == "000000000000000000block"
        assert block["timestamp"] == 1573040400
        assert block["size"] == 1234
        assert block["stripped_size"] == 1000
        assert block["weight"] == 4000
        assert block["nonce"] == "3039"
        assert block["bits"] == "17148edf"
        assert block["transaction_count"] == 2
        assert block["coinbase_param"] == "03e09209042f4d696e696e67"
        assert block["type"] == "block"

    def test_coinbase_tx(self):
        block, txs = _parse_btc_block_and_txs(self.SAMPLE_BLOCK)
        coinbase = txs[0]
        assert coinbase["is_coinbase"] is True
        assert coinbase["hash"] == "coinbase_tx_hash"
        assert coinbase["index"] == 0
        assert coinbase["block_number"] == 600000
        assert coinbase["block_hash"] == "000000000000000000block"
        assert coinbase["block_timestamp"] == 1573040400
        # Coinbase input is removed from inputs list
        assert len(coinbase["inputs"]) == 0
        assert coinbase["input_count"] == 0
        assert coinbase["output_count"] == 1
        assert coinbase["output_value"] == 625_000_000
        assert coinbase["input_value"] == 0
        assert coinbase["fee"] == 0

    def test_regular_tx(self):
        block, txs = _parse_btc_block_and_txs(self.SAMPLE_BLOCK)
        regular = txs[1]
        assert regular["is_coinbase"] is False
        assert regular["hash"] == "regular_tx_hash"
        assert regular["index"] == 1
        assert len(regular["inputs"]) == 1
        assert regular["inputs"][0]["spent_transaction_hash"] == "prev_tx_hash"
        assert regular["inputs"][0]["spent_output_index"] == 0
        assert len(regular["outputs"]) == 2
        assert regular["outputs"][0]["value"] == 50_000_000
        assert regular["outputs"][1]["value"] == 49_000_000
        assert regular["output_value"] == 99_000_000
        # Input value is 0 (not resolved yet — sum of empty list)
        assert regular["input_value"] == 0
        assert regular["fee"] == 0 - 99_000_000  # input(0) - output

    def test_zcash_shielded_joinsplit(self):
        """ZCash vjoinsplit creates shielded inputs/outputs."""
        block_raw = {
            "hash": "zcash_block",
            "height": 100,
            "time": 1000000,
            "size": 500,
            "version": 1,
            "merkleroot": "mk",
            "nonce": "00",
            "bits": "1d00ffff",
            "tx": [
                {
                    "txid": "zec_tx",
                    "size": 300,
                    "vsize": 300,
                    "version": 1,
                    "locktime": 0,
                    "vin": [{"coinbase": "00", "sequence": 0}],
                    "vout": [
                        {
                            "value": 0.0,
                            "n": 0,
                            "scriptPubKey": {
                                "asm": "",
                                "hex": "",
                                "type": "nulldata",
                            },
                        }
                    ],
                    "vjoinsplit": [
                        {"vpub_old": 0.0, "vpub_new": 10.0},
                    ],
                }
            ],
        }
        block, txs = _parse_btc_block_and_txs(block_raw)
        tx = txs[0]
        # vpub_new (z→t) > 0 → shielded INPUT (funds the transaction)
        shielded_inputs = [i for i in tx["inputs"] if i.get("type") == "shielded"]
        assert len(shielded_inputs) == 1
        assert shielded_inputs[0]["value"] == 1_000_000_000

    def test_zcash_value_balance(self):
        """ZCash valueBalanceZat creates shielded inputs or outputs."""
        block_raw = {
            "hash": "zcash_block2",
            "height": 200,
            "time": 2000000,
            "size": 500,
            "version": 1,
            "merkleroot": "mk",
            "nonce": "00",
            "bits": "1d00ffff",
            "tx": [
                {
                    "txid": "zec_tx2",
                    "size": 300,
                    "vsize": 300,
                    "version": 1,
                    "locktime": 0,
                    "vin": [
                        {
                            "txid": "prev",
                            "vout": 0,
                            "scriptSig": {"asm": "", "hex": ""},
                            "sequence": 0,
                        }
                    ],
                    "vout": [
                        {
                            "value": 1.0,
                            "n": 0,
                            "scriptPubKey": {
                                "asm": "",
                                "hex": "00",
                                "type": "pubkeyhash",
                                "addresses": ["t1addr"],
                            },
                        }
                    ],
                    "valueBalanceZat": -500_000_000,  # negative → shielded output
                }
            ],
        }
        block, txs = _parse_btc_block_and_txs(block_raw)
        tx = txs[0]
        shielded_outputs = [o for o in tx["outputs"] if o["type"] == "shielded"]
        assert len(shielded_outputs) == 1
        assert shielded_outputs[0]["value"] == 500_000_000

    def test_zcash_v6_ironwood_bundle(self):
        """ZCash NU6.3 v6 tx: the ironwood bundle is accounted, not ignored.

        Mainnet block 3,479,000, tx af9eae20...9b82 — a fully shielded v6
        transaction with no transparent inputs or outputs, whose entire value
        movement is an Ironwood balance of +40,000 zat. Positive means value
        left the pool, so it becomes a shielded input; with nothing on the
        transparent side, that amount is exactly the fee.
        """
        block_raw = load_zcash_block(3_479_000)
        _, txs = _parse_btc_block_and_txs(block_raw, network="zec")
        tx = tx_by_hash(
            txs, "af9eae20963e5e53e2bda3275ffcd1d19decad6530ce46f55b83585336819b82"
        )

        assert tx["ironwood_value_balance"] == 40_000
        assert [i["value"] for i in shielded(tx["inputs"])] == [40_000]
        assert shielded(tx["outputs"]) == []
        assert tx["input_value"] == 40_000
        assert tx["output_value"] == 0
        assert tx["fee"] == 40_000

    def test_regular_tx_with_prevout(self):
        """Verbosity 3: prevout resolves input_value and fee correctly."""
        block_raw = {
            "hash": "000000000000000000block_v3",
            "height": 800000,
            "time": 1690000000,
            "size": 1000,
            "version": 536870912,
            "merkleroot": "merkle",
            "nonce": 0,
            "bits": "17148edf",
            "tx": [
                {
                    "txid": "coinbase_v3",
                    "size": 200,
                    "vsize": 180,
                    "version": 2,
                    "locktime": 0,
                    "vin": [{"coinbase": "03", "sequence": 4294967295}],
                    "vout": [
                        {
                            "value": 6.25,
                            "n": 0,
                            "scriptPubKey": {
                                "type": "pubkeyhash",
                                "addresses": ["1Miner"],
                            },
                        }
                    ],
                },
                {
                    "txid": "tx_with_prevout",
                    "size": 300,
                    "vsize": 250,
                    "version": 2,
                    "locktime": 0,
                    "vin": [
                        {
                            "txid": "prev_tx_1",
                            "vout": 0,
                            "scriptSig": {"asm": "sig", "hex": "00"},
                            "sequence": 4294967295,
                            "prevout": {
                                "value": 1.0,
                                "scriptPubKey": {
                                    "type": "witness_v0_keyhash",
                                    "address": "bc1qsender",
                                },
                            },
                        }
                    ],
                    "vout": [
                        {
                            "value": 0.999,
                            "n": 0,
                            "scriptPubKey": {
                                "type": "witness_v0_keyhash",
                                "address": "bc1qrecipient",
                            },
                        }
                    ],
                },
            ],
        }
        block, txs = _parse_btc_block_and_txs(block_raw)
        regular = txs[1]
        assert regular["input_value"] == 100_000_000
        assert regular["output_value"] == 99_900_000
        assert regular["fee"] == 100_000  # 0.001 BTC fee
        assert regular["inputs"][0]["addresses"] == ["bc1qsender"]
        assert regular["inputs"][0]["value"] == 100_000_000

    def test_empty_block(self):
        """Block with no transactions."""
        block_raw = {
            "hash": "empty_block",
            "height": 0,
            "time": 0,
            "size": 100,
            "version": 1,
            "merkleroot": "00",
            "nonce": 0,
            "bits": "1d00ffff",
            "tx": [],
        }
        block, txs = _parse_btc_block_and_txs(block_raw)
        assert block["transaction_count"] == 0
        assert block["coinbase_param"] is None
        assert len(txs) == 0


class TestZcashShieldedPools:
    """Orchard (NU5) and Ironwood (NU6.3) value-balance accounting.

    Every fixture here is an unmodified mainnet ``getblock(<height>, 2)``
    response, so the values, the bundle shape and the key sets are the node's
    rather than this file's. Heights and txids are recorded in each docstring
    so any assertion can be re-checked against the chain.

    Transparent input values are resolved in a later stage, so ``fee`` is only
    meaningful here for transactions with no transparent inputs; those are
    exactly the fully shielded ones the assertions below use it on.
    """

    def parse(self, height):
        return _parse_btc_block_and_txs(load_zcash_block(height), network="zec")[1]

    def test_orchard_shielding_becomes_a_shielded_output(self):
        """7bd7717f...fe69 @ 2,501,409 — Orchard -362,999,000, one t-input.

        The transparent direction into Orchard: a negative balance produces a
        shielded output of the negated amount, so the value entering the pool
        is still accounted for on the output side. The transparent input is
        larger than that by the fee, but its value is resolved in a later
        stage and is not visible here.
        """
        tx = tx_by_hash(
            self.parse(2_501_409),
            "7bd7717f8897e323840ac0f6b09518e580dbca1fa77dbd20ed0b90fc56d2fe69",
        )

        assert tx["orchard_value_balance"] == -362_999_000
        assert shielded(tx["inputs"]) == []
        assert [o["value"] for o in shielded(tx["outputs"])] == [362_999_000]
        assert tx["output_value"] == 362_999_000

    def test_positive_balance_can_be_a_fee_rather_than_an_unshielding(self):
        """224daf9f...7c5a @ 2,501,409 — Orchard +1,000 and nothing else.

        Three Orchard actions, no transparent legs, and a balance of exactly
        the 1,000 zat fee: value left the pool only to pay the miner. The
        shielded input is right for value conservation, but reading it as a
        z->t transfer of spendable value would be wrong, which is why the
        CHANGELOG says so explicitly.
        """
        tx = tx_by_hash(
            self.parse(2_501_409),
            "224daf9f01485fceacfe1fa49daf4d498c23a5c58ff237d28aa1495cd8bc7c5a",
        )

        assert tx["orchard_value_balance"] == 1_000
        assert [i["value"] for i in shielded(tx["inputs"])] == [1_000]
        assert tx["output_count"] == 0
        assert tx["fee"] == 1_000

    def test_orchard_unshielding_becomes_a_shielded_input(self):
        """3115796d...9e53 @ 3,479,000 — Orchard +1,020,000, Ironwood -1,000,000.

        A pool migration: value leaves Orchard and all but the 20,000 zat fee
        enters Ironwood. It is the clearest demonstration that the two pools
        are read independently and accumulate rather than overwrite.
        """
        tx = tx_by_hash(
            self.parse(3_479_000),
            "3115796d7ebd1f35d270229221166f2318888587153a294dfb712ef620899e53",
        )

        assert tx["orchard_value_balance"] == 1_020_000
        assert tx["ironwood_value_balance"] == -1_000_000
        assert [i["value"] for i in shielded(tx["inputs"])] == [1_020_000]
        assert [o["value"] for o in shielded(tx["outputs"])] == [1_000_000]
        assert tx["fee"] == 20_000

    def test_ironwood_shielding_becomes_a_shielded_output(self):
        """e6b8618c...8f1b @ 3,479,000 — Ironwood -2,132,669 with transparent input.

        Negative means value entered the pool, so it is emitted as a shielded
        output of the negated amount alongside the transparent output.
        """
        tx = tx_by_hash(
            self.parse(3_479_000),
            "e6b8618cfeccce30cec8ad811768231f82bb1f93b8cec4d789537cdf2bde8f1b",
        )

        assert tx["ironwood_value_balance"] == -2_132_669
        assert shielded(tx["inputs"]) == []
        assert [o["value"] for o in shielded(tx["outputs"])] == [2_132_669]
        assert tx["output_count"] == 2

    def test_mixed_orchard_and_sapling_legs_accumulate(self):
        """bd84ece1...6517 @ 1,687,194 — Sapling +89,000, Orchard -88,000.

        No transparent legs at all, so the two shielded amounts have to account
        for the whole transaction and their difference is the 1,000 zat fee.
        A sign error or a missed leg shows up here as a fee of 89,000 — which
        is what the parser reported while the bundle was blacklisted — or as a
        negative one.
        """
        tx = tx_by_hash(
            self.parse(1_687_194),
            "bd84ece1a1f9930085768a880d99445ee1ee13686c2e5979b30d920e90866517",
        )

        assert tx["sapling_value_balance"] == 89_000
        assert tx["orchard_value_balance"] == -88_000
        assert tx["ironwood_value_balance"] == 0
        assert [i["value"] for i in shielded(tx["inputs"])] == [89_000]
        assert [o["value"] for o in shielded(tx["outputs"])] == [88_000]
        assert tx["input_value"] - tx["output_value"] == 1_000
        assert tx["fee"] == 1_000

    def test_fully_shielded_fee_is_paid_out_of_the_pool(self):
        """aee031d4...60a2 @ 3,479,000 — Ironwood +300,015,000, one t-output.

        The positive balance is not a 300,015,000 zat unshielding of spendable
        value: 300,000,000 reaches the transparent output and the remaining
        15,000 is the fee. Mapping the balance to a shielded input is what
        makes the two sides balance.
        """
        tx = tx_by_hash(
            self.parse(3_479_000),
            "aee031d4299acacdfc4d1b0bfc6d34d760f5b759e187a202a917f3b297af60a2",
        )

        assert tx["ironwood_value_balance"] == 300_015_000
        assert [i["value"] for i in shielded(tx["inputs"])] == [300_015_000]
        assert tx["output_value"] == 300_000_000
        assert tx["fee"] == 15_000

    def test_empty_orchard_bundle_contributes_nothing(self):
        """Every NU5+ transaction carries an ``orchard`` key, usually empty.

        The empty bundle is a distinct three-key shape with no flags, anchor,
        proof or bindingSig; it must validate and emit no shielded I/O.
        """
        raw = load_zcash_block(1_687_194)
        empty = [
            tx for tx in raw["tx"] if "orchard" in tx and not tx["orchard"]["actions"]
        ]
        assert empty, "fixture no longer contains an empty orchard bundle"
        assert all(
            set(tx["orchard"]) == {"actions", "valueBalance", "valueBalanceZat"}
            for tx in empty
        )

        parsed = {tx["hash"]: tx for tx in self.parse(1_687_194)}
        for tx in empty:
            assert parsed[tx["txid"]]["orchard_value_balance"] == 0
            assert shielded(parsed[tx["txid"]]["inputs"]) == []
            assert shielded(parsed[tx["txid"]]["outputs"]) == []

    def test_value_balance_zat_is_read_verbatim_not_converted(self):
        """The bundle amount is already zatoshi, so it must not be scaled.

        The float sibling sits right next to it in the response; passing that
        one through _btc_to_satoshi, or the integer through it a second time,
        is a 10^8 error that no hand-written fixture would catch because it
        would share the mistaken assumption.
        """
        raw = load_zcash_block(3_479_000)
        parsed = {tx["hash"]: tx for tx in self.parse(3_479_000)}
        seen = 0
        for tx in raw["tx"]:
            for pool in ("orchard", "ironwood"):
                bundle = tx.get(pool)
                if bundle is None:
                    continue
                seen += 1
                assert (
                    parsed[tx["txid"]][f"{pool}_value_balance"]
                    == (bundle["valueBalanceZat"])
                )
                assert round(bundle["valueBalance"] * 1e8) == bundle["valueBalanceZat"]
        assert seen >= 8

    def test_three_shielded_legs_accumulate_in_pool_order(self):
        """6f37465a...04c8 @ 3,463,373 — Sapling, Orchard and Ironwood at once.

        Rare (31 such transactions in the whole NU6.3 range so far) and the
        strongest accumulation check available: three balances of two different
        signs on one transaction, with no transparent leg to absorb a mistake.
        Legs are appended Sprout, Sapling, Orchard, Ironwood, so the two
        positive ones arrive as inputs in that order.
        """
        tx = tx_by_hash(
            self.parse(3_463_373),
            "6f37465ac2a718135c61fb62049e6205977da2042774002d301d018a76da04c8",
        )

        assert tx["sapling_value_balance"] == -500_000
        assert tx["orchard_value_balance"] == 24_898
        assert tx["ironwood_value_balance"] == 505_102
        assert [i["value"] for i in shielded(tx["inputs"])] == [24_898, 505_102]
        assert [o["value"] for o in shielded(tx["outputs"])] == [500_000]
        assert tx["fee"] == 30_000

    def test_sapling_shielding_alongside_ironwood_in_one_block(self):
        """Block 3,463,373 also carries single-pool legs of both signs.

        7e39f93e...5cdc is a v4 transaction shielding 18.77 ZEC into Sapling
        with fifteen transparent inputs; 83528670...b942 is a v6 one shielding
        1.25 ZEC into Ironwood. Both are negative balances and must produce a
        shielded output rather than an input.
        """
        txs = self.parse(3_463_373)

        sapling = tx_by_hash(
            txs, "7e39f93eb6542dad5cdcbffaa2fb7a88524da7df3195849a44fb467cc47c1c90"
        )
        assert sapling["sapling_value_balance"] == -1_877_909_839
        assert [o["value"] for o in shielded(sapling["outputs"])] == [1_877_909_839]

        ironwood = tx_by_hash(
            txs, "83528670357dab26b94211f39d5dcf2e7efa776d7ca68c8762e49676e0562780"
        )
        assert ironwood["ironwood_value_balance"] == -124_985_000
        assert [o["value"] for o in shielded(ironwood["outputs"])] == [124_985_000]

    def test_sapling_reads_the_integer_field_not_the_float(self):
        """Both Sapling amounts are on every transaction; only the int is read.

        Mainnet block 600,000, 00c8e2ed...af10 carries valueBalance -0.9199
        and valueBalanceZat -91990000. Dropping the integer leaves the float
        in place and must produce no shielded output, which is what keeps a
        revert to the lossy float from passing unnoticed.
        """
        raw = copy.deepcopy(load_zcash_block(600_000))
        target = next(
            tx
            for tx in raw["tx"]
            if tx["txid"]
            == "00c8e2ede256065b03a37936f97e85fd67206273cb3d9464d2cfc5b45911af10"
        )
        assert target["valueBalance"] == -0.9199
        del target["valueBalanceZat"]

        _, txs = _parse_btc_block_and_txs(raw, network="zec")
        tx = tx_by_hash(txs, target["txid"])

        assert tx["sapling_value_balance"] == 0
        assert shielded(tx["outputs"]) == []
        # the Sprout leg is untouched by the Sapling field being absent
        assert [i["value"] for i in shielded(tx["inputs"])] == [92_000_000]

    def test_pre_nu5_transactions_carry_an_empty_orchard_bundle(self):
        """Mainnet block 600,000 — every transaction is v4 and long pre-NU5.

        Zebra emits ``orchard`` unconditionally, so the bundle is present and
        empty on transactions that predate the pool by a million blocks. This
        is why ``orchard`` is an optional known key rather than something
        gated on transaction version: a version gate would reject these.
        """
        raw = load_zcash_block(600_000)

        assert {tx["version"] for tx in raw["tx"]} == {4}
        assert all("orchard" in tx for tx in raw["tx"])
        assert all(tx["orchard"]["actions"] == [] for tx in raw["tx"])
        assert not any("ironwood" in tx for tx in raw["tx"])

        for tx in self.parse(600_000):
            assert tx["orchard_value_balance"] == 0
            assert tx["ironwood_value_balance"] == 0

    def test_sprout_to_sapling_migration_balances(self):
        """00c8e2ed...af10 @ 600,000 — Sprout vpub_new 0.92, Sapling -0.9199.

        A fully shielded pool migration predating Orchard, kept as a control:
        the Sprout and Sapling paths must keep producing the same shielded I/O
        and the same 10,000 zat fee now that a third path runs after them.
        """
        tx = tx_by_hash(
            self.parse(600_000),
            "00c8e2ede256065b03a37936f97e85fd67206273cb3d9464d2cfc5b45911af10",
        )

        assert tx["sapling_value_balance"] == -91_990_000
        assert [i["value"] for i in shielded(tx["inputs"])] == [92_000_000]
        assert [o["value"] for o in shielded(tx["outputs"])] == [91_990_000]
        assert tx["fee"] == 10_000


class TestZcashShieldedFieldValidation:
    """The bundle is enumerated strictly, so a new field fails loudly."""

    # Every stored block, oldest first. The four NU6.3 ones were picked so that
    # between them they carry all seven distinct transaction key sets observed
    # over the whole NU6.3 range.
    BLOCKS = (600_000, 1_687_194, 2_501_409, 3_442_130, 3_442_400, 3_463_373, 3_479_000)
    NU63_BLOCKS = (3_442_130, 3_442_400, 3_463_373, 3_479_000)

    def test_validation_runs_at_every_level(self):
        """validate_rpc_fields does not recurse, so each level is called directly.

        Counts the real calls made while parsing unmodified mainnet blocks, to
        prove the bundle, action and flags levels are reached rather than
        merely defined.
        """
        contexts = {}
        real = rpc_utxo.validate_rpc_fields

        def spy(json_keys, known, blacklist, context):
            contexts[context] = contexts.get(context, 0) + 1
            return real(json_keys, known, blacklist, context)

        with patch.object(rpc_utxo, "validate_rpc_fields", spy):
            for height in self.BLOCKS:
                _parse_btc_block_and_txs(load_zcash_block(height), network="zec")

        assert contexts == {
            "block": 7,
            "transaction": 40,
            "vin": 102,
            "scriptSig": 95,
            "vout": 96,
            "scriptPubKey": 96,
            "vjoinsplit": 8,
            "orchard bundle": 40,
            "ironwood bundle": 8,
            "orchard action": 11,
            "ironwood action": 15,
            "orchard flags": 5,
            "ironwood flags": 8,
        }

    def test_distinct_transaction_key_sets_are_covered(self):
        """The NU6.3 fixtures carry every transaction shape the range contains.

        A census of all 52,157 blocks from NU6.3 activation to the chain tip
        (328,134 transactions) found exactly seven distinct transaction key
        sets. ``ironwood``, ``authdigest``, ``bindingSig``, ``joinSplitPubKey``
        and ``joinSplitSig`` are each optional and vary independently, and the
        transaction-level validation has to accept all seven combinations —
        including the rarest, Sprout joinsplits still appearing in the NU6.3
        range, which numbered 14 transactions in that census.
        """
        key_sets = set()
        for height in self.NU63_BLOCKS:
            key_sets |= {frozenset(tx) for tx in load_zcash_block(height)["tx"]}

        assert len(key_sets) == 7
        for optional in ("ironwood", "authdigest", "bindingSig", "joinSplitSig"):
            assert any(optional in ks for ks in key_sets)
            assert any(optional not in ks for ks in key_sets)

    @pytest.mark.parametrize(
        "path, context",
        [
            (("orchard",), "orchard bundle"),
            (("orchard", "actions", 0), "orchard action"),
            (("orchard", "flags"), "orchard flags"),
            (("ironwood",), "ironwood bundle"),
            (("ironwood", "actions", 0), "ironwood action"),
            (("ironwood", "flags"), "ironwood flags"),
        ],
    )
    def test_unknown_field_raises_at_each_level(self, path, context):
        """A key that is neither read nor blacklisted must abort the parse."""
        raw = copy.deepcopy(load_zcash_block(3_479_000))
        target = next(
            tx for tx in raw["tx"] if path[0] in tx and tx[path[0]]["actions"]
        )
        for key in path:
            target = target[key]
        target["someFieldZcashAdded"] = True

        with pytest.raises(ValueError, match=f"in {context}"):
            _parse_btc_block_and_txs(raw, network="zec")

    def test_enable_cross_address_flag_is_already_tolerated(self):
        """ZIP-229's third flag bit is accepted before any node emits it.

        Zebra carries enableCrossAddress in its consensus types but its RPC
        flags struct exposes only enableSpends and enableOutputs, so it is
        absent from every bundle on chain today. Listing it in advance keeps a
        Zebra release that adds the serde field from breaking every Ironwood
        transaction at once.
        """
        raw = copy.deepcopy(load_zcash_block(3_479_000))
        touched = 0
        for tx in raw["tx"]:
            for pool in ("orchard", "ironwood"):
                bundle = tx.get(pool)
                if bundle and bundle.get("flags") is not None:
                    assert "enableCrossAddress" not in bundle["flags"]
                    bundle["flags"]["enableCrossAddress"] = True
                    touched += 1
        assert touched > 0

        _, with_flag = _parse_btc_block_and_txs(raw, network="zec")
        _, without = _parse_btc_block_and_txs(
            load_zcash_block(3_479_000), network="zec"
        )
        assert with_flag == without


class TestResolveUnresolvedInputs:
    """Tests for _resolve_unresolved_inputs (v2 getrawtransaction fallback)."""

    def _make_exporter(self):
        """Create an exporter without connecting to a node."""
        with patch.object(BtcBlockExporter, "__init__", lambda self, **kw: None):
            exp = BtcBlockExporter.__new__(BtcBlockExporter)
            exp.max_workers = 2
            exp._output_cache = {}
            exp.fail_on_unresolved_inputs = True
            return exp

    def _tx_with_unresolvable_input(self):
        return [
            {
                "hash": "tx_x",
                "is_coinbase": False,
                "inputs": [
                    {
                        "spent_transaction_hash": "missing_tx",
                        "spent_output_index": 0,
                        "value": None,
                        "addresses": [],
                        "type": None,
                    },
                ],
                "outputs": [
                    {"index": 0, "value": 10, "addresses": ["a"], "type": "p2pkh"},
                ],
                "input_value": 0,
                "output_value": 10,
                "fee": -10,
            },
        ]

    def test_unresolved_input_raises_by_default(self):
        """A spent input that can't be resolved aborts ingest by default,
        rather than silently writing a null-value/null-address input."""
        exp = self._make_exporter()
        transactions = self._tx_with_unresolvable_input()

        with patch.object(exp, "_batch_getrawtransaction", return_value={}):
            with pytest.raises(RuntimeError, match="could not be resolved"):
                exp._resolve_unresolved_inputs(transactions)

    def test_unresolved_input_warns_when_disabled(self):
        """With fail_on_unresolved_inputs=False the input is written null."""
        exp = self._make_exporter()
        exp.fail_on_unresolved_inputs = False
        transactions = self._tx_with_unresolvable_input()

        with patch.object(exp, "_batch_getrawtransaction", return_value={}):
            exp._resolve_unresolved_inputs(transactions)  # must not raise

        inp = transactions[0]["inputs"][0]
        assert inp["value"] is None
        assert inp["addresses"] == []

    def test_within_batch_resolution(self):
        """Inputs spending outputs from the same batch are resolved without RPC."""
        exp = self._make_exporter()

        transactions = [
            {
                "hash": "tx_a",
                "is_coinbase": False,
                "inputs": [],
                "outputs": [
                    {
                        "index": 0,
                        "value": 50_000_000,
                        "addresses": ["addr1"],
                        "type": "p2pkh",
                    },
                ],
                "input_value": 0,
                "output_value": 50_000_000,
                "fee": -50_000_000,
            },
            {
                "hash": "tx_b",
                "is_coinbase": False,
                "inputs": [
                    {
                        "spent_transaction_hash": "tx_a",
                        "spent_output_index": 0,
                        "value": None,
                        "addresses": [],
                        "type": None,
                    },
                ],
                "outputs": [
                    {
                        "index": 0,
                        "value": 49_000_000,
                        "addresses": ["addr2"],
                        "type": "p2pkh",
                    },
                ],
                "input_value": 0,
                "output_value": 49_000_000,
                "fee": -49_000_000,
            },
        ]

        exp._resolve_unresolved_inputs(transactions)

        inp = transactions[1]["inputs"][0]
        assert inp["value"] == 50_000_000
        assert inp["addresses"] == ["addr1"]
        assert inp["type"] == "p2pkh"
        assert transactions[1]["input_value"] == 50_000_000
        assert transactions[1]["fee"] == 1_000_000

    def test_rpc_resolution(self):
        """Inputs referencing txs outside the batch are resolved via RPC."""
        exp = self._make_exporter()

        transactions = [
            {
                "hash": "tx_in_batch",
                "is_coinbase": False,
                "inputs": [
                    {
                        "spent_transaction_hash": "external_tx",
                        "spent_output_index": 1,
                        "value": None,
                        "addresses": [],
                        "type": None,
                    },
                ],
                "outputs": [
                    {
                        "index": 0,
                        "value": 30_000_000,
                        "addresses": ["addr3"],
                        "type": "p2pkh",
                    },
                ],
                "input_value": 0,
                "output_value": 30_000_000,
                "fee": -30_000_000,
            },
        ]

        # Mock the RPC call to return a fake spent tx (tuple cache entries)
        fake_rpc_result = {
            "external_tx": {
                1: _make_cache_entry(31_000_000, ["addr_ext"], "p2sh", None),
            }
        }
        with patch.object(
            exp, "_batch_getrawtransaction", return_value=fake_rpc_result
        ):
            exp._resolve_unresolved_inputs(transactions)

        inp = transactions[0]["inputs"][0]
        assert inp["value"] == 31_000_000
        assert inp["addresses"] == ["addr_ext"]
        assert inp["type"] == "p2sh"
        assert transactions[0]["input_value"] == 31_000_000
        assert transactions[0]["fee"] == 1_000_000

    def test_coinbase_skipped(self):
        """Coinbase transactions don't get fee recomputed."""
        exp = self._make_exporter()

        transactions = [
            {
                "hash": "coinbase_tx",
                "is_coinbase": True,
                "inputs": [],
                "outputs": [
                    {
                        "index": 0,
                        "value": 625_000_000,
                        "addresses": ["miner"],
                        "type": "p2pkh",
                    },
                ],
                "input_value": 0,
                "output_value": 625_000_000,
                "fee": 0,
            },
        ]

        exp._resolve_unresolved_inputs(transactions)
        assert transactions[0]["fee"] == 0

    def test_no_rpc_when_all_resolved_in_batch(self):
        """No getrawtransaction call when all inputs resolve within batch."""
        exp = self._make_exporter()

        transactions = [
            {
                "hash": "producer",
                "is_coinbase": True,
                "inputs": [],
                "outputs": [
                    {"index": 0, "value": 100, "addresses": ["a"], "type": "p2pkh"},
                ],
                "input_value": 0,
                "output_value": 100,
                "fee": 0,
            },
            {
                "hash": "consumer",
                "is_coinbase": False,
                "inputs": [
                    {
                        "spent_transaction_hash": "producer",
                        "spent_output_index": 0,
                        "value": None,
                        "addresses": [],
                        "type": None,
                    },
                ],
                "outputs": [
                    {"index": 0, "value": 90, "addresses": ["b"], "type": "p2pkh"},
                ],
                "input_value": 0,
                "output_value": 90,
                "fee": -90,
            },
        ]

        with patch.object(exp, "_batch_getrawtransaction") as mock_rpc:
            exp._resolve_unresolved_inputs(transactions)
            mock_rpc.assert_not_called()

        assert transactions[1]["inputs"][0]["value"] == 100
