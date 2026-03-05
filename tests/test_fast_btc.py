"""Tests for fast_btc.py — direct JSON-RPC UTXO block/tx exporter."""

from unittest.mock import patch

from graphsenselib.ingest.fast_btc import (
    FastBtcBlockExporter,
    _btc_to_satoshi,
    _nonce_to_hex,
    _parse_btc_block_and_txs,
    _parse_input,
    _parse_output,
    _script_hex_to_non_standard_address,
)


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

    def test_input_with_prevout_p2pk_no_address(self):
        """Verbosity 3: P2PK prevout has no address → derive nonstandard."""
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
        """ZCash valueBalance creates shielded inputs or outputs."""
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
                    "valueBalance": -5.0,  # negative → shielded output
                }
            ],
        }
        block, txs = _parse_btc_block_and_txs(block_raw)
        tx = txs[0]
        shielded_outputs = [o for o in tx["outputs"] if o["type"] == "shielded"]
        assert len(shielded_outputs) == 1
        assert shielded_outputs[0]["value"] == 500_000_000

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


class TestResolveUnresolvedInputs:
    """Tests for _resolve_unresolved_inputs (v2 getrawtransaction fallback)."""

    def _make_exporter(self):
        """Create an exporter without connecting to a node."""
        with patch.object(FastBtcBlockExporter, "__init__", lambda self, **kw: None):
            exp = FastBtcBlockExporter.__new__(FastBtcBlockExporter)
            exp.max_workers = 2
            exp._output_cache = {}
            return exp

    def test_within_batch_resolution(self):
        """Inputs spending outputs from the same batch are resolved without RPC."""
        exp = self._make_exporter()

        transactions = [
            {
                "hash": "tx_a",
                "is_coinbase": False,
                "inputs": [],
                "outputs": [
                    {"index": 0, "value": 50_000_000, "addresses": ["addr1"], "type": "p2pkh"},
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
                    {"index": 0, "value": 49_000_000, "addresses": ["addr2"], "type": "p2pkh"},
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
                    {"index": 0, "value": 30_000_000, "addresses": ["addr3"], "type": "p2pkh"},
                ],
                "input_value": 0,
                "output_value": 30_000_000,
                "fee": -30_000_000,
            },
        ]

        # Mock the RPC call to return a fake spent tx
        fake_rpc_result = {
            "external_tx": {
                1: {"value": 31_000_000, "addresses": ["addr_ext"], "type": "p2sh"},
            }
        }
        with patch.object(exp, "_batch_getrawtransaction", return_value=fake_rpc_result):
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
                    {"index": 0, "value": 625_000_000, "addresses": ["miner"], "type": "p2pkh"},
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
