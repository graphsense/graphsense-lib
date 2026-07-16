"""The Delta/parquet sink must store txinwitness as raw bytes, not hex text.

The node's JSON-RPC delivers witness elements as hex strings. The Cassandra
path unhexes them (tx_io_summary); the parquet prep converted
spent_transaction_hash but left txinwitness as strings, so pyarrow silently
wrote the UTF-8 bytes of the hex text into the binary column — every Delta
lake row carries hex-as-bytes witness (and downstream consumers like the
pubkey extractor, which parse witness structurally, find nothing).
"""

from graphsenselib.ingest.utxo import prepare_transactions_inplace_parquet

SIG_HEX = "3045022100" + "ab" * 66
PUBKEY_HEX = "03" + "cd" * 32


def _tx(witness):
    return {
        "index": 0,
        "block_number": 100,
        "hash": "ab" * 32,
        "is_coinbase": False,
        "block_timestamp": 1_700_000_000,
        "input_value": 100,
        "output_value": 90,
        "block_hash": "cd" * 32,
        "coinjoin": False,
        "inputs": [
            {
                "spent_transaction_hash": "11" * 32,
                "script_asm": "asm",
                "required_signatures": None,
                "script_hex": "76a914",
                "txinwitness": witness,
            }
        ],
        "outputs": [{"script_asm": "asm", "script_hex": "76a988"}],
    }


def test_witness_hex_strings_become_bytes():
    tx = _tx([SIG_HEX, PUBKEY_HEX])
    prepare_transactions_inplace_parquet([tx], "ltc")
    assert tx["inputs"][0]["txinwitness"] == [
        bytes.fromhex(SIG_HEX),
        bytes.fromhex(PUBKEY_HEX),
    ]


def test_witness_none_stays_none():
    tx = _tx(None)
    prepare_transactions_inplace_parquet([tx], "ltc")
    assert tx["inputs"][0]["txinwitness"] is None


def test_witness_empty_elements_survive():
    # CHECKMULTISIG dummy: witness stacks legitimately contain "" elements
    tx = _tx(["", SIG_HEX])
    prepare_transactions_inplace_parquet([tx], "ltc")
    assert tx["inputs"][0]["txinwitness"] == [b"", bytes.fromhex(SIG_HEX)]


def test_witness_already_bytes_is_idempotent():
    tx = _tx([bytes.fromhex(SIG_HEX)])
    prepare_transactions_inplace_parquet([tx], "ltc")
    assert tx["inputs"][0]["txinwitness"] == [bytes.fromhex(SIG_HEX)]
