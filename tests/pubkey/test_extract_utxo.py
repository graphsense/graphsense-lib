"""Tests for UTXO signing-pubkey extraction.

The extraction never re-derives the secp256k1 curve maths itself — it only
*selects* the right push out of scriptSig / witness and normalises the
result. So the fixtures here are short: a known-valid compressed pubkey
embedded in canonical P2PKH and P2WPKH input shapes.
"""

from graphsenselib.pubkey.extract import (
    _parse_script_pushes,
    _pubkey_from_input,
    _to_compressed,
    extract_pubkeys_utxo,
)

# A real compressed secp256k1 pubkey lifted from the ECDSA-recovery test
# dataset (tests/utils/resources/ecrecover_test_dataset.json).
_PUBKEY_COMPRESSED_HEX = (
    "035088337106d55746a3cc7a6b93b1eca9babd0e7bc8609ff90288093e29ea8ccb"
)
_PUBKEY_COMPRESSED = bytes.fromhex(_PUBKEY_COMPRESSED_HEX)


def _push(payload: bytes) -> bytes:
    """Emit a minimal-encoded push of ``payload`` (matches Bitcoin Core)."""
    n = len(payload)
    if n <= 0x4B:
        return bytes([n]) + payload
    if n <= 0xFF:
        return b"\x4c" + bytes([n]) + payload
    if n <= 0xFFFF:
        return b"\x4d" + n.to_bytes(2, "little") + payload
    return b"\x4e" + n.to_bytes(4, "little") + payload


def test_to_compressed_passthrough():
    assert _to_compressed(_PUBKEY_COMPRESSED) == _PUBKEY_COMPRESSED


def test_to_compressed_rejects_garbage():
    assert _to_compressed(b"") is None
    assert _to_compressed(b"\x00" * 33) is None  # bad prefix
    assert _to_compressed(b"\x02" + b"\x00" * 32) is None  # off-curve


def test_parse_script_pushes_p2pkh_shape():
    fake_sig = b"\x30" + b"\x44" * 70  # 71-byte placeholder, only length matters
    script = _push(fake_sig) + _push(_PUBKEY_COMPRESSED)
    pushes = _parse_script_pushes(script)
    assert len(pushes) == 2
    assert pushes[0] == fake_sig
    assert pushes[1] == _PUBKEY_COMPRESSED


def test_pubkey_from_p2wpkh_witness():
    fake_sig = b"\x30" + b"\x44" * 70
    inp = {"txinwitness": [fake_sig, _PUBKEY_COMPRESSED], "script_hex": b""}
    assert _pubkey_from_input(inp) == _PUBKEY_COMPRESSED


def test_pubkey_from_p2pkh_scriptsig():
    fake_sig = b"\x30" + b"\x44" * 70
    script = _push(fake_sig) + _push(_PUBKEY_COMPRESSED)
    inp = {"txinwitness": [], "script_hex": script}
    assert _pubkey_from_input(inp) == _PUBKEY_COMPRESSED


def test_pubkey_from_input_accepts_hex_scripthex():
    fake_sig = b"\x30" + b"\x44" * 70
    script = _push(fake_sig) + _push(_PUBKEY_COMPRESSED)
    inp = {"txinwitness": None, "script_hex": script.hex()}
    assert _pubkey_from_input(inp) == _PUBKEY_COMPRESSED


def test_pubkey_from_input_p2pk_or_coinbase_returns_none():
    # P2PK scriptSig is just <sig>; no pubkey present in the input itself.
    fake_sig = b"\x30" + b"\x44" * 70
    inp = {"txinwitness": None, "script_hex": _push(fake_sig)}
    assert _pubkey_from_input(inp) is None

    # Coinbase: arbitrary data, no parseable pubkey.
    coinbase_script = b"\x03\x01\x02\x03"  # OP_PUSH3 + 3 bytes
    inp_cb = {"txinwitness": None, "script_hex": coinbase_script}
    assert _pubkey_from_input(inp_cb) is None


def test_extract_pubkeys_utxo_dedupes_across_inputs():
    fake_sig = b"\x30" + b"\x44" * 70
    inputs = [
        {"txinwitness": [fake_sig, _PUBKEY_COMPRESSED], "script_hex": b""},
        {
            "txinwitness": [],
            "script_hex": _push(fake_sig) + _push(_PUBKEY_COMPRESSED),
        },
    ]
    result = extract_pubkeys_utxo(inputs)
    # extract_pubkeys_utxo does not dedupe internally — the caller (Spark
    # job) calls dropDuplicates afterwards. We just check both inputs
    # contribute the same pubkey.
    assert result == [_PUBKEY_COMPRESSED, _PUBKEY_COMPRESSED]
