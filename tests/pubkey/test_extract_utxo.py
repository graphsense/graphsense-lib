"""Tests for UTXO signing-pubkey extraction.

The extraction never re-derives the secp256k1 curve maths itself — it only
*selects* the right push out of scriptSig / witness and normalises the
result. So the fixtures here are short: a known-valid compressed pubkey
embedded in canonical P2PKH and P2WPKH input shapes.
"""

from graphsenselib.pubkey.extract import (
    _parse_script_pushes,
    _pubkey_from_input,
    _pubkeys_from_input,
    _pubkeys_from_multisig_script,
    _pubkeys_from_output,
    _to_compressed,
    extract_pubkeys_utxo,
    extract_pubkeys_utxo_outputs,
)

# A real compressed secp256k1 pubkey lifted from the ECDSA-recovery test
# dataset (tests/utils/resources/ecrecover_test_dataset.json).
_PUBKEY_COMPRESSED_HEX = (
    "035088337106d55746a3cc7a6b93b1eca9babd0e7bc8609ff90288093e29ea8ccb"
)
_PUBKEY_COMPRESSED = bytes.fromhex(_PUBKEY_COMPRESSED_HEX)

# A second valid on-curve compressed key (the secp256k1 generator with an
# odd-y prefix), used to build multi-key multisig fixtures.
_PUBKEY2_COMPRESSED = bytes.fromhex(
    "0379be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
)


def _multisig_script(m: int, keys: list[bytes]) -> bytes:
    """Build an ``OP_m <k1>…<kn> OP_n OP_CHECKMULTISIG`` redeem/witness script."""
    op_m = bytes([0x50 + m])
    op_n = bytes([0x50 + len(keys)])
    body = b"".join(_push(k) for k in keys)
    return op_m + body + op_n + b"\xae"


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


def test_multisig_script_parser_extracts_all_keys():
    script = _multisig_script(2, [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED])
    assert _pubkeys_from_multisig_script(script) == [
        _PUBKEY_COMPRESSED,
        _PUBKEY2_COMPRESSED,
    ]


def test_multisig_script_parser_rejects_non_multisig():
    # A bare compressed key is shorter than the min script len and does not
    # end in OP_CHECKMULTISIG -> not a multisig script.
    assert _pubkeys_from_multisig_script(_PUBKEY_COMPRESSED) == []
    # P2PKH redeem script (ends in OP_CHECKSIG 0xac), not multisig.
    p2pkh = b"\x76\xa9\x14" + b"\x00" * 20 + b"\x88\xac"
    assert _pubkeys_from_multisig_script(p2pkh) == []


def test_pubkeys_from_p2sh_multisig_scriptsig():
    # P2SH 2-of-2 spend: OP_0 <sig1> <sig2> <redeemScript>
    redeem = _multisig_script(2, [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED])
    fake_sig = b"\x30" + b"\x44" * 70
    script = b"\x00" + _push(fake_sig) + _push(fake_sig) + _push(redeem)
    keys = _pubkeys_from_input({"txinwitness": None, "script_hex": script})
    assert keys == [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED]


def test_pubkeys_from_p2wsh_multisig_witness():
    # P2WSH 2-of-2 spend: ["", sig1, sig2, witnessScript]
    witness_script = _multisig_script(2, [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED])
    fake_sig = b"\x30" + b"\x44" * 70
    inp = {
        "txinwitness": [b"", fake_sig, fake_sig, witness_script],
        "script_hex": b"",
    }
    assert _pubkeys_from_input(inp) == [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED]


def test_p2wpkh_still_yields_single_key():
    fake_sig = b"\x30" + b"\x44" * 70
    inp = {"txinwitness": [fake_sig, _PUBKEY_COMPRESSED], "script_hex": b""}
    assert _pubkeys_from_input(inp) == [_PUBKEY_COMPRESSED]


def test_multisig_keys_flow_through_extract_pubkeys_utxo():
    redeem = _multisig_script(2, [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED])
    fake_sig = b"\x30" + b"\x44" * 70
    inputs = [
        {"txinwitness": [fake_sig, _PUBKEY_COMPRESSED], "script_hex": b""},
        {
            "txinwitness": None,
            "script_hex": b"\x00" + _push(fake_sig) + _push(fake_sig) + _push(redeem),
        },
    ]
    assert extract_pubkeys_utxo(inputs) == [
        _PUBKEY_COMPRESSED,
        _PUBKEY_COMPRESSED,
        _PUBKEY2_COMPRESSED,
    ]


def test_pubkeys_from_p2pk_output():
    # P2PK output: <pubkey> OP_CHECKSIG
    for key in (_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED):
        script = _push(key) + b"\xac"
        assert _pubkeys_from_output({"script_hex": script}) == [key]


def test_pubkeys_from_p2pk_output_uncompressed():
    # Uncompressed 65-byte P2PK key is normalised to compressed form.
    uncompressed = bytes.fromhex(
        "04"
        "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
        "483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8"
    )
    script = _push(uncompressed) + b"\xac"
    # compressed generator has even y -> 0x02 prefix
    assert _pubkeys_from_output({"script_hex": script}) == [
        bytes.fromhex(
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
        )
    ]


def test_pubkeys_from_p2ms_output():
    # bare P2MS output: OP_1 <pk1> <pk2> OP_2 OP_CHECKMULTISIG
    script = _multisig_script(1, [_PUBKEY_COMPRESSED, _PUBKEY2_COMPRESSED])
    assert _pubkeys_from_output({"script_hex": script}) == [
        _PUBKEY_COMPRESSED,
        _PUBKEY2_COMPRESSED,
    ]


def test_p2pkh_output_yields_no_key():
    # P2PKH output ends in OP_CHECKSIG too, but only pushes a 20-byte hash.
    p2pkh = b"\x76\xa9\x14" + b"\x11" * 20 + b"\x88\xac"
    assert _pubkeys_from_output({"script_hex": p2pkh}) == []


def test_extract_pubkeys_utxo_outputs_flattens():
    p2pk = _push(_PUBKEY_COMPRESSED) + b"\xac"
    p2ms = _multisig_script(1, [_PUBKEY2_COMPRESSED])
    outputs = [{"script_hex": p2pk}, {"script_hex": p2ms}, {"script_hex": b""}]
    assert extract_pubkeys_utxo_outputs(outputs) == [
        _PUBKEY_COMPRESSED,
        _PUBKEY2_COMPRESSED,
    ]


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
