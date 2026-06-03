"""Unit tests for the btcpy-free output-script parser (`_parse_script_native`).

Vectors are lifted from btcpy's own `tests/data/scripts.json` /
`unknownscripts.json`, with the expected outputs computed by btcpy — so the
native parser is held byte-for-byte compatible with the implementation it
replaces. These tests do NOT require btcpy at runtime (they validate the native
parser standalone, i.e. the post-btcpy state); the final `test_native_matches_btcpy`
re-checks equivalence against btcpy while it is still installed and auto-skips
once it is removed.
"""

import pytest

from graphsenselib.ingest.utxo import (
    P2pkParserException,
    UnknownScriptType,
    _parse_script_btcpy,
    _parse_script_native,
)

# (script_hex, expected_addresses_or_None, expected_type) — from scripts.json.
_HANDLED = [
    (
        "a914ed4a0e1af5316666499ec6f8a5a99bf4abaf754987",
        ["3PKgnSpij9jRKRDwHKGYAbANAoBFyPJHYr"],
        "p2sh",
    ),
    (
        "76a914df76c017354ac39bde796abe4294d31de8b5788a88ac",
        ["1MNZwhTBHN3QTXkwob7NvhVaTVKUm7MRCg"],
        "p2pkh",
    ),
    (
        "4104ea0d6650c8305f1213a89c65fc8f4343a5dac8e985c869e51d3aa02879b57c60"
        "cff49fcb99314d02dfc612d654e4333150ef61fa569c1c66415602cae387baf7ac",
        ["1BDvQZjaAJH4ecZ8aL3fYgTi7rnn3o2thE"],
        "p2pk",
    ),
    (
        "522102c08786d63f78bd0a6777ffe9c978cf5899756cfc32bfad09a89e211aeb9262"
        "4221033e81519ecf373ea3a5c7e1c051b71a898fb3438c9550e274d980f147eb4d06"
        "9d21036d568125a969dc78b963b494fa7ed5f20ee9c2f2fc2c57f86c5df63089f2ed"
        "3a53ae",
        [
            "1PfTD843HiN2PPpxFnMwyxN7se6MEf2ech",
            "1JXc8zsSeAPwqfAzLbBnZxNTfetZexH2bW",
            "1Ng4YU2e2H3E86syX2qrsmD9opBHZ42vCF",
        ],
        "multisig",
    ),
    (
        "6a28444f4350524f4f463832bd18ceb0a7861f2a8198013047a3fb861261523c0fc4"
        "164abc044e517702",
        None,
        "nulldata",
    ),
    (
        "0014f81b6a6cfaaf19dbd9e56b9cab2d8a457608ad8e",
        ["bc1qlqdk5m864uvahk09dww2ktv2g4mq3tvw4p49r3"],
        "p2wpkhv0",
    ),
    (
        "0020cdbf909e935c855d3e8d1b61aeb9c5e3c03ae8021b286839b1a72f2e48fdba70",
        ["bc1qeklep85ntjz4605drds6aww9u0qr46qzrv5xswd35uhjuj8ahfcqgf6hak"],
        "p2wshv0",
    ),
]

# Scripts btcpy rejects as unknown (from unknownscripts.json + the typed
# timelock scripts in scripts.json the project does not handle).
_UNKNOWN = [
    "5152ae",  # OP_1 OP_2 OP_CHECKMULTISIG (no keys)
    "6a",  # bare OP_RETURN (not nulldata)
    "6a4b",  # OP_RETURN + malformed pushdata
    "6368",  # OP_IF OP_ENDIF
    "4bac",  # truncated push + OP_CHECKSIG
    "004b",  # OP_0 + malformed push
    "a94b87",  # OP_HASH160 + malformed push + OP_EQUAL
    "76a94b88ac",  # P2PKH shape with malformed push
    "ac",  # bare OP_CHECKSIG
    "00",  # bare OP_0
    "a987",  # OP_HASH160 OP_EQUAL (no hash)
    "51ae",  # OP_1 OP_CHECKMULTISIG (no keys)
    # if/else timelock scripts (scripts.json) — not a handled type
    "6352210384478d41e71dc6c3f9edde0f928a47d1b724c05984ebfb4e7d0422e80abe95ff"
    "2103eb27fa93667e4f48e36071eb21c7229e5416ff0abd2886d59c8f314fb3cbee4052ae"
    "67037b9710b175210384478d41e71dc6c3f9edde0f928a47d1b724c05984ebfb4e7d0422"
    "e80abe95ffac68",
]

# P2PK-shaped script whose pushed "key" is not a valid pubkey (lifted from the
# existing btcpy test) — must raise P2pkParserException like btcpy.
_UNPARSEABLE_P2PK = (
    "419e000000416e6f7468657220746578742077617320656d62656464656420696e746f20"
    "74686520626c6f636b20636861696e2e20546865207374616e6461726420ac"
)


@pytest.mark.parametrize("script_hex,addresses,script_type", _HANDLED)
def test_native_parses_standard_scripts(script_hex, addresses, script_type):
    assert _parse_script_native(script_hex) == (addresses, script_type)


@pytest.mark.parametrize("script_hex", _UNKNOWN)
def test_native_rejects_unknown_scripts(script_hex):
    with pytest.raises(UnknownScriptType):
        _parse_script_native(script_hex)


def test_native_unparseable_p2pk_raises():
    with pytest.raises(P2pkParserException):
        _parse_script_native(_UNPARSEABLE_P2PK)


@pytest.mark.parametrize(
    "script_hex",
    [h for h, _, _ in _HANDLED] + _UNKNOWN + [_UNPARSEABLE_P2PK],
)
def test_native_matches_btcpy(script_hex):
    """Equivalence to btcpy while it is still installed (skips after removal)."""
    pytest.importorskip("btcpy")

    def outcome(fn):
        try:
            addrs, script_type = fn(script_hex)
            return ("ok", tuple(addrs) if addrs else None, script_type)
        except Exception as e:  # noqa: BLE001 — compare outcome shape
            return ("err", type(e).__name__)

    assert outcome(_parse_script_native) == outcome(_parse_script_btcpy)
