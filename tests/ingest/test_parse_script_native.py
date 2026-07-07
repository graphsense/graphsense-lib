"""Unit tests for the output-script parser (`parse_script`).

Vectors are lifted from the retired btcpy library's own `tests/data/scripts.json`
/ `unknownscripts.json`, with the expected outputs computed by btcpy — so the
parser stays byte-for-byte compatible with the implementation it replaced
(validated in a month-long production shadow run before btcpy was dropped,
2026-07).
"""

import pytest

from graphsenselib.ingest.utxo import (
    P2pkParserException,
    UnknownScriptType,
    parse_script,
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

# Test pubkeys for the template-quirk vectors below.
_PK = "02bd518511a74ae03011ecd13d0282c91efe26e124fea565695cc866397daae1e6"
_PK_ADDR = "18318B2aEkBjpZmJMnAf8N4ax2251RntbM"
_BADPK = "05" + _PK[2:]  # 33 bytes, invalid SEC1 prefix

# btcpy matches templates token-wise, with several quirks the native parser
# replicates exactly (vectors verified against btcpy; see 2.14.1 changelog).
_HANDLED += [
    # non-minimal pushes are accepted everywhere
    ("76a94c14" + "00" * 20 + "88ac", ["1111111111111111111114oLvT2"], "p2pkh"),
    ("a94c14" + "00" * 20 + "87", ["31h1vYVSYuKP6AhS86fbRdMw9XHieotbST"], "p2sh"),
    (
        "004c14" + "00" * 20,
        ["bc1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq9e75rs"],
        "p2wpkhv0",
    ),
    ("4c21" + _PK + "ac", [_PK_ADDR], "p2pk"),
    # hybrid (0x06/0x07) uncompressed pubkeys are accepted
    ("41" + "06" + "00" * 64 + "ac", ["1KfGd4BLUo2WkVPP8ANf2GveoNnrgsgrcC"], "p2pk"),
    # nulldata payload may be a small-int opcode (OP_0/OP_1..OP_16)
    ("6a53", None, "nulldata"),
    ("6a00", None, "nulldata"),
    # multisig: N is the numeric value of the last push, not necessarily OP_N
    ("51" + "21" + _PK + "21" + _PK + "0102ae", [_PK_ADDR, _PK_ADDR], "multisig"),
    # multisig: the M slot may be any push, value unchecked
    (
        "21" + _BADPK + "21" + _PK + "21" + _PK + "52ae",
        [_PK_ADDR, _PK_ADDR],
        "multisig",
    ),
    # multisig: invalid-format keys are dropped, not rejected
    ("51" + "21" + _BADPK + "51ae", [], "multisig"),
    # multisig: btcpy swallows a truncated trailing push (header consumed,
    # last byte still matches OP_CHECKMULTISIG)
    ("51" + "21" + _PK + "5113ae", [_PK_ADDR], "multisig"),
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
    # 2026-06-10 LTC incident: OP_5 OP_5 OP_ADD OP_DROP <pk> OP_CHECKSIG — the
    # old native parser skipped the prefix opcodes and misclassified it p2pk
    "55559375" + "21" + _PK + "ac",
    # extra opcodes anywhere inside the multisig template are nonstandard
    "5175" + "21" + _PK + "51ae",
    "55559375" + "51" + "21" + _PK + "51ae",
    # key-push count must equal the numeric value of N
    "51" + "21" + _PK + "52ae",
    "0000ae",  # OP_0 is not a valid N
    "5120" + "00" * 32,  # taproot (witness v1) is not handled by btcpy
    "6a4f",  # OP_1NEGATE has StackData length 0, outside nulldata's <1-83>
    "6a4c00",  # empty push, same
    "6a4c54" + "00" * 84,  # 84-byte payload, above nulldata's limit
]

# P2PK-shaped script whose pushed "key" is not a valid pubkey (lifted from the
# existing btcpy test) — must raise P2pkParserException like btcpy.
_UNPARSEABLE_P2PK = (
    "419e000000416e6f7468657220746578742077617320656d62656464656420696e746f20"
    "74686520626c6f636b20636861696e2e20546865207374616e6461726420ac"
)


@pytest.mark.parametrize("script_hex,addresses,script_type", _HANDLED)
def test_parses_standard_scripts(script_hex, addresses, script_type):
    assert parse_script(script_hex) == (addresses, script_type)


@pytest.mark.parametrize("script_hex", _UNKNOWN)
def test_rejects_unknown_scripts(script_hex):
    with pytest.raises(UnknownScriptType):
        parse_script(script_hex)


def test_unparseable_p2pk_raises():
    with pytest.raises(P2pkParserException):
        parse_script(_UNPARSEABLE_P2PK)


# Multisig-shaped scripts whose N (or M) slot is a data push with an empty
# payload: btcpy crashed with IndexError in StackData.__int__, and the parser
# preserves that behaviour.
_INT_OF_EMPTY_PUSH = [
    "21" + _PK + "21" + _PK + "4c00ae",  # N is an empty push
    "4c00" + "21" + _PK + "51ae",  # M is an empty push
]


@pytest.mark.parametrize("script_hex", _INT_OF_EMPTY_PUSH)
def test_replicates_btcpy_indexerror(script_hex):
    with pytest.raises(IndexError):
        parse_script(script_hex)
