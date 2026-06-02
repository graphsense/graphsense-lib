from graphsenselib.utils.ec import (
    is_valid_secp256k1_pubkey,
    secp256k1_compress,
    secp256k1_decompress,
)
from graphsenselib.utils.generic import custom_json_decoder


def test_is_valid_secp256k1_pubkey_fast_matches_slow():
    """The coincurve fast path must agree with the pure-Python oracle."""
    valid_comp = bytes.fromhex(
        "035088337106d55746a3cc7a6b93b1eca9babd0e7bc8609ff90288093e29ea8ccb"
    )
    gen_comp = bytes.fromhex(
        "0379be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
    )
    cases = [
        valid_comp,
        gen_comp,
        secp256k1_decompress(valid_comp),  # uncompressed form, should be valid
        b"\x02" + b"\x00" * 32,  # x == 0, rejected by both
        b"\x00" * 33,  # bad prefix
        b"\x02" + b"\xff" * 32,  # x >= p, off-curve / out of range
    ]
    for pk in cases:
        assert is_valid_secp256k1_pubkey(pk, fast=True) == is_valid_secp256k1_pubkey(
            pk, fast=False
        ), pk.hex()
    # sanity on the absolute verdicts
    assert is_valid_secp256k1_pubkey(valid_comp) is True
    assert is_valid_secp256k1_pubkey(b"\x02" + b"\x00" * 32) is False


def test_is_valid_secp256k1_pubkey_surfaces_misuse():
    """Non-bytes input is a bug, not an invalid key — it must NOT be silently
    swallowed into a False (which would drop keys with no signal)."""
    import pytest

    with pytest.raises(TypeError):
        is_valid_secp256k1_pubkey(None)  # ty: ignore[invalid-argument-type]


def test_ecrecover_pubkey():
    import json
    from graphsenselib.utils.signature import (
        eth_get_msg_hash_from_signature_data,
        eth_recover_pubkey,
    )
    from . import resources
    import importlib.resources
    from eth_keys.datatypes import PublicKey

    with (
        importlib.resources.files(resources)
        .joinpath("ecrecover_test_dataset.json")
        .open("r") as f
    ):
        test_set = json.load(f, object_hook=custom_json_decoder)

        for t in test_set:
            for (
                _,
                original_from,
                _,
                vrs,
                msg_hash,
                pubkey_hex,
                sdata,
            ) in test_set[t]:
                msg_hash_recomputed = eth_get_msg_hash_from_signature_data(sdata)

                assert msg_hash_recomputed.hex() == msg_hash, (
                    f"Hash mismatch {msg_hash_recomputed.hex()} != {msg_hash}"
                )

                pubkey = eth_recover_pubkey(vrs, msg_hash_recomputed)
                assert pubkey.to_compressed_bytes().hex() == pubkey_hex, (
                    f"Pubkey mismatch {pubkey.to_compressed_bytes().hex()} != {pubkey_hex}"
                )

                original_pubkey = PublicKey.from_compressed_bytes(
                    bytes.fromhex(pubkey_hex)
                )
                assert pubkey == original_pubkey, "Pubkey objects differ"

                assert pubkey.to_checksum_address().lower() == original_from, (
                    f"Address mismatch {pubkey.to_checksum_address().lower()} != {original_from}"
                )

                ucomp_public_key_bytes = b"\x04" + bytes.fromhex(str(pubkey)[2:])
                comp_public_key_bytes = secp256k1_compress(ucomp_public_key_bytes)

                assert (
                    secp256k1_decompress(comp_public_key_bytes)
                    == ucomp_public_key_bytes
                ), (
                    f"Decompressed pubkey mismatch {secp256k1_decompress(comp_public_key_bytes)} != {ucomp_public_key_bytes}"
                )

                assert is_valid_secp256k1_pubkey(comp_public_key_bytes), (
                    "Compressed pubkey not valid"
                )
