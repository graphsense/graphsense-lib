from graphsenselib.utils.ec import (
    is_valid_secp256k1_pubkey,
    secp256k1_compress,
    secp256k1_decompress,
)
from graphsenselib.utils.generic import custom_json_decoder


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
