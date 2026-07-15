# -*- coding: utf-8 -*-
"""Parity between the two independent per-network address version tables.

The ingest P2PK deriver (``rpc_utxo._PUBKEY_ADDRESS_VERSION``) and the pubkey
dataset's address deriver (``pubkey_to_address.MAINNET_ADDRESS_SPECS``) each
carry their own copy of the P2PKH/t1 version bytes. A P2PK output's ingested
address is only useful if it is byte-identical to the ``pubkey_by_address`` key
the dataset derives for the same pubkey — otherwise the address never joins back
to its pubkey. These tests fail if the two tables drift (the doge-class bug: a
network missing from the ingest map silently falls back to BTC's ``0x00``).
"""

import pytest

from graphsenselib.ingest.rpc_utxo import (
    _PUBKEY_ADDRESS_VERSION,
    _p2pk_address_from_script,
)
from graphsenselib.utils.pubkey_to_address import (
    MAINNET_ADDRESS_SPECS,
    compress_public_key,
    convert_pubkey_to_addresses,
)

# ingest network code -> (MAINNET_ADDRESS_SPECS group, P2PKH/t1 version field).
# BCH legacy base58 shares BTC's version bytes, so its P2PK output is emitted as
# a "1..." address that lives under the "bitcoin" spec group.
_VERSION_SPEC_MAP = {
    "btc": ("bitcoin", "p2pkh"),
    "bch": ("bitcoin", "p2pkh"),
    "ltc": ("litecoin", "p2pkh"),
    "doge": ("dogecoin", "p2pkh"),
    "zec": ("zcash", "t1_p2pkh"),
}

# ingest network -> (convert_pubkey_to_addresses currency, compressed-key field,
# uncompressed-key field). The ingest deriver hashes the key exactly as it
# appears in the P2PK script, so a compressed script must land on the compressed
# form and an uncompressed script on the uncompressed form. BCH's ingested P2PK
# address is legacy base58, which the dataset derives under "btc".
_E2E_PARITY = {
    "btc": ("btc", "p2pkh", "p2pkh_uncomp"),
    "bch": ("btc", "p2pkh", "p2pkh_uncomp"),
    "ltc": ("ltc", "p2pkh", "p2pkh_uncomp"),
    "doge": ("doge", "p2pkh", "p2pkh_uncomp"),
    "zec": ("zec", "t1_p2pkh", "t1_p2pkh_uncomp"),
}

# Real mainnet secp256k1 point (uncompressed) taken from the P2PK output of tx
# 1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w — a genuine on-curve key so coincurve can
# compress it.
_UNCOMP_PUBKEY = (
    "04a9d6840fdd1497b3067b8066db783acf90bf42071a38fe2cf6d2d8a04835d0b5"
    "c45716d8d6012ab5d56c7824c39718f7bc7486d389cd0047f53785f9a63c0c9d"
)


def test_version_tables_cover_the_same_networks():
    # If a chain is added to one table but not the other, the ingest deriver
    # silently falls back to BTC's 0x00 for it. Force both to move together.
    assert set(_PUBKEY_ADDRESS_VERSION) == set(_VERSION_SPEC_MAP), (
        "ingest _PUBKEY_ADDRESS_VERSION and the parity map disagree on covered "
        "networks; update both (and MAINNET_ADDRESS_SPECS) when adding a chain"
    )


@pytest.mark.parametrize("net", sorted(_VERSION_SPEC_MAP))
def test_version_byte_parity(net):
    group, field = _VERSION_SPEC_MAP[net]
    assert _PUBKEY_ADDRESS_VERSION[net] == MAINNET_ADDRESS_SPECS[group][field], (
        f"P2PK version byte for {net!r} disagrees with "
        f"MAINNET_ADDRESS_SPECS[{group!r}][{field!r}]"
    )


@pytest.mark.parametrize("net", sorted(_E2E_PARITY))
def test_ingested_p2pk_address_is_derivable_from_pubkey(net):
    # End-to-end: the address the ingest deriver stores for a P2PK output must
    # be one the pubkey dataset also derives for that key, for both encodings.
    currency, comp_field, uncomp_field = _E2E_PARITY[net]
    comp_pubkey = compress_public_key(_UNCOMP_PUBKEY)

    comp_script = "21" + comp_pubkey + "ac"  # <push 33> <pubkey> OP_CHECKSIG
    uncomp_script = "41" + _UNCOMP_PUBKEY + "ac"  # <push 65> <pubkey> OP_CHECKSIG

    derived = convert_pubkey_to_addresses(comp_pubkey, currencies=[currency])[currency]

    assert _p2pk_address_from_script(comp_script, net) == derived[comp_field]
    assert _p2pk_address_from_script(uncomp_script, net) == derived[uncomp_field]
