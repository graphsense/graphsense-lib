"""Tests for account-model (ETH-style) signing-pubkey extraction.

Drives ``extract_pubkey_account`` with delta-shaped tx rows reconstructed
from the existing ECDSA-recovery test dataset
(tests/utils/resources/ecrecover_test_dataset.json) so the extraction layer
is tested against the same canonical vectors as the underlying signature
helper.
"""

import importlib.resources
import json
from typing import Any, Dict

import pytest

from graphsenselib.pubkey.extract import extract_pubkey_account
from graphsenselib.utils.generic import custom_json_decoder


def _load_dataset():
    from tests.utils import resources  # type: ignore[import-not-found]

    with (
        importlib.resources.files(resources)
        .joinpath("ecrecover_test_dataset.json")
        .open("r") as f
    ):
        return json.load(f, object_hook=custom_json_decoder)


def _sdata_to_delta_row(
    sdata: Dict[str, Any], vrs, tx_type: int, from_address=None
) -> Dict[str, Any]:
    """Translate a signature_data dict into a delta-style tx_row.

    ``signature_data`` uses eth_account's serializable-tx field names
    (camelCase, raw ints, bytes for binary fields). Our delta lake stores
    snake_case columns with varints encoded as big-endian binary blobs.
    Convert one to the other so the test exercises ``extract_pubkey_account``
    end-to-end including its blob→int parsing.
    """

    def _be(x) -> bytes:
        if x is None or x == 0:
            return b"\x00"
        n = (x.bit_length() + 7) // 8
        return x.to_bytes(n, "big")

    v, r, s = vrs
    row: Dict[str, Any] = {
        "transaction_type": tx_type,
        "nonce": sdata.get("nonce", 0),
        "to_address": sdata.get("to") or b"",
        "value": _be(sdata.get("value", 0)),
        "gas": sdata.get("gas", 0),
        "input": sdata.get("data") or b"",
        "max_fee_per_gas": sdata.get("maxFeePerGas"),
        "max_priority_fee_per_gas": sdata.get("maxPriorityFeePerGas"),
        "max_fee_per_blob_gas": sdata.get("maxFeePerBlobGas"),
        "blob_versioned_hashes": sdata.get("blobVersionedHashes") or [],
        "access_list": sdata.get("accessList") or [],
        "gas_price": _be(sdata.get("gasPrice", 0)),
        "v": v,
        "r": _be(r),
        "s": _be(s),
    }
    if from_address is not None:
        row["from_address"] = from_address
    return row


def _walk_dataset():
    dataset = _load_dataset()
    for type_key, rows in dataset.items():
        if not rows:
            continue
        tx_type = int(type_key, 16) if type_key.startswith("0x") else int(type_key)
        if tx_type < 0:
            continue
        for record in rows:
            _, _from, _, vrs, _msg_hash, pubkey_hex, sdata = record
            yield tx_type, _from, vrs, pubkey_hex, sdata


@pytest.mark.parametrize(
    "tx_type,from_addr,vrs,expected_pubkey_hex,sdata",
    list(_walk_dataset()),
)
def test_extract_pubkey_account_recovers_known_pubkey(
    tx_type, from_addr, vrs, expected_pubkey_hex, sdata
):
    # Thread the real from_address through so the recovery AND the ETH
    # from-address self-check are both exercised on every canonical vector.
    row = _sdata_to_delta_row(sdata, vrs, tx_type, from_address=from_addr)
    recovered = extract_pubkey_account(row, currency="eth")
    assert recovered is not None, "Pubkey recovery returned None"
    assert recovered.hex() == expected_pubkey_hex


def test_eth_from_address_mismatch_is_dropped():
    tx_type, from_addr, vrs, _pubkey_hex, sdata = next(_walk_dataset())
    # Corrupt the from_address: a correctly recovered ETH key whose address
    # does not match `from` can only mean a bad recovery -> must be dropped.
    row = _sdata_to_delta_row(sdata, vrs, tx_type, from_address="0x" + "00" * 20)
    assert extract_pubkey_account(row, currency="eth") is None


def test_eth_from_address_match_accepts_bytes_form():
    tx_type, from_addr, vrs, expected_pubkey_hex, sdata = next(_walk_dataset())
    # Production stores from_address as a 20-byte blob, not a 0x string.
    from_bytes = bytes.fromhex(
        from_addr[2:] if from_addr.startswith("0x") else from_addr
    )
    row = _sdata_to_delta_row(sdata, vrs, tx_type, from_address=from_bytes)
    recovered = extract_pubkey_account(row, currency="eth")
    assert recovered is not None
    assert recovered.hex() == expected_pubkey_hex


def test_extract_pubkey_account_returns_none_when_signature_missing():
    row = {
        "transaction_type": 0,
        "nonce": 1,
        "gas_price": b"\x00",
        "gas": 21000,
        "to_address": b"\x00" * 20,
        "value": b"\x00",
        "input": b"",
        "v": None,
        "r": None,
        "s": None,
    }
    assert extract_pubkey_account(row, currency="eth") is None


def test_extract_pubkey_account_returns_none_for_zero_signature():
    row = {
        "transaction_type": 0,
        "nonce": 1,
        "gas_price": b"\x00",
        "gas": 21000,
        "to_address": b"\x00" * 20,
        "value": b"\x00",
        "input": b"",
        "v": 27,
        "r": b"\x00",
        "s": b"\x00",
    }
    assert extract_pubkey_account(row, currency="eth") is None
