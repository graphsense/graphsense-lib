import pytest

from graphsenselib.utils.accountmodel import hex_str_to_bytes
from graphsenselib.utils.tron import (
    add_tron_prefix,
    decode_note,
    evm_to_bytes,
    evm_to_tron_address_string,
    partial_tron_to_partial_evm,
    tron_address_equal,
    tron_address_to_bytes,
    tron_address_to_evm_string,
    tron_address_to_legacy,
    tron_address_to_legacy_string,
)

# first is evm address and second is tron address
correct_encodings = [
    (
        "414450cf8c8b6a8229b7f628e36b3a658e84441b6f",
        "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu",
    ),
    ("4450cf8c8b6a8229b7f628e36b3a658e84441b6f", "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu"),
    (
        "0x4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
        "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu",
    ),
    (
        "0xe552f6487585c2b58bc2c9bb4492bc1f17132cd0",
        "TWsm8HtU2A5eEzoT8ev8yaoFjHsXLLrckb",
    ),
    (
        "0x5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
        "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnat",
    ),
]


def test_add_tron_prefix():
    # prefix must not be specified
    assert hex_str_to_bytes(
        "414450cf8c8b6a8229b7f628e36b3a658e84441b6f"
    ) == add_tron_prefix(
        hex_str_to_bytes("414450cf8c8b6a8229b7f628e36b3a658e84441b6f"[2:])
    )


def test_evm_to_tron_address():
    for evm, tron in correct_encodings:
        assert evm_to_tron_address_string(evm) == tron


def test_tron_address_equality():
    for evm, tron in correct_encodings:
        assert evm_to_bytes(evm) == tron_address_to_bytes(tron)
        assert tron_address_equal(evm, tron)


def test_checksum_error():
    with pytest.raises(ValueError):
        # introduced an error in the checksum postfix qnat -> qnaf
        tron_address_to_legacy("THKJYuUmMKKARNf7s2VT51g5uPY6KEqnaf")

    assert (
        tron_address_to_legacy_string(
            "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnaf", validate=False
        )
        == "0x5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2"
    )

    assert tron_address_to_legacy(
        "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnaf", validate=False
    ) == evm_to_bytes("0x5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2")


def test_tron_bijection():
    original_evm = "0x5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2"
    original_tron = "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnat"

    f = evm_to_tron_address_string
    g = tron_address_to_evm_string

    assert g(f(original_evm)) == original_evm
    assert f(g(original_tron)) == original_tron


def test_decode_note():
    assert decode_note("63616c6c") == "call"


def test_partial_prefix_conversion():
    eth_style = "0x5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2"
    tron_style = "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnat"

    for slice_len in range(34):
        actual = eth_style[:slice_len]
        converted = partial_tron_to_partial_evm(tron_style[:slice_len])
        assert actual == converted

    for slice_len in range(34, 43):
        actual = eth_style[:slice_len]
        converted = partial_tron_to_partial_evm(tron_style[:slice_len])
        assert actual == converted[:slice_len]
