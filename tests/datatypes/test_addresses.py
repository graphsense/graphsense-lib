from graphsenselib.datatypes.address import AddressUtxo
from graphsenselib.utils import DataObject as D


def test_is_bech32(capsys):
    a = AddressUtxo(
        "bc1qu5ujlp9dkvtgl98jakvw9ggj9uwyk79qhvwvrg",
        D(**{"bech_32_prefix": "bc", "address_prefix_length": 5}),
    )

    assert a.is_bech32 is True
    assert a.prefix == "1qu5u"


def test_isnot_bech32_if_no_prefix_config(capsys):
    a = AddressUtxo(
        "bc1qu5ujlp9dkvtgl98jakvw9ggj9uwyk79qhvwvrg",
        D(**{"bech_32_prefix": "", "address_prefix_length": 5}),
    )

    assert a.is_bech32 is False
    assert a.prefix == "bc1qu"


def test_isnot_bech32(capsys):
    a = AddressUtxo(
        "3MzhkeE3EjenfoaYm6KxLaDLryQ3ZYytF9",
        D(**{"bech_32_prefix": "bc", "address_prefix_length": 5}),
    )

    assert a.is_bech32 is False
    assert a.prefix == "3Mzhk"
