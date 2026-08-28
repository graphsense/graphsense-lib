import pytest

from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)


def test_sub_tx_de_and_encoding():
    internal_str = (
        "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b_I1"
    )
    internal = SubTransactionIdentifier.from_string(internal_str)

    assert (
        internal.tx_hash
        == "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b"
    )
    assert internal.tx_type == SubTransactionType.InternalTx
    assert internal.sub_index == 1
    assert internal.to_string() == internal_str

    token_str = (
        "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b_T1123"
    )
    token = SubTransactionIdentifier.from_string(token_str)

    assert (
        token.tx_hash
        == "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b"
    )
    assert token.tx_type == SubTransactionType.ERC20
    assert token.sub_index == 1123
    assert token.to_string() == token_str
    assert (
        token.to_string(type_overwrite=SubTransactionType.ExternalTx)
        == "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b"
    )
    assert (
        token.to_string(type_overwrite=SubTransactionType.GenericLog)
        == "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b_L1123"
    )

    with pytest.raises(ValueError):
        SubTransactionIdentifier.from_string(
            "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b_Laaa"
        )


def test_marker_letter_is_case_insensitive():
    # The marker case carries no information and hand-typed identifiers are
    # often lowercased: "_t1" must parse like "_T1". to_string stays canonical
    # uppercase.
    h = "0x76f4263391a7d72f66cb1f254e8643e37ca739ab2859b9e9cd5b5bda3194332b"
    token = SubTransactionIdentifier.from_string(f"{h}_t1123")
    assert token.tx_type == SubTransactionType.ERC20
    assert token.sub_index == 1123
    assert token.to_string() == f"{h}_T1123"

    internal = SubTransactionIdentifier.from_string(f"{h}_i1")
    assert internal.tx_type == SubTransactionType.InternalTx
    assert internal.sub_index == 1
    assert internal.to_string() == f"{h}_I1"

    log = SubTransactionIdentifier.from_string(f"{h}_l7")
    assert log.tx_type == SubTransactionType.GenericLog
    assert log.sub_index == 7
    assert log.to_string() == f"{h}_L7"


def test_marker_must_prefix_the_index():
    # "_TT5" used to slip through the old strip()-based parser as index 5;
    # the marker must be exactly one letter followed by digits.
    with pytest.raises(ValueError):
        SubTransactionIdentifier.from_string("aa" * 32 + "_TT5")


def test_unknown_subtx_type_raises_value_error():
    # An unknown type prefix must raise ValueError so service layers that
    # translate ValueError into a 400 catch it (a bare Exception surfaced
    # as a 500 to API clients).
    with pytest.raises(ValueError, match="Unknown transaction type"):
        SubTransactionIdentifier.from_string("aa" * 32 + "_Q1")
