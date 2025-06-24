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
