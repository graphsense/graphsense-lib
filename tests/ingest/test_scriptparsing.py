# -*- coding: utf-8 -*-
import pytest

from graphsenselib.ingest.utxo import (
    P2pkParserException,
    UnknownAddressType,
    UnknownScriptType,
    address_as_string,
    addresstype_to_int,
    parse_script,
)


def test_pubkey():
    pytest.importorskip("btcpy")
    scripthex = "4104a9d6840fdd1497b3067b8066db783acf90bf42071a38fe2cf6d2d8a04835d0b5c45716d8d6012ab5d56c7824c39718f7bc7486d389cd0047f53785f9a63c0c9dac"  # noqa
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "p2pk"
    assert adress_list == ["1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w"]


def test_pubkeyhash():
    pytest.importorskip("btcpy")
    scripthex = "76a91455240639b8ef1882127252c12f90354cc2fa85c088ac"
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "p2pkh"
    assert adress_list == ["18mBbKWTGAbdbP2h24Nbx5AtjWLxScJP94"]


def test_scripthash():
    pytest.importorskip("btcpy")
    scripthex = "a91475b18a0606d57fb14a607e04ff6bcd81db44639187"
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "p2sh"
    assert adress_list == ["3CRKfdBV78DD2R44SrbySmWvC9VdRRdHxQ"]


def test_multisig():
    pytest.importorskip("btcpy")
    scripthex = "514104d68bea20391d02ce6d668b1bf811f40954d9a8a1eb0c7087ddf33b5f4ad95b3c00c7542006dd4d8654f4e2975e59cc3203057f3ed8dc1a859c4d6e64dc841f0921028b6e78f41eff35d18875b5c450c62d1963792c7288e0ed40f63f258f0f633eaa52ae"  # noqa
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "multisig"
    assert adress_list == [
        "19zUcPa1ENfnSy8RMJ3AbF4TXMaZrskKKb",
        "1C3gCLqhxPb8asjtuLBe6dF5FzXRo5nANe",
    ]


def test_witness_v0_keyhash():
    pytest.importorskip("btcpy")
    scripthex = "0014f9da60876f467c88ccd5317f5fc21d124b2538c2"
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "p2wpkhv0"
    assert adress_list == ["bc1ql8dxppm0ge7g3nx4x9l4lssazf9j2wxzxq8xcx"]


def test_witness_v0_witness_v0_scripthash():
    pytest.importorskip("btcpy")
    scripthex = "0020bbb988ee4511a6edb5b16f4ea76ed3fd2766b5b60fb6299a9827bc8b9193ad51"
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "p2wshv0"
    assert adress_list == [
        "bc1qhwuc3mj9zxnwmdd3da82wmknl5nkdddkp7mznx5cy77ghyvn44gsnn4qal"
    ]


def test_nulldata():
    pytest.importorskip("btcpy")
    scripthex = (
        "6a24b9e11b6d96269d6f48b50c1c17f1ad926aa88fa60629e5ad084df1ef889901eb9cfe11b5"
    )
    adress_list, script_type = parse_script(scripthex)
    assert script_type == "nulldata"
    assert adress_list is None


def test_witness_v1_taproot_is_not_supported():
    pytest.importorskip("btcpy")
    scripthex = "5120667bdd93c7c029767fd516d2ea292624b938fefefa175ac9f1220cf508963ff3"
    with pytest.raises(UnknownScriptType):
        _, _ = parse_script(scripthex)


def test_pubkey_but_unparseable():
    pytest.importorskip("btcpy")
    # https://www.blockchain.com/btc/tx/71bbaef28e09d8d6fadd41f053db7768dbb5fa4570f06b961dfc29db3dc00b1d
    scripthex = "41617320656d62656464656420696e746f20746865207075626c6963206b65792e000000000000000000000000000000000000000000000000000000000000000000ac"  # noqa
    scripthex = "419e000000416e6f7468657220746578742077617320656d62656464656420696e746f2074686520626c6f636b20636861696e2e20546865207374616e6461726420ac"  # noqa
    with pytest.raises(P2pkParserException):
        _, _ = parse_script(scripthex)


def test_address_conversion():
    for script_type in ["witness_unknown", "nonstandard", "null", "nulldata"]:
        assert address_as_string({"type": script_type}) is None

    for addresses, script_type in [
        (["36TcL12bmahTRrd3dDLRRLQBHYp64xUdng"], "pubkey"),
        (
            [
                "36TcL12bmahTRrd3dDLRRLQBHYp64xUdng",
                "3GCsjdaiebDCBFAoiVv4odtP1dmruRBJCy",
            ],
            "multisig",
        ),
    ]:
        assert (
            address_as_string({"type": script_type, "addresses": addresses})
            == addresses
        )


def test_addresstype_conversion():
    with pytest.raises(UnknownAddressType):
        addresstype_to_int("unknown_type_string")

    assert addresstype_to_int("nonstandard") == 1

    assert addresstype_to_int("pubkey") == 2
    assert addresstype_to_int("p2pk") == 2

    assert addresstype_to_int("pubkeyhash") == 3
    assert addresstype_to_int("p2pkh") == 3

    assert addresstype_to_int("multisig_pubkey") == 4

    assert addresstype_to_int("scripthash") == 5
    assert addresstype_to_int("p2sh") == 5

    assert addresstype_to_int("multisig") == 6

    assert addresstype_to_int("null") == 7
    assert addresstype_to_int("nulldata") == 7

    assert addresstype_to_int("witness_v0_keyhash") == 8
    assert addresstype_to_int("p2wpkhv0") == 8

    assert addresstype_to_int("witness_v0_scripthash") == 9
    assert addresstype_to_int("p2wshv0") == 9

    assert addresstype_to_int("witness_unknown") == 10

    assert addresstype_to_int("witness_v1_taproot") == 11
