# flake8: noqa: F501
import random
from functools import reduce
import pytest

from graphsenselib.utils.address import (
    InvalidAddress,
    address_to_bytes,
    address_to_str,
    address_to_user_format,
    cannonicalize_address,
    validate_address,
    validate_btc_address,
    validate_bch_address,
    validate_ltc_address,
    validate_zec_address,
    validate_eth_address,
    validate_trx_address,
)

from . import resources
import importlib.resources

testset_correct = [
    (
        "trx",
        "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu",
        None,
        "4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
    ),
    (
        "TRX",
        "TWsm8HtU2A5eEzoT8ev8yaoFjHsXLLrckb",
        None,
        "e552f6487585c2b58bc2c9bb4492bc1f17132cd0",
    ),
    (
        "trx",
        "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnat",
        None,
        "5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
    ),
    (
        "eth",
        "4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
        None,
        "4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
    ),
    (
        "eth",
        "e552f6487585c2b58bc2c9bb4492bc1f17132cd0",
        None,
        "e552f6487585c2b58bc2c9bb4492bc1f17132cd0",
    ),
    (
        "eth",
        "5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
        None,
        "5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
    ),
    (
        "eth",
        "0x4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
        "4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
        "4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
    ),
    (
        "eth",
        "0xe552f6487585c2b58bc2c9bb4492bc1f17132cd0",
        "e552f6487585c2b58bc2c9bb4492bc1f17132cd0",
        "e552f6487585c2b58bc2c9bb4492bc1f17132cd0",
    ),
    (
        "eth",
        "0x5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
        "5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
        "5095d4f4d26ebc672ca12fc0e3a48d6ce3b169d2",
    ),
    (
        "eth",
        "0xFF4c1897369C0DFa37e3442BB08c810A33214349",
        "FF4c1897369C0DFa37e3442BB08c810A33214349".lower(),
        "ff4c1897369c0dfa37e3442bb08c810a33214349",
    ),
    (
        "bch",
        "bitcoincash:pvhfadjrmc4adxl8payajshf34wauh4jlrfnpuyetfnzh5llrnh9w3q8y9qcp",
        "B5Fj9EniQGXUKCCx1dfm7XXARMf38wBgtjPTQv9uaR24eTrj3T",
        "2c53eb24ebaa6107dc4cc3380659ed1df7ca6559c32372e8d2b5db63627589908499bcab0db0",
    ),
    (
        "bch",
        "bitcoincash:pvqljqfuvj0dqf4l6s9mradymsvmucwe8w7cm84yyt8p2dtwj93xcuqwaelt6",
        "AjbGjygbRL7MwFc3PJ9KWVqMdUGeHsFxXDaiGzyTqQnBKP4iWo",
        "2ab8d0af9a236541d5dcf9035d225379dc5595c4264733f87cd8aa43ae5bc58b8b4d712a7af0",
    ),
    (
        "bch",
        "bitcoincash:pv4dcn3jppealt8y9t6egv4aj8snhtt4qqz006u27dezdwj65xkxuqaqmccfz",
        "B3bhUDrC913vH9rRaxnhToG4fi1hp8B55Sg1RHGE4AdCRoti1o",
        "2c38e970dc8c2410f6449c998b8ba96ef4049ea069c082c515aa0165140e10a94c66fd2a06f0",
    ),
    (
        "bch",
        "B5Fj9EniQGXUKCCx1dfm7XXARMf38wBgtjPTQv9uaR24eTrj3T",
        None,
        "2c53eb24ebaa6107dc4cc3380659ed1df7ca6559c32372e8d2b5db63627589908499bcab0db0",
    ),
    (
        "bch",
        "AjbGjygbRL7MwFc3PJ9KWVqMdUGeHsFxXDaiGzyTqQnBKP4iWo",
        None,
        "2ab8d0af9a236541d5dcf9035d225379dc5595c4264733f87cd8aa43ae5bc58b8b4d712a7af0",
    ),
    (
        "bch",
        "B3bhUDrC913vH9rRaxnhToG4fi1hp8B55Sg1RHGE4AdCRoti1o",
        None,
        "2c38e970dc8c2410f6449c998b8ba96ef4049ea069c082c515aa0165140e10a94c66fd2a06f0",
    ),
    (
        "btc",
        "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",
        None,
        "0563456d808ab988b4e31b51ca555b235d230e27a8513ce42770",
    ),
    (
        "btc",
        "18mBbKWTGAbdbP2h24Nbx5AtjWLxScJP94",
        None,
        "048b4b8d379b40a8e58d70a90845a3e052b4ade5386a44972440",
    ),
    (
        "btc",
        "19zUcPa1ENfnSy8RMJ3AbF4TXMaZrskKKb",
        None,
        "049e9c9178813969ee6b92195520ca8cf11b7d58a1cb3b134e30",
    ),
    (
        "btc",
        "1C3gCLqhxPb8asjtuLBe6dF5FzXRo5nANe",
        None,
        "04c0e8314c69e178c88b3af4d542e61a53c53fa7d9bc5b8a5a60",
    ),
    (
        "btc",
        "bc1ql8dxppm0ge7g3nx4x9l4lssazf9j2wxzxq8xcx",
        None,
        "85988180838708271025a7c94941d61c681681145e0ca1932cf1c31c12076470",
    ),
    (
        "btc",
        "36TcL12bmahTRrd3dDLRRLQBHYp64xUdng",
        None,
        "0c66e45010a3b62a5b67294394d51965460b460c06138725ba80",
    ),
    (
        "btc",
        "3GCsjdaiebDCBFAoiVv4odtP1dmruRBJCy",
        None,
        "0d0333ae58aa9a334c2cf2afa9dd84be5d17065b72d592d23390",
    ),
    (
        "ltc",
        "LiM295pXpydyt4ahLxEYJeevpLtm2e6LSv",
        None,
        "52a542245c1fc39979d048a95383a04a69b6c14d2d0a61946b60",
    ),
    (
        "ltc",
        "ltc1qq4zfq0p9s5e69y4vp0kx5gvjvpku8zz7c6q9ga",
        None,
        "80c66204158328140219155a6c61563424171d524d4cd0977480c37d96c1189780",
    ),
    (
        "ltc",
        "ltc1qmrf5gsptkp2xsunvxc57txjhr53w4d5003smpe",
        None,
        "80c66205c10a54944231708b1d17543476557cc1d36045523d6395410491702680",
    ),
    (
        "btc",
        "17VZNX1SN5NtKa8UQFxwQbFeFc3iqRYhem",
        None,
        "04776159f05a5855b44e221c60fe376233e63e40eac598299ad0",
    ),
    (
        "btc",
        "3EktnHQD7RiAE6uzMj2ZifT9YgRrkSgzQX",
        None,
        "0ceb34b9160d1d9a8a386d7a56b0a1aa76c9828672b1aa3a61f0",
    ),
    (
        "btc",
        "5Hwgr3u458GLafKBgxtssHSPqJnYoGrSzgQsPwLFhLNYskDPyyA",
        None,
        "151de8c83d441484148a74cba38d33cd1697c52ba0bd0c9aea86335f750fa545a0cec357e79280",
    ),  # Private key (WIF, uncompressed pubkey)
    (
        "btc",
        "5TfQjD9DLFeUFmDiDrzsdtSGQss93o4pvsmQcgmjfcQVLsEgAoM",
        None,
        "15b9d8acd24d50f99c3ed36a372eb3974690633cc90ef130db3b58928b6b9e461d5333a82af540",
    ),  # Private key (WIF, uncompressed pubkey, Electrum-defined and now deprecated)
    (
        "btc",
        "L1aW4aubDFB7yfras2S1mN3bqg9nwySY8nkoLmJebSLD5BWv3ENZ",
        None,
        "50189e122d6334f2c7e67ca2cc2681b560e3c6826edf96a022eb2f52d4a68da50d14b7b60ce5a1",
    ),  # Private key (WIF, compressed pubkey)
    (
        "btc",
        "xpub661MyMwAqRbcEYS8w7XLSVeEsBXy79zSzH1J8vCdxAZningWLdN3",
        None,
        "e30d63186055e55dcac598e43a0688dc77d469d98eccb7f91c9e9ae91052236325e0a86eaaea1e525583",
    ),  # BIP32 pubkey
    (
        "btc",
        "xprv9s21ZrQH143K24Mfq5zL5MhWK9hUhhGbd45hLXo2Pq2oqzMMo63o",
        None,
        "e30cb62730818726110440d3084567c45e94155a5e4c9a5ca6942394416951fbc25f10afc7a555bc60ef",
    ),  # BIP32 private key
    (
        "btc",
        "cNJFgo1driFnPcBdBX8BrJrpxchBWXwXCvNH5SoSkdcF6JXXwHMm",
        None,
        "91648fa2f065caa3ee5e42e52df20bc92cb0e24a4b79fddf33659115abdab2590f1927dfdd156d",
    ),  # Testnet Private key (WIF, compressed pubkey)
    (
        "btc",
        "tpubD6NzVbkrYhZ4WLczPJWReQycCJdd6YVWXubbVUFnJ5KgU5MDQrD9",
        None,
        "d30d633465ba763b3282984479493a5d2799998e643129651a075e7f58e375c3ee4854e870554d632349",
    ),  # Testnet BIP32 pubkey
    (
        "btc",
        "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        None,
        "8598813d540839b05a4c730e2561551120de10d7841506551cf7d73482923160",
    ),  # Bech32 pubkey hash or script hash
    (
        "btc",
        "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx",
        None,
        "8598813d540839b05a4c730e2561551120de10d7841506551cf7d71c24c34470",
    ),  # Bech32 pubkey hash or script hash
    (
        "btc",
        "nonstandard08348844dbe5a6ef742b5e64407d8462b3c472b0",
        None,
        "cb3cb7e25ca8976a012441492451689a91a51e9a88143986a47145"
        "048a091470e61271480e6040",  # nonstandard address
    ),
]


def test_address_converter():
    compression_btclike = []
    for n, a, ra, ob in testset_correct:
        b = address_to_bytes(n, a)
        c = address_to_str(n, b)
        if ra is None:
            assert a == c
        else:
            assert ra == c
        assert b.hex() == ob

        if n != "trx" and n != "eth":
            compression_btclike.append(len(b) / len(a.encode("utf-8")))

    avg_compression_btclike = sum(compression_btclike) / len(compression_btclike)

    assert avg_compression_btclike < 0.75


def test_address_converter_inject_nonencodable_char():
    rpmts = [
        ("0", "!"),
        ("1", "!"),
        ("V", "!"),
        ("a", "!"),
        ("5", "!"),
        ("j", "!"),
        ("q", "!"),
    ]

    for n, a, ra, ob in testset_correct:
        ac = reduce(lambda a, kv: a.replace(*kv), rpmts, a)
        with pytest.raises(InvalidAddress):
            address_to_bytes(n, ac)


def test_address_converter_testset():
    tests = 0
    pct = 0.8
    networks = ["bch", "btc", "ltc", "zec"]
    addresses_per_network = 100000
    pct_error = len(networks) * addresses_per_network * (0.01)
    e_tests = len(networks) * addresses_per_network * (1 - pct) - pct_error

    for network in networks:
        resource_package = resources
        resource_name = f"{network}_addresses.txt"
        # Use files() and open_text() for modern resource access
        with (
            importlib.resources.files(resource_package)
            .joinpath(resource_name)
            .open("r") as f
        ):
            for a in f:
                if random.uniform(0, 1) > pct:
                    a = a.strip()
                    b = address_to_bytes(network, a)
                    c = address_to_str(network, b)
                    assert a == c
                    tests += 1

    assert tests > e_tests


def test_address_conversions():
    addr = "TAzsQ9Gx8eqFNFSKbeXrbi45CuVPHzA8wr"

    assert (
        cannonicalize_address("trx", addr).hex()
        == "0b48984414cc0c6a8e599fb6e3bc11e599de2e24"
    )

    assert address_to_user_format("trx", cannonicalize_address("trx", addr)) == addr

    assert address_to_user_format("trx", addr) == addr

    eaddr = "0x0b48984414cc0c6a8e599fb6e3bc11e599de2e24"

    assert address_to_user_format("eth", cannonicalize_address("eth", eaddr)) == eaddr

    assert (
        cannonicalize_address("eth", eaddr).hex()
        == "0b48984414cc0c6a8e599fb6e3bc11e599de2e24"
    )

    with pytest.raises(ValueError):
        cannonicalize_address("eth", addr).hex()


class TestAddressValidation:
    """Test cryptocurrency address validation functions"""

    def test_validate_btc_addresses(self):
        """Test Bitcoin address validation"""
        # Valid Bitcoin addresses
        valid_btc = [
            "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",
            "18mBbKWTGAbdbP2h24Nbx5AtjWLxScJP94",
            "bc1ql8dxppm0ge7g3nx4x9l4lssazf9j2wxzxq8xcx",
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
            "36TcL12bmahTRrd3dDLRRLQBHYp64xUdng",
            "3GCsjdaiebDCBFAoiVv4odtP1dmruRBJCy",
        ]

        for addr in valid_btc:
            assert validate_btc_address(addr), (
                f"Valid BTC address {addr} should pass validation"
            )
            assert validate_address("btc", addr), (
                f"Valid BTC address {addr} should pass validation"
            )

        # Invalid Bitcoin addresses
        invalid_btc = [
            "",
            "invalid",
            "1InvalidAddress",
            "bc1invalid",
            "0x1234567890abcdef1234567890abcdef12345678",
        ]

        for addr in invalid_btc:
            assert not validate_btc_address(addr), (
                f"Invalid BTC address {addr} should fail validation"
            )
            assert not validate_address("btc", addr), (
                f"Invalid BTC address {addr} should fail validation"
            )

    def test_validate_bch_addresses(self):
        """Test Bitcoin Cash address validation"""
        # Valid BCH addresses
        valid_bch = [
            "bitcoincash:pvhfadjrmc4adxl8payajshf34wauh4jlrfnpuyetfnzh5llrnh9w3q8y9qcp",
            "B5Fj9EniQGXUKCCx1dfm7XXARMf38wBgtjPTQv9uaR24eTrj3T",
            "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",  # Legacy format
        ]

        for addr in valid_bch:
            assert validate_bch_address(addr), (
                f"Valid BCH address {addr} should pass validation"
            )
            assert validate_address("bch", addr), (
                f"Valid BCH address {addr} should pass validation"
            )

        # Invalid BCH addresses
        invalid_bch = [
            "",
            "invalid",
            "bitcoincash:invalid",
            "0x1234567890abcdef1234567890abcdef12345678",
        ]

        for addr in invalid_bch:
            assert not validate_bch_address(addr), (
                f"Invalid BCH address {addr} should fail validation"
            )
            assert not validate_address("bch", addr), (
                f"Invalid BCH address {addr} should fail validation"
            )

    def test_validate_ltc_addresses(self):
        """Test Litecoin address validation"""
        # Valid LTC addresses
        valid_ltc = [
            "LiM295pXpydyt4ahLxEYJeevpLtm2e6LSv",
            "ltc1qq4zfq0p9s5e69y4vp0kx5gvjvpku8zz7c6q9ga",
            "ltc1qmrf5gsptkp2xsunvxc57txjhr53w4d5003smpe",
            "3GCsjdaiebDCBFAoiVv4odtP1dmruRBJCy",
        ]

        for addr in valid_ltc:
            assert validate_ltc_address(addr), (
                f"Valid LTC address {addr} should pass validation"
            )
            assert validate_address("ltc", addr), (
                f"Valid LTC address {addr} should pass validation"
            )

        # Invalid LTC addresses
        invalid_ltc = [
            "",
            "invalid",
            "ltc1invalid",
            "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",  # BTC address
            "0x1234567890abcdef1234567890abcdef12345678",
        ]

        for addr in invalid_ltc:
            assert not validate_ltc_address(addr), (
                f"Invalid LTC address {addr} should fail validation"
            )
            assert not validate_address("ltc", addr), (
                f"Invalid LTC address {addr} should fail validation"
            )

    def test_validate_zec_addresses(self):
        """Test Zcash address validation"""
        # Valid ZEC addresses (transparent)
        valid_zec = [
            "t1LEhsLv5W5USqfzyvJrAiRFV9qbehtBYKP",
            "t1UCM8GLR6ysUsgG2SbWR1on1kyHYAy71Eb",
            "t3UeqMiZA7z1BdQNsKNWh1iUgSa2KmoceJA",
        ]

        for addr in valid_zec:
            assert validate_zec_address(addr), (
                f"Valid ZEC address {addr} should pass validation"
            )
            assert validate_address("zec", addr), (
                f"Valid ZEC address {addr} should pass validation"
            )

        # Invalid ZEC addresses
        invalid_zec = [
            "",
            "invalid",
            "z1invalid",
            "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",  # BTC address
            "0x1234567890abcdef1234567890abcdef12345678",
        ]

        for addr in invalid_zec:
            assert not validate_zec_address(addr), (
                f"Invalid ZEC address {addr} should fail validation"
            )
            assert not validate_address("zec", addr), (
                f"Invalid ZEC address {addr} should fail validation"
            )

    def test_validate_eth_addresses(self):
        """Test Ethereum address validation"""
        # Valid ETH addresses
        valid_eth = [
            "0x4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
            "4450cf8c8b6a8229b7f628e36b3a658e84441b6f",
            "0xFF4c1897369C0DFa37e3442BB08c810A33214349",
            "0xff4c1897369c0dfa37e3442bb08c810a33214349",
            "0XFF4C1897369C0DFA37E3442BB08C810A33214349",
        ]

        for addr in valid_eth:
            assert validate_eth_address(addr), (
                f"Valid ETH address {addr} should pass validation"
            )
            assert validate_address("eth", addr), (
                f"Valid ETH address {addr} should pass validation"
            )

        # Invalid ETH addresses
        invalid_eth = [
            "",
            "invalid",
            "0x123",  # Too short
            "0x4450cf8c8b6a8229b7f628e36b3a658e84441b6f1",  # Too long
            "0x4450cf8c8b6a8229b7f628e36b3a658e84441b6g",  # Invalid hex
            "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",  # BTC address
        ]

        for addr in invalid_eth:
            assert not validate_eth_address(addr), (
                f"Invalid ETH address {addr} should fail validation"
            )
            assert not validate_address("eth", addr), (
                f"Invalid ETH address {addr} should fail validation"
            )

    def test_validate_trx_addresses(self):
        """Test TRON address validation"""
        # Valid TRX addresses
        valid_trx = [
            "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu",
            "TWsm8HtU2A5eEzoT8ev8yaoFjHsXLLrckb",
            "THKJYuUmMKKARNf7s2VT51g5uPY6KEqnat",
            "TAzsQ9Gx8eqFNFSKbeXrbi45CuVPHzA8wr",
        ]

        for addr in valid_trx:
            assert validate_trx_address(addr), (
                f"Valid TRX address {addr} should pass validation"
            )
            assert validate_address("trx", addr), (
                f"Valid TRX address {addr} should pass validation"
            )

        # Invalid TRX addresses
        invalid_trx = [
            "",
            "invalid",
            "T123",  # Too short
            "TGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu1",  # Too long
            "AGCRkw1Vq759FBCrwxkZGgqZbRX1WkBHSu",  # Wrong prefix
            "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w",  # BTC address
            "0x4450cf8c8b6a8229b7f628e36b3a658e84441b6f",  # ETH address
        ]

        for addr in invalid_trx:
            assert not validate_trx_address(addr), (
                f"Invalid TRX address {addr} should fail validation"
            )
            assert not validate_address("trx", addr), (
                f"Invalid TRX address {addr} should fail validation"
            )

    def test_validate_address_unsupported_currency(self):
        """Test validation with unsupported currency"""
        with pytest.raises(ValueError, match="Unsupported currency"):
            validate_address("unsupported", "some_address")

    def test_validate_address_empty_inputs(self):
        """Test validation with empty inputs"""
        assert not validate_address("", "some_address")
        assert not validate_address("btc", "")
        assert not validate_address("", "")

    def test_validate_address_case_insensitive_currency(self):
        """Test validation with different currency case"""
        address = "1ND5TQ2AnQatxqmHrdMT8utb3aWgLKsc9w"
        assert validate_address("BTC", address)
        assert validate_address("btc", address)
        assert validate_address("Btc", address)
