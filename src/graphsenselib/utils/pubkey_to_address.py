#!/usr/bin/env python
# coding: utf-8
""" """

from typing import Dict, List
import base58
import bech32
from binascii import hexlify, unhexlify
import hashlib

from coincurve.keys import PublicKey
from eth_keys import keys

from graphsenselib.utils.tron import evm_to_tron_address_string


# --- Helper Functions ---
def hash160(public_key_bytes):
    """Hashes public key bytes using SHA-256 then RIPEMD-160."""
    sha256_hash = hashlib.sha256(public_key_bytes).digest()
    ripemd160_hash = hashlib.new("ripemd160", sha256_hash).digest()
    return ripemd160_hash


def double_sha256(data_bytes):
    """Hashes data bytes using SHA-256 twice."""
    return hashlib.sha256(hashlib.sha256(data_bytes).digest()).digest()


def base58check_encode(prefix_bytes, data_bytes):
    """Encodes data with a prefix using Base58Check."""
    prefixed_data = prefix_bytes + data_bytes
    checksum = double_sha256(prefixed_data)[:4]
    return base58.b58encode(prefixed_data + checksum).decode("utf-8")


def hex_to_bytes(hex_str: str) -> bytes:
    """Converts a hexadecimal string to bytes."""
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    try:
        return bytes.fromhex(hex_str)
    except ValueError:
        raise ValueError("Invalid hexadecimal string provided.")


# --- Bech32 Conversion Helper ---
def convertbits(data, frombits, tobits, pad=True):
    """General power-of-2 base conversion."""
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            return None  # Invalid input
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits > 0:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None  # Invalid padding
    return ret


def bech32_segwit_encode(hrp, witver, witprog):
    """Encodes a SegWit address using Bech32."""
    if not (0 <= witver <= 16):
        return "Invalid witness version"
    if witver == 0:
        if len(witprog) != 20 and len(witprog) != 32:
            return "Invalid witness program length for version 0"
    # Convert 8-bit witness program to 5-bit integers
    data_5bit = convertbits(list(witprog), 8, 5)
    if data_5bit is None:
        return "Error converting bits for Bech32"

    # Prepend witness version (as a 5-bit integer) to the data
    payload = [witver] + data_5bit

    # Use the standard bech32 encode function
    try:
        return bech32.bech32_encode(hrp, payload)
    except Exception as e:
        return f"Bech32 encoding failed: {e}"


# --- Mainnet Address Version Bytes and HRPs ---
MAINNET_ADDRESS_SPECS = {
    "bitcoin": {
        "p2pkh": hex_to_bytes("00"),
        "p2sh": hex_to_bytes("05"),  # used for P2SH-P2WPKH or other script hashes
        "p2wpkh_hrp": "bc",
    },
    "dogecoin": {
        "p2pkh": hex_to_bytes("1e"),  # D prefix
        "p2sh": hex_to_bytes("16"),  # A or 9 prefix (for script hashes)
    },
    "litecoin": {
        "p2pkh": hex_to_bytes("30"),  # L or K prefix
        "p2sh": hex_to_bytes("32"),  # 3 prefix (for script hashes)
        "p2wpkh_hrp": "ltc",
    },
    "zcash": {  # Zcash t-addresses only
        "t1_p2pkh": hex_to_bytes("1cb8"),  # t1 prefix
        "t3_p2sh": hex_to_bytes("1cbd"),  # t3 prefix (for script hashes)
    },
}


# --- Address Conversion Functions ---
def get_bitcoin_addresses(pubkey_hex):
    """
    Converts a Bitcoin public key hex string to mainnet address formats.
    Handles both compressed and uncompressed public keys for P2PKH.
    Note: P2WPKH and P2SH-P2WPKH standards use the hash of the compressed public key.
    """
    pubkey_hash = hash160(hex_to_bytes(pubkey_hex))
    pubkey_hash_uncomp = hash160(hex_to_bytes(uncompress_public_key(pubkey_hex)))

    addresses = {}
    specs = MAINNET_ADDRESS_SPECS["bitcoin"]

    # P2PKH Address (derived from hash160 of the public key)
    # Can be generated from compressed or uncompressed public key, resulting in different addresses.
    addresses["p2pkh"] = base58check_encode(specs["p2pkh"], pubkey_hash)
    addresses["p2pkh_uncomp"] = base58check_encode(specs["p2pkh"], pubkey_hash_uncomp)

    # P2WPKH (Native SegWit) Address (derived from hash160 of the *compressed* public key)
    # Witness version 0, witness program is the pubkey_hash (20 bytes)
    addresses["p2wpkh_bech32"] = bech32_segwit_encode(
        specs["p2wpkh_hrp"], 0, pubkey_hash
    )

    # P2SH-P2WPKH (Nested SegWit) Address
    # The redeem script for P2SH-P2WPKH is OP_0 (0x00) followed by the 20-byte public key hash.
    # The address is the Base58Check encoding of the P2SH prefix + hash160 of this redeem script.
    redeem_script = hex_to_bytes("0014") + pubkey_hash
    redeem_script_hash = hash160(redeem_script)
    addresses["p2sh_p2wpkh"] = base58check_encode(specs["p2sh"], redeem_script_hash)

    return addresses


def get_dogecoin_addresses(pubkey_hex):
    """
    Converts a Dogecoin public key hex string to mainnet address formats.
    Dogecoin does not natively support SegWit (P2WPKH or P2SH-P2WPKH).
    P2SH addresses are derived from a script hash, not directly a single public key hash.
    """
    pubkey_hash = hash160(hex_to_bytes(pubkey_hex))
    pubkey_hash_uncomp = hash160(hex_to_bytes(uncompress_public_key(pubkey_hex)))

    addresses = {}
    specs = MAINNET_ADDRESS_SPECS["dogecoin"]

    # P2PKH Address (derived from hash160 of the public key)
    addresses["p2pkh"] = base58check_encode(specs["p2pkh"], pubkey_hash)
    addresses["p2pkh_uncomp"] = base58check_encode(specs["p2pkh"], pubkey_hash_uncomp)

    # P2SH Address (derived from a script hash, not directly a single pubkey hash)
    # For demonstration purposes, we'll show the format using the pubkey_hash
    # as the data to be encoded, although typically this would be the hash160 of a redeem script.
    addresses["p2sh"] = base58check_encode(specs["p2sh"], pubkey_hash)

    return addresses


def get_litecoin_addresses(pubkey_hex):
    """
    Converts a Litecoin public key hex string to mainnet address formats.
    Handles both compressed and uncompressed public keys for P2PKH.
    Note: P2WPKH and P2SH-P2WPKH standards use the hash of the compressed public key.
    """
    pubkey_hash = hash160(hex_to_bytes(pubkey_hex))
    pubkey_hash_uncomp = hash160(hex_to_bytes(uncompress_public_key(pubkey_hex)))

    addresses = {}
    specs = MAINNET_ADDRESS_SPECS["litecoin"]

    # P2PKH Address (derived from hash160 of the public key)
    addresses["p2pkh"] = base58check_encode(specs["p2pkh"], pubkey_hash)
    addresses["p2pkh_uncomp"] = base58check_encode(specs["p2pkh"], pubkey_hash_uncomp)

    # P2WPKH (Native SegWit) Address (derived from hash160 of the *compressed* public key)
    addresses["p2wpkh_bech32"] = bech32_segwit_encode(
        specs["p2wpkh_hrp"], 0, pubkey_hash
    )

    # P2SH-P2WPKH (Nested SegWit) Address
    # The redeem script for P2SH-P2WPKH is OP_0 (0x00) followed by the 20-byte public key hash.
    # The address is the Base58Check encoding of the P2SH prefix + hash160 of this redeem script.
    redeem_script = hex_to_bytes("0014") + pubkey_hash
    redeem_script_hash = hash160(redeem_script)
    addresses["p2sh_p2wpkh"] = base58check_encode(specs["p2sh"], redeem_script_hash)

    return addresses


def get_zcash_addresses(pubkey_hex):
    """
    Converts a Zcash public key hex string to mainnet t-address formats.
    Zcash z-addresses (Sapling, Orchard) are not derived from a public key in this manner.
    T-addresses are analogous to Bitcoin's P2PKH and P2SH.
    """
    pubkey_hash = hash160(hex_to_bytes(pubkey_hex))
    # pubkey_hash_uncomp = hash160(
    #    hex_to_bytes(uncompress_public_key(pubkey_hex))
    # )

    addresses = {}
    specs = MAINNET_ADDRESS_SPECS["zcash"]

    # t1 (P2PKH) Address (derived from hash160 of the public key)
    # Zcash t1 addresses use a 2-byte prefix.
    addresses["t1_p2pkh"] = base58check_encode(specs["t1_p2pkh"], pubkey_hash)
    # addresses["t1_p2pkh_uncomp"] = base58check_encode(
    #    specs["p2pkh"], pubkey_hash_uncomp
    # )

    # t3 (P2SH) Address (derived from a script hash, not directly a single pubkey hash)
    # Zcash t3 addresses use a 2-byte prefix.
    # For demonstration, we'll show the format using the pubkey_hash
    # as the data to be encoded, although typically this would be the hash160 of a redeem script.
    addresses["t3_p2sh"] = base58check_encode(specs["t3_p2sh"], pubkey_hash)

    return addresses


def get_ethereum_address(pubkey_comp_hex: str) -> str:
    """
    Derives an Ethereum address from a compressed public key.

    Args:
        pubkey_comp_hex: The hexadecimal representation of the compressed
                         public key

    Returns:
        str: The Ethereum address with '0x' prefix.
    """

    if pubkey_comp_hex.startswith("0x"):
        pubkey_comp_hex = pubkey_comp_hex[2:]  # remove '0x' prefix

    eth_addr = keys.PublicKey.from_compressed_bytes(
        hex_to_bytes(pubkey_comp_hex)
    ).to_address()

    return eth_addr


def get_tron_address(pubkey_comp_hex: str) -> str:
    """
    Derives a Tron address from a compressed public key.

    Args:
        pubkey_comp_hex: The hexadecimal representation of the compressed
                         public key

    Returns:
        str: The Tron address with '41' prefix.
    """
    eth = get_ethereum_address(pubkey_comp_hex)  # Ensure the public key is valid
    return evm_to_tron_address_string(eth)


# --- Main Conversion Function ---
def convert_pubkey_to_addresses(
    pubkey_hex: str, currencies: List[str] = ["btc", "doge", "ltc", "zec", "eth", "trx"]
) -> Dict[str, Dict[str, str]]:
    """
    Converts a public key hexadecimal string to various mainnet address formats
    for specified cryptocurrencies.

    Args:
        pubkey_hex (str): The public key as a hexadecimal string.
                          Can be compressed (66 chars) or uncompressed (130 chars).
        currencies (list): A list of currency names (e.g., ["btc", "ltc"]).

    Returns:
        dict: A dictionary where keys are currency names and values are
              dictionaries of address formats and their corresponding addresses.
              Includes error messages if a currency is invalid.
    """
    all_addresses = {}
    valid_currencies = ["btc", "doge", "ltc", "zec", "eth", "trx", "bch"]

    if not isinstance(pubkey_hex, str) or not pubkey_hex:
        raise ValueError("Invalid public key hex string provided.")

    # validate public key length (basic check)
    if len(pubkey_hex) not in [66, 130]:
        raise ValueError(
            "Expected key length 66 (compressed) or 130 (uncompressed) characters."
        )
    elif len(pubkey_hex) == 130:
        pubkey_hex_comp = compress_public_key(pubkey_hex)
    else:
        pubkey_hex_comp = pubkey_hex

    for currency in currencies:
        if currency not in valid_currencies:
            raise ValueError(f"Unsupported currency: {currency}")

        try:
            if currency == "btc":
                all_addresses["btc"] = get_bitcoin_addresses(pubkey_hex_comp)
            elif currency == "doge":
                all_addresses["doge"] = get_dogecoin_addresses(pubkey_hex_comp)
            elif currency == "ltc":
                all_addresses["ltc"] = get_litecoin_addresses(pubkey_hex_comp)
            elif currency == "zec":
                all_addresses["zec"] = get_zcash_addresses(pubkey_hex_comp)
            elif currency == "eth":
                all_addresses["eth"] = {"eth": get_ethereum_address(pubkey_hex_comp)}
            elif currency == "trx":
                all_addresses["trx"] = {"trx": get_tron_address(pubkey_hex_comp)}

        except ValueError as e:
            all_addresses[currency] = {
                "error": f"Error processing pubkey for {currency}: {e}"
            }
        except Exception as e:
            all_addresses[currency] = {
                "error": f"An unexpected error occurred for {currency}: {e}"
            }

    return all_addresses


def compress_public_key(uncompressed_pubkey_hex: str) -> str:
    """
    Converts an uncompressed public key hexadecimal string to a compressed one.

    Args:
        uncompressed_pubkey_hex (str): The uncompressed public key hex string (130 characters, starts with '04').

    Returns:
        str: The compressed public key hex string (66 characters, starts with '02' or '03').
        None: If the input is not a valid uncompressed public key hex.
    """
    pubkey_bytes = unhexlify(uncompressed_pubkey_hex)

    # validate if it's an uncompressed key
    # (starts with 0x04 and has correct length)
    if len(pubkey_bytes) != 65 or pubkey_bytes[0] != 0x04:
        raise ValueError("Error: Input is not a valid uncompressed public key hex.")

    public_key = PublicKey(pubkey_bytes)
    compressed_pubkey_bytes = public_key.format(compressed=True)
    return hexlify(compressed_pubkey_bytes).decode("utf-8")


def uncompress_public_key(compressed_pubkey_hex: str) -> str:
    """
    Converts a compressed public key hexadecimal string to an uncompressed one.

    Args:
        compressed_pubkey_hex (str): The compressed public key hex string (66 characters, starts with '02' or '03').

    Returns:
        str: The uncompressed public key hex string (130 characters, starts with '04').
        None: If the input is not a valid compressed public key hex.
    """

    pubkey_bytes = unhexlify(compressed_pubkey_hex)

    # validate if it's a compressed key
    # (starts with 0x02 or 0x03 and has correct length)
    if len(pubkey_bytes) != 33 or (pubkey_bytes[0] != 0x02 and pubkey_bytes[0] != 0x03):
        raise ValueError("Error: Input is not a valid compressed public key hex.")

    public_key = PublicKey(pubkey_bytes)
    uncompressed_pubkey_bytes = public_key.format(compressed=False)
    return hexlify(uncompressed_pubkey_bytes).decode("utf-8")
