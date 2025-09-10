import hashlib
import math
import re
from abc import ABC, abstractmethod
from collections import Counter
from functools import reduce
from typing import Optional, Union

from bitarray import bitarray
from bitarray.util import ba2int
from cashaddress.convert import InvalidAddress as BCHInvalidAddress

from .accountmodel import (
    eth_address_to_hex,
    hex_str_to_bytes,
    hex_to_bytes,
    is_hex_string,
    strip_0x,
)
from .bch import bch_address_to_legacy, try_bch_address_to_legacy
from .tron import (
    evm_to_tron_address_string,
    tron_address_to_evm,
    tron_address_to_evm_string,
)


class InvalidAddress(Exception):
    pass


class BitCoder:
    def __init__(self, alpha: str, bit_width: int):
        assert Counter(alpha).most_common(1)[0][1] == 1, (
            f"Alphabet has duplicate {Counter(alpha).most_common(1)}"
        )
        bits = math.ceil(math.log(len(alpha) + 1, 2))
        assert bit_width == bits, "CAUTION: Bit width change, this breaks decoding"

        codewords = [(k, "{:b}".format(i + 1).zfill(bits)) for i, k in enumerate(alpha)]
        self.decode_table = {code: letter for letter, code in codewords}
        self.encode_table = {letter: bitarray(code) for letter, code in codewords}
        self.bits = bits

    @property
    def alphabet(self) -> str:
        return "".join(self.encode_table.keys())

    def encode_bitarray(self, str_data: str) -> bitarray:
        try:
            return reduce(
                lambda b, char: b + self.encode_table[char], str_data, bitarray()
            )
        except KeyError as e:
            raise InvalidAddress(f"{e} not in alphabet") from e

    def encode(self, str_data: str) -> bytes:
        return self.encode_bitarray(str_data).tobytes()

    def decode_bitarray(self, arr: bitarray) -> str:
        chunks = [arr[i : i + self.bits] for i in range(0, len(arr), self.bits)]
        try:
            return "".join(
                [
                    self.decode_table[code.to01()]
                    for code in chunks
                    if len(code) == self.bits and ba2int(code) != 0
                ]
            )
        except KeyError as e:
            raise InvalidAddress(f"{e} not in alphabet") from e

    def decode(self, byts: bytes) -> str:
        arr = bitarray()
        arr.frombytes(byts)
        return self.decode_bitarray(arr)


class Base58BitCoder(BitCoder):
    def __init__(self):
        super().__init__(
            "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz", bit_width=6
        )


class Bech32BitCoder(BitCoder):
    def __init__(self):
        # bech32 alphabet + b1 to support bc1 prefix mainnet addresses
        super().__init__("qpzry9x8gf2tvdw0s3jn54khce6mua7lb1", bit_width=6)


class Base62BitCoder(BitCoder):
    def __init__(self):
        super().__init__(
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
            bit_width=6,
        )


class AddressConverter(ABC):
    @property
    def supports_partial_address_conversion(self) -> bool:
        return False

    def to_canonical_address_str(self, address: str) -> str:
        return address

    def to_user_address_str(self, address: str) -> str:
        return address

    @abstractmethod
    def to_bytes(self, address: str) -> bytes:
        pass

    @abstractmethod
    def to_str(self, address_byte: bytes) -> str:
        pass


class AddressConverterEth(AddressConverter):
    def to_canonical_address_str(self, address: str) -> str:
        return strip_0x(address.lower())  # ty: ignore[invalid-return-type]

    def to_bytes(self, address: str) -> bytes:
        try:
            r = hex_to_bytes(self.to_canonical_address_str(address))
        except ValueError as e:
            raise InvalidAddress(f"{address} is not a valid evm address.") from e
        if r is None:
            raise InvalidAddress(f"Could not convert address {address} to bytes.")
        return r

    def to_str(self, address_byte: bytes) -> str:
        return self.to_user_address_str(address_byte.hex())


class AddressConverterTrx(AddressConverterEth):
    def to_canonical_address_str(self, address: str) -> str:
        return strip_0x(tron_address_to_evm_string(address))  # ty: ignore[invalid-return-type]

    def to_user_address_str(self, address: str):
        return evm_to_tron_address_string(address)


class AddressConverterBtcLike(AddressConverter):
    def __init__(
        self,
        bech32_prefix: Optional[str] = None,
        nonstandard_prefix: Optional[str] = None,
    ) -> None:
        self.base58codec = Base58BitCoder()
        self.base62codec = Base62BitCoder()
        if bech32_prefix is not None:
            self.bech32codec = Bech32BitCoder()
            self.bech32_prefix_bit = self.bech32codec.encode_bitarray(bech32_prefix)

            # make sure that the bech32 prefix is not a valid base58 prefix
            # https://en.bitcoin.it/wiki/List_of_address_prefixes
            assert self.base58codec.decode_bitarray(self.bech32_prefix_bit) in [
                "YCRa",
                "ZRa",
            ], (
                "if new bech prefixes are added make sure that there are no possible "
                "collisions with other addresses e.g. ZRa is the base58 "
                "version of bc1, no valid btc address starts this way so "
                "no decoding collisions are possible."
            )

            assert self.base62codec.decode_bitarray(self.bech32_prefix_bit) in [
                "VBOX",
                "WOX",
            ], (
                "Collision check for bech32 prefix against base62 failed."
                "Make sure the decoded prefix is not a valid btc address and add here"
            )

        if nonstandard_prefix is not None:
            self.nonstandard_prefix_bit = self.base62codec.encode_bitarray(
                nonstandard_prefix
            )
            assert (
                self.base58codec.decode_bitarray(self.nonstandard_prefix_bit)
                == "rsrwxdrgdvg"
            ), (
                "Collision check for nonstandard prefix against base58 failed."
                "Make sure the decoded prefix is not a valid btc address and add here"
            )

        self.bech32_prefix = bech32_prefix
        self.nonstandard_prefix = nonstandard_prefix

    @property
    def supports_bech32(self) -> bool:
        return self.bech32_prefix is not None

    def get_codec(self, address: Union[str, bytes]) -> BitCoder:
        if self.supports_bech32 and self.is_bech32(address):
            return self.bech32codec
        elif self.is_nonstandard(address):
            return self.base62codec
        else:
            return self.base58codec

    def is_bech32(self, address: Union[str, bytes]) -> bool:
        return (
            self.is_bech32_bytes(address)
            if isinstance(address, bytes)
            else self.is_bech32_str(address)
        )

    def is_nonstandard(self, address: Union[str, bytes]) -> bool:
        return (
            self.is_nonstandard_bytes(address)
            if isinstance(address, bytes)
            else self.is_nonstandard_str(address)
        )

    def is_bech32_str(self, address: str) -> bool:
        return address.startswith(self.bech32_prefix) if self.bech32_prefix else False

    def is_bech32_bytes(self, address: bytes) -> bool:
        arr = bitarray()
        arr.frombytes(address)
        return arr.find(self.bech32_prefix_bit) == 0 if self.bech32_prefix else False

    def is_nonstandard_str(self, address: str) -> bool:
        return (
            address.startswith(self.nonstandard_prefix)
            if self.nonstandard_prefix
            else False
        )

    def is_nonstandard_bytes(self, address: bytes) -> bool:
        arr = bitarray()
        arr.frombytes(address)
        return (
            arr.find(self.nonstandard_prefix_bit) == 0
            if self.nonstandard_prefix
            else False
        )

    def to_bytes(self, address: str) -> bytes:
        c = self.get_codec(address)
        return c.encode(self.to_canonical_address_str(address))

    def to_str(self, address_byte: bytes) -> str:
        c = self.get_codec(address_byte)
        return c.decode(address_byte)


class AddressConverterBch(AddressConverterBtcLike):
    def __init__(self):
        super().__init__(bech32_prefix=None, nonstandard_prefix="nonstandard")

    def to_canonical_address_str(self, address: str) -> str:
        if address.startswith(self.nonstandard_prefix):
            return address
        try:
            return bch_address_to_legacy(address)
        except BCHInvalidAddress as e:
            raise InvalidAddress(f"{address} is not a valid bch address.") from e


converters = {
    "eth": AddressConverterEth(),
    "trx": AddressConverterTrx(),
    "ltc": AddressConverterBtcLike(
        bech32_prefix="ltc1", nonstandard_prefix="nonstandard"
    ),
    "btc": AddressConverterBtcLike(
        bech32_prefix="bc1", nonstandard_prefix="nonstandard"
    ),
    "bch": AddressConverterBch(),
    "zec": AddressConverterBtcLike(
        bech32_prefix=None, nonstandard_prefix="nonstandard"
    ),
}


def address_to_bytes(network: str, address: str) -> bytes:
    c = converters.get(network.lower(), None)
    if c is not None:
        return c.to_bytes(address)
    else:
        raise ValueError("No address converter configured for network {network}")


def address_to_str(network: str, address: bytes) -> str:
    c = converters.get(network.lower(), None)
    if c is not None:
        return c.to_str(address)
    else:
        raise ValueError("No address converter configured for network {network}")


def cannonicalize_address(currency, address: str):
    if currency == "trx":
        return tron_address_to_evm(address, validate=False)
    elif currency == "bch":
        return try_bch_address_to_legacy(address)
    elif currency == "eth":
        return hex_str_to_bytes(strip_0x(address))  # ty: ignore[invalid-argument-type]
    elif isinstance(address, str):
        return address
    else:
        raise ValueError()


def address_to_user_format(currency, db_address) -> str:
    if currency == "eth":
        if isinstance(db_address, bytes):
            return eth_address_to_hex(db_address)
        else:
            return db_address.lower()
    elif currency == "trx":
        if isinstance(db_address, bytes):
            return evm_to_tron_address_string(eth_address_to_hex(db_address))
        else:
            if is_hex_string(db_address):
                return evm_to_tron_address_string(db_address)
            else:
                return db_address
    elif isinstance(db_address, str):
        return db_address
    else:
        raise Exception(f"Don't know how to decode db address, {db_address} {currency}")


def base58_check_decode(s: str) -> bytes:
    """Decode Base58Check string to bytes"""
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

    decoded = 0
    multi = 1
    for char in reversed(s):
        if char not in alphabet:
            raise ValueError(f"Invalid character '{char}' in base58 string")
        decoded += multi * alphabet.index(char)
        multi *= 58

    h = f"{decoded:x}"
    if len(h) % 2:
        h = "0" + h

    res = bytes.fromhex(h)

    # Handle leading zeros
    pad = 0
    for c in s:
        if c == alphabet[0]:
            pad += 1
        else:
            break

    return bytes([0] * pad) + res


def base58_check_validate(s: str) -> bool:
    """Validate Base58Check string"""
    try:
        decoded = base58_check_decode(s)
        if len(decoded) < 4:
            return False

        payload = decoded[:-4]
        checksum = decoded[-4:]

        hash_result = hashlib.sha256(hashlib.sha256(payload).digest()).digest()
        return hash_result[:4] == checksum
    except (ValueError, Exception):
        return False


def bech32_polymod(values):
    """Bech32 polymod function"""
    GEN = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i in range(5):
            chk ^= GEN[i] if ((b >> i) & 1) else 0
    return chk


def bech32_hrp_expand(hrp):
    """Expand human readable part"""
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]


def bech32_verify_checksum(hrp, data):
    """Verify bech32 checksum"""
    return bech32_polymod(bech32_hrp_expand(hrp) + data) == 1


def bech32_validate(s: str, expected_hrp: Optional[str] = None) -> bool:
    """Validate bech32 string"""
    if not s:
        return False

    s = s.lower()
    pos = s.rfind("1")
    if pos < 1 or pos + 7 > len(s) or pos + 1 + 6 > len(s):
        return False

    hrp = s[:pos]
    data_part = s[pos + 1 :]

    if expected_hrp and hrp != expected_hrp:
        return False

    # Check characters
    charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    for c in data_part:
        if c not in charset:
            return False

    data = [charset.find(c) for c in data_part]

    return bech32_verify_checksum(hrp, data)


def validate_btc_address(address: str) -> bool:
    """Validate Bitcoin address"""
    if not address:
        return False

    # Bech32 (segwit) addresses
    if address.lower().startswith("bc1"):
        return bech32_validate(address, "bc")
    elif address.lower().startswith("tb1"):  # testnet
        return bech32_validate(address, "tb")

    # Legacy addresses (Base58Check)
    elif address[0] in "13":  # mainnet
        return base58_check_validate(address)
    elif address[0] in "2mn":  # testnet
        return base58_check_validate(address)
    elif address[0] in "59LKc":  # private keys and extended keys
        return base58_check_validate(address)

    return False


def validate_bch_address(address: str) -> bool:
    """Validate Bitcoin Cash address"""
    if not address:
        return False

    # CashAddr format
    if ":" in address:
        try:
            from cashaddress import convert

            convert.to_legacy_address(address)
            return True
        except Exception:
            return False

    # Legacy format
    return base58_check_validate(address)


def validate_ltc_address(address: str) -> bool:
    """Validate Litecoin address"""
    if not address:
        return False

    # Bech32 (segwit) addresses
    if address.lower().startswith("ltc1"):
        return bech32_validate(address, "ltc")

    # Legacy addresses
    elif address[0] in "L3M":  # mainnet
        return base58_check_validate(address)
    elif address[0] in "2mn":  # testnet
        return base58_check_validate(address)

    return False


def validate_zec_address(address: str) -> bool:
    """Validate Zcash address"""
    if not address:
        return False

    # Zcash transparent addresses (mainnet)
    if address.startswith("t1") or address.startswith("t3"):
        return base58_check_validate(address)

    # Zcash transparent addresses (testnet)
    elif address.startswith("tm"):
        return base58_check_validate(address)

    # Shielded addresses (Sprout)
    elif address.startswith("zc") and len(address) == 95:
        return base58_check_validate(address)

    # Shielded addresses (Sapling)
    elif (address.startswith("zs") or address.startswith("ztestsapling")) and len(
        address
    ) in [78, 88]:
        return base58_check_validate(address)

    return False


def validate_eth_address(address: str) -> bool:
    """Validate Ethereum address with checksum"""
    if not address:
        return False

    # Remove 0x prefix if present
    if address.startswith("0x") or address.startswith("0X"):
        address = address[2:]

    # Check length
    if len(address) != 40:
        return False

    # Check if all characters are hex
    if not re.match(r"^[0-9a-fA-F]{40}$", address):
        return False

    # If all lowercase or all uppercase, no checksum validation needed
    if address == address.lower() or address == address.upper():
        return True

    # Validate checksum using Keccak-256
    try:
        # Try to use pycryptodome Keccak
        try:
            from Crypto.Hash import keccak

            hash_obj = keccak.new(digest_bits=256)
            hash_obj.update(address.lower().encode("utf-8"))
            hash_hex = hash_obj.hexdigest()
        except ImportError:
            # Fallback: accept if valid hex without checksum validation
            return True

        # Check each character
        for i, char in enumerate(address):
            if char.isalpha():
                # If hash digit >= 8, char should be uppercase
                if int(hash_hex[i], 16) >= 8:
                    if char != char.upper():
                        return False
                else:
                    if char != char.lower():
                        return False

        return True
    except Exception:
        # Fallback: accept if valid hex without checksum validation
        return True


def validate_trx_address(address: str) -> bool:
    """Validate TRON address"""
    if not address:
        return False

    # TRON addresses start with 'T' and are 34 characters long
    if not address.startswith("T") or len(address) != 34:
        return False

    # Validate using base58check
    return base58_check_validate(address)


def validate_address(currency: str, address: str) -> bool:
    """
    Validate cryptocurrency address for the given currency.

    Args:
        currency: Currency code (btc, bch, ltc, zec, eth, trx)
        address: Address to validate

    Returns:
        bool: True if address is valid, False otherwise
    """
    if not currency or not address:
        return False

    currency = currency.lower()

    validation_functions = {
        "btc": validate_btc_address,
        "bch": validate_bch_address,
        "ltc": validate_ltc_address,
        "zec": validate_zec_address,
        "eth": validate_eth_address,
        "trx": validate_trx_address,
    }

    validator = validation_functions.get(currency)
    if not validator:
        raise ValueError(f"Unsupported currency: {currency}")

    try:
        return validator(address)
    except Exception:
        return False
