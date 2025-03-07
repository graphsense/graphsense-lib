import math
from abc import ABC, abstractmethod
from collections import Counter
from functools import reduce
from typing import Optional, Union

from bitarray import bitarray
from bitarray.util import ba2int
from cashaddress.convert import InvalidAddress as BCHInvalidAddress

from .accountmodel import hex_to_bytes, strip_0x
from .bch import bch_address_to_legacy
from .tron import evm_to_tron_address_string, tron_address_to_evm_string


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
        return strip_0x(address.lower())

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
        return strip_0x(tron_address_to_evm_string(address))

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
