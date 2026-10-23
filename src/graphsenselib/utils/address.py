import hashlib
import itertools
import math
import re
from abc import ABC, abstractmethod
from collections import Counter
from functools import reduce
from typing import Callable, Iterator, Optional, Union, List

import base58
from bitarray import bitarray
from bitarray.util import ba2int
from graphsenselib.utils.bch import InvalidAddress as BCHInvalidAddress

from .accountmodel import (
    eth_address_to_hex,
    hex_str_to_bytes,
    hex_to_bytes,
    is_hex_string,
    strip_0x,
)
from .bch import (
    bch_address_to_legacy,
    checked_bch_address_to_legacy,
    try_bch_address_to_legacy,
)
from .tron import (
    evm_to_tron_address_string,
    tron_address_to_evm,
    tron_address_to_evm_string,
)


class InvalidAddress(ValueError):
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
        if address.startswith(self.nonstandard_prefix):  # ty: ignore[invalid-argument-type]
            return address
        try:
            return bch_address_to_legacy(address)
        except BCHInvalidAddress as e:
            raise InvalidAddress(f"{address} is not a valid bch address.") from e


class AddressConverterBchWithNonstandardFallback(AddressConverterBch):
    # Specialized BCH converter that falls back to nonstandard prefix for
    # addresses that cannot be converted to legacy format
    # E.g. bc1 addresses that are valid BTC addresses but not valid BCH addresses
    # happend in tx f39592c35da4260b06baa47f62a181fe95b3d7b45b5205879552b4b22c852abf

    def __init__(
        self,
        non_standard_fallback_prefixes: List[str] = [],
        nonstandard_prefix: str = "nonstandard",
    ):
        self.non_standard_fallback_prefixes = non_standard_fallback_prefixes
        self.nonstandard_prefix = nonstandard_prefix
        super().__init__()

    def to_str(self, address_byte):
        return super().to_str(address_byte).removeprefix(self.nonstandard_prefix)  # ty: ignore[no-matching-overload]

    def to_bytes(self, address: str) -> bytes:
        if any(
            address.startswith(prefix) for prefix in self.non_standard_fallback_prefixes
        ):
            return super().to_bytes(f"{self.nonstandard_prefix}{address}")
        else:
            return super().to_bytes(address)


converters = {
    "eth": AddressConverterEth(),
    "trx": AddressConverterTrx(),
    "ltc": AddressConverterBtcLike(
        bech32_prefix="ltc1", nonstandard_prefix="nonstandard"
    ),
    "btc": AddressConverterBtcLike(
        bech32_prefix="bc1", nonstandard_prefix="nonstandard"
    ),
    "bch": AddressConverterBchWithNonstandardFallback(
        non_standard_fallback_prefixes=["bc1"]
    ),
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


# Uppercase bech32 human-readable parts per network. BIP173 allows an
# all-uppercase address (QR codes use it); the stored form is lowercase.
_UPPERCASE_BECH32_PREFIXES = {
    "btc": ("BC1", "TB1", "BCRT1"),
    "ltc": ("LTC1", "TLTC1", "RLTC1"),
}

_TRX_HEX_ADDRESS = re.compile(r"41[0-9a-fA-F]{40}")


def lowercase_uppercase_bech32(currency, address: str) -> str:
    """BIP173 allows a bech32 address in all uppercase too (QR codes use it),
    but the database stores the lowercase form. Mixed case is invalid bech32
    and stays as it is. Also works on the start of an address (search input)."""
    if address.isupper() and address.startswith(
        _UPPERCASE_BECH32_PREFIXES.get(currency, ())
    ):
        return address.lower()
    return address


def _trx_address_to_bytes(address: str) -> bytes:
    """User-supplied TRX address (base58 ``T…`` or hex ``41…``) to its stored
    bytes, verifying the base58 checksum (which also rejects a truncated or
    extended string) and, on a full-length address, the 0x41 prefix."""
    if _TRX_HEX_ADDRESS.fullmatch(address):
        return bytes.fromhex(address[2:])
    raw = base58.b58decode(address)
    if len(raw) < 5 or (len(raw) == 25 and raw[0] != 0x41):
        raise ValueError(f"{address} is not a TRX address")
    return tron_address_to_evm(address, validate=True)


def cannonicalize_address(currency, address: str):
    if currency == "trx":
        return _trx_address_to_bytes(address)
    elif currency in _UPPERCASE_BECH32_PREFIXES and isinstance(address, str):
        return lowercase_uppercase_bech32(currency, address)
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


# Map each case-folded character to every base58 letter that folds to it.
# base58 omits 'I', 'O' and 'l', so a few folds are single-valued: an 'i'/'o'
# is always lower-case and an 'l' must be 'L'. Digits 1-9 fold to themselves.
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_CASE_VARIANTS: dict = {}
for _ch in _BASE58_ALPHABET:
    _BASE58_CASE_VARIANTS.setdefault(_ch.lower(), []).append(_ch)


def _base58_case_variants(s: str) -> List[List[str]]:
    """Per-position list of base58 letters that could have produced each char."""
    out = []
    for c in s:
        v = _BASE58_CASE_VARIANTS.get(c.lower())
        if v is None:
            raise ValueError(f"'{c}' is not a base58 character")
        out.append(v)
    return out


def count_base58_case_candidates(s: str) -> int:
    """Number of case variants ``recover_base58_case`` would have to test.

    Only characters with an ambiguous case (i.e. both cases are valid base58)
    branch; digits and the asymmetric 'i'/'o'/'l' are fixed.
    """
    n = 1
    for v in _base58_case_variants(s):
        n *= len(v)
    return n


def iter_base58_case_recovery(
    s: str, validate: Callable[[str], bool] = base58_check_validate
) -> Iterator[str]:
    """Lazily yield every case variant of ``s`` that passes ``validate``."""
    for combo in itertools.product(*_base58_case_variants(s)):
        candidate = "".join(combo)
        if validate(candidate):
            yield candidate


def recover_base58_case(
    s: str,
    validate: Callable[[str], bool] = base58_check_validate,
    max_candidates: int = 1 << 28,
) -> Optional[str]:
    """Recover the original letter-casing of a base58check string whose case was
    lost (e.g. it was upper- or lower-cased).

    base58check carries a 4-byte SHA256d checksum, and SHA256 is not invertible,
    so there is no algebraic shortcut: we enumerate the valid case variants of
    each letter and return the first one whose checksum is intact. The search is
    pruned to genuinely case-ambiguous characters (digits and 'i'/'o'/'l' are
    fixed), and the 32-bit checksum makes a hit effectively unique, so returning
    the first match is safe and roughly halves the average work.

    For a typical 34-char address this is ~2^24..2^28 candidates (minutes of
    pure-Python checking worst case). If the address is expected to live in a
    dataset, a case-insensitive lookup there is far cheaper than this brute force.

    Returns the recovered string, or ``None`` if no case variant validates
    (e.g. the input is corrupted beyond just its casing).

    Raises ``ValueError`` if the input contains a non-base58 character, or if the
    case-variant search space exceeds ``max_candidates`` (raise the limit to
    brute-force anyway).
    """
    total = count_base58_case_candidates(s)
    if total > max_candidates:
        raise ValueError(
            f"case-variant search space ({total}) exceeds max_candidates "
            f"({max_candidates}); raise the limit to brute-force anyway"
        )
    return next(iter_base58_case_recovery(s, validate=validate), None)


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


# Checksum constants for the two segwit encodings (BIP-173 / BIP-350).
BECH32_CONST = 1
BECH32M_CONST = 0x2BC830A3


def bech32_checksum_spec(hrp, data):
    """Return 'bech32', 'bech32m', or None for hrp + data (incl. checksum)."""
    const = bech32_polymod(bech32_hrp_expand(hrp) + data)
    if const == BECH32_CONST:
        return "bech32"
    if const == BECH32M_CONST:
        return "bech32m"
    return None


def bech32_convertbits(data, frombits, tobits, pad=True):
    """General power-of-2 base conversion (BIP-173 reference)."""
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            return None
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return ret


def bech32_validate(s: str, expected_hrp: Optional[str] = None) -> bool:
    """Validate a segwit (bech32/bech32m) address per BIP-173 and BIP-350.

    Witness v0 (e.g. bc1q…) is bech32-encoded; witness v1+ (Taproot, bc1p…)
    is bech32m-encoded. The witness version selects which checksum is required,
    so a single bech32-only check produces false negatives on Taproot.
    """
    if not s:
        return False

    # BIP-173 forbids mixed case (all-lower or all-upper only).
    if s != s.lower() and s != s.upper():
        return False
    s = s.lower()

    if len(s) > 90:
        return False

    pos = s.rfind("1")
    if pos < 1 or pos + 7 > len(s):
        return False

    hrp = s[:pos]
    data_part = s[pos + 1 :]

    if expected_hrp and hrp != expected_hrp:
        return False

    charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    if any(c not in charset for c in data_part):
        return False

    data = [charset.find(c) for c in data_part]

    spec = bech32_checksum_spec(hrp, data)
    if spec is None:
        return False

    # Drop the 6-symbol checksum; remainder is witness version + program.
    payload = data[:-6]
    if not payload:
        return False

    witver = payload[0]
    if witver > 16:
        return False

    witprog = bech32_convertbits(payload[1:], 5, 8, False)
    if witprog is None or len(witprog) < 2 or len(witprog) > 40:
        return False

    # BIP-350: v0 must use bech32 (and 20- or 32-byte program);
    # v1..v16 (incl. Taproot) must use bech32m.
    if witver == 0:
        return spec == "bech32" and len(witprog) in (20, 32)
    return spec == "bech32m"


# bech32 human-readable parts per network (mainnet, testnet, regtest).
_BECH32_HRPS = {
    "btc": ("bc", "tb", "bcrt"),
    "ltc": ("ltc", "tltc", "rltc"),
}
# Networks whose non-bech32 addresses are base58check (ZEC transparent only).
_BASE58CHECK_NETWORKS = frozenset({"btc", "ltc", "bch", "zec"})
_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def _bech32_checksum_ok(s: str) -> bool:
    """Only the checksum: either constant counts, whatever the witness
    version or program, so an odd but real output is never called a typo."""
    if s != s.lower() and s != s.upper():
        return False
    s = s.lower()
    pos = s.rfind("1")
    if pos < 1 or pos + 7 > len(s):
        return False
    data_part = s[pos + 1 :]
    if any(c not in _BECH32_CHARSET for c in data_part):
        return False
    data = [_BECH32_CHARSET.find(c) for c in data_part]
    return bech32_checksum_spec(s[:pos], data) is not None


def address_checksum_invalid(network: str, address) -> bool:
    """True when ``address`` is in a checksummed UTXO format (bech32 with a
    known prefix, or base58check) and its checksum fails: a typo.

    Conservative on purpose, for turning a "not found" into "invalid": an
    unrecognised format, a ``nonstandard`` placeholder or another network
    gives False. Callers ask only after a lookup found nothing, so an
    indexed address is never rejected, whatever its checksum.
    """
    if not isinstance(address, str) or not address:
        return False
    if address.startswith("nonstandard"):
        return False
    network = network.lower()
    lowered = address.lower()
    base58_shaped = network in _BASE58CHECK_NETWORKS and all(
        c in _BASE58_ALPHABET for c in address
    )
    # A valid base58 address is no typo, also one that starts like bech32 by
    # chance (LTC: always "L", then random characters, so "LTC1...").
    if base58_shaped and base58_check_validate(address):
        return False
    for hrp in _BECH32_HRPS.get(network, ()):
        if lowered.startswith(hrp + "1"):
            return not _bech32_checksum_ok(address)
    return base58_shaped


BECH32_MIXED_CASE_HINT = (
    "upper and lower case mixed; a bech32 address is all lowercase or all uppercase"
)


def bech32_mixed_case(network: str, address) -> bool:
    """True when ``address`` mixes upper and lower case but in lowercase is a
    valid bech32 address of ``network``: only the case is wrong (BIP173
    allows all lowercase or all uppercase). A base58 address that starts
    like bech32 by chance is never one: in lowercase its checksum fails."""
    if not isinstance(address, str) or address in (address.lower(), address.upper()):
        return False
    lowered = address.lower()
    return any(
        lowered.startswith(hrp + "1") for hrp in _BECH32_HRPS.get(network.lower(), ())
    ) and _bech32_checksum_ok(lowered)


def base58_check_payload(s: str) -> Optional[bytes]:
    """The payload (version + body, without checksum) of a Base58Check
    string, or None if it is not one."""
    try:
        decoded = base58_check_decode(s)
    except ValueError:
        return None
    if len(decoded) < 5:
        return None
    payload, checksum = decoded[:-4], decoded[-4:]
    if hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] != checksum:
        return None
    return payload


def base58_version_ok(s: str, versions: dict) -> bool:
    """True if ``s`` is Base58Check with one of ``versions`` (version bytes
    -> allowed body lengths). A valid checksum alone says nothing: private
    keys, extended keys and other chains' addresses have one too."""
    payload = base58_check_payload(s)
    if payload is None:
        return False
    for version, lengths in versions.items():
        if payload.startswith(version) and len(payload) - len(version) in lengths:
            return True
    return False


def bech32_data(s: str, expected_hrp: str) -> Optional[tuple]:
    """(checksum spec, 5-bit data without checksum) of a bech32/bech32m
    string with ``expected_hrp``, else None. Unlike ``bech32_validate`` no
    segwit rules and no 90-character limit (Zcash addresses are longer)."""
    if s != s.lower() and s != s.upper():
        return None
    s = s.lower()
    pos = s.rfind("1")
    if pos < 1 or pos + 7 > len(s) or s[:pos] != expected_hrp:
        return None
    data_part = s[pos + 1 :]
    if any(c not in _BECH32_CHARSET for c in data_part):
        return None
    data = [_BECH32_CHARSET.find(c) for c in data_part]
    spec = bech32_checksum_spec(expected_hrp, data)
    if spec is None:
        return None
    return spec, data[:-6]


# Base58Check version bytes -> length of the hash that follows.
_BTC_BASE58_VERSIONS = {
    b"\x00": {20},  # P2PKH
    b"\x05": {20},  # P2SH
    b"\x6f": {20},  # testnet P2PKH
    b"\xc4": {20},  # testnet P2SH
}
# BCH legacy: P2SH32 has a 32-byte hash (see utils/bch.py).
_BCH_BASE58_VERSIONS = {**_BTC_BASE58_VERSIONS, b"\x05": {20, 32}}
_LTC_BASE58_VERSIONS = {
    b"\x30": {20},  # L, P2PKH
    b"\x32": {20},  # M, P2SH
    b"\x05": {20},  # 3, P2SH (old, shared with BTC)
    b"\x6f": {20},  # testnet P2PKH
    b"\xc4": {20},  # testnet P2SH (old)
    b"\x3a": {20},  # testnet P2SH
}
_ZEC_BASE58_VERSIONS = {
    b"\x1c\xb8": {20},  # t1, P2PKH
    b"\x1c\xbd": {20},  # t3, P2SH
    b"\x1d\x25": {20},  # tm, testnet P2PKH
    b"\x1c\xba": {20},  # t2, testnet P2SH
    b"\x16\x9a": {64},  # zc, Sprout
    b"\x16\xb6": {64},  # zt, testnet Sprout
}
_TRX_BASE58_VERSIONS = {b"\x41": {20}}


def validate_btc_address(address: str) -> bool:
    """Validate Bitcoin address"""
    if not address:
        return False

    # Bech32 (segwit) addresses
    if address.lower().startswith("bc1"):
        return bech32_validate(address, "bc")
    elif address.lower().startswith("tb1"):  # testnet
        return bech32_validate(address, "tb")

    # Legacy addresses (Base58Check). Private keys (WIF) and extended keys
    # have a valid checksum too, but are not addresses.
    return base58_version_ok(address, _BTC_BASE58_VERSIONS)


def validate_bch_address(address: str) -> bool:
    """Validate Bitcoin Cash address"""
    if not address:
        return False

    # CashAddr format, with or without the bitcoincash: prefix
    try:
        legacy = checked_bch_address_to_legacy(address)
    except BCHInvalidAddress:
        return False
    if legacy != address:
        return True

    # Legacy format
    return base58_version_ok(address, _BCH_BASE58_VERSIONS)


def validate_ltc_address(address: str) -> bool:
    """Validate Litecoin address"""
    if not address:
        return False

    # Bech32 (segwit) addresses
    if address.lower().startswith("ltc1"):
        return bech32_validate(address, "ltc")

    # Legacy addresses
    return base58_version_ok(address, _LTC_BASE58_VERSIONS)


def validate_zec_address(address: str) -> bool:
    """Validate Zcash address"""
    if not address:
        return False

    lowered = address.lower()
    # Sapling (ZIP-173): bech32, 11-byte diversifier + 32-byte pk_d
    for hrp in ("zs", "ztestsapling"):
        if lowered.startswith(hrp + "1"):
            decoded = bech32_data(address, hrp)
            if decoded is None or decoded[0] != "bech32":
                return False
            body = bech32_convertbits(decoded[1], 5, 8, False)
            return body is not None and len(body) == 43

    # Unified addresses (ZIP-316): bech32m over an F4Jumbled body, so only
    # the checksum can be checked offline
    for hrp in ("u", "utest"):
        if lowered.startswith(hrp + "1"):
            decoded = bech32_data(address, hrp)
            return decoded is not None and decoded[0] == "bech32m"

    # Transparent and Sprout addresses
    return base58_version_ok(address, _ZEC_BASE58_VERSIONS)


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

    return base58_version_ok(address, _TRX_BASE58_VERSIONS)


def validate_xrp_address(address: str) -> bool:
    """Validate an XRP classic (r...) address.

    XRP uses base58check with the same SHA256d checksum as Bitcoin but a
    different alphabet (the "ripple" dictionary), so we decode against that
    alphabet and let ``b58decode_check`` verify the 4-byte checksum.
    """
    if not address or not address.startswith("r"):
        return False

    import base58

    try:
        base58.b58decode_check(address, alphabet=base58.RIPPLE_ALPHABET)
        return True
    except Exception:
        return False


def validate_sol_address(address: str) -> bool:
    """Validate a Solana address: base58 of a 32-byte public key. There is
    no checksum, so only the alphabet and the length can be checked."""
    if not address or not 32 <= len(address) <= 44:
        return False
    try:
        return len(base58.b58decode(address)) == 32
    except ValueError:
        return False


def validate_algo_address(address: str) -> bool:
    """Validate an Algorand address: base32 (no padding) of a 32-byte public
    key and a 4-byte checksum, the last 4 bytes of its SHA-512/256."""
    import base64

    if not address or len(address) != 58:
        return False
    try:
        raw = base64.b32decode(address + "======")
    except ValueError:
        return False
    # the last character carries 2 padding bits: only one spelling is valid
    if base64.b32encode(raw).decode().rstrip("=") != address:
        return False
    key, checksum = raw[:32], raw[32:]
    return hashlib.new("sha512_256", key).digest()[-4:] == checksum


def _crc16_xmodem(data: bytes) -> int:
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
            crc &= 0xFFFF
    return crc


_TON_RAW = re.compile(r"(0|-1):[0-9a-fA-F]{64}")


def validate_ton_address(address: str) -> bool:
    """Validate a TON address: raw (``0:<64 hex>``) or user-friendly, 48
    characters of base64 (url-safe or standard) over a flag byte, the
    workchain, a 32-byte hash and a CRC16-XMODEM of the 34 bytes before."""
    import base64

    if not address:
        return False
    if _TON_RAW.fullmatch(address):
        return True
    if len(address) != 48:
        return False
    try:
        raw = base64.urlsafe_b64decode(address.replace("+", "-").replace("/", "_"))
    except ValueError:
        return False
    if len(raw) != 36 or raw[0] & 0x7F not in (0x11, 0x51) or raw[1] not in (0, 255):
        return False
    return _crc16_xmodem(raw[:34]) == int.from_bytes(raw[34:], "big")


_EOSIO_NAME = re.compile(r"[a-z1-5.]{0,11}[a-z1-5]|[a-z1-5.]{12}[a-j1-5]")


def validate_eosio_address(address: str) -> bool:
    """Validate an EOSIO account name: up to 12 characters of a-z, 1-5 and
    '.', not ending in '.', and a 13th from a-j and 1-5. No checksum."""
    return bool(address) and _EOSIO_NAME.fullmatch(address) is not None


# EVM chains share Ethereum's address format.
EVM_NETWORKS = frozenset(
    {"eth", "etc", "bsc", "arb", "avax", "base", "pol", "matic", "op", "zkf", "kcc"}
)


def validate_address(currency: str, address: str) -> bool:
    """
    Validate cryptocurrency address for the given currency.

    Args:
        currency: Currency code or network (btc, bch, ltc, zec, eth, trx,
            xrp, sol, algo, eosio, ton, or an EVM network such as bsc)
        address: Address to validate

    Returns:
        bool: True if address is valid, False otherwise
    """
    if not currency or not address:
        return False

    validator = _VALIDATORS.get(currency.lower())
    if not validator:
        raise ValueError(f"Unsupported currency: {currency}")

    try:
        return validator(address)
    except Exception:
        return False


_VALIDATORS = {
    "btc": validate_btc_address,
    "bch": validate_bch_address,
    "ltc": validate_ltc_address,
    "zec": validate_zec_address,
    "eth": validate_eth_address,
    "trx": validate_trx_address,
    "xrp": validate_xrp_address,
    "sol": validate_sol_address,
    "algo": validate_algo_address,
    "eosio": validate_eosio_address,
    "ton": validate_ton_address,
    **{n: validate_eth_address for n in EVM_NETWORKS},
}


def is_verifiable_network(network: str) -> bool:
    """True if ``validate_address`` has a validator for ``network``."""
    return bool(network) and network.lower() in _VALIDATORS
