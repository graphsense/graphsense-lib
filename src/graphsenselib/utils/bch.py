"""Fast BCH cashaddr → legacy address conversion.

Replaces the pure-Python `cashaddress` library with an optimized
implementation. The conversion logic:

  1. Decode bech32 payload from the cashaddr string
  2. Convert 5-bit groups → 8-bit groups
  3. Map cashaddr version → legacy version byte
  4. Base58Check encode (double-SHA256 checksum + base58)

Checksum verification is skipped because addresses come from the
trusted node RPC response. This is the main performance win over
the cashaddress library.
"""

from functools import lru_cache
from hashlib import sha256

_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_CHARSET_LOOKUP = {c: i for i, c in enumerate(_CHARSET)}

_B58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# cashaddr version byte → legacy version byte
# P2PKH: cashaddr 0 → legacy 0, P2SH: cashaddr 8 → legacy 5
# P2SH32: cashaddr 11 → legacy 5 (https://bch.info/en/upgrade)
_CASH_TO_LEGACY_VERSION = {0: 0, 8: 5, 11: 5}

_PREFIX_LEN = len("bitcoincash:")


class InvalidAddress(ValueError):
    pass


def _b58encode_check(payload: bytes) -> str:
    """Base58Check encode: payload + 4-byte double-SHA256 checksum."""
    checksum = sha256(sha256(payload).digest()).digest()[:4]
    data = payload + checksum

    n_leading = 0
    for b in data:
        if b != 0:
            break
        n_leading += 1

    num = int.from_bytes(data, "big")
    chars = []
    while num > 0:
        num, rem = divmod(num, 58)
        chars.append(_B58_ALPHABET[rem])
    chars.reverse()

    return "1" * n_leading + bytes(chars).decode()


@lru_cache(maxsize=2**20)
def bch_address_to_legacy(address: str) -> str:
    """Convert a BCH cashaddr (bitcoincash:qp...) to legacy format (1.../3...).

    Skips checksum verification (trusted node data). Raises InvalidAddress
    on structurally malformed input.
    """
    if address.startswith("bitcoincash:"):
        base32str = address[_PREFIX_LEN:]
    elif address.startswith("BITCOINCASH:"):
        base32str = address[_PREFIX_LEN:].lower()
    elif ":" in address:
        raise InvalidAddress(f"Unsupported prefix in {address}")
    else:
        # Already a legacy address — return as-is
        return address

    if ":" in base32str:
        raise InvalidAddress("Cash address contains more than one colon character")

    # Bech32 decode, stripping 8-char checksum (we skip verification)
    payload_str = base32str[:-8]
    lookup = _CHARSET_LOOKUP
    n = len(payload_str)
    decoded = bytearray(n)
    for i in range(n):
        v = lookup.get(payload_str[i])
        if v is None:
            raise InvalidAddress(f"Invalid character '{payload_str[i]}' in cashaddr")
        decoded[i] = v

    # Convert 5-bit → 8-bit
    acc = 0
    bits = 0
    result = bytearray()
    for value in decoded:
        acc = (acc << 5) | value
        bits += 5
        if bits >= 8:
            bits -= 8
            result.append((acc >> bits) & 0xFF)

    if not result:
        raise InvalidAddress(f"Cashaddr payload too short in {address}")

    cash_version = result[0]
    hash_bytes = bytes(result[1:])

    legacy_version = _CASH_TO_LEGACY_VERSION.get(cash_version)
    if legacy_version is None:
        raise InvalidAddress(f"Unknown cashaddr version {cash_version}")

    return _b58encode_check(bytes([legacy_version]) + hash_bytes)


_PREFIX = "bitcoincash"
_PREFIX_VALUES = [ord(c) & 0x1F for c in _PREFIX] + [0]

# Hash length in bytes per cashaddr version byte (the low 3 bits encode it).
_HASH_LEN_BY_VERSION = {0: 20, 8: 20, 11: 32}

# Prefixless cashaddr lengths: 34 + 8 chars for a 20-byte hash, 53 + 8 for 32.
_PREFIXLESS_LENGTHS = (42, 61)


def _polymod(values) -> int:
    c = 1
    for d in values:
        c0 = c >> 35
        c = ((c & 0x07FFFFFFFF) << 5) ^ d
        if c0 & 0x01:
            c ^= 0x98F2BC8E61
        if c0 & 0x02:
            c ^= 0x79B76D99E2
        if c0 & 0x04:
            c ^= 0xF33E5FB3C4
        if c0 & 0x08:
            c ^= 0xAE2EABE2A8
        if c0 & 0x10:
            c ^= 0x1E4F43E470
    return c ^ 1


def _looks_like_cashaddr(address: str) -> bool:
    lower = address.lower()
    if lower.startswith(_PREFIX + ":"):
        return True
    return (
        ":" not in address
        and len(address) in _PREFIXLESS_LENGTHS
        and lower[0] in "qp"
        and all(c in _CHARSET_LOOKUP for c in lower)
    )


@lru_cache(maxsize=2**16)
def checked_bch_address_to_legacy(address: str) -> str:
    """Convert a user-supplied cashaddr to legacy, verifying it fully.

    Unlike ``bch_address_to_legacy`` (the unchecked fast path for node data),
    this verifies the checksum, the hash length for the version byte and the
    zero padding, so a typo raises instead of decoding to another address.
    Accepts an all-lowercase or all-uppercase payload, with the
    ``bitcoincash:`` prefix in either case or without it; a legacy address
    is returned as-is.
    """
    if not _looks_like_cashaddr(address):
        if ":" in address:
            raise InvalidAddress(f"Unsupported prefix in {address}")
        return address

    payload = address.split(":", 1)[1] if ":" in address else address
    if ":" in payload:
        raise InvalidAddress("Cash address contains more than one colon character")
    if payload != payload.lower() and payload != payload.upper():
        raise InvalidAddress(f"Cashaddr {address} mixes upper and lower case")
    payload = payload.lower()

    if len(payload) <= 8 or any(c not in _CHARSET_LOOKUP for c in payload):
        raise InvalidAddress(f"Malformed cashaddr {address}")
    values = [_CHARSET_LOOKUP[c] for c in payload]
    if _polymod(_PREFIX_VALUES + values) != 0:
        raise InvalidAddress(f"Invalid cashaddr checksum in {address}")

    data = values[:-8]
    nbits = len(data) * 5
    pad_bits = nbits % 8
    acc = 0
    for v in data:
        acc = (acc << 5) | v
    if pad_bits >= 5 or acc & ((1 << pad_bits) - 1):
        raise InvalidAddress(f"Invalid cashaddr padding in {address}")
    raw = (acc >> pad_bits).to_bytes(nbits // 8, "big")

    expected_len = _HASH_LEN_BY_VERSION.get(raw[0])
    if expected_len is None:
        raise InvalidAddress(f"Unknown cashaddr version {raw[0]}")
    if len(raw) - 1 != expected_len:
        raise InvalidAddress(f"Wrong hash length for cashaddr version in {address}")

    return bch_address_to_legacy(f"{_PREFIX}:{payload}")


def try_bch_address_to_legacy(address: str) -> str:
    """Canonicalize a user-supplied BCH address to legacy.

    Cashaddr input (any accepted spelling) is verified and converted, raising
    InvalidAddress if it is malformed; anything else is returned as-is.
    """
    return checked_bch_address_to_legacy(address)
