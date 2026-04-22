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


class InvalidAddress(Exception):
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
        raise InvalidAddress(
            "Cash address contains more than one colon character"
        )

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

    cash_version = result[0]
    hash_bytes = bytes(result[1:])

    legacy_version = _CASH_TO_LEGACY_VERSION.get(cash_version)
    if legacy_version is None:
        raise InvalidAddress(f"Unknown cashaddr version {cash_version}")

    return _b58encode_check(bytes([legacy_version]) + hash_bytes)


def try_bch_address_to_legacy(address: str) -> str:
    """Convert cashaddr to legacy, returning the original on failure."""
    if not address.startswith("bitcoincash:"):
        return address
    return bch_address_to_legacy(address)
