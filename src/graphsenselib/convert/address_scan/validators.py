"""Self-contained, stdlib-only cryptocurrency address validators.

WARNING — read before editing:
    graphsenselib itself does NOT use this module. ``detectors.py`` imports the
    battle-tested validators from :mod:`graphsenselib.utils.address` (backed by
    ``base58`` / ``bech32`` / a keccak lib). This file exists ONLY so the
    standalone ``graphsense-python`` client can be published without depending on
    graphsenselib or those third-party libraries: the sync script vendors this
    package and rewrites ``detectors.py``'s import to point here instead.

    Because there are now two implementations, they can drift. To guard against
    that, ``tests/convert/address_scan/test_validators_crosscheck.py`` asserts
    this module agrees with :mod:`graphsenselib.utils.address` over a large
    corpus. Keep the two in lockstep; the tests are the contract.

Everything here is pure Python + stdlib (``hashlib`` only). The base58check and
bech32 routines are copied verbatim from ``utils/address.py``; only the two
spots that used third-party libraries there are reimplemented locally: the
EIP-55 keccak (pure-Python Keccak-256) and the XRP ripple-alphabet base58check.
"""

from __future__ import annotations

import hashlib
import re

# --- base58check (Bitcoin alphabet) -----------------------------------------
# Copied verbatim from graphsenselib.utils.address.
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def base58_check_decode(s: str) -> bytes:
    """Decode Base58Check string to bytes."""
    alphabet = _BASE58_ALPHABET

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

    pad = 0
    for c in s:
        if c == alphabet[0]:
            pad += 1
        else:
            break

    return bytes([0] * pad) + res


def base58_check_validate(s: str) -> bool:
    """Validate Base58Check string (4-byte double-SHA256 checksum)."""
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


# --- bech32 / bech32m (BIP-173 / BIP-350) -----------------------------------
# Copied verbatim from graphsenselib.utils.address.
BECH32_CONST = 1
BECH32M_CONST = 0x2BC830A3


def bech32_polymod(values):
    GEN = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i in range(5):
            chk ^= GEN[i] if ((b >> i) & 1) else 0
    return chk


def bech32_hrp_expand(hrp):
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]


def bech32_checksum_spec(hrp, data):
    const = bech32_polymod(bech32_hrp_expand(hrp) + data)
    if const == BECH32_CONST:
        return "bech32"
    if const == BECH32M_CONST:
        return "bech32m"
    return None


def bech32_convertbits(data, frombits, tobits, pad=True):
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


def bech32_validate(s: str, expected_hrp: "str | None" = None) -> bool:
    if not s:
        return False

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

    payload = data[:-6]
    if not payload:
        return False

    witver = payload[0]
    if witver > 16:
        return False

    witprog = bech32_convertbits(payload[1:], 5, 8, False)
    if witprog is None or len(witprog) < 2 or len(witprog) > 40:
        return False

    if witver == 0:
        return spec == "bech32" and len(witprog) in (20, 32)
    return spec == "bech32m"


# --- Keccak-256 (pure Python; replaces the keccak lib used upstream) ---------


def _rotl(x: int, n: int) -> int:
    x &= (1 << 64) - 1
    return ((x << n) | (x >> (64 - n))) & ((1 << 64) - 1)


def _keccak256(data: bytes) -> bytes:
    """Minimal Keccak-256 (Ethereum variant). Pure Python, no deps."""
    r_bytes = 1088 // 8
    lanes = [[0] * 5 for _ in range(5)]
    rc = [
        0x0000000000000001,
        0x0000000000008082,
        0x800000000000808A,
        0x8000000080008000,
        0x000000000000808B,
        0x0000000080000001,
        0x8000000080008081,
        0x8000000000008009,
        0x000000000000008A,
        0x0000000000000088,
        0x0000000080008009,
        0x000000008000000A,
        0x000000008000808B,
        0x800000000000008B,
        0x8000000000008089,
        0x8000000000008003,
        0x8000000000008002,
        0x8000000000000080,
        0x000000000000800A,
        0x800000008000000A,
        0x8000000080008081,
        0x8000000000008080,
        0x0000000080000001,
        0x8000000080008008,
    ]
    rot = [
        [0, 36, 3, 41, 18],
        [1, 44, 10, 45, 2],
        [62, 6, 43, 15, 61],
        [28, 55, 25, 21, 56],
        [27, 20, 39, 8, 14],
    ]

    def keccak_f(st):
        for rnd in range(24):
            c = [st[x][0] ^ st[x][1] ^ st[x][2] ^ st[x][3] ^ st[x][4] for x in range(5)]
            d = [c[(x - 1) % 5] ^ _rotl(c[(x + 1) % 5], 1) for x in range(5)]
            for x in range(5):
                for y in range(5):
                    st[x][y] ^= d[x]
            b = [[0] * 5 for _ in range(5)]
            for x in range(5):
                for y in range(5):
                    b[y][(2 * x + 3 * y) % 5] = _rotl(st[x][y], rot[x][y])
            for x in range(5):
                for y in range(5):
                    st[x][y] = b[x][y] ^ ((~b[(x + 1) % 5][y]) & b[(x + 2) % 5][y])
            st[0][0] ^= rc[rnd]

    msg = bytearray(data)
    msg.append(0x01)
    while len(msg) % r_bytes != 0:
        msg.append(0x00)
    msg[-1] ^= 0x80
    for off in range(0, len(msg), r_bytes):
        block = msg[off : off + r_bytes]
        for i in range(r_bytes // 8):
            lane = int.from_bytes(block[i * 8 : i * 8 + 8], "little")
            lanes[i % 5][i // 5] ^= lane
        keccak_f(lanes)
    out = bytearray()
    for i in range(4):
        out += lanes[i % 5][i // 5].to_bytes(8, "little")
    return bytes(out[:32])


# --- per-currency validators ------------------------------------------------
# btc/ltc/zec/trx copied verbatim from utils/address.py (pure already).


def validate_btc_address(address: str) -> bool:
    if not address:
        return False
    if address.lower().startswith("bc1"):
        return bech32_validate(address, "bc")
    elif address.lower().startswith("tb1"):
        return bech32_validate(address, "tb")
    elif address[0] in "13":
        return base58_check_validate(address)
    elif address[0] in "2mn":
        return base58_check_validate(address)
    elif address[0] in "59LKc":
        return base58_check_validate(address)
    return False


def validate_ltc_address(address: str) -> bool:
    if not address:
        return False
    if address.lower().startswith("ltc1"):
        return bech32_validate(address, "ltc")
    elif address[0] in "L3M":
        return base58_check_validate(address)
    elif address[0] in "2mn":
        return base58_check_validate(address)
    return False


def validate_zec_address(address: str) -> bool:
    if not address:
        return False
    if address.startswith("t1") or address.startswith("t3"):
        return base58_check_validate(address)
    elif address.startswith("tm"):
        return base58_check_validate(address)
    elif address.startswith("zc") and len(address) == 95:
        return base58_check_validate(address)
    elif (address.startswith("zs") or address.startswith("ztestsapling")) and len(
        address
    ) in [78, 88]:
        return base58_check_validate(address)
    return False


def validate_trx_address(address: str) -> bool:
    if not address:
        return False
    if not address.startswith("T") or len(address) != 34:
        return False
    return base58_check_validate(address)


def validate_eth_address(address: str) -> bool:
    """EIP-55 validation. Mirrors utils/address.py but uses pure-Python keccak."""
    if not address:
        return False
    if address.startswith("0x") or address.startswith("0X"):
        address = address[2:]
    if len(address) != 40:
        return False
    if not re.match(r"^[0-9a-fA-F]{40}$", address):
        return False
    if address == address.lower() or address == address.upper():
        return True
    try:
        hash_hex = _keccak256(address.lower().encode("utf-8")).hex()
        for i, char in enumerate(address):
            if char.isalpha():
                if int(hash_hex[i], 16) >= 8:
                    if char != char.upper():
                        return False
                else:
                    if char != char.lower():
                        return False
        return True
    except Exception:
        return True


def validate_xrp_address(address: str) -> bool:
    """Validate an XRP classic (r...) address (ripple-alphabet base58check).

    Pure-Python replacement for the ``base58`` lib call in utils/address.py.
    """
    if not address or not address.startswith("r"):
        return False
    ripple = "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz"
    ripple_map = {c: i for i, c in enumerate(ripple)}
    num = 0
    for ch in address:
        val = ripple_map.get(ch)
        if val is None:
            return False
        num = num * 58 + val
    raw = num.to_bytes((num.bit_length() + 7) // 8, "big")
    raw = bytes(len(address) - len(address.lstrip("r"))) + raw
    if len(raw) < 5:
        return False
    payload, checksum = raw[:-4], raw[-4:]
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] == checksum


def validate_address(currency: str, address: str) -> bool:
    """Validate ``address`` for ``currency`` (btc/ltc/eth/trx/zec/xrp)."""
    if not currency or not address:
        return False

    currency = currency.lower()
    validation_functions = {
        "btc": validate_btc_address,
        "ltc": validate_ltc_address,
        "zec": validate_zec_address,
        "eth": validate_eth_address,
        "trx": validate_trx_address,
        "xrp": validate_xrp_address,
    }
    validator = validation_functions.get(currency)
    if not validator:
        raise ValueError(f"Unsupported currency: {currency}")
    try:
        return validator(address)
    except Exception:
        return False
