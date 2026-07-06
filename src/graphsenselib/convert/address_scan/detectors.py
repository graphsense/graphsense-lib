"""Regex detectors and validators for the address scanner.

Each detector pairs a permissive regex (to *extract* address-shaped runs) with
a validator (to *confirm* them). The validators reuse the checksum logic in
:mod:`graphsenselib.utils.address` wherever possible; only XMR (no cheap
checksum) and the opt-in tx-hash candidates are format-level heuristics.

Supported with full checksum validation:
  - Bitcoin  : legacy (1.../3...), bech32/bech32m (bc1...)
  - Litecoin : legacy (L/M...), bech32 (ltc1...)
  - Ethereum : 0x + 40 hex (EIP-55 mixed-case checksum verified when present)
  - TRON     : T... base58check
  - Zcash    : transparent t1/t3 base58check
  - XRP      : classic r-addresses (base58check, ripple alphabet)
Reported as candidates only (no cheap checksum):
  - Monero   : 4.../8... 95-char addresses
"""

from __future__ import annotations

from typing import Callable, NamedTuple, Optional

from graphsenselib.utils.address import validate_address, validate_xrp_address


def decodes_to_text(hex_body: str, threshold: float = 0.7) -> bool:
    """True if ``hex_body`` decodes mostly to printable ASCII.

    SQL dumps store blobs as ``0x...``/hex literals; a run that decodes to
    readable text is encoded content (e.g. stored HTML/JS), not an address.
    Real ETH/XMR addresses are effectively random bytes and fail this test.
    """
    try:
        raw = bytes.fromhex(hex_body)
    except ValueError:
        return False
    if not raw:
        return False
    printable = sum(1 for b in raw if 0x20 <= b <= 0x7E)
    return printable / len(raw) >= threshold


def _eth_valid(addr: str) -> bool:
    # Drop hex-encoded text blobs before the (stricter) EIP-55 check.
    if decodes_to_text(addr[2:]):
        return False
    return validate_address("eth", addr)


def _xmr_candidate_valid(s: str) -> bool:
    """Monero has no cheap checksum, so reject the common false positives:
    pure-hex strings (SQL blob literals) and hex that decodes to text."""
    if all(c in "0123456789abcdefABCDEF" for c in s):
        return False  # a real base58 Monero address is ~never all-hex
    if decodes_to_text(s):
        return False
    return True


def txhash_valid(s: str) -> bool:
    """A tx hash is 32 random bytes with NO checksum -- unverifiable by form.
    We can only drop the obvious non-hashes: hex that decodes to ASCII text."""
    return not decodes_to_text(s[2:] if s[:2].lower() == "0x" else s)


class Detector(NamedTuple):
    label: str
    pattern: str
    validator: Optional[Callable[[str], bool]]


# Lookarounds keep matches from being slices of a longer hex/base58 run.
_H = r"[a-fA-F0-9]"

DETECTORS: list[Detector] = [
    Detector(
        "BTC legacy/P2SH",
        r"[13][1-9A-HJ-NP-Za-km-z]{25,34}",
        lambda m: validate_address("btc", m),
    ),
    Detector(
        "BTC bech32", r"bc1[0-9ac-hj-np-z]{11,87}", lambda m: validate_address("btc", m)
    ),
    Detector(
        "LTC legacy",
        r"[LM][1-9A-HJ-NP-Za-km-z]{25,34}",
        lambda m: validate_address("ltc", m),
    ),
    Detector(
        "LTC bech32",
        r"ltc1[0-9ac-hj-np-z]{11,87}",
        lambda m: validate_address("ltc", m),
    ),
    Detector("ETH", rf"(?<!{_H})0x{_H}{{40}}(?!{_H})", _eth_valid),
    Detector("TRX", r"T[1-9A-HJ-NP-Za-km-z]{33}", lambda m: validate_address("trx", m)),
    Detector(
        "ZEC transparent",
        r"t[13][1-9A-HJ-NP-Za-km-z]{33}",
        lambda m: validate_address("zec", m),
    ),
    Detector("XRP", r"r[1-9A-HJ-NP-Za-km-z]{24,34}", validate_xrp_address),
    Detector(
        "XMR (candidate)", r"[48][0-9AB][1-9A-HJ-NP-Za-km-z]{93}", _xmr_candidate_valid
    ),
]

# Opt-in only (--tx-hashes): format-level, NOT checksum-verifiable. Every 64-hex
# digest (SHA-256 file hashes, tokens, etc.) matches, so results are candidates.
# The 0x form is matched first; the bare form excludes an 'x'-prefixed run so a
# single 0x<64hex> is not double-counted.
TX_DETECTORS: list[Detector] = [
    Detector(
        "TX-hash candidate (0x+64hex)", rf"(?<!{_H})0x{_H}{{64}}(?!{_H})", txhash_valid
    ),
    Detector(
        "TX-hash candidate (64hex)",
        rf"(?<![a-fA-F0-9xX]){_H}{{64}}(?!{_H})",
        txhash_valid,
    ),
]
