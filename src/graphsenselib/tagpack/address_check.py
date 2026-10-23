"""Why a tagpack address is invalid, and how to fix it.

``validate_address`` only says yes or no. Fixing thousands of invalid
addresses needs more: a hint per address (offline, cheapest checks first)
and where it is in the pack. Single-character typos cannot be resolved
offline; for those the hint is "checksum mismatch".
"""

import json
import re
from dataclasses import dataclass, field
from typing import Optional

from graphsenselib.tagpack.constants import suggest_networks_from_currency
from graphsenselib.utils.address import (
    EVM_NETWORKS,
    address_checksum_invalid,
    base58_check_payload,
    is_verifiable_network,
    validate_address,
)

# Value of the tag (or header) field ``address_validation`` that exempts
# addresses kept verbatim on purpose; say why in the tag's context.
ADDRESS_VALIDATION_SKIP = "skip"

# Literal escape sequences that were never decoded (e.g. "Н" written
# into an unquoted YAML scalar): a backslash followed by u+4 hex, x+2 hex
# or n, in the parsed value.
UNDECODED_ESCAPE = re.compile(r"\\(u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|n)")

# Tried in this order when looking for the network an address belongs to.
# SOL last: any base58 string of the right length passes it.
_OTHER_NETWORKS = ("btc", "ltc", "zec", "trx", "xrp", "eth", "bch", "sol")

_EVM_ADDRESS = re.compile(r"0[xX][0-9a-fA-F]{40}")
_HEX_TEXT = re.compile(r"0[xX]((?:[0-9a-fA-F]{2})+)")
_TRON_HEX = re.compile(r"(?:0[xX]|41)?([0-9a-fA-F]{40})")
_EXTENDED_KEY = re.compile(r"[xyzYZuvUVt](?:pub|prv)[1-9A-HJ-NP-Za-km-z]{50,}")
_ID_NUMBER = re.compile(r"[0-9]+|.*[/()].*")
_BASE58_CHARS = re.compile(r"[1-9A-HJ-NP-Za-km-z]+")

# Shortest length of a complete base58 address per network.
_MIN_BASE58_LENGTH = {"btc": 26, "bch": 26, "ltc": 26, "zec": 35, "trx": 34}
# bech32 prefixes -> expected lengths (witness v0: P2WPKH, P2WSH; v1: P2TR).
_BECH32_LENGTHS = {
    "bc1q": (42, 62),
    "bc1p": (62,),
    "ltc1q": (43, 63),
    "zs1": (78,),
}
_WIF_VERSIONS = (0x80, 0xEF, 0xB0)  # BTC, testnet, LTC


@dataclass
class AddressFinding:
    """One invalid address in a pack. ``line`` is 0 when unknown."""

    file: str
    line: int
    network: str
    address: str
    hint: str = ""
    context: object = field(default=None, repr=False, compare=False)

    def as_row(self) -> str:
        return "\t".join(
            [self.file, str(self.line or ""), self.network, self.address, self.hint]
        )


REPORT_HEADER = "file\tline\tnetwork\taddress\thint"


def _try_validate(network: str, address: str) -> bool:
    try:
        return validate_address(network, address)
    except ValueError:
        return False


def _context_chain(context) -> Optional[str]:
    """The chain a tag's context names ({"chain": "BSC"}), if any."""
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except ValueError:
            return None
    if not isinstance(context, dict):
        return None
    for key in ("chain", "network", "blockchain"):
        value = context.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return None


def _other_networks(network: str, address: str) -> list:
    """Upper-case networks other than ``network`` that accept ``address``.
    EVM chains are reported once, as ETH."""
    own = {network, "eth"} if network in EVM_NETWORKS else {network}
    found = [n for n in _OTHER_NETWORKS if n not in own and _try_validate(n, address)]
    # a BTC legacy address is a valid BCH legacy address too: name BTC only
    if "btc" in found and "bch" in found:
        found.remove("bch")
    return [n.upper() for n in found]


def token_chains(network: str) -> list:
    """The chains (lower case) a token such as USDT lives on, if ``network``
    is a token rather than a chain (network defaults to the currency)."""
    if is_verifiable_network(network):
        return []
    return [c.lower() for c in suggest_networks_from_currency(network.upper())]


def validate_token_address(network: str, address: str) -> Optional[bool]:
    """True if ``address`` is valid on one of the chains of the token
    ``network``, False if on none, None if that cannot be told (a chain
    without a validator, or ``network`` is no known token)."""
    chains = token_chains(network)
    if not chains:
        return None
    if any(_try_validate(c, address) for c in chains):
        return True
    return False if all(is_verifiable_network(c) for c in chains) else None


def _likely_chain(address: str, chains: list) -> str:
    if address[:2] in ("0x", "0X"):
        evm = [c for c in chains if c in EVM_NETWORKS]
        if evm:
            return evm[0]
    if address.startswith("T") and "trx" in chains:
        return "trx"
    return chains[0]


def _tron_from_hex(hexbody: str) -> str:
    import base58

    return base58.b58encode_check(bytes.fromhex("41" + hexbody)).decode()


def address_hint(network: str, address: str, context=None) -> str:
    """A short, actionable reason why ``address`` is not a valid address of
    ``network`` (lower case). Assumes ``validate_address`` already said no."""
    net = network.upper()

    chains = token_chains(network)
    if chains:
        chain = _likely_chain(address, chains)
        names = ", ".join(c.upper() for c in chains)
        return (
            f"{net} is a token; not valid on any of its chains ({names}); "
            f"as {chain.upper()}: {address_hint(chain, address, context)}"
        )

    m = UNDECODED_ESCAPE.search(address)
    if m:
        return f"contains the undecoded escape sequence {m.group(0)!r}; decode it"

    if not address.isascii():
        chars = ", ".join(
            f"{c!r} (U+{ord(c):04X}) at position {i + 1}"
            for i, c in enumerate(address)
            if ord(c) > 127
        )
        return f"non-ASCII characters, look-alikes of Latin letters? {chars}"

    if _ID_NUMBER.fullmatch(address):
        return "not an address (an ID or registration number?)"

    if _EXTENDED_KEY.fullmatch(address):
        return "an extended key (xpub/xprv), not an address"

    payload = (
        base58_check_payload(address) if _BASE58_CHARS.fullmatch(address) else None
    )
    if payload and payload[0] in _WIF_VERSIONS and len(payload) in (33, 34):
        return "a private key (WIF), not an address; remove it, it is a leak"

    evm_shaped = _EVM_ADDRESS.fullmatch(address) is not None

    if network == "trx":
        m = _TRON_HEX.fullmatch(address)
        if m:
            return (
                "TRON address in hex; base58check is "
                f"{_tron_from_hex(m.group(1).lower())}"
            )

    if not evm_shaped:
        m = _HEX_TEXT.fullmatch(address)
        if m:
            try:
                text = bytes.fromhex(m.group(1)).decode("ascii")
            except UnicodeDecodeError:
                text = None
            if text and text.isprintable():
                if _try_validate(network, text):
                    return f"hex-encoded text of the {net} address {text}"
                others = _other_networks(network, text)
                if others:
                    return (
                        f"hex-encoded text of {text}, a valid "
                        f"{'/'.join(others)} address"
                    )

    if evm_shaped and network in EVM_NETWORKS:
        if _try_validate(network, address.lower()):
            return (
                "EIP-55 checksum case is wrong, check for a typo "
                "(lowercasing passes, but hides typos)"
            )

    if evm_shaped and network not in EVM_NETWORKS:
        chain = _context_chain(context)
        if chain:
            return (
                f"an EVM address, and the context names chain {chain}: a token "
                f"on another chain? Set network to {chain}"
            )
        return f"an EVM address, not a valid {net} address; wrong network?"

    others = _other_networks(network, address)
    if others:
        return f"a valid {'/'.join(others)} address; wrong network?"

    lowered = address.lower()
    for prefix, lengths in _BECH32_LENGTHS.items():
        if lowered.startswith(prefix) and len(address) < min(lengths):
            expected = " or ".join(str(n) for n in lengths)
            return (
                f"truncated? {prefix}… addresses have {expected} characters, "
                f"this one {len(address)}"
            )
    min_len = _MIN_BASE58_LENGTH.get(network)
    if min_len and _BASE58_CHARS.fullmatch(address) and len(address) < min_len:
        return f"truncated? {len(address)} characters, too short for {net}"

    tron_shaped = (
        network == "trx"
        and address.startswith("T")
        and len(address) == 34
        and _BASE58_CHARS.fullmatch(address)
    )
    if address_checksum_invalid(network, address) or (tron_shaped and not payload):
        return "checksum mismatch, probably a typo"

    if payload:
        return (
            f"valid Base58Check, but version 0x{payload[0]:02x} with "
            f"{len(payload) - 1} bytes is not a {net} address"
        )

    return f"not a valid {net} address"


def autofill_hints(invalid: list, valid: set) -> dict:
    """Spot runs of invalid addresses that differ only in a trailing number
    (a spreadsheet's autofill dragged the cell down). ``invalid`` is a list of
    addresses, ``valid`` the pack's valid ones. Returns address -> hint."""
    groups = {}
    for address in invalid:
        stem = address.rstrip("0123456789")
        if len(stem) >= 20 and stem != address:
            groups.setdefault(stem, []).append(address)
    valid_by_stem = {}
    for address in valid:
        stem = address.rstrip("0123456789")
        if stem in groups:
            valid_by_stem.setdefault(stem, address)
    hints = {}
    for stem, members in groups.items():
        if len(members) < 3:
            continue
        hint = (
            f"one of {len(members)} addresses differing only in a trailing number "
            "(autofill?)"
        )
        if stem in valid_by_stem:
            hint += f"; the valid one in this pack is {valid_by_stem[stem]}"
        for address in members:
            hints[address] = hint
    return hints


_ADDRESS_LINE = re.compile(r"""^\s*(?:-\s+)?address:\s*['"]?([^'"#\s]+)""")


def address_line_numbers(path: str, addresses: set) -> dict:
    """First line number of each of ``addresses`` in the pack file at
    ``path``, for ``address:`` lines in block style. Others are missing."""
    found = {}
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for no, line in enumerate(f, 1):
                m = _ADDRESS_LINE.match(line)
                if m and m.group(1) in addresses and m.group(1) not in found:
                    found[m.group(1)] = no
    except OSError:
        pass
    return found
