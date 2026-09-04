"""Comparing v2 and v3 output field by field.

The goal of the v3 work is that the REST layer produces the same answers from
either backend. This module is what decides whether two answers *are* the same,
which is less obvious than it sounds: several fields cannot agree by
construction, and comparing them raw would drown the real differences.

Three normalisations, each for a difference that is known and explained:

* **Surrogate ids are dropped.** v2's ``address_id`` is assigned at write time;
  v3 has none (removing it is what removed the BTC int32 ceiling), so
  :mod:`graphsense_v3.db.legacy` synthesises one. The two cannot match and
  neither is wrong. Same for ``tx_id``: v2's is dense and sequential, v3's is
  ``(block_id << 32) + index``. The transaction HASH is the comparable
  identity, and it is compared.
* **P2PK addresses are re-versioned.** The LTC lake predates the 2026-06-15
  network-aware P2PK fix (c103323c), so v3 reads ``1...`` where production has
  ``L...`` -- the same hash160 under BTC's version byte instead of LTC's. Both
  sides are re-versioned to the network's own byte before comparing, so the
  ~28% of early-chain addresses affected do not swamp the report. Remove this
  once the lake is re-ingested; it is a workaround, not a rule.
* **Numbers are compared by value.** A ``varint`` arrives as ``Decimal`` from
  one path and ``int`` from another; ``1`` and ``1.0`` are the same balance.

Everything else is compared exactly. A normalisation added here is a claim that
a difference does not matter -- make it explicitly, or the harness starts
reporting agreement it has not established.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

#: Base58 alphabet, in the order the checksum encoding uses.
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

#: Fields whose values cannot agree between the two backends, with the reason.
#: An entry is a DECISION; a field silently missing from a comparison is a bug.
IGNORED_FIELDS: dict = {
    "address_id": "v2 assigns a surrogate id at write time; v3 keys on the bytes",
    "address_id_group": "derived from address_id, so equally arbitrary",
    "cluster_id": "v3 has no clusters yet (D9)",
    "entity": "the entity id, same reason",
    "entity_id": "the entity id, same reason",
    "tx_id": "v2's is dense and sequential, v3's is (block_id << 32) + index -- "
    "the tx HASH is the comparable identity and IS compared",
    "no_addresses": "a cluster property",
    "no_entities": "v3 has no clusters yet (D9); the adapter reports 0 so the "
    "model can be built, which is not a measurement",
}


@dataclass
class Difference:
    """One field that did not match."""

    path: str
    left: Any
    right: Any

    def __str__(self) -> str:
        return f"{self.path}: v2={self.left!r} v3={self.right!r}"


@dataclass
class Report:
    """The outcome of comparing one call.

    ``skipped`` carries the reason a call could not be made at all -- v3 has no
    cluster tables yet, so nine of the adapter's methods raise. A skipped call
    is NOT an agreement, and `agrees` says so: counting it as one would let the
    report claim parity for features that were never exercised.
    """

    label: str
    differences: list = field(default_factory=list)
    ignored: list = field(default_factory=list)
    skipped: Optional[str] = None

    @property
    def agrees(self) -> bool:
        return self.skipped is None and not self.differences


def _b58decode(text: str) -> bytes:
    number = 0
    for char in text:
        number = number * 58 + _B58.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big")
    return b"\x00" * (len(text) - len(text.lstrip("1"))) + raw


def _b58encode(raw: bytes) -> str:
    number = int.from_bytes(raw, "big")
    out = ""
    while number:
        number, remainder = divmod(number, 58)
        out = _B58[remainder] + out
    return "1" * (len(raw) - len(raw.lstrip(b"\x00"))) + out


def _b58check(version: bytes, payload: bytes) -> str:
    body = version + payload
    digest = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    return _b58encode(body + digest)


def reversion_address(network: str, address: str) -> str:
    """A base58 address re-encoded with ``network``'s own P2PKH version byte.

    Leaves alone anything that is not a base58check address with a *different*
    version: bech32 passes through, and so does an address already carrying the
    right byte. Only the hash160 is preserved, which is the part both backends
    agree on -- the version byte is exactly what the stale lake gets wrong.
    """
    from graphsenselib.ingest.rpc_utxo import _PUBKEY_ADDRESS_VERSION

    want = _PUBKEY_ADDRESS_VERSION.get(network.lower())
    if want is None or not address or not all(c in _B58 for c in address):
        return address
    try:
        raw = _b58decode(address)
    except ValueError:
        return address
    if len(raw) < 5:
        return address
    body, checksum = raw[:-4], raw[-4:]
    if hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4] != checksum:
        return address  # not base58check; leave it exactly as it is
    if body[: len(want)] == want:
        return address

    # Only rewrite a version that is ANOTHER NETWORK'S P2PKH byte. Rewriting on
    # length alone is wrong: LTC's P2SH address is also one version byte plus a
    # 20-byte hash, so a length test turns a valid P2SH address into a valid,
    # different P2PKH one -- a wrong answer rather than a reported mismatch.
    for other in _PUBKEY_ADDRESS_VERSION.values():
        if body[: len(other)] == other and len(body) == len(other) + 20:
            return _b58check(want, body[len(other) :])
    return address


def normalise(value: Any, network: str) -> Any:
    """A value with the known-incomparable differences flattened out."""
    if isinstance(value, dict):
        return {
            key: normalise(item, network)
            for key, item in value.items()
            if key not in IGNORED_FIELDS
        }
    if isinstance(value, (list, tuple)):
        return [normalise(item, network) for item in value]
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        return reversion_address(network, value)
    return value


def diff(left: Any, right: Any, network: str, *, path: str = "") -> list:
    """Every field where two normalised structures disagree.

    Lists are compared positionally: order is part of the answer for a
    transaction listing, and a harness that sorted them would hide a paging bug.
    """
    left = normalise(left, network)
    right = normalise(right, network)
    return _walk(left, right, path or "$")


def _walk(left: Any, right: Any, path: str) -> list:
    if isinstance(left, dict) and isinstance(right, dict):
        out = []
        for key in sorted(set(left) | set(right)):
            out += _walk(left.get(key), right.get(key), f"{path}.{key}")
        return out
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return [
                Difference(f"{path}[]", f"{len(left)} items", f"{len(right)} items")
            ]
        out = []
        for index, (a, b) in enumerate(zip(left, right)):
            out += _walk(a, b, f"{path}[{index}]")
        return out
    if left != right:
        return [Difference(path, left, right)]
    return []


def compare(label: str, left: Any, right: Any, network: str) -> Report:
    """One call, compared. ``left`` is v2, ``right`` is v3."""
    ignored = sorted(_ignored_in(left) | _ignored_in(right))
    return Report(label, diff(left, right, network), ignored)


def skipped(label: str, reason: str) -> Report:
    """A call that could not be made, recorded as neither agreement nor diff."""
    return Report(label, skipped=reason)


def _ignored_in(value: Any) -> set:
    if isinstance(value, dict):
        found = {key for key in value if key in IGNORED_FIELDS}
        for item in value.values():
            found |= _ignored_in(item)
        return found
    if isinstance(value, (list, tuple)):
        found: set = set()
        for item in value:
            found |= _ignored_in(item)
        return found
    return set()


def report(reports: list) -> str:
    """A readable summary. Agreement is stated with what it EXCLUDED, because
    'these agree' means nothing without knowing what was not compared."""
    lines = ["", "=" * 78, "v2 vs v3 service comparison", "=" * 78, ""]
    passed_over = [r for r in reports if r.skipped is not None]
    disagreed = [r for r in reports if not r.agrees and r.skipped is None]
    for entry in reports:
        if entry.skipped is not None:
            lines.append(f"  skip  {entry.label}  ({entry.skipped})")
            continue
        mark = "ok  " if entry.agrees else "DIFF"
        lines.append(f"  {mark}  {entry.label}  ({len(entry.differences)} differences)")
    if disagreed:
        lines += ["", "-" * 78, "DIFFERENCES", "-" * 78]
        for entry in disagreed:
            lines.append(f"\n  {entry.label}")
            for difference in entry.differences:
                lines.append(f"    {difference}")
    excluded: set = set()
    for entry in reports:
        excluded |= set(entry.ignored)
    if excluded:
        lines += ["", "-" * 78, "NOT COMPARED", "-" * 78, ""]
        for name in sorted(excluded):
            lines.append(f"  {name}: {IGNORED_FIELDS[name]}")
    if passed_over:
        lines += ["", "-" * 78, "NOT RUN", "-" * 78, ""]
        for entry in passed_over:
            lines.append(f"  {entry.label}: {entry.skipped}")
    tail = f"{len(reports)} calls, {len(disagreed)} with differences"
    if passed_over:
        tail += f", {len(passed_over)} not run"
    lines += ["", "=" * 78, tail, "=" * 78, ""]
    return "\n".join(lines)


def summarise(reports: list) -> Optional[str]:
    """One line, for a caller that only wants the verdict."""
    if not reports:
        return None
    agreed = sum(1 for r in reports if r.agrees)
    passed_over = sum(1 for r in reports if r.skipped is not None)
    line = f"{agreed}/{len(reports)} calls agree"
    return f"{line}, {passed_over} not run" if passed_over else line
