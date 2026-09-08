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
* **P2PK addresses are NOT re-versioned here, deliberately.** They used to be:
  the LTC lake predates the 2026-06-15 P2PK fix, so v3 read ``1...`` where
  production has ``L...``. That repair now happens on the WRITE path
  (:func:`graphsense_v3.codec.reversion_address`, applied by the encode UDFs),
  and doing it here as well would hide the very thing it fixes -- a genuine v3
  version-byte bug would be normalised away before either side was compared.
* **Numbers are compared by value.** A ``varint`` arrives as ``Decimal`` from
  one path and ``int`` from another; ``1`` and ``1.0`` are the same balance.
There used to be a fourth, netting v3's gross transaction legs into v2's
per-transaction rows. It is gone because the WRITER now nets them
(:func:`graphsense_v3.spark.derived_utxo.net_legs`) -- the two backends agree on
the shape, so there is nothing left to reconcile.

Everything else is compared exactly. A normalisation added here is a claim that
a difference does not matter -- make it explicitly, or the harness starts
reporting agreement it has not established.
"""

from __future__ import annotations

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
    # Range-dependent. v2's keyspace is kept current by the delta updater while
    # a v3 keyspace is a snapshot at the block its backfill reached, so these
    # count different spans of chain and cannot agree until both cover the same
    # range. NOTE what this costs: a real counting bug in any of them is
    # invisible here, and only a range-matched comparison would catch it.
    "no_blocks": "range-dependent; v2 is live and v3 is a snapshot",
    "no_txs": "range-dependent; v2 is live and v3 is a snapshot",
    "no_address_relations": "range-dependent; v2 is live and v3 is a snapshot",
    # `timestamp` is deliberately NOT here. It is range-dependent on the
    # statistics response, but it is also on every transaction and block, and a
    # transaction timestamp mismatch is what exposed the direction bug --
    # ignoring it by NAME would blind this harness to its own best signal. See
    # CALL_IGNORED_FIELDS for where the statistics line is excused instead.
}

#: The same, but scoped to ONE call. A field can be incomparable in one
#: response and the harness's sharpest signal in another, and `IGNORED_FIELDS`
#: cannot say that -- it matches by name, everywhere. Keyed by the call label,
#: so a name excused here stays compared on every other call.
CALL_IGNORED_FIELDS: dict = {
    "get_currency_statistics": {
        "timestamp": "range-dependent; it is the LAST BLOCK's timestamp, and "
        "v2 is live where v3 is a snapshot -- the same reason as no_blocks",
    },
}


#: Fields whose PRESENCE is the contract but whose value is not. A paging token
#: is opaque and backend-specific -- v2's "49469955:1" and v3's tx_id cursor
#: cannot match and neither is wrong. What must agree is whether there IS
#: another page: comparing the values would report a difference on every paged
#: call, and dropping the field entirely would hide a backend that never pages
#: at all, which is a real bug this harness has already caught once.
PRESENCE_ONLY_FIELDS: dict = {
    "next_page": "an opaque paging token; only whether one exists is comparable",
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
    #: Wall-clock milliseconds for each side, when the caller measured them.
    #: Separate from correctness: a fast wrong answer is still wrong, so these
    #: never affect `agrees`.
    left_ms: Optional[float] = None
    right_ms: Optional[float] = None

    @property
    def agrees(self) -> bool:
        return self.skipped is None and not self.differences


def normalise(value: Any, network: str, ignored: Optional[dict] = None) -> Any:
    """A value with the known-incomparable differences flattened out."""
    ignored = IGNORED_FIELDS if ignored is None else ignored
    if isinstance(value, dict):
        return {
            key: (
                _presence(item)
                if key in PRESENCE_ONLY_FIELDS
                else normalise(item, network, ignored)
            )
            for key, item in value.items()
            if key not in ignored
        }
    if isinstance(value, (list, tuple)):
        return [normalise(item, network, ignored) for item in value]
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _presence(value: Any) -> Optional[str]:
    """ "<a token>" or None -- enough to compare, not enough to false-alarm."""
    return None if value in (None, "") else "<a token>"


def diff(
    left: Any,
    right: Any,
    network: str,
    *,
    path: str = "",
    ignored: Optional[dict] = None,
) -> list:
    """Every field where two normalised structures disagree.

    Lists are compared positionally: order is part of the answer for a
    transaction listing, and a harness that sorted them would hide a paging bug.
    """
    left = normalise(left, network, ignored)
    right = normalise(right, network, ignored)
    return _walk(left, right, path or "$")


#: What names an item of a list, most specific first. A length mismatch is
#: reported by IDENTITY where the items have one: "68 items" against "72 items"
#: says a page is short without saying which rows are missing, and that is the
#: one question such a difference always raises.
IDENTITY_FIELDS = ("identifier", "tx_hash", "address", "block_id", "height")

#: How many identities a length mismatch prints per side. A windowed backend
#: differs by hundreds of rows and the identities are then noise; a handful is
#: enough to see WHICH rows, and the count still says how many.
IDENTITY_SAMPLE = 6


def _identity(item: Any) -> Optional[str]:
    """What to call one item of a list, or None if it has no name."""
    if not isinstance(item, dict):
        return None
    for field_name in IDENTITY_FIELDS:
        value = item.get(field_name)
        if value not in (None, "", [], {}):
            return f"{field_name}={value}"
    return None


def _population(items: list, count: int) -> str:
    """``"N items"``, plus the first few identities when they exist."""
    named = [name for name in (_identity(item) for item in items) if name]
    if not named:
        return f"{count} items"
    shown = ", ".join(named[:IDENTITY_SAMPLE])
    if len(named) > IDENTITY_SAMPLE:
        shown += f", +{len(named) - IDENTITY_SAMPLE} more"
    return f"{count} items [{shown}]"


def _walk(left: Any, right: Any, path: str) -> list:
    if isinstance(left, dict) and isinstance(right, dict):
        out = []
        for key in sorted(set(left) | set(right)):
            out += _walk(left.get(key), right.get(key), f"{path}.{key}")
        return out
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return [
                Difference(
                    f"{path}[]",
                    _population(left, len(left)),
                    _population(right, len(right)),
                )
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
    excused = call_ignored_fields(label)
    found = sorted(_ignored_in(left, excused) | _ignored_in(right, excused))
    return Report(label, diff(left, right, network, ignored=excused), found)


def call_ignored_fields(label: str) -> dict:
    """`IGNORED_FIELDS` plus whatever THIS call excuses.

    The label carries its arguments (``get_address(0xaa)``), so the match is on
    the method name in front of them.
    """
    name = label.split("(")[0]
    return {**IGNORED_FIELDS, **CALL_IGNORED_FIELDS.get(name, {})}


def skipped(label: str, reason: str) -> Report:
    """A call that could not be made, recorded as neither agreement nor diff."""
    return Report(label, skipped=reason)


def _ignored_in(value: Any, ignored: Optional[dict] = None) -> set:
    ignored = IGNORED_FIELDS if ignored is None else ignored
    if isinstance(value, dict):
        found = {key for key in value if key in ignored}
        for item in value.values():
            found |= _ignored_in(item, ignored)
        return found
    if isinstance(value, (list, tuple)):
        found: set = set()
        for item in value:
            found |= _ignored_in(item, ignored)
        return found
    return set()


def _reason_for(name: str) -> str:
    """Why a field was not compared -- globally, or on the one call that
    excuses it."""
    if name in IGNORED_FIELDS:
        return IGNORED_FIELDS[name]
    for call, fields in CALL_IGNORED_FIELDS.items():
        if name in fields:
            return f"{fields[name]} (on {call} only)"
    return "no reason recorded, which is a bug in this harness"


def report(reports: list, notes: Optional[list] = None) -> str:
    """A readable summary. Agreement is stated with what it EXCLUDED, because
    'these agree' means nothing without knowing what was not compared.

    ``notes`` are caveats about the RUN rather than about a field -- a stubbed
    subsystem, say. They print at the top, where a pasted report cannot lose
    them, because a caveat that only lives in a CLI flag is one nobody
    remembers a week later.
    """
    lines = ["", "=" * 78, "v2 vs v3 service comparison", "=" * 78, ""]
    for note in notes or []:
        lines += [f"  !! {note}", ""]
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
            lines.append(f"  {name}: {_reason_for(name)}")
    if passed_over:
        lines += ["", "-" * 78, "NOT RUN", "-" * 78, ""]
        for entry in passed_over:
            lines.append(f"  {entry.label}: {entry.skipped}")
    lines += _timing_lines(reports)
    tail = f"{len(reports)} calls, {len(disagreed)} with differences"
    if passed_over:
        tail += f", {len(passed_over)} not run"
    lines += ["", "=" * 78, tail, "=" * 78, ""]
    return "\n".join(lines)


def _timing_lines(reports: list) -> list:
    """Median latency per call, v2 against v3.

    MEDIAN, not mean: one cold connection or one stalled node otherwise moves
    the number more than the backends differ. Reported per call NAME rather
    than per fixture, since one address is not a measurement.
    """
    import statistics
    from collections import defaultdict

    timed = defaultdict(lambda: ([], []))
    for entry in reports:
        if entry.left_ms is None or entry.right_ms is None:
            continue
        name = entry.label.split("(")[0]
        timed[name][0].append(entry.left_ms)
        timed[name][1].append(entry.right_ms)
    if not timed:
        return []

    lines = ["", "-" * 78, "TIMING (median ms, not a correctness signal)", "-" * 78, ""]
    lines.append(f"  {'call':<34}{'n':>4}{'v2':>10}{'v3':>10}{'v3/v2':>9}")
    for name in sorted(timed):
        left, right = timed[name]
        v2 = statistics.median(left)
        v3 = statistics.median(right)
        ratio = f"{v3 / v2:.2f}x" if v2 else "-"
        lines.append(f"  {name:<34}{len(left):>4}{v2:>10.1f}{v3:>10.1f}{ratio:>9}")
    lines.append("")
    # NOT unconditional. When the two sides ran different settings the caller
    # puts a note in `notes` saying which, and repeating "the difference is the
    # DAL" here would contradict it -- a silently false claim in the output is
    # worse than the distortion it describes.
    lines.append(
        "  Same cluster and same service stack on both sides; where the two ran "
        "different"
    )
    lines.append(
        "  settings it is stated above, and otherwise the difference is the DAL."
    )
    lines.append(
        "  v2 caches rates (alru_cache on get_rates), so repeated fixtures "
        "favour it slightly."
    )
    return lines


def summarise(reports: list) -> Optional[str]:
    """One line, for a caller that only wants the verdict."""
    if not reports:
        return None
    agreed = sum(1 for r in reports if r.agrees)
    passed_over = sum(1 for r in reports if r.skipped is not None)
    line = f"{agreed}/{len(reports)} calls agree"
    return f"{line}, {passed_over} not run" if passed_over else line
