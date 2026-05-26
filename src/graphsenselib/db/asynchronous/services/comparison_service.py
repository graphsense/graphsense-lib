"""Pairwise comparison of UTXO transactions."""

from __future__ import annotations

import asyncio
from typing import Any, Optional, Union

from graphsenselib.errors import BadUserInputException
from graphsenselib.db.asynchronous.services.common import cannonicalize_address
from graphsenselib.db.asynchronous.services.models import (
    ComparisonSignalInternal,
    ComparisonSummaryInternal,
    ComparisonVerdictInternal,
    LineageEdgeInternal,
    TransactionComparisonInternal,
    TxAccount,
    TxCharacteristicsInternal,
    TxComparedItemInternal,
    TxRef,
    TxUtxo,
)
from graphsenselib.utils.bitcoin import is_rbf_signaled
from graphsenselib.utils.rest_utils import is_eth_like


# ---------------------------------------------------------------------------
# Script-type derivation from address strings
# ---------------------------------------------------------------------------

# Address-string-prefix table. Witness presence is heuristic for v1: P2SH
# wraps may or may not carry witness data, so we mark it ambiguous and let
# the witness_present signal return inconclusive for that case.
_SCRIPT_TYPE_UNKNOWN = "UNKNOWN"


def script_type_from_address(addr: str) -> str:
    if not addr:
        return _SCRIPT_TYPE_UNKNOWN
    if addr == "coinbase":
        return "COINBASE"
    if addr.startswith("bc1q") or addr.startswith("tb1q"):
        return "P2WSH" if len(addr) > 50 else "P2WPKH"
    if addr.startswith("bc1p") or addr.startswith("tb1p"):
        return "P2TR"
    if addr.startswith("1") or addr.startswith("m") or addr.startswith("n"):
        return "P2PKH"
    if addr.startswith("3") or addr.startswith("2"):
        return "P2SH"
    return _SCRIPT_TYPE_UNKNOWN


def _has_witness_for_type(script_type: str) -> Optional[bool]:
    """True/False if determinable from script type alone, None if ambiguous."""
    if script_type in {"P2WPKH", "P2WSH", "P2TR"}:
        return True
    if script_type == "P2PKH":
        return False
    if script_type == "P2SH":
        return None
    return None


def _aggregate_inputs_have_witness(inputs: list, in_types: list[str]) -> Optional[bool]:
    """Per-tx witness presence. Prefers ground-truth ``has_witness`` from
    TxValue when at least one input carries it; falls back to script-type
    inference (which is None / inconclusive for P2SH).
    """
    flags = [getattr(i, "has_witness", None) for i in inputs]
    truth = [f for f in flags if f is not None]
    if truth:
        if all(f is True for f in truth):
            return True
        if all(f is False for f in truth):
            return False
        return None  # mixed across inputs of the same tx, treat as unresolvable

    inferred = [_has_witness_for_type(t) for t in in_types]
    if not inferred or any(f is None for f in inferred):
        return None
    if all(f is True for f in inferred):
        return True
    if all(f is False for f in inferred):
        return False
    return None


def _unique_script_types(addrs_per_io: list[list[str]]) -> list[str]:
    return list(
        dict.fromkeys(
            script_type_from_address(a) for addrs in addrs_per_io for a in addrs
        )
    )


# ---------------------------------------------------------------------------
# Characteristics extraction
# ---------------------------------------------------------------------------
def _aggregate_inputs_signal_rbf(inputs: list) -> Optional[bool]:
    if not inputs:
        return None
    sequences = [getattr(i, "sequence", None) for i in inputs]
    if all(s is None for s in sequences):
        return None
    return is_rbf_signaled(sequences)


def _consensus_change_addresses(tx: TxUtxo) -> list[str]:
    """Change addresses from the change-heuristics consensus, in order.

    Empty when no change heuristic is attached or the consensus is empty.
    Addresses returned raw (un-canonicalized); the caller canonicalizes.
    """
    h = tx.heuristics
    if h is None or h.change_heuristics is None:
        return []
    return [entry.output.address for entry in h.change_heuristics.consensus]


def _canonical_input_addresses(currency: str, tx: TxUtxo) -> list[str]:
    """Canonicalized input addresses, deduplicated, in order of first
    appearance. Inputs without an address (e.g., coinbase) are skipped."""
    return list(
        dict.fromkeys(
            cannonicalize_address(currency, inp.address[0])
            for inp in tx.inputs or []
            if inp is not None and inp.address
        )
    )


def _bip69_outputs_sorted(outputs: list) -> Optional[bool]:
    """True if outputs are strictly ascending by amount (BIP69-compatible).

    BIP69 ties on amount are broken by script_hex; the schema only stores
    script_hex for OP_RETURNs, so any tied amounts force ``None``.
    """
    if len(outputs) < 2:
        return None
    amounts = [o.value.value for o in outputs]
    has_tie = False
    for prev, curr in zip(amounts, amounts[1:]):
        if curr < prev:
            return False
        if curr == prev:
            has_tie = True
    return None if has_tie else True


def extract_characteristics(tx: TxUtxo) -> TxCharacteristicsInternal:
    inputs = tx.inputs or []
    outputs = tx.outputs or []
    in_types = _unique_script_types([i.address for i in inputs])
    out_types = _unique_script_types([o.address for o in outputs])
    inputs_have_witness = _aggregate_inputs_have_witness(inputs, in_types)
    inputs_signal_rbf = _aggregate_inputs_signal_rbf(inputs)
    bip69_outputs_sorted = _bip69_outputs_sorted(outputs)

    total_in = tx.total_input.value
    total_out = tx.total_output.value
    fee = total_in - total_out if not tx.coinbase else None

    coinjoin = tx.heuristics and tx.heuristics.coinjoin_heuristics
    cj_detected = bool(coinjoin and coinjoin.consensus and coinjoin.consensus.detected)
    cj_protocol = (
        coinjoin.consensus.sources[0]
        if cj_detected and coinjoin.consensus.sources
        else None
    )

    return TxCharacteristicsInternal(
        inputs_script_types=in_types,
        outputs_script_types=out_types,
        inputs_have_witness=inputs_have_witness,
        n_inputs=tx.no_inputs,
        n_outputs=tx.no_outputs,
        total_input_sat=total_in,
        total_output_sat=total_out,
        fee_sat=fee,
        tx_version=tx.version,
        locktime=tx.lock_time,
        inputs_signal_rbf=inputs_signal_rbf,
        block_height=tx.height,
        bip69_outputs_sorted=bip69_outputs_sorted,
        coinjoin_detected=cj_detected,
        coinjoin_protocol=cj_protocol,
    )


# ---------------------------------------------------------------------------
# Signal extractors
# ---------------------------------------------------------------------------
def signal_script_type(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    per_tx = [
        ",".join(sorted(c.inputs_script_types)) if c.inputs_script_types else None
        for c in chars
    ]
    distinct = {p for p in per_tx if p is not None}
    if len(distinct) <= 1 and None not in per_tx:
        verdict, weight = "match", 5
    elif None in per_tx:
        verdict, weight = "inconclusive", 0
    else:
        verdict, weight = "mismatch", -80
    return ComparisonSignalInternal(
        name="script_type",
        kind="discriminator",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


def signal_witness_present(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """Score signal: True/False per tx based on whether any input carries
    witness data. ``inconclusive`` when any tx's witness status is
    unresolvable (P2SH inputs without ground-truth ``has_witness``)."""
    per_tx_bool = [c.inputs_have_witness for c in chars]
    per_tx = [None if v is None else ("true" if v else "false") for v in per_tx_bool]
    distinct = {v for v in per_tx_bool if v is not None}
    if None in per_tx_bool:
        verdict, weight = "inconclusive", 0
    elif len(distinct) <= 1:
        verdict, weight = "match", 3
    else:
        verdict, weight = "mismatch", -20
    return ComparisonSignalInternal(
        name="witness_present",
        kind="score",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


def signal_tx_version(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    per_tx = [None if c.tx_version is None else f"v{c.tx_version}" for c in chars]
    distinct = {v for v in per_tx if v is not None}
    if None in per_tx:
        verdict, weight = "inconclusive", 0
    elif len(distinct) <= 1:
        verdict, weight = "match", 5
    else:
        verdict, weight = "mismatch", -30
    return ComparisonSignalInternal(
        name="tx_version",
        kind="discriminator",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


def signal_rbf(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    per_tx_bool = [c.inputs_signal_rbf for c in chars]
    per_tx = [None if v is None else ("rbf" if v else "final") for v in per_tx_bool]
    distinct = {v for v in per_tx_bool if v is not None}
    if None in per_tx_bool:
        verdict, weight = "inconclusive", 0
    elif len(distinct) <= 1:
        verdict, weight = "match", 3
    else:
        verdict, weight = "mismatch", -25
    return ComparisonSignalInternal(
        name="rbf",
        kind="discriminator",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


# Locktime values >= this are interpreted as unix timestamps (BIP65), not
# block heights; extremely rare in practice and never matches Core's
# anti-fee-sniping pattern.
_LOCKTIME_TIMESTAMP_THRESHOLD = 500_000_000

# Window (in blocks) within which a non-zero locktime below the tx's own
# height is treated as anti-fee-sniping. Bitcoin Core uses height (with up
# to 100 blocks subtracted in 10% of cases); Electrum is similar.
_ANTI_SNIPING_WINDOW = 100


def _classify_locktime(locktime: Optional[int], height: Optional[int]) -> Optional[str]:
    if locktime is None:
        return None
    if locktime == 0:
        return "zero"
    if (
        height is not None
        and 0 < locktime < _LOCKTIME_TIMESTAMP_THRESHOLD
        and locktime <= height
        and height - locktime <= _ANTI_SNIPING_WINDOW
    ):
        return "anti_sniping"
    return "other"


def signal_locktime_pattern(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    per_tx = [_classify_locktime(c.locktime, c.block_height) for c in chars]
    distinct = {v for v in per_tx if v is not None}
    if None in per_tx:
        verdict, weight = "inconclusive", 0
    elif len(distinct) <= 1:
        verdict, weight = "match", 4
    else:
        verdict, weight = "mismatch", -15
    return ComparisonSignalInternal(
        name="locktime_pattern",
        kind="discriminator",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


def signal_bip69_outputs_sorted(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """Soft signal: ``kind="score"`` rather than ``"discriminator"`` because
    output ordering can match BIP69 by accident (single output, or amounts
    that happen to be ascending), and many wallets simply don't enforce it.
    A mismatch shouldn't flip the cluster=different row to ``unlinked``.
    """
    per_tx_bool = [c.bip69_outputs_sorted for c in chars]
    per_tx = [
        None if v is None else ("sorted" if v else "unsorted") for v in per_tx_bool
    ]
    distinct = {v for v in per_tx_bool if v is not None}
    if None in per_tx_bool:
        verdict, weight = "inconclusive", 0
    elif len(distinct) <= 1:
        verdict, weight = "match", 2
    else:
        verdict, weight = "mismatch", -10
    return ComparisonSignalInternal(
        name="bip69_outputs_sorted",
        kind="score",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


def signal_exchange_input_overlap(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """Qualifier signal: ``match`` when every compared tx has at least one
    exchange-tagged input. That makes any apparent cluster overlap weak
    evidence (exchanges merge many users), so ``aggregate_verdict``
    demotes ``cluster=same`` rows when this signal matches.

    ``weight=0`` because the signal is informational; its effect is
    expressed as a verdict demotion rather than a score contribution.
    """
    per_tx_bool = [c.inputs_have_exchange for c in chars]
    per_tx = [
        None if v is None else ("exchange" if v else "non_exchange")
        for v in per_tx_bool
    ]
    if any(v is None for v in per_tx_bool):
        verdict = "inconclusive"
    elif all(v for v in per_tx_bool):
        verdict = "match"
    else:
        verdict = "mismatch"
    return ComparisonSignalInternal(
        name="exchange_input_overlap",
        kind="linkage",
        per_tx=per_tx,
        verdict=verdict,
        weight=0,
    )


def _connected_components(adj: list[set[int]]) -> int:
    """Count connected components in an undirected graph given by ``adj``."""
    n = len(adj)
    visited: set[int] = set()
    components = 0
    for start in range(n):
        if start in visited:
            continue
        components += 1
        stack = [start]
        while stack:
            v = stack.pop()
            if v in visited:
                continue
            visited.add(v)
            stack.extend(adj[v] - visited)
    return components


def signal_output_count_shape(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """Bucket each tx's output count into ``single`` (1), ``pay_plus_change``
    (2), or ``many`` (≥3). Most personal txs land in ``pay_plus_change``;
    sweeps in ``single``; exchange/batch payouts in ``many``. ``score``-kind
    because output count varies within a single actor (sweep vs. spend)."""

    def _bucket(n: int) -> str:
        if n == 1:
            return "single"
        if n == 2:
            return "pay_plus_change"
        return "many"

    per_tx = [_bucket(c.n_outputs) for c in chars]
    distinct = set(per_tx)
    if len(distinct) <= 1:
        verdict, weight = "match", 3
    else:
        verdict, weight = "mismatch", -10
    return ComparisonSignalInternal(
        name="output_count_shape",
        kind="score",
        per_tx=per_tx,
        verdict=verdict,
        weight=weight,
    )


def signal_direct_input_overlap(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """``match`` if the compared txs form a single connected component when
    edges are drawn between any two txs sharing at least one input address.
    Raw-fact stronger cousin of ``shared_cluster`` that bypasses cluster
    heuristics. Informational for v1 (``weight=0``)."""
    n = len(chars)
    sets = [set(c.input_addresses_canon) for c in chars]

    if any(not s for s in sets):
        per_tx = [None] * n
        return ComparisonSignalInternal(
            name="direct_input_overlap",
            kind="linkage",
            per_tx=per_tx,
            verdict="inconclusive",
            weight=0,
        )

    adj: list[set[int]] = [set() for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if sets[i] & sets[j]:
                adj[i].add(j)
                adj[j].add(i)

    per_tx: list[Optional[str]] = []
    for i in range(n):
        shared = set()
        for j in range(n):
            if i == j:
                continue
            shared |= sets[i] & sets[j]
        per_tx.append(",".join(sorted(shared)) if shared else None)

    verdict = "match" if n > 1 and _connected_components(adj) == 1 else "mismatch"
    return ComparisonSignalInternal(
        name="direct_input_overlap",
        kind="linkage",
        per_tx=per_tx,
        verdict=verdict,
        weight=0,
    )


def signal_change_chain(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """``match`` if any compared tx's heuristic change address is spent as
    an input by another compared tx. Strong continuation evidence even when
    cluster verdict is ``unknown``. Informational for v1 (``weight=0``)."""
    n = len(chars)
    changes = [set(c.change_addresses_canon) for c in chars]
    inputs = [set(c.input_addresses_canon) for c in chars]

    # all(empty), not any(empty) like sibling linkage signals: change addresses
    # come from a heuristic that legitimately picks nothing for many txs (no
    # consensus, single-output, coinjoin-shaped). Skipping the signal only when
    # *no* tx has a candidate avoids over-suppressing for the common case.
    if all(not s for s in changes):
        return ComparisonSignalInternal(
            name="change_chain",
            kind="linkage",
            per_tx=[None] * n,
            verdict="inconclusive",
            weight=0,
        )

    has_edge = False
    per_tx: list[Optional[str]] = []
    for i in range(n):
        consumed_by_others: set[str] = set()
        for j in range(n):
            if i == j:
                continue
            consumed_by_others |= changes[i] & inputs[j]
        if consumed_by_others:
            has_edge = True
        per_tx.append(
            ",".join(sorted(consumed_by_others)) if consumed_by_others else None
        )

    verdict = "match" if has_edge else "mismatch"
    return ComparisonSignalInternal(
        name="change_chain",
        kind="linkage",
        per_tx=per_tx,
        verdict=verdict,
        weight=0,
    )


def signal_common_ancestor(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """``match`` if any pair of compared txs share at least one one-hop
    ancestor tx (both spend outputs of the same parent tx). Different shape
    from ``utxo_linkage`` which captures direct A→B spending. Informational
    for v1 (``weight=0``)."""
    n = len(chars)
    parents = [set(c.parent_tx_hashes) for c in chars]

    if any(not s for s in parents):
        return ComparisonSignalInternal(
            name="common_ancestor",
            kind="linkage",
            per_tx=[None] * n,
            verdict="inconclusive",
            weight=0,
        )

    has_edge = False
    per_tx: list[Optional[str]] = []
    for i in range(n):
        shared = set()
        for j in range(n):
            if i == j:
                continue
            shared |= parents[i] & parents[j]
        if shared:
            has_edge = True
        per_tx.append(",".join(sorted(shared)) if shared else None)

    verdict = "match" if has_edge else "mismatch"
    return ComparisonSignalInternal(
        name="common_ancestor",
        kind="linkage",
        per_tx=per_tx,
        verdict=verdict,
        weight=0,
    )


def signal_utxo_linkage(
    chars: list[TxCharacteristicsInternal],
) -> ComparisonSignalInternal:
    """Linkage signal that's ``match`` iff every compared tx is reachable
    from every other via direct UTXO spending edges (treated as undirected).

    Reads ``utxo_parent_indexes`` populated during ``compare_txs`` orchestration.

    Informational for v1: ``weight=0`` and ``aggregate_verdict`` does not
    consume this signal. It's surfaced in the response so consumers can see
    the on-chain edges; future versions can promote it into the verdict.
    """
    n = len(chars)
    adj: list[set[int]] = [set() for _ in range(n)]
    for i, c in enumerate(chars):
        for j in c.utxo_parent_indexes:
            adj[i].add(j)
            adj[j].add(i)

    per_tx = [
        ",".join(str(j) for j in sorted(adj[i])) if adj[i] else None for i in range(n)
    ]

    verdict = "match" if _connected_components(adj) == 1 and n > 1 else "mismatch"
    return ComparisonSignalInternal(
        name="utxo_linkage",
        kind="linkage",
        per_tx=per_tx,
        verdict=verdict,
        weight=0,
    )


def signal_shared_cluster(
    chars: list[TxCharacteristicsInternal],
    cluster_verdict: str,
) -> ComparisonSignalInternal:
    """Primary linkage gate. Categorical: ``weight=0``. Cluster overlap is
    expressed via the verdict (match/mismatch/inconclusive); ``aggregate_verdict``
    consumes ``cluster_verdict`` directly to gate the ``linked`` tier."""
    per_tx = [
        ",".join(str(cid) for cid in sorted(c.input_cluster_ids))
        if c.input_cluster_ids
        else None
        for c in chars
    ]
    if cluster_verdict == "same":
        verdict = "match"
    elif cluster_verdict == "different":
        verdict = "mismatch"
    else:
        verdict = "inconclusive"
    return ComparisonSignalInternal(
        name="shared_cluster",
        kind="linkage",
        per_tx=per_tx,
        verdict=verdict,
        weight=0,
    )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def _usd_fiat(values) -> Optional[float]:
    """Pull the USD fiat amount off a Values object, or None if absent."""
    for fv in values.fiat_values:
        if fv.code.lower() == "usd":
            return fv.value
    return None


def build_summary(
    currency: str,
    txs: list[Union[TxUtxo, TxAccount]],
) -> ComparisonSummaryInternal:
    """Aggregate stats over the compared txs, derived straight from the tx
    headers (value, fee, counts, height, timestamp). Needs nothing from the
    characteristics, so the summary-only path can build it without fetching
    IO or running the analysis.

    Currency-aware. ``total_value`` is the queried currency's native base
    unit (UTXO: summed outputs; account: summed native transfers). Account
    token transfers (``token_tx_id`` set) carry no native-unit amount, so
    they are excluded from ``total_value`` and that exclusion is recorded in
    ``notes``. ``total_value_usd`` sums the USD fiat across every tx (native
    and token) and so is comparable across assets; if some txs lack a USD
    rate the available ones are summed and a note flags the partial total.
    ``total_fee`` stays in the native unit (gas is always native)."""
    notes: list[str] = []
    if is_eth_like(currency):
        native_txs = [t for t in txs if t.token_tx_id is None]
        total_value = sum(t.value.value for t in native_txs)
        fees = [t.fee.value for t in txs if t.fee is not None]
        total_inputs = None
        total_outputs = None
        usd_values = [_usd_fiat(t.value) for t in txs]
        n_token = len(txs) - len(native_txs)
        if n_token:
            notes.append(
                f"total_value covers native transfers only; {n_token} token "
                "transfer(s) excluded (their value is in total_value_usd)"
            )
    else:
        total_value = sum(t.total_output.value for t in txs)
        fees = [
            t.total_input.value - t.total_output.value for t in txs if not t.coinbase
        ]
        total_inputs = sum(t.no_inputs for t in txs)
        total_outputs = sum(t.no_outputs for t in txs)
        usd_values = [_usd_fiat(t.total_output) for t in txs]

    present = [v for v in usd_values if v is not None]
    n_missing = len(usd_values) - len(present)
    if not present:
        total_value_usd = None
        notes.append("total_value_usd unavailable: no USD rate for any tx")
    else:
        total_value_usd = sum(present)
        if n_missing:
            notes.append(
                f"total_value_usd is partial: {n_missing} of {len(usd_values)} "
                "txs had no USD rate"
            )

    return ComparisonSummaryInternal(
        tx_count=len(txs),
        currency=currency,
        total_value=total_value,
        total_value_usd=total_value_usd,
        total_fee=sum(fees) if fees else None,
        total_inputs=total_inputs,
        total_outputs=total_outputs,
        block_min=min(t.height for t in txs),
        block_max=max(t.height for t in txs),
        timestamp_min=min(t.timestamp for t in txs),
        timestamp_max=max(t.timestamp for t in txs),
        notes=notes,
    )


def compute_cluster_verdict(chars: list[TxCharacteristicsInternal]) -> str:
    """Return ``"same"`` if every tx shares at least one input cluster id,
    ``"different"`` if every tx has resolvable clusters but the intersection
    is empty, and ``"unknown"`` if any tx has no resolvable input cluster.
    """
    cluster_sets = [set(c.input_cluster_ids) for c in chars]
    if any(not s for s in cluster_sets):
        return "unknown"
    return "same" if set.intersection(*cluster_sets) else "different"


# Verdict thresholds on the weighted mismatch sum (``mis_w``):
#   * ``mis_w ≤ -60`` (or cluster=different) → ``likely_unlinked``
#   * ``mis_w < -30``                          → ``potential_unlink``
# Note: the strict inequality on ``potential_unlink`` leaves
# ``mis_w ∈ [-30, 0)`` (e.g., a single rbf -25 mismatch) outside any unlink
# tier; those weak negatives fall to ``inconclusive``.
_LIKELY_UNLINKED_THRESHOLD = -60
_POTENTIAL_UNLINK_THRESHOLD = -30


def _clamp_confidence(value: int) -> int:
    return max(0, min(100, value))


def aggregate_verdict(
    signals: list[ComparisonSignalInternal],
    chars: list[TxCharacteristicsInternal],
    cluster_verdict: str,
) -> ComparisonVerdictInternal:
    """Resolve verdict tier from signals + cluster state.

    Linkage signals are categorical gates, counted not weighted. Only
    discriminators and scores contribute to the weighted sums ``mis_w`` /
    ``match_w``. The verdict spectrum is selected by ``(linkage_count,
    mis_w, match_w, cluster_verdict, common_ancestor)``.
    """
    # Linkage gates: count categorically; ignore their weights.
    # ``cluster_verdict`` is the only source for the shared_cluster gate;
    # the ``shared_cluster`` signal is informational on the API response.
    linkage_signal_names = {
        s.name
        for s in signals
        if s.kind == "linkage" and s.verdict == "match" and s.name != "shared_cluster"
    }
    common_ancestor_match = "common_ancestor" in linkage_signal_names
    exchange_overlap = "exchange_input_overlap" in linkage_signal_names
    shared_cluster_gate = cluster_verdict == "same"
    # Add ``shared_cluster`` to the gate set when cluster=same; exclude
    # ``exchange_input_overlap`` because it's a demoting qualifier and must
    # not count toward promotion.
    gates = (
        linkage_signal_names | {"shared_cluster"}
        if shared_cluster_gate
        else linkage_signal_names
    )
    promotion_gates = gates - {"exchange_input_overlap"}
    linkage_count = len(promotion_gates)
    primary_linkage = shared_cluster_gate or common_ancestor_match

    # Weighted sums from discriminators + scores only.
    weighted = [s for s in signals if s.kind in ("discriminator", "score")]
    mis_w = sum(s.weight for s in weighted if s.verdict == "mismatch")
    match_w = sum(s.weight for s in weighted if s.verdict == "match")
    score_total = float(mis_w + match_w)

    discriminator_hits = [
        s.name for s in signals if s.kind == "discriminator" and s.verdict == "mismatch"
    ]
    promotion_hits = sorted(promotion_gates)

    notes: list[str] = []
    if any(c.coinjoin_detected for c in chars):
        notes.append("At least one tx is detected as a coinjoin")

    # Tier rules, applied in priority order; first match wins.

    # 1. unlinked: cluster=different + weighted mismatch.
    if cluster_verdict == "different" and mis_w < 0:
        relation = "unlinked"
        confidence = 95
        notes.append(
            "Cluster splits these txs and discriminators contradict; "
            "strong evidence of separate actors."
        )

    # 2. linked: primary linkage gate + clean fingerprint. Exchange overlap
    #    on cluster=same demotes to likely_linked (qualifier rule).
    elif primary_linkage and mis_w == 0:
        if exchange_overlap and cluster_verdict == "same":
            relation = "likely_linked"
            confidence = 65
            notes.append(
                "Cluster overlap is exchange-tagged; exchanges merge "
                "many users, so this is weak evidence of linkage."
            )
        else:
            relation = "linked"
            # Stronger weighted agreement nudges within the cap.
            confidence = _clamp_confidence(95 + match_w // 25)
            if cluster_verdict == "same":
                notes.append("All compared txs share at least one input cluster.")
            elif common_ancestor_match:
                notes.append(
                    "Compared txs share a one-hop ancestor: direct on-chain linkage."
                )

    # 3. likely_linked: any linkage gate fires. Includes the spec-gap case
    #    of cluster=same with a discriminator mismatch (cluster-merge or
    #    wallet upgrade) which lands here because mis_w != 0 above.
    elif linkage_count >= 1:
        relation = "likely_linked"
        if cluster_verdict == "same" and discriminator_hits:
            # More negative weighted evidence past -30 lowers confidence.
            confidence = _clamp_confidence(60 + (mis_w + 30) // 5)
            notes.append(
                "Cluster overlap despite discriminator mismatch: "
                "possible cluster-merge artifact or wallet upgrade."
            )
            if exchange_overlap:
                confidence = _clamp_confidence(confidence - 15)
                notes.append(
                    "Exchange-tagged inputs further weaken the cluster "
                    "overlap evidence."
                )
        elif cluster_verdict == "different":
            confidence = 65
            notes.append(
                f"Cluster splits these txs, but on-chain linkage "
                f"({', '.join(promotion_hits)}) supports a connection."
            )
        else:
            confidence = 60
            notes.append(
                f"On-chain linkage ({', '.join(promotion_hits)}) supports a connection."
            )

    # 4. likely_unlinked: cluster=different alone, or a strong negative
    #    weighted sum (≤ -60 per spec).
    elif cluster_verdict == "different" or mis_w <= _LIKELY_UNLINKED_THRESHOLD:
        relation = "likely_unlinked"
        confidence = 75 if mis_w <= _LIKELY_UNLINKED_THRESHOLD else 65

    # 5. potential_unlink: no linkage; mis_w < -30 (strict, per spec).
    elif mis_w < _POTENTIAL_UNLINK_THRESHOLD:
        relation = "potential_unlink"
        confidence = 50

    # 6. potential_link: no linkage; mis_w == 0; match_w > 0. Discriminator /
    #    score agreement alone cannot cross into likely_linked without an
    #    actual on-chain linkage gate.
    elif match_w > 0:
        relation = "potential_link"
        confidence = 35

    # 7. inconclusive: nothing fired in either direction.
    else:
        relation = "inconclusive"
        confidence = 30

    return ComparisonVerdictInternal(
        relation=relation,
        confidence=confidence,
        cluster_verdict=cluster_verdict,
        discriminator_hits=discriminator_hits,
        score_total=score_total,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Cluster lookup
# ---------------------------------------------------------------------------


async def _fetch_input_address_clusters(
    db: Any,
    currency: str,
    txs: list[TxUtxo],
) -> dict[str, int]:
    """Batch-resolve all input addresses across the compared txs to cluster ids.

    Mirrors the pattern used by ``_prefetch_addresses`` in heuristics_service.py
    but operates across multiple txs at once. Returns a map of canonical
    address → ``cluster_id`` (``-1`` if unresolved).
    """
    unique: set[str] = set()
    for tx in txs:
        for inp in tx.inputs or []:
            if inp is None or not inp.address:
                continue
            unique.add(cannonicalize_address(currency, inp.address[0]))

    if not unique:
        return {}

    addrs = list(unique)
    info = await db.get_addresses_light(currency, addrs)
    return {a: ((info.get(a) or {}).get("cluster_id", -1) or -1) for a in addrs}


async def _fetch_input_address_exchange_flags(
    txs_service: Any,
    currency: str,
    txs: list[TxUtxo],
    tagstore_groups: list[str],
) -> dict[str, bool]:
    """Return ``{canonical_address → is_exchange}`` for every input address
    seen across the compared txs. ``True`` means ``broad_category == "exchange"``.

    Returns an empty dict when no tags service is wired up; the caller then
    treats per-tx ``inputs_have_exchange`` as ``None`` (signal becomes
    inconclusive). Mirrors the pattern in ``_any_input_is_exchange`` in
    heuristics_service.py.
    """
    tags_service = getattr(txs_service, "tags_service", None)
    if tags_service is None:
        return {}

    unique: set[str] = set()
    for tx in txs:
        for inp in tx.inputs or []:
            if inp is None or not inp.address:
                continue
            unique.add(cannonicalize_address(currency, inp.address[0]))
    if not unique:
        return {}

    addrs = list(unique)
    summaries = await tags_service.get_tag_summaries_by_subject_ids(
        currency,
        addrs,
        tagstore_groups=tagstore_groups,
        include_best_cluster_tag=True,
    )
    return {
        a: bool(s is not None and s.broad_category == "exchange")
        for a, s in summaries.items()
    }


def _inputs_have_exchange_for_tx(
    currency: str, tx: TxUtxo, addr_to_is_exchange: dict[str, bool]
) -> Optional[bool]:
    """Per-tx flag: True if any input address is exchange-tagged. None when
    no input addresses had any tag info (signal is inconclusive)."""
    if not addr_to_is_exchange:
        return None
    saw_any = False
    for inp in tx.inputs or []:
        if inp is None or not inp.address:
            continue
        canon = cannonicalize_address(currency, inp.address[0])
        if canon in addr_to_is_exchange:
            saw_any = True
            if addr_to_is_exchange[canon]:
                return True
    return False if saw_any else None


async def _fetch_parent_refs(
    txs_service: Any, currency: str, tx_hashes: list[str]
) -> list[list[TxRef]]:
    """For each compared tx, return the spending refs for the one-hop ancestor
    outputs it directly spends. One ``get_spending_txs`` call per tx, run
    concurrently. Refs pointing back to the compared tx itself are filtered out.

    Raw refs (not deduplicated): each ref carries the spent output index and
    the spending input index, which lineage edges need. Hash-level dedup for
    ``parent_tx_hashes`` is done downstream by ``_parent_hashes_from_refs``.
    """

    async def refs_of(h: str) -> list[TxRef]:
        refs = await txs_service.get_spending_txs(currency, h, None)
        return [ref for ref in refs if ref.tx_hash != h]

    return await asyncio.gather(*[refs_of(h) for h in tx_hashes])


def _parent_hashes_from_refs(refs_per_tx: list[list[TxRef]]) -> list[list[str]]:
    """Deduplicated, order-preserving parent tx hashes per compared tx.

    Keeps ancestors outside the compared set; ``signal_common_ancestor``
    matches on external ancestors too. Self references are already filtered
    by ``_fetch_parent_refs``.
    """
    return [list(dict.fromkeys(ref.tx_hash for ref in refs)) for refs in refs_per_tx]


def _lineage_edges_from_refs(
    refs_per_tx: list[list[TxRef]], tx_hashes: list[str]
) -> list[LineageEdgeInternal]:
    """Build ``output_spent_by_input`` lineage edges between compared txs.

    ``refs_per_tx[i]`` are the spending refs for compared tx ``i`` (the
    spender): each names a parent tx whose output ``output_index`` is consumed
    by input ``input_index`` of tx ``i``. Edges are restricted to parents that
    are themselves in the compared set; self references are dropped. Each ref
    yields a distinct edge, so a tx spending several outputs of the same parent
    produces one edge per spent output, each with its own io indexes.
    """
    h_to_idx = {h: i for i, h in enumerate(tx_hashes)}
    edges: list[LineageEdgeInternal] = []
    for i, refs in enumerate(refs_per_tx):
        for ref in refs:
            j = h_to_idx.get(ref.tx_hash)
            if j is None or j == i:
                continue
            edges.append(
                LineageEdgeInternal(
                    from_idx=j,
                    to_idx=i,
                    kind="output_spent_by_input",
                    out_index=ref.output_index,
                    in_index=ref.input_index,
                )
            )
    return edges


def _utxo_parent_indexes_from_hashes(
    parent_hashes: list[list[str]], tx_hashes: list[str]
) -> list[list[int]]:
    """Project per-tx parent hashes onto compared-tx indexes, in order of
    first appearance, with self-references already filtered out upstream."""
    h_to_idx = {h: i for i, h in enumerate(tx_hashes)}
    return [
        list(
            dict.fromkeys(
                j for h in parents if (j := h_to_idx.get(h)) is not None and j != i
            )
        )
        for i, parents in enumerate(parent_hashes)
    ]


def _input_cluster_ids_for_tx(
    currency: str, tx: TxUtxo, addr_to_cluster: dict[str, int]
) -> list[int]:
    """Distinct cluster ids for the input addresses of a single tx, in
    order of first appearance. Unresolved addresses (cluster_id == -1)
    are dropped."""
    cids = (
        addr_to_cluster.get(cannonicalize_address(currency, inp.address[0]), -1)
        for inp in tx.inputs or []
        if inp is not None and inp.address
    )
    return list(dict.fromkeys(cid for cid in cids if cid != -1))


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------
async def compare_txs(
    txs_service,
    currency: str,
    tx_hashes: list[str],
    include_details: bool,
    include_characteristics: bool,
    include_signals: bool,
    tagstore_groups: list[str],
    include_analysis: bool = True,
) -> TransactionComparisonInternal:
    # Dedup hashes (order-preserving) up front: a repeated hash would otherwise
    # be fetched twice, double-counted in the summary, and trivially compare as
    # linked to itself. Need 2+ distinct txs to have anything to compare.
    tx_hashes = list(dict.fromkeys(tx_hashes))
    if len(tx_hashes) < 2:
        raise BadUserInputException(
            "/txs/compare needs at least 2 distinct transaction hashes."
        )

    # The fingerprinting analysis (signals, lineage, verdict) and the per-tx
    # characteristics are UTXO-only. Account chains (ETH/TRX) are supported in
    # summary-only mode (include_analysis=False), where we just aggregate tx
    # headers; characteristics stay off for them.
    account_like = is_eth_like(currency)
    if account_like and include_analysis:
        raise BadUserInputException(
            f"/txs/compare fingerprinting analysis is UTXO-only; '{currency}' "
            "is account-based. Set include_analysis=false for a summary."
        )
    want_characteristics = include_characteristics and not account_like

    # IO + heuristics are only needed to build characteristics or run the
    # analysis; a pure summary-only request fetches tx headers alone. Account
    # txs have no IO decomposition or heuristics, so they stay header-only.
    need_io = (include_analysis or want_characteristics) and not account_like
    fetched: list[Union[TxUtxo, TxAccount]] = await asyncio.gather(
        *[
            txs_service.get_tx(
                currency,
                h,
                None,
                include_io=need_io,
                include_nonstandard_io=False,
                include_io_index=False,
                include_heuristics=["all_coinjoin", "all_change"] if need_io else [],
                tagstore_groups=tagstore_groups,
            )
            for h in tx_hashes
        ]
    )

    # Summary-only mode: skip the expensive orchestration (cluster lookups,
    # spending-tx fetches, exchange-tag lookups), the signals, and the
    # verdict. Characteristics are still built when requested (they only
    # need the per-tx data already fetched), but their orchestration-derived
    # fields (cluster ids, parents, canon addresses) stay empty.
    if not include_analysis:
        chars_only = (
            [extract_characteristics(tx) for tx in fetched]
            if want_characteristics
            else None
        )
        items = [
            TxComparedItemInternal(
                tx_hash=tx_hashes[i],
                characteristics=chars_only[i] if chars_only is not None else None,
                details=fetched[i] if include_details else None,
            )
            for i in range(len(fetched))
        ]
        # The summary must reflect all asset movements. For account chains
        # ``get_tx`` returns only the base/native transaction (no token legs),
        # so its token transfers would be invisible to ``build_summary`` and
        # their USD never folded into ``total_value_usd``. Fetch the full
        # asset-flow set per hash (base tx + token transfers) instead, so
        # ``build_summary`` can sum token USD and flag the excluded token
        # transfers. UTXO txs carry their full IO already, so reuse ``fetched``.
        if account_like:
            flow_lists = await asyncio.gather(
                *[
                    txs_service.get_asset_flows_within_tx(
                        currency,
                        h,
                        include_internal_txs=False,
                        include_token_txs=True,
                        include_base_transaction=True,
                    )
                    for h in tx_hashes
                ]
            )
            summary_txs = [leg for fl in flow_lists for leg in fl.txs]
        else:
            summary_txs = fetched
        return TransactionComparisonInternal(
            txs=items,
            signals=[],
            lineage=[],
            summary=build_summary(currency, summary_txs),
            verdict=None,
        )

    addr_to_cluster, parent_refs, addr_to_is_exchange = await asyncio.gather(
        _fetch_input_address_clusters(txs_service.db, currency, fetched),
        _fetch_parent_refs(txs_service, currency, tx_hashes),
        _fetch_input_address_exchange_flags(
            txs_service, currency, fetched, tagstore_groups
        ),
    )
    parent_hashes = _parent_hashes_from_refs(parent_refs)
    parent_indexes = _utxo_parent_indexes_from_hashes(parent_hashes, tx_hashes)
    lineage = _lineage_edges_from_refs(parent_refs, tx_hashes)

    chars: list[TxCharacteristicsInternal] = []
    for i, tx in enumerate(fetched):
        c = extract_characteristics(tx)
        c.input_addresses_canon = _canonical_input_addresses(currency, tx)
        c.change_addresses_canon = [
            cannonicalize_address(currency, a) for a in _consensus_change_addresses(tx)
        ]
        c.parent_tx_hashes = parent_hashes[i]
        c.input_cluster_ids = _input_cluster_ids_for_tx(currency, tx, addr_to_cluster)
        c.utxo_parent_indexes = parent_indexes[i]
        c.inputs_have_exchange = _inputs_have_exchange_for_tx(
            currency, tx, addr_to_is_exchange
        )
        chars.append(c)

    # Signals always computed; the verdict aggregator depends on them.
    # ``include_signals`` only suppresses returning them on the response.
    cluster_verdict = compute_cluster_verdict(chars)
    signals = [
        signal_script_type(chars),
        signal_witness_present(chars),
        signal_tx_version(chars),
        signal_rbf(chars),
        signal_locktime_pattern(chars),
        signal_bip69_outputs_sorted(chars),
        signal_output_count_shape(chars),
        signal_shared_cluster(chars, cluster_verdict),
        signal_exchange_input_overlap(chars),
        signal_direct_input_overlap(chars),
        signal_change_chain(chars),
        signal_common_ancestor(chars),
        signal_utxo_linkage(chars),
    ]
    summary = build_summary(currency, fetched)
    verdict = aggregate_verdict(signals, chars, cluster_verdict)

    items = [
        TxComparedItemInternal(
            tx_hash=tx_hashes[i],
            characteristics=chars[i] if include_characteristics else None,
            details=fetched[i] if include_details else None,
        )
        for i in range(len(fetched))
    ]

    return TransactionComparisonInternal(
        txs=items,
        signals=signals if include_signals else [],
        lineage=lineage,
        summary=summary,
        verdict=verdict,
    )
