"""Pairwise comparison of UTXO transactions."""

from __future__ import annotations

import asyncio
from typing import Any, Optional, Union

from graphsenselib.errors import BadUserInputException
from graphsenselib.db.asynchronous.services.common import (
    cannonicalize_address,
    canonical_tx_hash,
    dedup_refs,
)
from graphsenselib.db.asynchronous.services.models import (
    ComparisonSignalInternal,
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

# Work bound for a single compare request: the address and cluster
# prefetches scale with the combined IO count of the fetched txs, which the
# ref-count cap alone does not bound (one consolidation tx can carry 20k
# inputs).
_MAX_TOTAL_IOS = 20_000


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
    elif mis_w == 0 and match_w > 0:
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
    addr_to_cluster: dict[str, int],
    tagstore_groups: list[str],
) -> dict[str, bool]:
    """Return ``{canonical_address → is_exchange}`` for every input address.

    ``True`` iff the address's cluster has at least one cluster-definer tag
    with ``concept_id="exchange"``. Uses the cheap existence query
    ``which_clusters_have_concept`` instead of the cluster-size-dependent
    ``best_cluster_tag`` digest path: the older implementation pulled a full
    ``TagSummary`` per address only to read ``broad_category``, which on a
    23M-address exchange cluster cost ~2 s per request.

    Semantic shift vs the previous implementation: the previous check fired
    only when the **weighted-most-common** concept across an address's tags
    was ``"exchange"``; this one fires when **any** cluster-definer tag on
    the cluster carries that concept. More inclusive, which is the right
    direction for the demoting-qualifier caller
    (``signal_exchange_input_overlap``): if there's meaningful evidence the
    cluster is an exchange, the shared-cluster linkage signal should be
    weakened.

    Returns an empty dict when no tags service is wired up; the caller then
    treats per-tx ``inputs_have_exchange`` as ``None`` (signal inconclusive).
    """
    tags_service = getattr(txs_service, "tags_service", None)
    if tags_service is None or not addr_to_cluster:
        return {}

    # cluster_id == -1 marks "unresolved" (set in _fetch_input_address_clusters);
    # filter those out so we don't pass garbage ids to the tagstore.
    unique_cluster_ids = list(
        {c for c in addr_to_cluster.values() if c is not None and c >= 0}
    )
    if not unique_cluster_ids:
        return {a: False for a in addr_to_cluster}

    exchange_clusters = await tags_service.which_clusters_have_concept(
        currency, unique_cluster_ids, tagstore_groups, "exchange"
    )
    return {a: (c in exchange_clusters) for a, c in addr_to_cluster.items()}


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
    include_lineage: bool = True,
    include_verdict: bool = True,
) -> TransactionComparisonInternal:
    # Canonicalize spellings up front (lowercase, no 0x): the db returns
    # parent refs in lowercase hex, so the h_to_idx maps built for lineage
    # and utxo_linkage must be keyed on the same form, and a repeated hash
    # would otherwise be fetched twice and trivially compare as linked to
    # itself. Need 2+ distinct txs to have anything to compare.
    tx_hashes = dedup_refs([canonical_tx_hash(h) for h in tx_hashes], key=lambda h: h)
    if len(tx_hashes) < 2:
        raise BadUserInputException(
            "/graph/compare needs at least 2 distinct transaction hashes."
        )

    # The fingerprinting analysis (signals, lineage, verdict) is BTC-only:
    # several signals (coinjoin variants, exchange overlap, change heuristics)
    # are tuned to BTC. Other chains use POST /graph/summary for
    # chain-agnostic aggregate stats over a set of transactions.
    if currency.lower() != "btc":
        raise BadUserInputException(
            f"/graph/compare is BTC-only; '{currency}' is not supported. "
            "Use /graph/summary for aggregate stats."
        )

    fetched: list[Union[TxUtxo, TxAccount]] = await asyncio.gather(
        *[
            txs_service.get_tx(
                currency,
                h,
                None,
                include_io=True,
                include_nonstandard_io=False,
                include_io_index=False,
                include_heuristics=["all_coinjoin", "all_change"],
                tagstore_groups=tagstore_groups,
                trace_account_chains=True,
            )
            for h in tx_hashes
        ]
    )

    total_ios = sum(
        t.no_inputs + t.no_outputs for t in fetched if isinstance(t, TxUtxo)
    )
    if total_ios > _MAX_TOTAL_IOS:
        raise BadUserInputException(
            f"transaction set too large to compare: {total_ios} combined "
            f"inputs and outputs exceed the limit of {_MAX_TOTAL_IOS}."
        )

    # Exchange-flag lookup now needs the address→cluster map, so it can't run
    # in parallel with cluster resolution like the old digest-based path did.
    # Resolve clusters and parent refs in parallel first, then derive the
    # exchange flags. The new lookup is cheap (existence query) so the lost
    # parallelism costs ~tens of ms vs the seconds saved by avoiding the
    # ``best_cluster_tag`` digest path.
    addr_to_cluster, parent_refs = await asyncio.gather(
        _fetch_input_address_clusters(txs_service.db, currency, fetched),
        _fetch_parent_refs(txs_service, currency, tx_hashes),
    )
    addr_to_is_exchange = await _fetch_input_address_exchange_flags(
        txs_service, currency, addr_to_cluster, tagstore_groups
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
    verdict = aggregate_verdict(signals, chars, cluster_verdict)

    items = [
        TxComparedItemInternal(
            tx_hash=tx_hashes[i],
            network=currency,
            characteristics=chars[i] if include_characteristics else None,
            details=fetched[i] if include_details else None,
        )
        for i in range(len(fetched))
    ]

    # Signals/lineage/verdict are always computed (the verdict depends on
    # the signals); the include flags only control what is returned. None
    # marks "excluded from the request" so the response omits the field,
    # keeping it distinguishable from computed-but-empty.
    return TransactionComparisonInternal(
        txs=items,
        signals=signals if include_signals else None,
        lineage=lineage if include_lineage else None,
        verdict=verdict if include_verdict else None,
    )
