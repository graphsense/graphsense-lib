"""API models for transaction comparison.

Internal counterparts live in db/asynchronous/services/models/__init__.py
(``*Internal`` suffix). The translator at web/translators.py:
to_api_transaction_comparison maps internal -> API. To add a field that
should reach the API, update all three.
"""

from typing import Literal, Optional, Union

from pydantic import Field

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.transactions import TxAccount, TxUtxo


SignalKind = Literal["discriminator", "score", "linkage"]
SignalVerdict = Literal["match", "mismatch", "inconclusive"]
ClusterVerdict = Literal["same", "different", "unknown"]
LineageKind = Literal[
    "output_spent_by_input",
    "shared_address",
    "shared_cluster",
]
ComparisonRelation = Literal[
    "linked",
    "likely_linked",
    "potential_link",
    "inconclusive",
    "potential_unlink",
    "likely_unlinked",
    "unlinked",
]


class TxCharacteristics(APIModel):
    """Extracted characteristics for a single transaction.

    ``input_script_types`` / ``output_script_types`` hold the distinct
    script types observed across the inputs/outputs, sorted for stable
    output. Empty list means none could be derived from address strings.

    Several internal fields are intentionally omitted from the API surface
    because the same information is exposed via the corresponding signals
    (``rbf``, ``witness_present``, ``bip69_outputs_sorted``,
    ``exchange_input_overlap``) and on-chain edge collections
    (``input_addresses_canon``, ``change_addresses_canon``,
    ``parent_tx_hashes``, ``utxo_parent_indexes``). Surface them here if a
    consumer needs the per-tx booleans alongside the comparison verdict.
    """

    input_script_types: list[str] = Field(default_factory=list)
    output_script_types: list[str] = Field(default_factory=list)
    n_inputs: int
    n_outputs: int
    total_input_sat: int
    total_output_sat: int
    fee_sat: Optional[int] = None
    tx_version: Optional[int] = None
    locktime: Optional[int] = None
    input_cluster_ids: list[int] = Field(default_factory=list)
    coinjoin_detected: bool = False
    coinjoin_protocol: Optional[str] = None


class ComparisonSignal(APIModel):
    """One row of the pairwise comparison table; values stringified per tx."""

    name: str
    kind: SignalKind
    per_tx: list[Optional[str]]
    verdict: SignalVerdict
    weight: int = 0


class LineageEdge(APIModel):
    """Direct on-chain relationship between two compared transactions."""

    from_idx: int
    to_idx: int
    kind: LineageKind
    out_index: Optional[int] = None
    in_index: Optional[int] = None


class ComparisonSummary(APIModel):
    """Aggregate stats over all compared transactions.

    ``total_value`` and ``total_fee`` are in the chain's base unit (satoshi
    for UTXO, wei/sun for account chains); ``total_value`` sums native
    transfers only (token transfers carry no native-unit amount).
    ``total_value_usd`` sums the USD fiat value across all transfers,
    including tokens, so it is comparable across assets. ``total_inputs`` /
    ``total_outputs`` are UTXO-only and omitted for account-model (ETH/TRX)
    summaries. ``notes`` flags caveats (e.g. a partial USD total when some
    txs had no rate, or token transfers excluded from ``total_value``).
    """

    tx_count: int
    currency: str
    total_value: int
    total_value_usd: Optional[float] = None
    total_fee: Optional[int] = None
    total_inputs: Optional[int] = None
    total_outputs: Optional[int] = None
    block_min: int
    block_max: int
    timestamp_min: int
    timestamp_max: int
    notes: list[str] = Field(default_factory=list)


class ComparisonVerdict(APIModel):
    """Aggregator's opinion. Sub-verdicts kept independent.

    ``confidence`` and ``score_total`` are tentative, weights have not yet
    been calibrated against ground-truth data.
    """

    relation: ComparisonRelation
    confidence: int
    cluster_verdict: ClusterVerdict
    discriminator_hits: list[str] = Field(default_factory=list)
    score_total: float = 0.0
    notes: list[str] = Field(default_factory=list)


class TxComparedItem(APIModel):
    """Per-tx entry. ``characteristics`` is populated when
    ``include_characteristics`` is set (default true), ``details`` when
    ``include_details`` is set."""

    tx_hash: str
    characteristics: Optional[TxCharacteristics] = None
    details: Optional[Union[TxUtxo, TxAccount]] = None


class TransactionComparison(APIModel):
    """Top-level response for /txs/compare."""

    txs: list[TxComparedItem]
    signals: list[ComparisonSignal]
    lineage: list[LineageEdge] = Field(default_factory=list)
    # Omitted (via response_model_exclude_none) when excluded from the
    # include list.
    verdict: Optional[ComparisonVerdict] = None
