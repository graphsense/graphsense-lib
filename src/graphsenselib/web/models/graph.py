"""API models for the graph endpoints (currency-less /graph/* family).

Hosts both the graph summary models and the transaction-comparison models.

Internal counterparts live in db/asynchronous/services/models/__init__.py
(``Graph*Internal`` for summary, ``*Internal`` for comparison). The
translators at web/translators.py (``to_api_graph_summary``,
``to_api_transaction_comparison``) map internal -> API.
"""

from typing import Literal, Optional, Union

from pydantic import Field, model_validator

from graphsenselib.db.asynchronous.services.models import (
    MAX_GRAPH_NODES,
    GraphNoteCode,
    SignalPerTxValue,
)
from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.transactions import TxAccount, TxUtxo
from graphsenselib.web.models.values import Rate, Values

CompareComponent = Literal[
    "characteristics", "details", "signals", "lineage", "verdict"
]


class GraphTxRef(APIModel):
    """A transaction reference: hash plus the network it lives on."""

    tx_hash: str = Field(max_length=128)
    network: str = Field(examples=["btc"], max_length=32)


class GraphAddressRef(APIModel):
    """An address reference: address plus the network it lives on."""

    address: str = Field(max_length=128)
    network: str = Field(examples=["btc"], max_length=32)


class GraphSummaryRequest(APIModel):
    """Request body for ``POST /graph/summary``.

    The node set is defined by ``txs`` and/or ``addresses``; every item
    carries its own network, so the set may span chains. Each non-empty
    list must hold at least 2 distinct entries (keyed on network +
    canonical hash/address, so spelling variants of one node collapse and
    count once); together they may hold at most 100. Fiat totals always
    carry every rate GraphSense stores (eur, usd)."""

    txs: list[GraphTxRef] = Field(default_factory=list, max_length=MAX_GRAPH_NODES)
    addresses: list[GraphAddressRef] = Field(
        default_factory=list, max_length=MAX_GRAPH_NODES
    )

    @model_validator(mode="after")
    def _cap_combined_nodes(self):
        if len(self.txs) + len(self.addresses) > MAX_GRAPH_NODES:
            raise ValueError(
                f"txs and addresses together may hold at most {MAX_GRAPH_NODES} entries"
            )
        return self


class GraphCompareRequest(APIModel):
    """Request body for ``POST /graph/compare``.

    The fingerprinting analysis is BTC-only for now; every ref's network
    must be ``btc`` (400 otherwise). Hashes are canonicalized (lowercase,
    no ``0x``) and duplicates collapsed, so the response's ``txs`` list —
    which every positional reference indexes into — may be shorter than
    this one. ``include`` selects response components; signals, lineage
    and verdict are always computed internally (the verdict depends on
    the signals), the list only controls what is returned. ``all``
    expands to every component."""

    txs: list[GraphTxRef] = Field(min_length=2, max_length=MAX_GRAPH_NODES)
    include: list[Union[Literal["all"], CompareComponent]] = Field(
        default=["characteristics", "signals", "lineage", "verdict"],
        min_length=1,
    )


class GraphNote(APIModel):
    """A caveat attached to a summary block. ``code`` is the stable
    machine-readable contract (closed vocabulary, shared with the internal
    model so new codes surface in the OpenAPI schema); ``message`` is
    display text and may be reworded without notice. ``network`` attributes
    overall-rollup notes to their source network. ``items`` carries the
    references a note applies to (e.g. the not-found tx hashes / addresses
    of a ``nodes_not_found`` note), so clients never have to parse
    ``message``."""

    code: GraphNoteCode
    message: str
    network: Optional[str] = None
    items: Optional[list[str]] = None


class GraphTxOverall(APIModel):
    """Network-agnostic rollup over all transactions in the set: fiat and
    timestamps only, since base units and block heights are not comparable
    across chains. ``total_value_fiat`` inherits the per-network gross
    semantics (UTXO: full output sums including change; linked txs
    double-count). Per-network notes carry their source network in
    ``network``."""

    tx_count: int
    total_value_fiat: list[Rate] = Field(default_factory=list)
    timestamp_min: int
    timestamp_max: int
    notes: list[GraphNote] = Field(default_factory=list)


class GraphTxNetworkSummary(APIModel):
    """Aggregate stats over one network's transactions.

    ``total_value.value`` is the network's native base unit (satoshi for
    UTXO, wei/sun for account chains) and sums native transfers only;
    ``total_value.fiat_values`` sum per fiat code across all transfers,
    including tokens. Totals are *gross*: for UTXO networks each tx
    contributes its full output sum — change outputs included — and a set
    containing linked txs (e.g. a peel chain) counts the same coins once
    per hop, so this is not "net value moved". Account-chain contributions
    are the native transfer values. ``total_fee`` stays in the native unit;
    for UTXO networks it is always known (``0`` for an all-coinbase set),
    while on account chains ``null`` means fee data was unavailable for at
    least one tx (a partial sum would silently understate).
    ``total_inputs`` / ``total_outputs`` are UTXO-only and omitted for
    account-model summaries. ``notes`` flags caveats. ``assets`` lists the
    distinct assets involved on this network (lowercase, native first then
    tokens sorted)."""

    network: str
    tx_count: int
    total_value: Values
    total_fee: Optional[int] = None
    total_inputs: Optional[int] = None
    total_outputs: Optional[int] = None
    block_min: int
    block_max: int
    timestamp_min: int
    timestamp_max: int
    notes: list[GraphNote] = Field(default_factory=list)
    assets: list[str] = Field(default_factory=list)


class GraphTxSummary(APIModel):
    overall: GraphTxOverall
    networks: list[GraphTxNetworkSummary]


class GraphAddressOverall(APIModel):
    """Network-agnostic rollup over all addresses in the set (fiat totals
    per code, usage span, tag overview). ``actors`` are distinct across
    networks."""

    address_count: int
    total_received_fiat: list[Rate] = Field(default_factory=list)
    total_spent_fiat: list[Rate] = Field(default_factory=list)
    balance_fiat: list[Rate] = Field(default_factory=list)
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    tagged_address_count: int = 0
    actors: list[LabeledItemRef] = Field(default_factory=list)
    notes: list[GraphNote] = Field(default_factory=list)


class GraphAddressNetworkSummary(APIModel):
    """Aggregate stats over one network's addresses. Value totals follow
    the ``Values`` pattern (native base unit plus per-code fiat sums);
    token holdings are excluded from native totals (noted). ``assets``
    lists the distinct assets involved on this network (lowercase, native
    first then tokens sorted)."""

    network: str
    address_count: int
    total_received: Values
    total_spent: Values
    balance: Values
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    tagged_address_count: int = 0
    actors: list[LabeledItemRef] = Field(default_factory=list)
    notes: list[GraphNote] = Field(default_factory=list)
    assets: list[str] = Field(default_factory=list)


class GraphAddressSummary(APIModel):
    overall: GraphAddressOverall
    networks: list[GraphAddressNetworkSummary]


class GraphSummary(APIModel):
    """Aggregate stats over a graph node set, split by node type. Each
    block is present iff the request carried that node type."""

    txs: Optional[GraphTxSummary] = None
    addresses: Optional[GraphAddressSummary] = None


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


class GraphTxCharacteristics(APIModel):
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


class GraphCompareSignal(APIModel):
    """One row of the pairwise comparison table.

    ``per_tx`` holds one typed observation per compared tx, aligned with
    the response's ``txs`` order. The value type depends on the signal:
    booleans for flag signals (``witness_present``, ``rbf`` — true means
    BIP-125 signaled, ``bip69_outputs_sorted``, ``exchange_input_overlap``
    — true means the tx has an exchange-tagged input); an integer for
    ``tx_version``; categorical snake_case strings for
    ``locktime_pattern`` (``zero``/``anti_sniping``/``other``) and
    ``output_count_shape`` (``single``/``pay_plus_change``/``many``);
    sorted string lists for ``script_type`` (the tx's distinct input
    script types), ``direct_input_overlap`` (input addresses shared with
    peer txs), ``change_chain`` (own change addresses spent by peer txs)
    and ``common_ancestor`` (parent tx hashes shared with peers); sorted
    integer lists for ``utxo_linkage`` (indexes of peer txs with a direct
    spend edge) and ``shared_cluster`` (the tx's own input cluster ids).
    ``null`` means the value was not derivable for that tx; an empty list
    means computed, but no items."""

    name: str
    kind: SignalKind
    per_tx: list[Optional[SignalPerTxValue]]
    verdict: SignalVerdict
    weight: int = 0


class GraphLineageEdge(APIModel):
    """Direct on-chain relationship between two compared transactions.
    ``from_idx``/``to_idx`` are positions in the response's ``txs`` list
    (deduped canonical order), not the request's."""

    from_idx: int
    to_idx: int
    kind: LineageKind
    out_index: Optional[int] = None
    in_index: Optional[int] = None


class GraphCompareVerdict(APIModel):
    """Aggregator's opinion. Sub-verdicts kept independent.

    Only the categorical tier (``relation``) is exposed. The internal
    aggregator also computes a numeric ``confidence`` and ``score_total``
    (see ``ComparisonVerdictInternal``), but their weights have not been
    calibrated against ground-truth data, so they stay backend-only —
    consumers would inevitably treat them as probabilities. Add them here
    once calibrated.
    """

    relation: ComparisonRelation
    cluster_verdict: ClusterVerdict
    discriminator_hits: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class GraphComparedTx(APIModel):
    """Per-tx entry. ``characteristics`` and ``details`` are populated iff
    the request's ``include`` list names them (``details`` is off by
    default)."""

    tx_hash: str
    network: str
    characteristics: Optional[GraphTxCharacteristics] = None
    details: Optional[Union[TxUtxo, TxAccount]] = None


class GraphComparison(APIModel):
    """Top-level response for /graph/compare."""

    txs: list[GraphComparedTx]
    # Excluded components are omitted from the response entirely (via
    # response_model_exclude_none); an included-but-empty list stays [].
    signals: Optional[list[GraphCompareSignal]] = None
    lineage: Optional[list[GraphLineageEdge]] = None
    verdict: Optional[GraphCompareVerdict] = None
