"""API models for the graph endpoints (currency-less /graph/* family).

Internal counterparts live in db/asynchronous/services/models/__init__.py
(``Graph*Internal``). The translator at web/translators.py
(``to_api_graph_summary``) maps internal -> API.
"""

from typing import Literal, Optional

from pydantic import Field

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.values import Rate, Values


class GraphTxRef(APIModel):
    """A transaction reference: hash plus the network it lives on."""

    tx_hash: str
    network: str = Field(examples=["btc"])


class GraphAddressRef(APIModel):
    """An address reference: address plus the network it lives on."""

    address: str
    network: str = Field(examples=["btc"])


class GraphSummaryRequest(APIModel):
    """Request body for ``POST /graph/summary``.

    The node set is defined by ``txs`` and/or ``addresses``; every item
    carries its own network, so the set may span chains. Each non-empty
    list must hold at least 2 distinct entries (keyed on network + hash);
    together they may hold at most 100. Fiat totals always carry every
    rate GraphSense stores (eur, usd)."""

    txs: list[GraphTxRef] = Field(default_factory=list, max_length=100)
    addresses: list[GraphAddressRef] = Field(default_factory=list, max_length=100)


class GraphCompareRequest(APIModel):
    """Request body for ``POST /graph/compare``.

    The fingerprinting analysis is BTC-only for now; every ref's network
    must be ``btc`` (400 otherwise). ``include`` selects response
    components; signals, lineage and verdict are always computed
    internally (the verdict depends on the signals), the list only
    controls what is returned. ``all`` expands to every component."""

    txs: list[GraphTxRef] = Field(min_length=2, max_length=100)
    include: list[
        Literal["all", "characteristics", "details", "signals", "lineage", "verdict"]
    ] = Field(default=["characteristics", "signals", "lineage", "verdict"])


class GraphTxOverall(APIModel):
    """Network-agnostic rollup over all transactions in the set: fiat and
    timestamps only, since base units and block heights are not comparable
    across chains. Per-network notes carry their network as prefix."""

    tx_count: int
    total_value_fiat: list[Rate] = Field(default_factory=list)
    timestamp_min: int
    timestamp_max: int
    notes: list[str] = Field(default_factory=list)


class GraphTxNetworkSummary(APIModel):
    """Aggregate stats over one network's transactions.

    ``total_value.value`` is the network's native base unit (satoshi for
    UTXO, wei/sun for account chains) and sums native transfers only;
    ``total_value.fiat_values`` sum per fiat code across all transfers,
    including tokens. ``total_fee`` stays in the native unit.
    ``total_inputs`` / ``total_outputs`` are UTXO-only and omitted for
    account-model summaries. ``notes`` flags caveats."""

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
    notes: list[str] = Field(default_factory=list)


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
    notes: list[str] = Field(default_factory=list)


class GraphAddressNetworkSummary(APIModel):
    """Aggregate stats over one network's addresses. Value totals follow
    the ``Values`` pattern (native base unit plus per-code fiat sums);
    token holdings are excluded from native totals (noted)."""

    network: str
    address_count: int
    total_received: Values
    total_spent: Values
    balance: Values
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    tagged_address_count: int = 0
    actors: list[LabeledItemRef] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class GraphAddressSummary(APIModel):
    overall: GraphAddressOverall
    networks: list[GraphAddressNetworkSummary]


class GraphSummary(APIModel):
    """Aggregate stats over a graph node set, split by node type. Each
    block is present iff the request carried that node type."""

    txs: Optional[GraphTxSummary] = None
    addresses: Optional[GraphAddressSummary] = None
