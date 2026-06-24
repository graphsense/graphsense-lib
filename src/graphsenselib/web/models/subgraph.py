"""API models for the subgraph endpoints.

Internal counterparts live in db/asynchronous/services/models/__init__.py
(``SubgraphSummaryInternal``). The translator at web/translators.py
(``to_api_subgraph_summary``) maps internal -> API.
"""

from typing import Literal, Optional

from pydantic import Field

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef


class SubgraphSummaryRequest(APIModel):
    """Request body for ``POST /{currency}/graph/summary``.

    The subgraph is defined by ``txs`` (transaction hashes) and/or
    ``addresses``. Each non-empty list must hold at least 2 distinct
    entries; together they may hold at most 100. ``fiat_currency``
    selects the currency for the fiat totals (only the rates GraphSense
    stores, usd and eur, are available; default usd).
    """

    txs: list[str] = Field(default_factory=list)
    addresses: list[str] = Field(default_factory=list)
    fiat_currency: Literal["usd", "eur"] = "usd"


class SubgraphTxSummary(APIModel):
    """Aggregate stats over the transactions in a subgraph.

    ``total_value`` and ``total_fee`` are in the chain's base unit (satoshi
    for UTXO, wei/sun for account chains); ``total_value`` sums native
    transfers only (token transfers carry no native-unit amount).
    ``total_value_fiat`` sums the fiat value (in ``fiat_currency``) across all
    transfers, including tokens, so it is comparable across assets.
    ``total_inputs`` / ``total_outputs`` are UTXO-only and omitted for
    account-model (ETH/TRX) summaries. ``notes`` flags caveats (e.g. a partial
    fiat total when some txs had no rate, or token transfers excluded from
    ``total_value``).
    """

    tx_count: int
    total_value: int
    total_value_fiat: Optional[float] = None
    fiat_currency: str = "usd"
    total_fee: Optional[int] = None
    total_inputs: Optional[int] = None
    total_outputs: Optional[int] = None
    block_min: int
    block_max: int
    timestamp_min: int
    timestamp_max: int
    notes: list[str] = Field(default_factory=list)


class SubgraphAddressSummary(APIModel):
    """Aggregate stats over the addresses in a subgraph.

    Value totals are in the chain's base unit; the ``*_fiat`` fields sum
    the ``fiat_currency`` value across the set. ``first_usage`` /
    ``last_usage`` span the set's on-chain activity and are omitted when
    no selected address has any. ``tagged_address_count`` counts
    addresses with at least one visible tag; ``actors`` lists the
    distinct actors across all tags on the set. ``notes`` flags caveats
    (partial fiat totals, token holdings excluded from native totals).
    """

    address_count: int
    total_received: int
    total_received_fiat: Optional[float] = None
    total_spent: int
    total_spent_fiat: Optional[float] = None
    balance: int
    balance_fiat: Optional[float] = None
    fiat_currency: str = "usd"
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    tagged_address_count: int = 0
    actors: list[LabeledItemRef] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class SubgraphSummary(APIModel):
    """Aggregate stats over a subgraph, split by node type.

    Each block is present iff the request carried that node type:
    ``txs`` summarizes the transactions, ``addresses`` the addresses.
    """

    currency: str
    txs: Optional[SubgraphTxSummary] = None
    addresses: Optional[SubgraphAddressSummary] = None
