"""API models for the subgraph endpoints.

Internal counterparts live in db/asynchronous/services/models/__init__.py
(``SubgraphSummaryInternal``). The translator at web/translators.py
(``to_api_subgraph_summary``) maps internal -> API.
"""

from typing import Literal, Optional

from pydantic import Field

from graphsenselib.web.models.base import APIModel


class SubgraphSummaryRequest(APIModel):
    """Request body for ``POST /{currency}/subgraph/summary``.

    The subgraph is defined by ``txs`` (transaction hashes). ``addresses`` is
    reserved for a future extension and must be empty for now; the node set
    (txs + addresses) must hold at least 2 and at most 100 distinct nodes.
    ``fiat_currency`` selects the currency for ``total_value_fiat`` (only the
    rates GraphSense stores, usd and eur, are available; default usd).
    """

    txs: list[str] = Field(default_factory=list)
    addresses: list[str] = Field(default_factory=list)
    fiat_currency: Literal["usd", "eur"] = "usd"


class SubgraphSummary(APIModel):
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
    currency: str
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
