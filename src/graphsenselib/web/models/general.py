"""General API models (stats, rates, taxonomy, actors, etc.)."""

from typing import Literal, Optional

from graphsenselib.web.models.base import APIModel, api_model_config
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.values import RATE_EXAMPLE, Rate

CURRENCY_STATS_EXAMPLE = {
    "name": "btc",
    "no_blocks": 750000,
    "no_address_relations": 1000000,
    "no_addresses": 500000,
    "no_entities": 200000,
    "no_txs": 800000,
    "no_labels": 10000,
    "no_tagged_addresses": 5000,
    "timestamp": 1625703347,
}

TOKEN_CONFIG_EXAMPLE = {
    "ticker": "USDT",
    "decimals": 6,
    "peg_currency": "USD",
}


class CurrencyStats(APIModel):
    """Currency statistics model."""

    model_config = api_model_config(CURRENCY_STATS_EXAMPLE)

    name: str
    no_blocks: int
    no_address_relations: int
    no_addresses: int
    no_entities: int
    no_txs: int
    no_labels: int
    no_tagged_addresses: int
    timestamp: int


class Stats(APIModel):
    """API statistics model."""

    model_config = api_model_config(
        {
            "currencies": [CURRENCY_STATS_EXAMPLE],
            "version": "1.0.0",
        }
    )

    currencies: list[CurrencyStats]
    version: Optional[str] = None
    request_timestamp: Optional[str] = None


class Rates(APIModel):
    """Exchange rates model."""

    model_config = api_model_config(
        {
            "rates": [RATE_EXAMPLE, {"code": "usd", "value": 0.2345}],
            "height": 47,
        }
    )

    rates: Optional[list[Rate]] = None
    height: Optional[int] = None


class Taxonomy(APIModel):
    """Taxonomy model."""

    taxonomy: str
    uri: str


class Concept(APIModel):
    """Concept model."""

    id: str
    label: str
    taxonomy: str
    uri: Optional[str] = None
    description: Optional[str] = None


class ActorContext(APIModel):
    """Actor context model."""

    uris: list[str]
    images: list[str]
    refs: list[str]
    coingecko_ids: list[str]
    defilama_ids: list[str]
    twitter_handle: Optional[str] = None
    github_organisation: Optional[str] = None
    legal_name: Optional[str] = None


class Actor(APIModel):
    """Actor model."""

    id: str
    label: str
    uri: str
    categories: list[LabeledItemRef]
    jurisdictions: list[LabeledItemRef]
    nr_tags: Optional[int] = None
    context: Optional[ActorContext] = None


class TokenConfig(APIModel):
    """Token configuration model."""

    model_config = api_model_config(TOKEN_CONFIG_EXAMPLE)

    ticker: str
    decimals: int
    peg_currency: Optional[str] = None
    contract_address: Optional[str] = None


class TokenConfigs(APIModel):
    """List of token configurations."""

    model_config = api_model_config(
        {
            "token_configs": [
                TOKEN_CONFIG_EXAMPLE,
                {"ticker": "WETH", "decimals": 18, "peg_currency": "ETH"},
            ]
        }
    )

    token_configs: list[TokenConfig]


class RelatedAddress(APIModel):
    """Related address model (cross-chain)."""

    address: str
    currency: str
    relation_type: Literal["pubkey"]


class RelatedAddresses(APIModel):
    """Paginated list of related addresses."""

    related_addresses: list[RelatedAddress]
    next_page: Optional[str] = None


class ExternalConversion(APIModel):
    """External conversion (DEX swap or bridge) model."""

    conversion_type: Literal["dex_swap", "bridge_tx"]
    from_address: str
    to_address: str
    from_asset: str
    to_asset: str
    from_amount: str
    to_amount: str
    from_asset_transfer: str
    to_asset_transfer: str
    from_network: str
    to_network: str
    from_is_supported_asset: bool
    to_is_supported_asset: bool
