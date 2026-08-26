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
    "network_type": "utxo",
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
    network_type: str
    # Contract extension shared with external GraphSense-compatible backends
    # (the provider-backed adapter): capability discovery for consumers.
    # ABSENT = full core GraphSense (every locally served network); present =
    # the exact subset of core features answered for this currency, from the
    # vocabulary "relations" (counterparty enumeration / pair edges),
    # "clusters" (address clustering), "tags" (TagStore data). The
    # external-backends middleware appends "tags" to declared lists because
    # tag routes are answered locally. Local serving never sets these fields;
    # response_model_exclude_none keeps them off the wire.
    capabilities: Optional[list[str]] = None
    # Network-behavior discovery so consumers can stop hardcoding a per-network
    # table for every new EVM chain. ABSENT = the consumer falls back to its
    # own tables. Naming follows TokenConfig (ticker/decimals): coin_ticker is
    # the GAS coin's lowercase ticker, which on L2s differs from the network
    # code (arb pays gas in "eth"; "arb" quotes the governance token).
    coin_ticker: Optional[str] = None
    coin_decimals: Optional[int] = None
    network_name: Optional[str] = None


class Stats(APIModel):
    """API statistics model."""

    model_config = api_model_config(
        {
            "currencies": [CURRENCY_STATS_EXAMPLE],
            "version": "1.0.0",
            "request_timestamp": "2026-07-28T12:00:00",
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
