"""General API models (stats, rates, taxonomy, actors, etc.)."""

from typing import Literal, Optional

from pydantic import Field

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


NETWORK_CAPABILITIES_EXAMPLE = {
    "network": "eth",
    "disabled": [],
}


class NetworkCapabilities(APIModel):
    """Feature availability of one network in this deployment."""

    model_config = api_model_config(NETWORK_CAPABILITIES_EXAMPLE)

    network: str
    disabled: list[str] = Field(
        description="Feature families NOT served for this network, from the "
        'vocabulary "relations" (neighbors/links counterparty enumeration), '
        '"clusters" (address clustering), "tags" (TagStore data), '
        '"conversions" (DEX-swap/bridge resolution on txs), "exact_stats" '
        "(the per-currency /stats numbers are placeholders, not "
        "pipeline-exact). Routes of a disabled family answer 501. Unknown "
        "flags must be ignored (forward-extensible). Core route families "
        "(address/tx/block detail, search, rates) are never listed here — "
        "they work on every served network."
    )


SERVICE_INFO_EXAMPLE = {
    "name": "notifications",
    "available": True,
    "version": "26.10.0rc1",
    "api": 1,
    "beta": True,
    "limits": {"instant_mail_interval_minutes": 5, "subscriptions_per_user": 100},
}


class ServiceInfo(APIModel):
    """An optional service of this deployment behind this API."""

    model_config = api_model_config(SERVICE_INFO_EXAMPLE)

    name: str = Field(
        description='The feature it serves, e.g. "notifications" for the '
        "/notifications/* routes."
    )
    available: bool = Field(
        description="Whether the service answered just now. False: it is "
        "enabled but unreachable; its routes answer 502 until it is back."
    )
    version: Optional[str] = Field(
        default=None, description="The service's release, when it answered."
    )
    api: Optional[int] = Field(
        default=None,
        description="Contract version of its routes; rises only on a breaking change.",
    )
    beta: bool = Field(default=False, description="Its routes may still change.")
    limits: Optional[dict[str, int]] = Field(
        default=None,
        description="Configured limits a client can show, by name, as the "
        "service reports them, e.g. instant_mail_interval_minutes (at most one "
        "instant email per user in that time) or subscriptions_per_user. "
        "Unknown names must be ignored.",
    )


class Capabilities(APIModel):
    """Per-network feature availability and the optional services of this
    deployment."""

    model_config = api_model_config(
        {
            "networks": [
                NETWORK_CAPABILITIES_EXAMPLE,
                {"network": "arb", "disabled": ["relations", "clusters"]},
            ],
            "services": [SERVICE_INFO_EXAMPLE],
        }
    )

    networks: list[NetworkCapabilities] = Field(
        description="One entry per served network. A network ABSENT from the "
        "list is fully enabled; so is every network of a deployment old "
        "enough to 404 this endpoint."
    )
    services: list[ServiceInfo] = Field(
        default_factory=list,
        description="Optional services enabled in this deployment, e.g. "
        "address notifications. A service ABSENT from the list is not "
        "offered. Unknown names must be ignored (forward-extensible).",
    )


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
