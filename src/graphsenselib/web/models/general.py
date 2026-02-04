"""General API models (stats, rates, taxonomy, actors, etc.)."""

from typing import Literal, Optional

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.values import Rate


class CurrencyStats(APIModel):
    """Currency statistics model."""

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

    currencies: list[CurrencyStats]
    version: Optional[str] = None
    request_timestamp: Optional[str] = None


class Rates(APIModel):
    """Exchange rates model."""

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

    ticker: str
    decimals: int
    peg_currency: Optional[str] = None
    contract_address: Optional[str] = None


class TokenConfigs(APIModel):
    """List of token configurations."""

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
