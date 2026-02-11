"""Simplified translation layer for converting graphsenselib models to API models.

This module provides conversion functions from graphsenselib Pydantic models
to the slim graphsenselib.web.models Pydantic models.

Conversion Patterns:
-------------------
Two patterns are used intentionally based on model complexity:

1. Simple one-liner (for flat objects with matching schemas):
   `Values.model_validate(pydantic_values.model_dump())`

2. Manual mapping (for nested/complex objects):
   Explicit field-by-field conversion when nested objects need recursive
   conversion or when business logic is required (e.g., empty list → None).

The choice of pattern depends on whether the API model schema exactly matches
the service model (use simple) or requires nested conversions/transformations
(use manual).
"""

from __future__ import annotations

from typing import Any, Union

from graphsenselib.db.asynchronous.services.models import Actor as PydanticActor
from graphsenselib.db.asynchronous.services.models import (
    ActorContext as PydanticActorContext,
)
from graphsenselib.db.asynchronous.services.models import Address as PydanticAddress
from graphsenselib.db.asynchronous.services.models import (
    AddressTag as PydanticAddressTag,
)
from graphsenselib.db.asynchronous.services.models import (
    AddressTagResult as PydanticAddressTagResult,
)
from graphsenselib.db.asynchronous.services.models import (
    AddressTxs as PydanticAddressTxs,
)
from graphsenselib.db.asynchronous.services.models import (
    AddressTxUtxo as PydanticAddressTxUtxo,
)
from graphsenselib.db.asynchronous.services.models import Block as PydanticBlock
from graphsenselib.db.asynchronous.services.models import (
    BlockAtDate as PydanticBlockAtDate,
)
from graphsenselib.db.asynchronous.services.models import Concept as PydanticConcept
from graphsenselib.db.asynchronous.services.models import (
    CrossChainPubkeyRelatedAddress as PydanticCrossChainPubkeyRelatedAddress,
)
from graphsenselib.db.asynchronous.services.models import (
    CrossChainPubkeyRelatedAddresses as PydanticCrossChainPubkeyRelatedAddresses,
)
from graphsenselib.db.asynchronous.services.models import (
    CurrencyStats as PydanticCurrencyStats,
)
from graphsenselib.db.asynchronous.services.models import Entity as PydanticEntity
from graphsenselib.db.asynchronous.services.models import (
    EntityAddresses as PydanticEntityAddresses,
)
from graphsenselib.db.asynchronous.services.models import (
    ExternalConversion as PydanticExternalConversion,
)
from graphsenselib.db.asynchronous.services.models import (
    LabelSummary as PydanticLabelSummary,
)
from graphsenselib.db.asynchronous.services.models import Links as PydanticLinks
from graphsenselib.db.asynchronous.services.models import LinkUtxo as PydanticLinkUtxo
from graphsenselib.db.asynchronous.services.models import (
    NeighborAddress as PydanticNeighborAddress,
)
from graphsenselib.db.asynchronous.services.models import (
    NeighborAddresses as PydanticNeighborAddresses,
)
from graphsenselib.db.asynchronous.services.models import (
    NeighborEntities as PydanticNeighborEntities,
)
from graphsenselib.db.asynchronous.services.models import (
    NeighborEntity as PydanticNeighborEntity,
)
from graphsenselib.db.asynchronous.services.models import RatesResponse as PydanticRates
from graphsenselib.db.asynchronous.services.models import (
    SearchResult as PydanticSearchResult,
)
from graphsenselib.db.asynchronous.services.models import Stats as PydanticStats
from graphsenselib.db.asynchronous.services.models import (
    TagCloudEntry as PydanticTagCloudEntry,
)
from graphsenselib.db.asynchronous.services.models import (
    TagSummary as PydanticTagSummary,
)
from graphsenselib.db.asynchronous.services.models import Taxonomy as PydanticTaxonomy
from graphsenselib.db.asynchronous.services.models import (
    TokenConfigs as PydanticTokenConfigs,
)
from graphsenselib.db.asynchronous.services.models import TxAccount as PydanticTxAccount
from graphsenselib.db.asynchronous.services.models import Txs as PydanticTxs
from graphsenselib.db.asynchronous.services.models import TxUtxo as PydanticTxUtxo
from graphsenselib.db.asynchronous.services.models import Values as PydanticValues

from graphsenselib.web.models import (
    Actor,
    ActorContext,
    Address,
    AddressTag,
    AddressTags,
    AddressTxs,
    AddressTxUtxo,
    Block,
    BlockAtDate,
    Concept,
    CurrencyStats,
    Entity,
    EntityAddresses,
    ExternalConversion,
    LabelSummary,
    LinkUtxo,
    Links,
    NeighborAddress,
    NeighborAddresses,
    NeighborEntities,
    NeighborEntity,
    Rates,
    RelatedAddress,
    RelatedAddresses,
    SearchResult,
    SearchResultByCurrency,
    Stats,
    TagCloudEntry,
    TagSummary,
    Taxonomy,
    TokenConfig,
    TokenConfigs,
    TxAccount,
    Txs,
    TxSummary,
    TxUtxo,
    TxValue,
    Values,
)


def to_api_values(pydantic_values: PydanticValues) -> Values:
    """Convert service Values to API Values."""
    return Values.model_validate(pydantic_values.model_dump())


def to_api_tx_ref(tx_ref) -> dict:
    """Convert service TxRef to dict."""
    return tx_ref.model_dump()


def to_api_tx_summary(data: dict) -> TxSummary:
    """Convert dict to API TxSummary."""
    return TxSummary.model_validate(data)


def to_api_values_dict(
    values_dict: dict[str, PydanticValues] | None,
) -> dict[str, Values] | None:
    """Convert a dict of Values."""
    if values_dict is None:
        return None
    return {k: to_api_values(v) for k, v in values_dict.items()}


def to_api_address_tag(pydantic_tag: PydanticAddressTag) -> AddressTag:
    """Convert service AddressTag to API AddressTag."""
    return AddressTag.model_validate(pydantic_tag.model_dump())


def to_api_address(pydantic_address: PydanticAddress) -> Address:
    """Convert service Address to API Address."""
    data = pydantic_address.model_dump()
    return Address.model_validate(data)


def to_api_entity(pydantic_entity: PydanticEntity) -> Entity:
    """Convert service Entity to API Entity."""
    data = pydantic_entity.model_dump()
    # Convert empty lists to None for optional fields (backward compatibility)
    if data.get("actors") == []:
        data["actors"] = None
    return Entity.model_validate(data)


def to_api_neighbor_address(
    pydantic_neighbor: PydanticNeighborAddress,
) -> NeighborAddress:
    """Convert service NeighborAddress to API NeighborAddress."""
    return NeighborAddress(
        labels=pydantic_neighbor.labels,
        value=to_api_values(pydantic_neighbor.value),
        token_values=to_api_values_dict(pydantic_neighbor.token_values),
        no_txs=pydantic_neighbor.no_txs,
        address=to_api_address(pydantic_neighbor.address),
    )


def to_api_neighbor_addresses(
    pydantic_neighbors: PydanticNeighborAddresses,
) -> NeighborAddresses:
    """Convert service NeighborAddresses to API NeighborAddresses."""
    return NeighborAddresses(
        next_page=pydantic_neighbors.next_page,
        neighbors=[to_api_neighbor_address(n) for n in pydantic_neighbors.neighbors],
    )


def to_api_neighbor_entity(pydantic_neighbor: PydanticNeighborEntity) -> NeighborEntity:
    """Convert service NeighborEntity to API NeighborEntity."""
    entity_val: Entity | int
    if isinstance(pydantic_neighbor.entity, PydanticEntity):
        entity_val = to_api_entity(pydantic_neighbor.entity)
    else:
        entity_val = pydantic_neighbor.entity

    return NeighborEntity(
        labels=pydantic_neighbor.labels,
        value=to_api_values(pydantic_neighbor.value),
        token_values=to_api_values_dict(pydantic_neighbor.token_values),
        no_txs=pydantic_neighbor.no_txs,
        entity=entity_val,
    )


def to_api_neighbor_entities(
    pydantic_neighbors: PydanticNeighborEntities,
) -> NeighborEntities:
    """Convert service NeighborEntities to API NeighborEntities."""
    return NeighborEntities(
        next_page=pydantic_neighbors.next_page,
        neighbors=[to_api_neighbor_entity(n) for n in pydantic_neighbors.neighbors],
    )


def to_api_entity_addresses(
    pydantic_result: PydanticEntityAddresses,
) -> EntityAddresses:
    """Convert service EntityAddresses to API EntityAddresses."""
    return EntityAddresses(
        next_page=pydantic_result.next_page,
        addresses=[to_api_address(addr) for addr in pydantic_result.addresses],
    )


def to_api_address_tag_result(
    pydantic_result: PydanticAddressTagResult,
) -> AddressTags:
    """Convert service AddressTagResult to API AddressTags."""
    return AddressTags(
        next_page=pydantic_result.next_page,
        address_tags=[to_api_address_tag(tag) for tag in pydantic_result.address_tags],
    )


def to_api_tx_value(pydantic_tx_value) -> TxValue:
    """Convert service TxValue to API TxValue."""
    return TxValue(
        address=pydantic_tx_value.address,
        value=to_api_values(pydantic_tx_value.value),
        index=pydantic_tx_value.index,
    )


def to_api_tx_utxo(pydantic_tx: PydanticTxUtxo) -> TxUtxo:
    """Convert service TxUtxo to API TxUtxo."""
    return TxUtxo(
        tx_type=pydantic_tx.tx_type,
        currency=pydantic_tx.currency,
        tx_hash=pydantic_tx.tx_hash,
        coinbase=pydantic_tx.coinbase,
        height=pydantic_tx.height,
        no_inputs=pydantic_tx.no_inputs,
        no_outputs=pydantic_tx.no_outputs,
        timestamp=pydantic_tx.timestamp,
        total_input=to_api_values(pydantic_tx.total_input),
        total_output=to_api_values(pydantic_tx.total_output),
        inputs=[to_api_tx_value(inp) for inp in pydantic_tx.inputs]
        if pydantic_tx.inputs
        else None,
        outputs=[to_api_tx_value(out) for out in pydantic_tx.outputs]
        if pydantic_tx.outputs
        else None,
    )


def to_api_tx_account(pydantic_tx: PydanticTxAccount) -> TxAccount:
    """Convert service TxAccount to API TxAccount."""
    return TxAccount(
        tx_type=pydantic_tx.tx_type,
        identifier=pydantic_tx.identifier,
        currency=pydantic_tx.currency,
        network=pydantic_tx.network,
        tx_hash=pydantic_tx.tx_hash,
        height=pydantic_tx.height,
        timestamp=pydantic_tx.timestamp,
        value=to_api_values(pydantic_tx.value),
        from_address=pydantic_tx.from_address,
        to_address=pydantic_tx.to_address,
        token_tx_id=pydantic_tx.token_tx_id,
        fee=to_api_values(pydantic_tx.fee) if pydantic_tx.fee else None,
        contract_creation=pydantic_tx.contract_creation,
        is_external=pydantic_tx.is_external,
    )


def to_api_tx(pydantic_tx: Union[PydanticTxUtxo, PydanticTxAccount]):
    """Convert service Tx to API Tx."""
    if isinstance(pydantic_tx, PydanticTxUtxo):
        return to_api_tx_utxo(pydantic_tx)
    return to_api_tx_account(pydantic_tx)


def to_api_txs(pydantic_txs: PydanticTxs) -> Txs:
    """Convert service Txs to API Txs."""
    return Txs(
        txs=[to_api_tx(tx) for tx in pydantic_txs.txs],
        next_page=str(pydantic_txs.next_page) if pydantic_txs.next_page else None,
    )


def to_api_address_tx_utxo(pydantic_tx: PydanticAddressTxUtxo) -> AddressTxUtxo:
    """Convert service AddressTxUtxo to API AddressTxUtxo."""
    return AddressTxUtxo(
        tx_type=pydantic_tx.tx_type,
        tx_hash=pydantic_tx.tx_hash,
        currency=pydantic_tx.currency,
        coinbase=pydantic_tx.coinbase,
        height=pydantic_tx.height,
        timestamp=pydantic_tx.timestamp,
        value=to_api_values(pydantic_tx.value),
    )


def to_api_address_txs(pydantic_txs: PydanticAddressTxs) -> AddressTxs:
    """Convert service AddressTxs to API AddressTxs."""
    return AddressTxs.model_validate(pydantic_txs.model_dump())


def to_api_link_utxo(pydantic_link: PydanticLinkUtxo) -> LinkUtxo:
    """Convert service LinkUtxo to API LinkUtxo."""
    return LinkUtxo(
        tx_type=pydantic_link.tx_type,
        tx_hash=pydantic_link.tx_hash,
        currency=pydantic_link.currency,
        height=pydantic_link.height,
        timestamp=pydantic_link.timestamp,
        input_value=to_api_values(pydantic_link.input_value),
        output_value=to_api_values(pydantic_link.output_value),
    )


def to_api_links(pydantic_links: PydanticLinks) -> Links:
    """Convert service Links to API Links."""
    api_links = []
    for link in pydantic_links.links:
        if isinstance(link, PydanticLinkUtxo):
            api_links.append(to_api_link_utxo(link))
        elif isinstance(link, PydanticAddressTxUtxo):
            api_links.append(to_api_address_tx_utxo(link))
        elif isinstance(link, PydanticTxAccount):
            api_links.append(to_api_tx_account(link))
        else:
            raise NotImplementedError(f"Unsupported link type: {type(link)}")
    return Links(next_page=pydantic_links.next_page, links=api_links)


def to_api_block(pydantic_block: PydanticBlock) -> Block:
    """Convert service Block to API Block."""
    return Block.model_validate(pydantic_block.model_dump())


def to_api_block_at_date(pydantic_block_at_date: PydanticBlockAtDate) -> BlockAtDate:
    """Convert service BlockAtDate to API BlockAtDate."""
    return BlockAtDate.model_validate(pydantic_block_at_date.model_dump())


def to_api_currency_stats(pydantic_stats: PydanticCurrencyStats) -> CurrencyStats:
    """Convert service CurrencyStats to API CurrencyStats."""
    return CurrencyStats.model_validate(pydantic_stats.model_dump())


def to_api_stats(pydantic_stats: PydanticStats) -> Stats:
    """Convert service Stats to API Stats."""
    return Stats(
        currencies=[to_api_currency_stats(cs) for cs in pydantic_stats.currencies],
        version=pydantic_stats.version,
        request_timestamp=pydantic_stats.request_timestamp,
    )


def to_api_rates(pydantic_rates: PydanticRates) -> Rates:
    """Convert service Rates to API Rates."""
    return Rates.model_validate(pydantic_rates.model_dump())


def to_api_taxonomy(pydantic_taxonomy: PydanticTaxonomy) -> Taxonomy:
    """Convert service Taxonomy to API Taxonomy."""
    return Taxonomy.model_validate(pydantic_taxonomy.model_dump())


def to_api_concept(pydantic_concept: PydanticConcept) -> Concept:
    """Convert service Concept to API Concept."""
    return Concept.model_validate(pydantic_concept.model_dump())


def to_api_actor_context(pydantic_context: PydanticActorContext) -> ActorContext:
    """Convert service ActorContext to API ActorContext."""
    return ActorContext.model_validate(pydantic_context.model_dump())


def to_api_actor(pydantic_actor: PydanticActor) -> Actor:
    """Convert service Actor to API Actor."""
    return Actor.model_validate(pydantic_actor.model_dump())


def to_api_tag_cloud_entry(pydantic_entry: PydanticTagCloudEntry) -> TagCloudEntry:
    """Convert service TagCloudEntry to API TagCloudEntry."""
    return TagCloudEntry.model_validate(pydantic_entry.model_dump())


def to_api_label_summary(pydantic_summary: PydanticLabelSummary) -> LabelSummary:
    """Convert service LabelSummary to API LabelSummary."""
    return LabelSummary.model_validate(pydantic_summary.model_dump())


def to_api_tag_summary(pydantic_summary: PydanticTagSummary) -> TagSummary:
    """Convert service TagSummary to API TagSummary."""
    return TagSummary(
        broad_category=pydantic_summary.broad_category,
        tag_count=pydantic_summary.tag_count,
        tag_count_indirect=pydantic_summary.tag_count_indirect,
        best_actor=pydantic_summary.best_actor,
        best_label=pydantic_summary.best_label,
        concept_tag_cloud={
            k: to_api_tag_cloud_entry(v)
            for k, v in pydantic_summary.concept_tag_cloud.items()
        },
        label_summary={
            k: to_api_label_summary(v)
            for k, v in pydantic_summary.label_summary.items()
        },
    )


def to_api_search_result_by_currency(pydantic_result) -> SearchResultByCurrency:
    """Convert service SearchResultByCurrency to API SearchResultByCurrency."""
    return SearchResultByCurrency.model_validate(pydantic_result.model_dump())


def to_api_search_result(pydantic_result: PydanticSearchResult) -> SearchResult:
    """Convert service SearchResult to API SearchResult."""
    return SearchResult.model_validate(pydantic_result.model_dump())


def to_api_token_configs(pydantic_configs: PydanticTokenConfigs) -> TokenConfigs:
    """Convert service TokenConfigs to API TokenConfigs."""
    return TokenConfigs(
        token_configs=[
            TokenConfig(
                ticker=config.ticker,
                decimals=config.decimals,
                peg_currency=config.peg_currency,
                contract_address=config.contract_address,
            )
            for config in pydantic_configs.token_configs
        ]
    )


def to_api_external_conversion(
    pydantic_conversion: PydanticExternalConversion,
) -> ExternalConversion:
    """Convert service ExternalConversion to API ExternalConversion."""
    ctype = "dex_swap"
    if pydantic_conversion.conversion_type == "swap":
        ctype = "dex_swap"
    elif pydantic_conversion.conversion_type == "bridge":
        ctype = "bridge_tx"

    return ExternalConversion(
        conversion_type=ctype,
        from_address=pydantic_conversion.from_address,
        to_address=pydantic_conversion.to_address,
        from_asset=pydantic_conversion.from_asset,
        to_asset=pydantic_conversion.to_asset,
        from_amount=pydantic_conversion.from_amount,
        to_amount=pydantic_conversion.to_amount,
        from_asset_transfer=pydantic_conversion.from_asset_transfer,
        to_asset_transfer=pydantic_conversion.to_asset_transfer,
        from_network=pydantic_conversion.from_network,
        to_network=pydantic_conversion.to_network,
        from_is_supported_asset=pydantic_conversion.from_is_supported_asset,
        to_is_supported_asset=pydantic_conversion.to_is_supported_asset,
    )


def to_api_cross_chain_pubkey_related_address(
    pydantic_address: PydanticCrossChainPubkeyRelatedAddress,
) -> RelatedAddress:
    """Convert service CrossChainPubkeyRelatedAddress to API RelatedAddress."""
    return RelatedAddress(
        currency=pydantic_address.network,
        address=pydantic_address.address,
        relation_type="pubkey",
    )


def to_api_cross_chain_pubkey_related_addresses(
    pydantic_addresses: PydanticCrossChainPubkeyRelatedAddresses,
) -> RelatedAddresses:
    """Convert service CrossChainPubkeyRelatedAddresses to API RelatedAddresses."""
    return RelatedAddresses(
        related_addresses=[
            to_api_cross_chain_pubkey_related_address(addr)
            for addr in pydantic_addresses.addresses
        ],
        next_page=pydantic_addresses.next_page,
    )


def pydantic_to_openapi(pydantic_obj: Any) -> Any:
    """Generic function to convert Pydantic objects to API objects based on type.

    This is a compatibility layer for the old API. New code should use
    the specific to_api_* functions directly.
    """
    if isinstance(pydantic_obj, list):
        return [pydantic_to_openapi(item) for item in pydantic_obj]

    if pydantic_obj is None or isinstance(pydantic_obj, (str, int, float, bool)):
        return pydantic_obj

    obj_type = type(pydantic_obj)
    type_name = obj_type.__name__

    # Map type names to converter functions
    converters = {
        "Values": to_api_values,
        "Address": to_api_address,
        "Entity": to_api_entity,
        "AddressTag": to_api_address_tag,
        "AddressTagResult": to_api_address_tag_result,
        "NeighborAddress": to_api_neighbor_address,
        "NeighborAddresses": to_api_neighbor_addresses,
        "NeighborEntity": to_api_neighbor_entity,
        "NeighborEntities": to_api_neighbor_entities,
        "EntityAddresses": to_api_entity_addresses,
        "TxUtxo": to_api_tx_utxo,
        "TxAccount": to_api_tx_account,
        "Txs": to_api_txs,
        "AddressTxUtxo": to_api_address_tx_utxo,
        "AddressTxs": to_api_address_txs,
        "LinkUtxo": to_api_link_utxo,
        "Links": to_api_links,
        "Block": to_api_block,
        "BlockAtDate": to_api_block_at_date,
        "Stats": to_api_stats,
        "CurrencyStats": to_api_currency_stats,
        "RatesResponse": to_api_rates,
        "Taxonomy": to_api_taxonomy,
        "Concept": to_api_concept,
        "Actor": to_api_actor,
        "ActorContext": to_api_actor_context,
        "TagCloudEntry": to_api_tag_cloud_entry,
        "LabelSummary": to_api_label_summary,
        "TagSummary": to_api_tag_summary,
        "SearchResult": to_api_search_result,
        "SearchResultByCurrency": to_api_search_result_by_currency,
        "TokenConfigs": to_api_token_configs,
        "ExternalConversion": to_api_external_conversion,
        "CrossChainPubkeyRelatedAddress": to_api_cross_chain_pubkey_related_address,
        "CrossChainPubkeyRelatedAddresses": to_api_cross_chain_pubkey_related_addresses,
    }

    if type_name in converters:
        return converters[type_name](pydantic_obj)

    raise NotImplementedError(f"No converter found for type: {type_name}")
