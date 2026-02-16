"""Address API routes"""

from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import (
    Address,
    AddressTags,
    AddressTxs,
    Entity,
    Links,
    NeighborAddresses,
    RelatedAddresses,
    TagSummary,
)
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
    normalize_page,
    parse_comma_separated_strings,
    parse_datetime,
    should_obfuscate_private_tags,
)
from graphsenselib.web.routes.params import (
    AddressPath,
    CurrencyPath,
    DirectionQuery,
    IncludeActorsQuery,
    IncludeBestClusterTagQuery,
    IncludeLabelsQuery,
    MaxDateQuery,
    MaxHeightQuery,
    MinDateQuery,
    MinHeightQuery,
    OptionalDirectionQuery,
    OrderQuery,
    PageQuery,
    PagesizeQuery,
    TokenCurrencyQuery,
)
import graphsenselib.web.service.addresses_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/addresses/{address}",
    summary="Get an address",
    operation_id="get_address",
    response_model=Address,
    response_model_exclude_none=True,
)
async def get_address(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    include_actors: IncludeActorsQuery = True,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get an address"""
    result = await service.get_address(
        ctx,
        currency=currency.lower(),
        address=address,
        include_actors=include_actors,
    )
    return result


@router.get(
    "/addresses/{address}/entity",
    summary="Get the entity of an address",
    operation_id="get_address_entity",
    response_model=Entity,
    response_model_exclude_none=True,
)
async def get_address_entity(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    include_actors: IncludeActorsQuery = True,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get the entity of an address"""
    result = await service.get_address_entity(
        ctx,
        currency=currency.lower(),
        address=address,
        include_actors=include_actors,
    )
    return result


@router.get(
    "/addresses/{address}/tag_summary",
    summary="Get attribution tag summary for a given address",
    operation_id="get_tag_summary_by_address",
    response_model=TagSummary,
    response_model_exclude_none=True,
)
async def get_tag_summary_by_address(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    include_best_cluster_tag: IncludeBestClusterTagQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get attribution tag summary for a given address"""
    ctx.obfuscate_private_tags = should_obfuscate_private_tags(request)

    result = await service.get_tag_summary_by_address(
        ctx,
        currency=currency.lower(),
        address=address,
        include_best_cluster_tag=include_best_cluster_tag,
    )
    return result


@router.get(
    "/addresses/{address}/tags",
    summary="Get attribution tags for a given address",
    operation_id="list_tags_by_address",
    response_model=AddressTags,
    response_model_exclude_none=True,
)
async def list_tags_by_address(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    include_best_cluster_tag: IncludeBestClusterTagQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get attribution tags for a given address"""
    result = await service.list_tags_by_address(
        ctx,
        currency=currency.lower(),
        address=address,
        page=normalize_page(page),
        pagesize=pagesize,
        include_best_cluster_tag=include_best_cluster_tag,
    )
    return result


@router.get(
    "/addresses/{address}/txs",
    summary="Get all transactions an address has been involved in",
    operation_id="list_address_txs",
    response_model=AddressTxs,
    response_model_exclude_none=True,
)
async def list_address_txs(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    direction: OptionalDirectionQuery = None,
    min_height: MinHeightQuery = None,
    max_height: MaxHeightQuery = None,
    min_date: MinDateQuery = None,
    max_date: MaxDateQuery = None,
    order: OrderQuery = None,
    token_currency: TokenCurrencyQuery = None,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get all transactions an address has been involved in"""
    result = await service.list_address_txs(
        ctx,
        currency=currency.lower(),
        address=address,
        direction=direction,
        min_height=min_height,
        max_height=max_height,
        min_date=parse_datetime(min_date),
        max_date=parse_datetime(max_date),
        order=order,
        token_currency=token_currency,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/addresses/{address}/neighbors",
    summary="Get an address's neighbors in the address graph",
    operation_id="list_address_neighbors",
    response_model=NeighborAddresses,
    response_model_exclude_none=True,
)
async def list_address_neighbors(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    direction: DirectionQuery,
    only_ids: Optional[str] = Query(
        None, description="Restrict result to given set of comma separated addresses"
    ),
    include_labels: IncludeLabelsQuery = None,
    include_actors: IncludeActorsQuery = True,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get an address's neighbors in the address graph"""
    result = await service.list_address_neighbors(
        ctx,
        currency=currency.lower(),
        address=address,
        direction=direction,
        only_ids=parse_comma_separated_strings(only_ids),
        include_labels=include_labels,
        include_actors=include_actors,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/addresses/{address}/links",
    summary="Get outgoing transactions between two addresses",
    operation_id="list_address_links",
    response_model=Links,
    response_model_exclude_none=True,
)
async def list_address_links(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    neighbor: str = Query(
        ...,
        description="Neighbor address",
        examples=["1FKCzy3BEtiZDhRDtivp7Y7RVb9edg5BH7"],
    ),
    min_height: MinHeightQuery = None,
    max_height: MaxHeightQuery = None,
    min_date: MinDateQuery = None,
    max_date: MaxDateQuery = None,
    order: OrderQuery = None,
    token_currency: TokenCurrencyQuery = None,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get outgoing transactions between two addresses"""
    result = await service.list_address_links(
        ctx,
        currency=currency.lower(),
        address=address,
        neighbor=neighbor,
        min_height=min_height,
        max_height=max_height,
        min_date=parse_datetime(min_date),
        max_date=parse_datetime(max_date),
        order=order,
        token_currency=token_currency,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/addresses/{address}/related_addresses",
    summary="Get related addresses to the input address",
    operation_id="list_related_addresses",
    response_model=RelatedAddresses,
    response_model_exclude_none=True,
)
async def list_related_addresses(
    request: Request,
    currency: CurrencyPath,
    address: AddressPath,
    address_relation_type: Literal["pubkey"] = Query(
        "pubkey",
        description="What type of related addresses to return",
        examples=["pubkey"],
    ),
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get related addresses to the input address"""
    result = await service.list_related_addresses(
        ctx,
        currency=currency.lower(),
        address=address,
        address_relation_type=address_relation_type,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result
