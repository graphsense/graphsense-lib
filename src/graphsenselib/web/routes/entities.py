"""Entity API routes"""

import logging
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Path, Query, Request

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import (
    AddressTags,
    AddressTxs,
    Entity,
    EntityAddresses,
    Links,
    NeighborEntities,
    SearchResultLevel1,
)
from graphsenselib.web.routes.base import (
    apply_plugin_hooks,
    get_services,
    get_tagstore_access_groups,
    make_ctx,
    normalize_page,
    parse_comma_separated_ints,
    parse_comma_separated_strings,
    parse_datetime,
    to_json_response,
)
import graphsenselib.web.service.entities_service as service

router = APIRouter()


@router.get(
    "/entities/{entity}",
    summary="Get an entity",
    operation_id="get_entity",
    response_model=Entity,
    response_model_exclude_none=True,
)
async def get_entity(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    exclude_best_address_tag: Optional[bool] = Query(
        None, description="Whether to exclude best address tag"
    ),
    include_actors: bool = Query(
        False, description="Whether to include actor information", examples=[True]
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get an entity"""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.get_entity(
        ctx,
        currency=currency,
        entity=entity,
        exclude_best_address_tag=exclude_best_address_tag,
        include_actors=include_actors,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/entities/{entity}/addresses",
    summary="Get an entity's addresses",
    operation_id="list_entity_addresses",
    response_model=EntityAddresses,
    response_model_exclude_none=True,
)
async def list_entity_addresses(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None,
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get an entity's addresses"""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.list_entity_addresses(
        ctx,
        currency=currency,
        entity=entity,
        page=normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/entities/{entity}/neighbors",
    summary="Get an entity's neighbors in the entity graph",
    operation_id="list_entity_neighbors",
    deprecated=True,
    response_model=NeighborEntities,
    response_model_exclude_none=True,
)
async def list_entity_neighbors(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    direction: Literal["in", "out"] = Query(
        ..., description="Incoming or outgoing neighbors", examples=["out"]
    ),
    only_ids: Optional[str] = Query(
        None, description="Restrict result to given set of comma separated IDs"
    ),
    include_labels: Optional[bool] = Query(
        None, description="Whether to include labels", examples=[True]
    ),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None,
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
    relations_only: Optional[bool] = Query(
        None, description="Return only relations without entity details"
    ),
    exclude_best_address_tag: Optional[bool] = Query(
        None, description="Whether to exclude best address tag"
    ),
    include_actors: bool = Query(
        False, description="Whether to include actor information", examples=[True]
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get an entity's neighbors in the entity graph"""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.list_entity_neighbors(
        ctx,
        currency=currency,
        entity=entity,
        direction=direction,
        only_ids=parse_comma_separated_ints(only_ids),
        include_labels=include_labels,
        page=normalize_page(page),
        pagesize=pagesize,
        relations_only=relations_only,
        exclude_best_address_tag=exclude_best_address_tag,
        include_actors=include_actors,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/entities/{entity}/links",
    summary="Get transactions between two entities",
    operation_id="list_entity_links",
    deprecated=True,
    response_model=Links,
    response_model_exclude_none=True,
)
async def list_entity_links(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    neighbor: int = Query(..., description="Neighbor entity ID", examples=[123456]),
    min_height: Optional[int] = Query(
        None, description="Return transactions starting from given height", examples=[1]
    ),
    max_height: Optional[int] = Query(
        None,
        description="Return transactions up to (including) given height",
        examples=[2],
    ),
    min_date: Optional[str] = Query(
        None, description="Min date of txs", examples=["2017-07-21T17:32:28Z"]
    ),
    max_date: Optional[str] = Query(
        None, description="Max date of txs", examples=["2017-07-21T17:32:28Z"]
    ),
    order: Optional[str] = Query(None, description="Sorting order", examples=["desc"]),
    token_currency: Optional[str] = Query(
        None,
        description="Return transactions of given token or base currency",
        examples=["WETH"],
    ),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None,
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get transactions between two entities"""
    currency = currency.lower()
    min_date_parsed = parse_datetime(min_date)
    max_date_parsed = parse_datetime(max_date)

    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.list_entity_links(
        ctx,
        currency=currency,
        entity=entity,
        neighbor=neighbor,
        min_height=min_height,
        max_height=max_height,
        min_date=min_date_parsed,
        max_date=max_date_parsed,
        order=order,
        token_currency=token_currency,
        page=normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/entities/{entity}/tags",
    summary="Get address tags for a given entity",
    operation_id="list_address_tags_by_entity",
    deprecated=True,
    response_model=AddressTags,
    response_model_exclude_none=True,
)
async def list_address_tags_by_entity(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None,
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get address tags for a given entity"""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.list_address_tags_by_entity(
        ctx,
        currency=currency,
        entity=entity,
        page=normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/entities/{entity}/txs",
    summary="Get all transactions an entity has been involved in",
    operation_id="list_entity_txs",
    deprecated=True,
    response_model=AddressTxs,
    response_model_exclude_none=True,
)
async def list_entity_txs(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    direction: Optional[str] = Query(
        None, description="Incoming or outgoing transactions", examples=["out"]
    ),
    min_height: Optional[int] = Query(
        None, description="Return transactions starting from given height", examples=[1]
    ),
    max_height: Optional[int] = Query(
        None,
        description="Return transactions up to (including) given height",
        examples=[2],
    ),
    min_date: Optional[str] = Query(
        None, description="Min date of txs", examples=["2017-07-21T17:32:28Z"]
    ),
    max_date: Optional[str] = Query(
        None, description="Max date of txs", examples=["2017-07-21T17:32:28Z"]
    ),
    order: Optional[str] = Query(None, description="Sorting order", examples=["desc"]),
    token_currency: Optional[str] = Query(
        None,
        description="Return transactions of given token or base currency",
        examples=["WETH"],
    ),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None,
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get all transactions an entity has been involved in"""
    currency = currency.lower()
    min_date_parsed = parse_datetime(min_date)
    max_date_parsed = parse_datetime(max_date)

    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.list_entity_txs(
        ctx,
        currency=currency,
        entity=entity,
        min_height=min_height,
        max_height=max_height,
        min_date=min_date_parsed,
        max_date=max_date_parsed,
        direction=direction,
        order=order,
        token_currency=token_currency,
        page=normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/entities/{entity}/search",
    summary="Search neighbors of an entity",
    operation_id="search_entity_neighbors",
    response_model=list[SearchResultLevel1],
    response_model_exclude_none=True,
)
async def search_entity_neighbors(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    entity: int = Path(..., description="The entity ID", examples=[67065]),
    direction: Literal["in", "out"] = Query(
        ..., description="Incoming or outgoing neighbors", examples=["out"]
    ),
    key: str = Query(..., description="Search key", examples=["category"]),
    value: str = Query(
        ..., description="Comma separated search values", examples=["Miner"]
    ),
    depth: int = Query(..., description="Search depth", examples=[2]),
    breadth: int = Query(..., description="Search breadth", examples=[16]),
    skip_num_addresses: Optional[int] = Query(
        None, description="Skip entities with more than N addresses"
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Search neighbors of an entity"""
    currency = currency.lower()
    ctx = make_ctx(
        request,
        services,
        tagstore_groups,
        logger=logging.getLogger(__name__),
    )

    result = await service.search_entity_neighbors(
        ctx,
        currency=currency,
        entity=entity,
        direction=direction,
        key=key,
        value=parse_comma_separated_strings(value) or [],
        depth=depth,
        breadth=breadth,
        skip_num_addresses=skip_num_addresses,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
