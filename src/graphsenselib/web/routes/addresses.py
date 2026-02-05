"""Address API routes"""

from typing import Literal, Optional

from fastapi import APIRouter, Depends, Path, Query, Request

from graphsenselib.web.dependencies import ServiceContainer
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
    RequestAdapter,
    apply_plugin_hooks,
    get_services,
    get_tagstore_access_groups,
    normalize_page,
    parse_comma_separated_strings,
    parse_datetime,
    to_json_response,
)
import graphsenselib.web.service.addresses_service as service

router = APIRouter()


@router.get(
    "/addresses/{address}",
    summary="Get an address",
    operation_id="get_address",
    response_model=Address,
    response_model_exclude_none=True,
)
async def get_address(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
    include_actors: bool = Query(
        True, description="Whether to include actor information", examples=[True]
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get an address"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_address(
        adapted_request,
        currency=currency,
        address=address,
        include_actors=include_actors,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/addresses/{address}/entity",
    summary="Get the entity of an address",
    operation_id="get_address_entity",
    response_model=Entity,
    response_model_exclude_none=True,
)
async def get_address_entity(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
    include_actors: bool = Query(
        True, description="Whether to include actor information", examples=[True]
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get the entity of an address"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_address_entity(
        adapted_request,
        currency=currency,
        address=address,
        include_actors=include_actors,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/addresses/{address}/tag_summary",
    summary="Get attribution tag summary for a given address",
    operation_id="get_tag_summary_by_address",
    response_model=TagSummary,
    response_model_exclude_none=True,
)
async def get_tag_summary_by_address(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
    include_best_cluster_tag: Optional[bool] = Query(
        None,
        description="If the best cluster tag should be inherited to the address level",
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get attribution tag summary for a given address"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_tag_summary_by_address(
        adapted_request,
        currency=currency,
        address=address,
        include_best_cluster_tag=include_best_cluster_tag,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/addresses/{address}/tags",
    summary="Get attribution tags for a given address",
    operation_id="list_tags_by_address",
    response_model=AddressTags,
    response_model_exclude_none=True,
)
async def list_tags_by_address(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
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
    include_best_cluster_tag: Optional[bool] = Query(
        None,
        description="If the best cluster tag should be inherited to the address level",
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get attribution tags for a given address"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_tags_by_address(
        adapted_request,
        currency=currency,
        address=address,
        page=normalize_page(page),
        pagesize=pagesize,
        include_best_cluster_tag=include_best_cluster_tag,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/addresses/{address}/txs",
    summary="Get all transactions an address has been involved in",
    operation_id="list_address_txs",
    response_model=AddressTxs,
    response_model_exclude_none=True,
)
async def list_address_txs(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
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
    """Get all transactions an address has been involved in"""
    currency = currency.lower()
    min_date_parsed = parse_datetime(min_date)
    max_date_parsed = parse_datetime(max_date)

    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_address_txs(
        adapted_request,
        currency=currency,
        address=address,
        direction=direction,
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
    "/addresses/{address}/neighbors",
    summary="Get an address's neighbors in the address graph",
    operation_id="list_address_neighbors",
    response_model=NeighborAddresses,
    response_model_exclude_none=True,
)
async def list_address_neighbors(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
    direction: Literal["in", "out"] = Query(
        ..., description="Incoming or outgoing neighbors", examples=["out"]
    ),
    only_ids: Optional[str] = Query(
        None, description="Restrict result to given set of comma separated addresses"
    ),
    include_labels: Optional[bool] = Query(
        None,
        description="Whether to include labels of first page of address tags",
        examples=[True],
    ),
    include_actors: bool = Query(
        True, description="Whether to include actor information", examples=[True]
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
    """Get an address's neighbors in the address graph"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_address_neighbors(
        adapted_request,
        currency=currency,
        address=address,
        direction=direction,
        only_ids=parse_comma_separated_strings(only_ids),
        include_labels=include_labels,
        include_actors=include_actors,
        page=normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/addresses/{address}/links",
    summary="Get outgoing transactions between two addresses",
    operation_id="list_address_links",
    response_model=Links,
    response_model_exclude_none=True,
)
async def list_address_links(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
    neighbor: str = Query(
        ...,
        description="Neighbor address",
        examples=["1FKCzy3BEtiZDhRDtivp7Y7RVb9edg5BH7"],
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
    """Get outgoing transactions between two addresses"""
    currency = currency.lower()
    min_date_parsed = parse_datetime(min_date)
    max_date_parsed = parse_datetime(max_date)

    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_address_links(
        adapted_request,
        currency=currency,
        address=address,
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
    "/addresses/{address}/related_addresses",
    summary="Get related addresses to the input address",
    operation_id="list_related_addresses",
    response_model=RelatedAddresses,
    response_model_exclude_none=True,
)
async def list_related_addresses(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    address: str = Path(
        ...,
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
    address_relation_type: Literal["pubkey"] = Query(
        "pubkey",
        description="What type of related addresses to return",
        examples=["pubkey"],
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
    """Get related addresses to the input address"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_related_addresses(
        adapted_request,
        currency=currency,
        address=address,
        address_relation_type=address_relation_type,
        page=normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
