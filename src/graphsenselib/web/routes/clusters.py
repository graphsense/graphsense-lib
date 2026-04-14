"""Cluster API routes.

These endpoints replace the deprecated `/entities/...` routes. Both sets are
served in parallel during the deprecation window and share the same service
layer (`graphsenselib.web.service.clusters_service`).
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

import graphsenselib.web.service.clusters_service as service
from graphsenselib.web.models import (
    AddressTags,
    AddressTxs,
    Cluster,
    ClusterAddresses,
    Links,
    NeighborClusters,
    SearchResultLevel1,
)
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
    normalize_page,
    parse_comma_separated_ints,
    parse_comma_separated_strings,
    parse_datetime,
)
from graphsenselib.web.routes.params import (
    ClusterPath,
    CurrencyPath,
    DirectionQuery,
    ExcludeBestAddressTagQuery,
    IncludeActorsQuery,
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
from graphsenselib.web.service import ServiceContext

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/clusters/{cluster}",
    summary="Get cluster details",
    description="Returns details for a single address cluster.",
    operation_id="get_cluster",
    response_model=Cluster,
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def get_cluster(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
    exclude_best_address_tag: ExcludeBestAddressTagQuery = None,
    include_actors: IncludeActorsQuery = False,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get a cluster."""
    result = await service.get_cluster(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
        exclude_best_address_tag=exclude_best_address_tag,
        include_actors=include_actors,
    )
    return result


@router.get(
    "/clusters/{cluster}/addresses",
    summary="List cluster addresses",
    description="Returns paginated addresses that belong to the cluster.",
    operation_id="list_cluster_addresses",
    response_model=ClusterAddresses,
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def list_cluster_addresses(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """List addresses that belong to a cluster."""
    result = await service.list_cluster_addresses(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/clusters/{cluster}/neighbors",
    summary="List neighboring clusters",
    description=(
        "Returns neighboring clusters connected to the given cluster in the "
        "cluster graph."
    ),
    operation_id="list_cluster_neighbors",
    deprecated=True,
    response_model=NeighborClusters,
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def list_cluster_neighbors(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
    direction: DirectionQuery,
    only_ids: Optional[str] = Query(
        None, description="Restrict result to given set of comma separated IDs"
    ),
    include_labels: IncludeLabelsQuery = None,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    relations_only: Optional[bool] = Query(
        None, description="Return only relations without cluster details"
    ),
    exclude_best_address_tag: ExcludeBestAddressTagQuery = None,
    include_actors: IncludeActorsQuery = False,
    ctx: ServiceContext = Depends(get_ctx),
):
    """List neighboring clusters in the cluster graph."""
    result = await service.list_cluster_neighbors(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
        direction=direction,
        only_ids=parse_comma_separated_ints(only_ids),
        include_labels=include_labels,
        page=normalize_page(page),
        pagesize=pagesize,
        relations_only=relations_only,
        exclude_best_address_tag=exclude_best_address_tag,
        include_actors=include_actors,
    )
    return result


@router.get(
    "/clusters/{cluster}/links",
    summary="List transactions between clusters",
    description=(
        "Returns paginated transaction links between the cluster and a neighbor "
        "cluster."
    ),
    operation_id="list_cluster_links",
    deprecated=True,
    response_model=Links,
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def list_cluster_links(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
    neighbor: int = Query(..., description="Neighbor cluster ID", examples=[123456]),
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
    """List transactions between two clusters."""
    result = await service.list_cluster_links(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
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
    "/clusters/{cluster}/tags",
    summary="List cluster address tags",
    description=(
        "Returns paginated attribution tags observed on addresses in the cluster."
    ),
    operation_id="list_address_tags_by_cluster",
    deprecated=True,
    response_model=AddressTags,
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def list_address_tags_by_cluster(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """List address tags for a cluster."""
    result = await service.list_address_tags_by_cluster(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/clusters/{cluster}/txs",
    summary="List cluster transactions",
    description=(
        "Returns paginated transactions involving the cluster, with optional "
        "height, date, direction, and token filters."
    ),
    operation_id="list_cluster_txs",
    deprecated=True,
    response_model=AddressTxs,
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def list_cluster_txs(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
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
    """List transactions involving a cluster."""
    result = await service.list_cluster_txs(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
        min_height=min_height,
        max_height=max_height,
        min_date=parse_datetime(min_date),
        max_date=parse_datetime(max_date),
        direction=direction,
        order=order,
        token_currency=token_currency,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/clusters/{cluster}/search",
    summary="Search cluster neighborhood",
    description=(
        "Returns matching neighboring clusters found by key/value criteria within "
        "the specified search depth and breadth."
    ),
    operation_id="search_cluster_neighbors",
    deprecated=True,
    response_model=list[SearchResultLevel1],
    response_model_exclude_none=True,
    responses={404: {"description": "Cluster not found for the selected currency."}},
)
async def search_cluster_neighbors(
    request: Request,
    currency: CurrencyPath,
    cluster: ClusterPath,
    direction: DirectionQuery,
    key: str = Query(..., description="Search key", examples=["category"]),
    value: str = Query(
        ..., description="Comma separated search values", examples=["Miner"]
    ),
    depth: int = Query(..., description="Search depth", examples=[2]),
    breadth: int = Query(..., description="Search breadth", examples=[16]),
    skip_num_addresses: Optional[int] = Query(
        None, description="Skip clusters with more than N addresses"
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Search neighbors of a cluster."""
    ctx.logger = logging.getLogger(__name__)

    result = await service.search_cluster_neighbors(
        ctx,
        currency=currency.lower(),
        cluster=cluster,
        direction=direction,
        key=key,
        value=parse_comma_separated_strings(value) or [],
        depth=depth,
        breadth=breadth,
        skip_num_addresses=skip_num_addresses,
    )
    return result
