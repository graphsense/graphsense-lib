"""General API routes (search, stats)"""

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import SearchResult, Stats
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
)
import graphsenselib.web.service.general_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/stats",
    summary="Get platform statistics for supported currencies",
    description="Returns per-currency platform statistics available in this API deployment.",
    operation_id="get_statistics",
    response_model=Stats,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Statistics grouped by supported currency."},
    },
)
async def get_statistics(
    request: Request,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns per-currency platform statistics."""
    result = await service.get_statistics(ctx, version=request.app.version)
    return result


@router.get(
    "/search",
    summary="Search addresses, transactions, actors, and labels",
    description=(
        "Returns matching addresses, transactions, actors, and labels for the query "
        "with optional currency and result-type filters."
    ),
    operation_id="search",
    response_model=SearchResult,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Search results grouped by requested result types."},
        422: {"description": "Validation error in query parameters."},
    },
)
async def search(
    request: Request,
    q: str = Query(
        ...,
        min_length=2,
        description="Search query (address, transaction, or label)",
        examples=["foo"],
    ),
    currency: Optional[str] = Query(
        None, description="The cryptocurrency (e.g., btc)", examples=["btc"]
    ),
    limit: int = Query(
        10, description="Maximum number of search results", examples=[10]
    ),
    include_sub_tx_identifiers: bool = Query(
        False,
        description="Whether to include sub-transaction identifiers",
        examples=[False],
    ),
    include_labels: bool = Query(
        True, description="Whether to include labels", examples=[True]
    ),
    include_actors: bool = Query(
        True, description="Whether to include actors", examples=[True]
    ),
    include_txs: bool = Query(
        True, description="Whether to include transactions", examples=[True]
    ),
    include_addresses: bool = Query(
        True, description="Whether to include addresses", examples=[True]
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns matching addresses, transactions, actors, and labels."""
    if currency is not None:
        currency = currency.lower()

    result = await service.search(
        ctx,
        q=q,
        currency=currency,
        limit=limit,
        include_sub_tx_identifiers=include_sub_tx_identifiers,
        include_labels=include_labels,
        include_actors=include_actors,
        include_txs=include_txs,
        include_addresses=include_addresses,
    )
    return result
