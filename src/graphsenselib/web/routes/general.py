"""General API routes (search, stats)"""

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import SearchResult, Stats
from graphsenselib.web.routes.base import (
    RequestAdapter,
    apply_plugin_hooks,
    get_services,
    get_show_private_tags,
    get_tagstore_access_groups,
    to_json_response,
)
from graphsenselib.web.security import get_api_key
import graphsenselib.web.service.general_service as service

router = APIRouter()


@router.get(
    "/stats",
    summary="Get statistics of supported currencies",
    operation_id="get_statistics",
    response_model=Stats,
    response_model_exclude_none=True,
)
async def get_statistics(
    request: Request,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """Get statistics of supported currencies"""
    # Create adapter for service layer
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.get_statistics(adapted_request)
    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/search",
    summary="Returns matching addresses, transactions and labels",
    operation_id="search",
    response_model=SearchResult,
    response_model_exclude_none=True,
    dependencies=[Depends(get_api_key)],
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
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """Returns matching addresses, transactions and labels"""
    # Normalize currency
    if currency is not None:
        currency = currency.lower()

    # Create adapter for service layer
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.search(
        adapted_request,
        q=q,
        currency=currency,
        limit=limit,
        include_sub_tx_identifiers=include_sub_tx_identifiers,
        include_labels=include_labels,
        include_actors=include_actors,
        include_txs=include_txs,
        include_addresses=include_addresses,
    )
    apply_plugin_hooks(request, result)
    return to_json_response(result)
