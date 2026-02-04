"""Block API routes"""

from typing import Union

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import Block, BlockAtDate, TxAccount, TxUtxo
from graphsenselib.web.routes.base import (
    RequestAdapter,
    apply_plugin_hooks,
    get_services,
    parse_datetime,
    to_json_response,
)
import graphsenselib.web.service.blocks_service as service

router = APIRouter()


@router.get(
    "/blocks/{height}",
    summary="Get a block by its height",
    operation_id="get_block",
    response_model=Block,
    response_model_exclude_none=True,
)
async def get_block(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    height: int = Path(..., description="The block height", example=1),
    services: ServiceContainer = Depends(get_services),
):
    """Get a block by its height"""
    currency = currency.lower()
    # Blocks don't have tags - skip tagstore_groups dependency overhead
    adapted_request = RequestAdapter(request, services, [])

    result = await service.get_block(
        adapted_request,
        currency=currency,
        height=height,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/blocks/{height}/txs",
    summary="Get block transactions",
    operation_id="list_block_txs",
    response_model=list[Union[TxUtxo, TxAccount]],
    response_model_exclude_none=True,
)
async def list_block_txs(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    height: int = Path(..., description="The block height", example=1),
    services: ServiceContainer = Depends(get_services),
):
    """Get block transactions"""
    currency = currency.lower()
    # Blocks don't have tags - skip tagstore_groups dependency overhead
    adapted_request = RequestAdapter(request, services, [])

    result = await service.list_block_txs(
        adapted_request,
        currency=currency,
        height=height,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/block_by_date/{date}",
    summary="Get block by date",
    operation_id="get_block_by_date",
    response_model=BlockAtDate,
    response_model_exclude_none=True,
)
async def get_block_by_date(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    date: str = Path(
        ..., description="The date (YYYY-MM-DD)", example="2017-07-21T17:32:28Z"
    ),
    services: ServiceContainer = Depends(get_services),
):
    """Get block by date"""
    currency = currency.lower()
    # Blocks don't have tags - skip tagstore_groups dependency overhead
    adapted_request = RequestAdapter(request, services, [])

    result = await service.get_block_by_date(
        adapted_request,
        currency=currency,
        date=parse_datetime(date),
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
