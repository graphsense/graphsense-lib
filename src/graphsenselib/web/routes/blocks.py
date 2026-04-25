"""Block API routes"""

from typing import Union

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import Block, BlockAtDate, TxAccount, TxUtxo
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx_no_tags,
    parse_datetime,
)
from graphsenselib.web.routes.params import (
    CurrencyPath,
    HeightPath,
)
import graphsenselib.web.service.blocks_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/blocks/{height}",
    summary="Get block details by height",
    description="Returns block metadata for the given block height.",
    operation_id="get_block",
    response_model=Block,
    response_model_exclude_none=True,
    responses={404: {"description": "Block not found for the selected currency."}},
)
async def get_block(
    request: Request,
    currency: CurrencyPath,
    height: HeightPath,
    ctx: ServiceContext = Depends(get_ctx_no_tags),
):
    """Get a block by its height"""
    result = await service.get_block(
        ctx,
        currency=currency.lower(),
        height=height,
    )
    return result


@router.get(
    "/blocks/{height}/txs",
    summary="List transactions in a block",
    description="Returns all transactions contained in the block at the given height.",
    operation_id="list_block_txs",
    response_model=list[Union[TxUtxo, TxAccount]],
    response_model_exclude_none=True,
    responses={404: {"description": "Block not found for the selected currency."}},
)
async def list_block_txs(
    request: Request,
    currency: CurrencyPath,
    height: HeightPath,
    ctx: ServiceContext = Depends(get_ctx_no_tags),
):
    """List transactions in a block."""
    result = await service.list_block_txs(
        ctx,
        currency=currency.lower(),
        height=height,
    )
    return result


@router.get(
    "/block_by_date/{date}",
    summary="Get block at or before a date",
    description=(
        "Returns the closest block for the provided timestamp and selected currency."
    ),
    operation_id="get_block_by_date",
    response_model=BlockAtDate,
    response_model_exclude_none=True,
    responses={404: {"description": "No block found for the provided date."}},
)
async def get_block_by_date(
    request: Request,
    currency: CurrencyPath,
    date: str = Path(
        ..., description="The date (YYYY-MM-DD)", examples=["2017-07-21T17:32:28Z"]
    ),
    ctx: ServiceContext = Depends(get_ctx_no_tags),
):
    """Get block by date"""
    result = await service.get_block_by_date(
        ctx,
        currency=currency.lower(),
        date=parse_datetime(date),
    )
    return result
