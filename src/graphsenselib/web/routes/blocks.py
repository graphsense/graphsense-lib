"""Block API routes"""

from typing import Union

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import Block, BlockAtDate, TxAccount, TxUtxo
from graphsenselib.web.routes.base import (
    get_ctx_no_tags,
    parse_datetime,
    respond,
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
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    height: int = Path(..., description="The block height", examples=[1]),
    ctx: ServiceContext = Depends(get_ctx_no_tags),
):
    """Get a block by its height"""
    result = await service.get_block(
        ctx,
        currency=currency.lower(),
        height=height,
    )
    return respond(request, result)


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
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    height: int = Path(..., description="The block height", examples=[1]),
    ctx: ServiceContext = Depends(get_ctx_no_tags),
):
    """Get block transactions"""
    result = await service.list_block_txs(
        ctx,
        currency=currency.lower(),
        height=height,
    )
    return respond(request, result)


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
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
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
    return respond(request, result)
