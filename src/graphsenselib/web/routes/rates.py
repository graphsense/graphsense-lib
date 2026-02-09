"""Rate API routes"""

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import Rates
from graphsenselib.web.routes.base import (
    get_ctx,
    respond,
)
import graphsenselib.web.service.rates_service as service

router = APIRouter()


@router.get(
    "/rates/{height}",
    summary="Get exchange rates for a given block height",
    operation_id="get_exchange_rates",
    response_model=Rates,
    response_model_exclude_none=True,
)
async def get_exchange_rates(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    height: int = Path(..., description="The block height", examples=[1]),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get exchange rates for a given block height"""
    result = await service.get_exchange_rates(
        ctx,
        currency=currency.lower(),
        height=height,
    )
    return respond(request, result)
