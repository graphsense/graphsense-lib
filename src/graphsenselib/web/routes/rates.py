"""Rate API routes"""

from fastapi import APIRouter, Depends, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import Rates
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
)
from graphsenselib.web.routes.params import (
    CurrencyPath,
    HeightPath,
)
import graphsenselib.web.service.rates_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/rates/{height}",
    summary="Get exchange rates for a given block height",
    operation_id="get_exchange_rates",
    response_model=Rates,
    response_model_exclude_none=True,
)
async def get_exchange_rates(
    request: Request,
    currency: CurrencyPath,
    height: HeightPath,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get exchange rates for a given block height"""
    result = await service.get_exchange_rates(
        ctx,
        currency=currency.lower(),
        height=height,
    )
    return result
