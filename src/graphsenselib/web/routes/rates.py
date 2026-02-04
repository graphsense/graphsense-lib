"""Rate API routes"""

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import Rates
from graphsenselib.web.routes.base import (
    RequestAdapter,
    apply_plugin_hooks,
    get_services,
    get_tagstore_access_groups,
    to_json_response,
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
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get exchange rates for a given block height"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_exchange_rates(
        adapted_request,
        currency=currency,
        height=height,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
