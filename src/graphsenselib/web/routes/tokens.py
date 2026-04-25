"""Token API routes"""

from fastapi import APIRouter, Depends, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import TokenConfigs
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
)
from graphsenselib.web.routes.params import CurrencyPath
import graphsenselib.web.service.tokens_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/supported_tokens",
    summary="List supported tokens for a currency",
    description="Returns token configurations supported for the requested base currency.",
    operation_id="list_supported_tokens",
    response_model=TokenConfigs,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Supported token configurations for the currency."},
        422: {"description": "Validation error in path/query parameters."},
    },
)
async def list_supported_tokens(
    request: Request,
    currency: CurrencyPath,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns supported token configurations for a currency."""
    result = await service.list_supported_tokens(
        ctx,
        currency=currency.lower(),
    )
    return result
