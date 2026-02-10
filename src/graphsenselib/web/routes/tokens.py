"""Token API routes"""

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import TokenConfigs
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
)
import graphsenselib.web.service.tokens_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/supported_tokens",
    summary="Get supported tokens for a currency",
    operation_id="list_supported_tokens",
    response_model=TokenConfigs,
    response_model_exclude_none=True,
)
async def list_supported_tokens(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., eth)", examples=["eth"]
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get supported tokens for a currency"""
    result = await service.list_supported_tokens(
        ctx,
        currency=currency.lower(),
    )
    return result
