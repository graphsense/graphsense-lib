"""Token API routes"""

from fastapi import APIRouter, Depends, Path, Request

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import TokenConfigs
from graphsenselib.web.routes.base import (
    apply_plugin_hooks,
    get_services,
    get_tagstore_access_groups,
    make_ctx,
    to_json_response,
)
import graphsenselib.web.service.tokens_service as service

router = APIRouter()


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
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get supported tokens for a currency"""
    currency = currency.lower()
    ctx = make_ctx(request, services, tagstore_groups)

    result = await service.list_supported_tokens(
        ctx,
        currency=currency,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
