"""Tags API routes"""

from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import BaseModel

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import (
    Actor,
    AddressTags,
    Concept,
    Taxonomy,
    UserTagReportResponse,
)
from graphsenselib.web.routes.base import (
    RequestAdapter,
    apply_plugin_hooks,
    get_services,
    get_show_private_tags,
    get_tagstore_access_groups,
    get_username,
    to_json_response,
)
import graphsenselib.web.service.tags_service as service

router = APIRouter()


def _normalize_page(page: Optional[str]) -> Optional[str]:
    """Convert empty string to None for pagination parameter."""
    if page is not None and page.strip() == "":
        return None
    return page


class UserReportedTag(BaseModel):
    """User reported tag model"""

    address: str
    network: str
    actor: Optional[str] = None
    label: str
    description: Optional[str] = None


@router.get(
    "/tags",
    summary="Get address tags by label",
    operation_id="list_address_tags",
    response_model=AddressTags,
    response_model_exclude_none=True,
)
async def list_address_tags(
    request: Request,
    label: str = Query(..., description="The label to search for", example="cimedy"),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None, ge=1, description="Number of items returned in a single page", example=10
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """Get address tags by label"""
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.list_address_tags(
        adapted_request,
        label=label,
        page=_normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/tags/actors/{actor}",
    summary="Get an actor by ID",
    operation_id="get_actor",
    response_model=Actor,
    response_model_exclude_none=True,
)
async def get_actor(
    request: Request,
    actor: str = Path(..., description="The actor ID", example="binance"),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """Get an actor by ID"""
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.get_actor(
        adapted_request,
        actor=actor,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/tags/actors/{actor}/tags",
    summary="Get tags associated with an actor",
    operation_id="get_actor_tags",
    response_model=AddressTags,
    response_model_exclude_none=True,
)
async def get_actor_tags(
    request: Request,
    actor: str = Path(..., description="The actor ID", example="binance"),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None, ge=1, description="Number of items returned in a single page", example=10
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """Get tags associated with an actor"""
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.get_actor_tags(
        adapted_request,
        actor=actor,
        page=_normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/tags/taxonomies",
    summary="List all taxonomies",
    operation_id="list_taxonomies",
    response_model=list[Taxonomy],
    response_model_exclude_none=True,
)
async def list_taxonomies(
    request: Request,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """List all taxonomies"""
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.list_taxonomies(adapted_request)

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/tags/taxonomies/{taxonomy}/concepts",
    summary="List concepts for a taxonomy",
    operation_id="list_concepts",
    response_model=list[Concept],
    response_model_exclude_none=True,
)
async def list_concepts(
    request: Request,
    taxonomy: str = Path(..., description="The taxonomy name", example="concept"),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
):
    """List concepts for a taxonomy"""
    adapted_request = RequestAdapter(
        request, services, tagstore_groups, show_private_tags=show_private
    )

    result = await service.list_concepts(
        adapted_request,
        taxonomy=taxonomy,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.post(
    "/tags/report-tag",
    summary="Report a new tag",
    operation_id="report_tag",
    response_model=UserTagReportResponse,
    response_model_exclude_none=True,
)
async def report_tag(
    request: Request,
    body: UserReportedTag,
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
    show_private: bool = Depends(get_show_private_tags),
    username: Optional[str] = Depends(get_username),
):
    """Report a new tag"""
    adapted_request = RequestAdapter(
        request,
        services,
        tagstore_groups,
        show_private_tags=show_private,
        username=username,
    )

    result = await service.report_tag(
        adapted_request,
        body=body,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
