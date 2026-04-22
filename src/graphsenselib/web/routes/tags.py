"""Tags API routes"""

from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, Request
from pydantic import BaseModel

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import (
    Actor,
    AddressTags,
    Concept,
    Taxonomy,
    UserTagReportResponse,
)
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
    get_username,
    normalize_page,
)
from graphsenselib.web.routes.params import (
    PageQuery,
    PagesizeQuery,
)
import graphsenselib.web.service.tags_service as service

router = APIRouter(route_class=PluginRoute)


class UserReportedTag(BaseModel):
    """User reported tag model"""

    address: str
    network: str
    actor: Optional[str] = None
    label: str
    description: Optional[str] = None


@router.get(
    "/tags",
    summary="List address tags by label",
    description="Returns paginated address tags matching the provided label.",
    operation_id="list_address_tags",
    response_model=AddressTags,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Paginated address tags for the requested label."},
        422: {"description": "Validation error in query parameters."},
    },
)
async def list_address_tags(
    request: Request,
    label: str = Query(..., description="The label to search for", examples=["cimedy"]),
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns paginated address tags for a label."""
    result = await service.list_address_tags(
        ctx,
        label=label,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/tags/actors/{actor}",
    summary="Get actor details by ID",
    description="Returns metadata for the actor identified by the actor ID.",
    operation_id="get_actor",
    response_model=Actor,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Actor metadata for the requested actor ID."},
        422: {"description": "Validation error in path parameters."},
    },
)
async def get_actor(
    request: Request,
    actor: str = Path(..., description="The actor ID", examples=["binance"]),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns actor metadata by actor ID."""
    result = await service.get_actor(
        ctx,
        actor=actor,
    )
    return result


@router.get(
    "/tags/actors/{actor}/tags",
    summary="List tags associated with an actor",
    description="Returns paginated address tags associated with the specified actor.",
    operation_id="get_actor_tags",
    response_model=AddressTags,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Paginated address tags linked to the actor."},
        422: {"description": "Validation error in path/query parameters."},
    },
)
async def get_actor_tags(
    request: Request,
    actor: str = Path(..., description="The actor ID", examples=["binance"]),
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns paginated tags associated with an actor."""
    result = await service.get_actor_tags(
        ctx,
        actor=actor,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


@router.get(
    "/tags/taxonomies",
    summary="List all taxonomies",
    description="Returns all available tag taxonomies.",
    operation_id="list_taxonomies",
    response_model=list[Taxonomy],
    response_model_exclude_none=True,
    responses={
        200: {"description": "List of available tag taxonomies."},
    },
)
async def list_taxonomies(
    request: Request,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns all available tag taxonomies."""
    result = await service.list_taxonomies(ctx)
    return result


@router.get(
    "/tags/taxonomies/{taxonomy}/concepts",
    summary="List concepts for a taxonomy",
    description="Returns all concepts defined for the specified taxonomy.",
    operation_id="list_concepts",
    response_model=list[Concept],
    response_model_exclude_none=True,
    responses={
        200: {"description": "List of concepts for the requested taxonomy."},
        422: {"description": "Validation error in path parameters."},
    },
)
async def list_concepts(
    request: Request,
    taxonomy: str = Path(..., description="The taxonomy name", examples=["concept"]),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns concepts for a taxonomy."""
    result = await service.list_concepts(
        ctx,
        taxonomy=taxonomy,
    )
    return result


@router.post(
    "/tags/report-tag",
    summary="Submit a user-reported tag",
    description="Stores a user-reported tag submission for review.",
    operation_id="report_tag",
    response_model=UserTagReportResponse,
    response_model_exclude_none=True,
    responses={
        200: {"description": "Confirmation that the tag report was accepted."},
        422: {"description": "Validation error in request body."},
    },
)
async def report_tag(
    request: Request,
    body: UserReportedTag,
    ctx: ServiceContext = Depends(get_ctx),
    username: Optional[str] = Depends(get_username),
):
    """Submits a new user-reported tag."""
    ctx.username = username

    result = await service.report_tag(
        ctx,
        body=body,
    )
    return result
