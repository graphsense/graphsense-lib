from graphsenselib.tagstore.db.queries import UserReportedAddressTag

from graphsenselib.web.service import parse_page_int_optional
from graphsenselib.web.models import UserTagReportResponse
from graphsenselib.web.translators import (
    to_api_actor,
    to_api_address_tag_result,
    to_api_concept,
    to_api_taxonomy,
)


async def get_actor(ctx, actor):
    pydantic_result = await ctx.services.tags_service.get_actor(actor)

    return to_api_actor(pydantic_result)


async def get_actor_tags(ctx, actor, page=None, pagesize=None):
    page = parse_page_int_optional(page)

    pydantic_result = await ctx.services.tags_service.get_actor_tags(
        actor, ctx.tagstore_groups, page, pagesize
    )

    return to_api_address_tag_result(pydantic_result)


async def list_address_tags(ctx, label, page=None, pagesize=None):
    page = parse_page_int_optional(page)

    pydantic_result = await ctx.services.tags_service.list_address_tags_by_label(
        label, ctx.tagstore_groups, page, pagesize
    )

    return to_api_address_tag_result(pydantic_result)


async def list_concepts(ctx, taxonomy):
    pydantic_results = await ctx.services.tags_service.list_concepts(taxonomy)

    return [to_api_concept(concept) for concept in pydantic_results]


async def list_taxonomies(ctx):
    pydantic_results = await ctx.services.tags_service.list_taxonomies()

    return [to_api_taxonomy(taxonomy) for taxonomy in pydantic_results]


async def report_tag(ctx, body):
    tag_acl_group = ctx.config.user_tag_reporting_acl_group

    tag_to_report = UserReportedAddressTag(
        address=body.address,
        network=body.network,
        actor=body.actor,
        label=body.label,
        description=body.description,
        user=ctx.username,
    )

    derId = await ctx.services.tags_service.report_tag(
        tag_to_report, ctx.config, tag_acl_group
    )

    return UserTagReportResponse(id=derId)
