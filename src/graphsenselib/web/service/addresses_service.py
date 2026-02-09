from typing import Optional

from graphsenselib.errors import BadUserInputException
from graphsenselib.tagstore.algorithms.obfuscate import obfuscate_tag_if_not_public

from graphsenselib.web.service import parse_page_int_optional
from graphsenselib.web.translators import (
    pydantic_to_openapi,
)


async def list_related_addresses(
    ctx,
    currency: str,
    address: str,
    address_relation_type: str,
    page: Optional[int] = None,
    pagesize: Optional[int] = None,
):
    page = parse_page_int_optional(page)

    if address_relation_type not in ["pubkey"]:
        raise BadUserInputException("Invalid address_relation_type. Must be 'pubkey'")

    pydantic_result = (
        await ctx.services.addresses_service.get_cross_chain_pubkey_related_addresses(
            address, network=currency, page=page, pagesize=pagesize
        )
    )

    return pydantic_to_openapi(pydantic_result)


async def get_tag_summary_by_address(
    ctx, currency, address, include_best_cluster_tag=False
):
    include_pubkey_derived_tags = ctx.config.include_pubkey_derived_tags
    tag_summary_only_propagate_high_confidence_actors = (
        ctx.config.tag_summary_only_propagate_high_confidence_actors
    )

    pydantic_result = await ctx.services.addresses_service.get_tag_summary_by_address(
        currency,
        address,
        ctx.tagstore_groups,
        include_best_cluster_tag,
        include_pubkey_derived_tags=include_pubkey_derived_tags,
        only_propagate_high_confidence_actors=tag_summary_only_propagate_high_confidence_actors,
        tag_transformer=(
            None if not ctx.obfuscate_private_tags else obfuscate_tag_if_not_public
        ),
    )

    return pydantic_to_openapi(pydantic_result)


async def get_address(ctx, currency, address, include_actors=True):
    pydantic_result = await ctx.services.addresses_service.get_address(
        currency, address, ctx.tagstore_groups, include_actors
    )

    return pydantic_to_openapi(pydantic_result)


async def list_tags_by_address(
    ctx, currency, address, page=None, pagesize=None, include_best_cluster_tag=False
):
    include_pubkey_derived_tags = ctx.config.include_pubkey_derived_tags

    pydantic_result = await ctx.services.addresses_service.list_tags_by_address(
        currency,
        address,
        ctx.tagstore_groups,
        ctx.cache,
        page,
        pagesize,
        include_best_cluster_tag,
        include_pubkey_derived_tags=include_pubkey_derived_tags,
    )

    return pydantic_to_openapi(pydantic_result)


async def list_address_txs(
    ctx,
    currency,
    address,
    min_height=None,
    max_height=None,
    min_date=None,
    max_date=None,
    direction=None,
    order="desc",
    token_currency=None,
    page=None,
    pagesize=None,
):
    pydantic_result = await ctx.services.addresses_service.list_address_txs(
        currency,
        address,
        min_height,
        max_height,
        min_date,
        max_date,
        direction,
        order,
        token_currency,
        page,
        pagesize,
    )

    return pydantic_to_openapi(pydantic_result)


async def list_address_neighbors(
    ctx,
    currency,
    address,
    direction,
    only_ids=None,
    include_labels=False,
    include_actors=True,
    page=None,
    pagesize=None,
):
    pydantic_result = await ctx.services.addresses_service.list_address_neighbors(
        currency,
        address,
        direction,
        ctx.tagstore_groups,
        only_ids,
        include_labels,
        include_actors,
        page,
        pagesize,
    )

    return pydantic_to_openapi(pydantic_result)


async def list_address_links(
    ctx,
    currency,
    address,
    neighbor,
    min_height=None,
    max_height=None,
    min_date=None,
    max_date=None,
    order="desc",
    token_currency=None,
    page=None,
    pagesize=None,
):
    request_timeout = ctx.config.address_links_request_timeout

    pydantic_result = await ctx.services.addresses_service.list_address_links(
        currency,
        address,
        neighbor,
        min_height,
        max_height,
        min_date,
        max_date,
        order,
        token_currency,
        page,
        pagesize,
        request_timeout,
    )

    return pydantic_to_openapi(pydantic_result)


async def get_address_entity(ctx, currency, address, include_actors=True):
    pydantic_result = await ctx.services.addresses_service.get_address_entity(
        currency, address, include_actors, ctx.tagstore_groups
    )

    return pydantic_to_openapi(pydantic_result)
