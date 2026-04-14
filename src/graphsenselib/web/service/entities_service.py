"""Deprecated: back-compat shim for graphsenselib.web.service.clusters_service.

The web service layer was renamed from `entities_service` to `clusters_service`
as part of the deprecation of `entity`/`entities` terminology in favor of
`cluster`/`clusters`. This module re-exports the renamed functions under their
old names AND their old `entity=` keyword argument so legacy callers (e.g. the
deprecated `routes/entities.py`, bulk requests with `entity` body keys) keep
working during the deprecation window.
"""

from graphsenselib.web.service import clusters_service as _cs

# Re-exports that do not take an entity/cluster kwarg
from graphsenselib.web.service.clusters_service import (  # noqa: F401
    MAX_DEPTH,
    SEARCH_TIMEOUT,
    bfs,
    get_address,
    recursive_search,
)


async def get_entity(
    ctx, currency, entity, exclude_best_address_tag=False, include_actors=False
):
    return await _cs.get_cluster(
        ctx,
        currency,
        cluster=entity,
        exclude_best_address_tag=exclude_best_address_tag,
        include_actors=include_actors,
    )


async def list_entity_addresses(ctx, currency, entity, page=None, pagesize=None):
    return await _cs.list_cluster_addresses(
        ctx, currency, cluster=entity, page=page, pagesize=pagesize
    )


async def list_entity_neighbors(
    ctx,
    currency,
    entity,
    direction,
    only_ids=None,
    include_labels=False,
    page=None,
    pagesize=None,
    relations_only=False,
    exclude_best_address_tag=False,
    include_actors=False,
):
    return await _cs.list_cluster_neighbors(
        ctx,
        currency,
        cluster=entity,
        direction=direction,
        only_ids=only_ids,
        include_labels=include_labels,
        page=page,
        pagesize=pagesize,
        relations_only=relations_only,
        exclude_best_address_tag=exclude_best_address_tag,
        include_actors=include_actors,
    )


async def list_entity_links(
    ctx,
    currency,
    entity,
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
    return await _cs.list_cluster_links(
        ctx,
        currency,
        cluster=entity,
        neighbor=neighbor,
        min_height=min_height,
        max_height=max_height,
        min_date=min_date,
        max_date=max_date,
        order=order,
        token_currency=token_currency,
        page=page,
        pagesize=pagesize,
    )


async def list_address_tags_by_entity(ctx, currency, entity, page=None, pagesize=None):
    return await _cs.list_address_tags_by_cluster(
        ctx, currency, cluster=entity, page=page, pagesize=pagesize
    )


async def list_entity_txs(
    ctx,
    currency,
    entity,
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
    return await _cs.list_cluster_txs(
        ctx,
        currency,
        cluster=entity,
        min_height=min_height,
        max_height=max_height,
        min_date=min_date,
        max_date=max_date,
        direction=direction,
        order=order,
        token_currency=token_currency,
        page=page,
        pagesize=pagesize,
    )


async def search_entity_neighbors(
    ctx,
    currency,
    entity,
    direction,
    key,
    value,
    depth,
    breadth,
    skip_num_addresses=None,
):
    return await _cs.search_cluster_neighbors(
        ctx,
        currency,
        cluster=entity,
        direction=direction,
        key=key,
        value=value,
        depth=depth,
        breadth=breadth,
        skip_num_addresses=skip_num_addresses,
    )


__all__ = [
    "MAX_DEPTH",
    "SEARCH_TIMEOUT",
    "bfs",
    "get_address",
    "get_entity",
    "list_address_tags_by_entity",
    "list_entity_addresses",
    "list_entity_links",
    "list_entity_neighbors",
    "list_entity_txs",
    "recursive_search",
    "search_entity_neighbors",
]
