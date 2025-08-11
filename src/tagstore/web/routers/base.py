from typing import List, Optional

from fastapi import APIRouter

from ....tagpack import __version__
from ...algorithms.tag_digest import TagDigest, compute_tag_digest
from ...db import (
    ActorPublic,
    LabelSearchResultPublic,
    TagPublic,
    TagstoreStatisticsPublic,
    TaxonomiesPublic,
)
from ..dependencies import TsACLGroupsParam, TsDbParam, TsPagingParam, TsTagsQueryParam

router = APIRouter()


@router.get("/version/", tags=["General"])
async def get_version() -> str:
    return __version__


@router.get("/acl_groups", tags=["General"], name="Get acl groups present in the db.")
async def get_acl_groups(db: TsDbParam) -> List[str]:
    """
    Loads ACL Groups available in the db..
    """
    return await db.get_acl_groups()


@router.get(
    "/taxonomy", tags=["General"], name="Get a labels and description for id values."
)
async def get_taxonomies(db: TsDbParam) -> TaxonomiesPublic:
    """
    Loads taxonomies (meta information and descriptions) used in this API.
    """
    return await db.get_taxonomies()


@router.get(
    "/statistics", tags=["General"], name="Get a per network statistic of tag counts."
)
async def get_statistics(db: TsDbParam) -> TagstoreStatisticsPublic:
    """
    Loads statistics on tag counts in the database.
    """
    return await db.get_network_statistics_cached()


@router.get(
    "/tags",
    tags=["Tags"],
    name="Get all tags for an query",
)
async def get_tags(
    query: TsTagsQueryParam,
    page: TsPagingParam,
    groups: TsACLGroupsParam,
    db: TsDbParam,
) -> List[TagPublic]:
    """
    Loads tags for a tx hash, address (subject_id),
      label (label), actor (actor_id) or cluster id (cluster_id)
    """
    if query.label is not None:
        return await db.get_tags_by_label(
            query.label, page.offset, page.limit, groups, network=query.network
        )
    elif query.actor_id is not None:
        return await db.get_tags_by_actorid(
            query.actor_id, page.offset, page.limit, groups, network=query.network
        )
    elif query.subject_id is not None:
        return await db.get_tags_by_subjectid(
            query.subject_id, page.offset, page.limit, groups, network=query.network
        )
    elif query.cluster_id is not None:
        return await db.get_tags_by_clusterid(
            query.cluster_id, query.network, page.offset, page.limit, groups
        )


@router.get(
    "/tag-digest/{tag_subject}",
    tags=["Digest"],
    name="Get a digest of all tags on an tx, address or other identifier",
)
async def get_tag_digest(
    subject_id: str,
    groups: TsACLGroupsParam,
    db: TsDbParam,
) -> TagDigest:
    """
    Loads tags for a tx hash, address (subject_id),
      label (label), actor (actor_id) or cluster id (cluster_id)
    """
    return compute_tag_digest(
        await db.get_tags_by_subjectid(subject_id, None, None, groups)
    )


@router.get(
    "/best-tag/{cluster_id}",
    tags=["Cluster"],
    name="Get the 'best' tag for a given cluster_id.",
)
async def get_best_cluster_tag(
    cluster_id: int,
    network: str,
    groups: TsACLGroupsParam,
    db: TsDbParam,
) -> Optional[TagPublic]:
    """
    Loads best cluster Tag.
    """
    return await db.get_best_cluster_tag(cluster_id, network.upper(), groups)


@router.get("/actor/{actor}", tags=["Actor"], name="Get an Actor by its id.")
async def get_actor_by_id(
    actor: str, db: TsDbParam, include_tag_count: bool = False
) -> Optional[ActorPublic]:
    """
    Loads actor by id
    """
    return await db.get_actor_by_id(actor, include_tag_count)


@router.get(
    "/search/{keyword}", tags=["Search"], name="Search labels in tags and actors"
)
async def search_labels(
    keyword: str, db: TsDbParam, groups: TsACLGroupsParam, limit: int = 5
) -> LabelSearchResultPublic:
    """
    Searches matching labels
    """
    return await db.search_labels(keyword, limit, groups)
