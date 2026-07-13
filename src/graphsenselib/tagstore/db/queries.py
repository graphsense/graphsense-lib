import json
import logging
import uuid
from datetime import timezone
from enum import IntEnum
from functools import wraps
from json import JSONDecodeError
from typing import Dict, List, Optional, Set

from pydantic import BaseModel, computed_field
from sqlalchemy import BigInteger, String, asc, bindparam, desc, distinct, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from sqlmodel import select, text
from sqlmodel.ext.asyncio.session import AsyncSession

from graphsenselib.utils.constants import (
    FRESH_CLUSTER_ID_OFFSET,
    is_fresh_cluster_id,
    to_raw_fresh_cluster_id,
)

from .database import get_db_engine_async
from .errors import TagAlreadyExistsException
from .models import (
    Actor,
    AddressClusterMapping,
    AddressClusterMappingV2,
    BestClusterTagView,
    BestClusterTagViewV2,
    Concept,
    Confidence,
    Country,
    Tag,
    TagConcept,
    TagCountByClusterView,
    TagCountByClusterViewV2,
    TagPack,
    TagSubject,
    TagType,
)

logger = logging.getLogger("uvicorn.error")


class Taxonomies(IntEnum):
    CONCEPT = 1
    CONFIDENCE = 2
    COUNTRY = 3
    TAG_SUBJECT = 4
    TAG_TYPE = 5


class InheritedFrom(IntEnum):
    CLUSTER = 1
    PUBKEY = 2
    PUBKEY_AND_CLUSTER = 3

    @staticmethod
    def from_string(s: str) -> "InheritedFrom":
        s_lower = s.lower()
        if s_lower == "cluster":
            return InheritedFrom.CLUSTER
        elif s_lower == "pubkey":
            return InheritedFrom.PUBKEY
        elif s_lower == "pubkey_and_cluster":
            return InheritedFrom.PUBKEY_AND_CLUSTER
        elif s_lower.startswith("pubkey_and_cluster"):
            return InheritedFrom.PUBKEY_AND_CLUSTER
        elif s_lower.startswith("pubkey_and_pubkey"):
            return InheritedFrom.PUBKEY_AND_CLUSTER
        else:
            raise ValueError(f"Unknown InheritedFrom string: {s}")


_ALL_TAXONOMIES = {
    Taxonomies.CONFIDENCE,
    Taxonomies.CONCEPT,
    Taxonomies.COUNTRY,
    Taxonomies.TAG_SUBJECT,
    Taxonomies.TAG_TYPE,
}

# Output Classes


class HumanReadableId(BaseModel):
    id: str  # noqa
    label: str


class ItemDescriptionPublic(HumanReadableId):
    description: str
    source: Optional[str]
    taxonomy: str


class LabelSearchResultPublic(BaseModel):
    actor_labels: List[HumanReadableId]
    tag_labels: List[HumanReadableId]


class ConfidencePublic(ItemDescriptionPublic):
    level: int


class ConceptsPublic(ItemDescriptionPublic):
    parent: Optional[str]
    is_abuse: bool


class TaxonomiesPublic(BaseModel):
    confidence: Optional[List[ConfidencePublic]]
    country: Optional[List[ItemDescriptionPublic]]
    tag_subject: Optional[List[ItemDescriptionPublic]]
    tag_type: Optional[List[ItemDescriptionPublic]]
    concept: Optional[List[ConceptsPublic]]


class ActorPublic(BaseModel):
    id: str  # noqa
    label: str
    primary_uri: str
    nr_tags: Optional[int]
    concepts: List[str]
    jurisdictions: List[str]
    additional_uris: List[str]
    image_links: List[str]
    online_references: List[str]
    coingecko_ids: List[str]
    defilama_ids: List[str]
    twitter_handles: List[str]
    github_organisations: List[str]
    legal_name: Optional[str]

    @classmethod
    def fromDB(cls, a: Actor, tag_count: Optional[int] = None) -> "TagPublic":
        additional_uris = []
        image_links = []
        online_references = []
        coingecko_ids = []
        defilama_ids = []
        twitter_handles = []
        gh_handles = []
        legal_name = None

        try:
            data = json.loads(a.context) if a.context is not None else {}

            # muliple twitter handles are string concatendated at the moment
            twitter_handles_t = [
                x.strip()
                for x in data.get("twitter_handle", "").split(",")
                if x.strip()
            ]

            # muliple gh orgas are string concatendated at the moment
            gh_orgas = [
                x.strip()
                for x in data.get("github_organisation", "").split(",")
                if x.strip()
            ]

            additional_uris.extend(data.get("uris", []))
            image_links.extend(data.get("images", []))
            online_references.extend(data.get("refs", []))
            coingecko_ids.extend(data.get("coingecko_ids", []))
            defilama_ids.extend(data.get("defilama_ids", []))
            twitter_handles.extend(twitter_handles_t)
            gh_handles.extend(gh_orgas)
            legal_name = data.get("legal_name", None)

        except JSONDecodeError:
            logger.error(f"Could not decode actor context: {a.context}")

        return cls(
            id=a.id,
            label=a.label,
            primary_uri=a.uri,
            concepts=[c.concept_id for c in a.concepts],
            jurisdictions=[c.country_id for c in a.jurisdictions],
            additional_uris=additional_uris,
            image_links=image_links,
            online_references=online_references,
            coingecko_ids=coingecko_ids,
            defilama_ids=defilama_ids,
            twitter_handles=twitter_handles,
            github_organisations=gh_handles,
            legal_name=legal_name,
            nr_tags=tag_count,
        )


class NetworkStatisticsPublic(BaseModel):
    nr_tags: int
    nr_identifiers_explicit: int
    nr_identifiers_implicit: Optional[int]
    nr_labels: int

    @classmethod
    def zero(Cls) -> "NetworkStatisticsPublic":
        return Cls(
            nr_tags=0, nr_identifiers_explicit=0, nr_identifiers_implicit=0, nr_labels=0
        )


class TagstoreStatisticsPublic(BaseModel):
    by_network: Dict[str, NetworkStatisticsPublic]


class TagPublic(BaseModel):
    identifier: str
    label: str
    source: str
    creator: str
    confidence: str
    confidence_level: int
    tag_subject: str
    tag_type: str
    actor: Optional[str]
    primary_concept: Optional[str]
    additional_concepts: List[str]
    is_cluster_definer: bool
    network: str
    lastmod: int
    group: str
    inherited_from: Optional[InheritedFrom]
    tagpack_title: str
    tagpack_uri: Optional[str]

    @computed_field
    @property
    def concepts(self) -> List[str]:
        return list(
            dict.fromkeys(
                (
                    [self.primary_concept] + self.additional_concepts
                    if (self.primary_concept)
                    else self.additional_concepts
                )
            ).keys()
        )

    @property
    def tagpack_is_public(self) -> bool:
        return self.group == "public"

    @classmethod
    def fromDB(cls, t: Tag, tp: TagPack, inherited_from=None) -> "TagPublic":
        c = t.concepts
        mainc = next(
            (x for x in c if x.concept_relation_annotation_id == "primary"), None
        )
        return cls(
            identifier=t.identifier,
            label=t.label,
            source=t.source or "unknown",
            creator=tp.creator,
            confidence=t.confidence_id,
            confidence_level=t.confidence.level,
            tag_subject=t.tag_subject_id,
            tag_type=t.tag_type_id,
            actor=t.actor_id,
            primary_concept=mainc.concept_id if mainc else None,
            additional_concepts=[x.concept_id for x in c if x != mainc],
            is_cluster_definer=t.is_cluster_definer,
            network=t.network,
            lastmod=int(round(t.lastmod.replace(tzinfo=timezone.utc).timestamp())),
            group=tp.acl_group,
            inherited_from=inherited_from,
            tagpack_title=tp.title,
            tagpack_uri=tp.uri,
        )


class UserReportedAddressTag(BaseModel):
    address: str
    network: str
    actor: Optional[str]
    label: str
    description: str
    user: Optional[str] = None


# Statements


def _get_tags_by_subjectid_stmt(
    identifier: str,
    offset: Optional[int],
    page_size: Optional[int],
    groups: List[str],
    network: Optional[str],
):
    q = (
        select(Tag, TagPack, Confidence)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(Tag.identifier == identifier)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .where(Confidence.id == Tag.confidence_id)
        .offset(offset)
        .limit(page_size)
        .order_by(desc(Confidence.level))
    )

    if network is not None:
        q = q.where(Tag.network == network)
    return q


def _get_tags_by_subjectids_stmt(
    identifiers: List[str],
    groups: List[str],
    network: Optional[str] = None,
):
    q = (
        select(Tag, TagPack, Confidence)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(Tag.identifier.in_(identifiers))
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .where(Confidence.id == Tag.confidence_id)
        .order_by(desc(Confidence.level))
    )

    if network is not None:
        q = q.where(Tag.network == network)
    return q


def _get_tag_by_id_stmt(tag_id: int, groups: List[str]):
    return (
        select(Tag, TagPack)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(Tag.id == tag_id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .limit(1)
    )


def _cluster_models(fresh: bool):
    """(mapping, best-tag view, count view) trio for one clustering regime."""
    if fresh:
        return (
            AddressClusterMappingV2,
            BestClusterTagViewV2,
            TagCountByClusterViewV2,
        )
    return (AddressClusterMapping, BestClusterTagView, TagCountByClusterView)


def _cluster_relations_for(cluster_id):
    """Route a public cluster id to its relations: (trio..., raw id).

    Public entity ids are self-describing: fresh-clustering ids are published
    shifted by ``FRESH_CLUSTER_ID_OFFSET`` and their mappings live in the
    parallel ``*_v2`` relations (keyed by the raw fresh id, root == min
    address id); ids below the offset are legacy ids keyed in the legacy
    relations. Routing on the id keeps reader and writer consistent per
    request — a fresh id can never consult the legacy-keyed relations and
    vice versa, which a global switch could not guarantee while networks
    migrate one at a time.
    """
    cluster_id = int(cluster_id)
    if is_fresh_cluster_id(cluster_id):
        return (*_cluster_models(fresh=True), to_raw_fresh_cluster_id(cluster_id))
    return (*_cluster_models(fresh=False), cluster_id)


def _routed_id_batches(cluster_ids):
    """Partition mixed public ids into one batch per relations set.

    Mixed lists are legal (e.g. legacy neighbor clusters of a fresh entity).
    Returns two triples ``(raw_ids, fresh, shift)`` where ``shift`` restores
    the public id on result rows (``raw + shift == public``).
    """
    ids = [int(c) for c in cluster_ids]
    legacy = [c for c in ids if not is_fresh_cluster_id(c)]
    fresh_raw = [to_raw_fresh_cluster_id(c) for c in ids if is_fresh_cluster_id(c)]
    return ((legacy, False, 0), (fresh_raw, True, FRESH_CLUSTER_ID_OFFSET))


def _get_best_cluster_tag_stmt(cluster_id: int, network: str, groups: List[str]):
    _, BestClusterTag, _, cluster_id = _cluster_relations_for(cluster_id)
    return (
        select(Tag, TagPack, Confidence)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(BestClusterTag.cluster_id == cluster_id)
        .where(BestClusterTag.network == network)
        .where(Tag.tagpack_id == TagPack.id)
        .where(BestClusterTag.tag_id == Tag.id)
        .where(TagPack.acl_group.in_(groups))
        .where(Confidence.id == Tag.confidence_id)
        .order_by(Confidence.level.desc())
        .limit(1)
    )


def _get_best_cluster_tag_winners_stmt(
    cluster_ids: List[int], network: str, groups: List[str], fresh: bool
):
    # Takes RAW ids of one regime (callers split mixed public id lists via
    # _routed_id_batches). Picks (cluster_id, winning_tag_id) per cluster at
    # the DB layer using Postgres DISTINCT ON. The result-set size is at most
    # len(cluster_ids), independent of how many cluster_definer tags any
    # single cluster carries — without this guard a heavily-tagged
    # cluster's tagset would be shipped back through the joinedloaded
    # relationships and Cartesian'd by Tag.concepts, observed at >5min on
    # a single cluster before this rewrite.
    _, BestClusterTag, _ = _cluster_models(fresh)
    return (
        select(BestClusterTag.cluster_id, Tag.id)
        .distinct(BestClusterTag.cluster_id)
        .where(BestClusterTag.cluster_id.in_(cluster_ids))
        .where(BestClusterTag.network == network)
        .where(BestClusterTag.tag_id == Tag.id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .where(Confidence.id == Tag.confidence_id)
        .order_by(BestClusterTag.cluster_id, Confidence.level.desc())
    )


def _get_clusters_with_concept_stmt(
    cluster_ids: List[int], network: str, groups: List[str], concept_id: str
):
    # Existence-only check using a correlated EXISTS so Postgres can short-
    # circuit at the first matching tag per cluster. A naive
    # ``SELECT DISTINCT cluster_id ... WHERE concept_id = :concept_id``
    # materializes the full join product (every matching tag in every input
    # cluster) before deduplicating — for an exchange cluster carrying
    # thousands of exchange-tagged cluster-definer tags, that was still
    # ~1.6 s per request. Driving from ``unnest(cluster_ids)`` with EXISTS
    # bounds the work to ``len(cluster_ids)`` per call.
    return text(
        """
        SELECT t.cluster_id
        FROM unnest(:cluster_ids) AS t(cluster_id)
        WHERE EXISTS (
            SELECT 1
            FROM best_cluster_tag bct
            JOIN tag tg ON bct.tag_id = tg.id
            JOIN tag_concept tc ON tc.tag_id = tg.id
            JOIN tagpack tp ON tg.tagpack = tp.id
            WHERE bct.cluster_id = t.cluster_id
              AND bct.network = :network
              AND tc.concept_id = :concept_id
              AND tp.acl_group = ANY(:groups)
        )
        """
    ).bindparams(
        bindparam("cluster_ids", value=cluster_ids, type_=ARRAY(BigInteger)),
        bindparam("network", value=network),
        bindparam("concept_id", value=concept_id),
        bindparam("groups", value=groups, type_=ARRAY(String)),
    )


def _get_tags_with_joinedloads_by_ids_stmt(tag_ids: List[int]):
    return (
        select(Tag, TagPack, Confidence)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(Tag.id.in_(tag_ids))
        .where(Tag.tagpack_id == TagPack.id)
        .where(Confidence.id == Tag.confidence_id)
    )


def _get_tags_by_actorid_stmt(
    actor: str,
    offset: Optional[int],
    page_size: Optional[int],
    groups: List[str],
    network: Optional[str],
):
    q = (
        select(Tag, TagPack, Confidence)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(Tag.actor_id == actor)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .where(Confidence.id == Tag.confidence_id)
        .offset(offset)
        .limit(page_size)
        .order_by(desc(Confidence.level))
    )
    if network is not None:
        q = q.where(Tag.network == network)
    return q


def _get_tags_by_clusterid_stmt(
    cluster_id: int,
    network: str,
    offset: Optional[int],
    page_size: Optional[int],
    groups: List[str],
    exclude_identifiers: Optional[List[str]],
):
    AddressClusterMap, _, _, cluster_id = _cluster_relations_for(cluster_id)
    q = (
        select(Tag, TagPack, AddressClusterMap, Confidence)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(AddressClusterMap.gs_cluster_id == cluster_id)
        .where(AddressClusterMap.address == Tag.identifier)
        .where(AddressClusterMap.network == Tag.network)
        .where(Tag.network == network)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .where(Confidence.id == Tag.confidence_id)
    )

    if exclude_identifiers is not None:
        q = q.where(Tag.identifier.not_in(exclude_identifiers))

    return q.offset(offset).limit(page_size).order_by(desc(Confidence.level))


def _get_tags_by_label_stmt(
    label: str,
    offset: Optional[int],
    page_size: Optional[int],
    groups: List[str],
    network: Optional[str],
):
    # Escape LIKE wildcards in user input so a bare "%" / "_" cannot turn the
    # search into a full scan / match-everything pattern.
    escaped = label.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    q = (
        select(Tag, TagPack)
        .options(joinedload(Tag.confidence))
        .options(joinedload(Tag.concepts))
        .options(joinedload(Tag.tag_type))
        .options(joinedload(Tag.tag_subject))
        .where(Tag.label.like(f"%{escaped}%", escape="\\"))
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        # deterministic order so OFFSET/LIMIT pages don't overlap or drop rows
        .order_by(Tag.id)
        .offset(offset)
        .limit(page_size)
    )
    if network is not None:
        q = q.where(Tag.network == network)
    return q


def _get_actor_by_id_stmt(actor: str):
    return select(Actor).where(Actor.id == actor)


def _get_actor_tag_count_stmt(actor: str):
    return select(func.count()).select_from(Tag).where(Tag.actor_id == actor)


def _get_per_network_statistics_stmt():
    return select(
        Tag.network,
        func.count(Tag.identifier),
        func.count(distinct(Tag.identifier)),
        func.count(distinct(Tag.label)),
    ).group_by(Tag.network)


def _get_per_network_statistics_cached_stmt():
    return text(
        "select network, nr_labels, nr_tags, nr_identifiers_explicit, nr_identifiers_implicit from statistics"
    )


def _get_count_by_cluster_stmt(cluster_id: int, network: str, groups: List[str]):
    _, _, TagCountByCluster, cluster_id = _cluster_relations_for(cluster_id)
    return (
        select(TagCountByCluster)
        .where(TagCountByCluster.network == network)
        .where(TagCountByCluster.gs_cluster_id == cluster_id)
        .where(TagCountByCluster.acl_group.in_(groups))
    )


def _get_count_by_clusters_batch_stmt(
    cluster_ids: List[int], network: str, groups: List[str], fresh: bool
):
    # Raw ids of one regime; see _routed_id_batches.
    _, _, TagCountByCluster = _cluster_models(fresh)
    return (
        select(
            TagCountByCluster.gs_cluster_id,
            func.sum(TagCountByCluster.count).label("total"),
        )
        .where(TagCountByCluster.network == network)
        .where(TagCountByCluster.gs_cluster_id.in_(cluster_ids))
        .where(TagCountByCluster.acl_group.in_(groups))
        .group_by(TagCountByCluster.gs_cluster_id)
    )


def _get_similar_actors_stmt(query: str, limit: int):
    return (
        select(
            Actor.label,
            Actor.id,
            func.similarity(Actor.label, query).label("sim_score"),
        )
        .where(Actor.label.op("%")(query))
        .order_by(desc("sim_score"))
        .limit(limit)
        .distinct()
    )


def _get_similar_tag_labels_stmt(query: str, limit: int, groups: List[str]):
    return (
        select(Tag.label, func.similarity(Tag.label, query).label("sim_score"))
        .where(Tag.label.op("%")(query))
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .group_by(Tag.label, "sim_score")
        .order_by(desc("sim_score"), Tag.label)
        .limit(limit)
    )


def _get_actors_for_subject_stmt(subject_id: str, groups: List[str]):
    return (
        select(Actor.id, Actor.label)
        .where(Tag.identifier == subject_id)
        .where(Actor.id.isnot(None))
        .where(Actor.id == Tag.actor_id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .order_by(Actor.label)
        .distinct()
    )


def _get_actors_for_clusterid_stmt(cluster_id: int, network: int, groups: List[str]):
    AddressClusterMap, _, _, cluster_id = _cluster_relations_for(cluster_id)
    return (
        select(Actor.id, Actor.label)
        .where(AddressClusterMap.gs_cluster_id == cluster_id)
        .where(AddressClusterMap.address == Tag.identifier)
        .where(AddressClusterMap.network == network)
        .where(Actor.id.isnot(None))
        .where(Actor.id == Tag.actor_id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .order_by(Actor.label)
        .distinct()
    )


def _get_actors_for_clusterids_batch_stmt(
    cluster_ids: List[int], network: str, groups: List[str], fresh: bool
):
    # Raw ids of one regime; see _routed_id_batches.
    AddressClusterMap, _, _ = _cluster_models(fresh)
    return (
        select(AddressClusterMap.gs_cluster_id, Actor.id, Actor.label)
        .where(AddressClusterMap.gs_cluster_id.in_(cluster_ids))
        .where(AddressClusterMap.address == Tag.identifier)
        .where(AddressClusterMap.network == network)
        .where(Actor.id.isnot(None))
        .where(Actor.id == Tag.actor_id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .order_by(AddressClusterMap.gs_cluster_id, Actor.label)
        .distinct()
    )


def _get_labels_by_subjectid_stmt(subject_id: str, groups: List[str]):
    return (
        select(Tag.label)
        .where(Tag.identifier == subject_id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .order_by(asc(Tag.label))
        .distinct()
    )


def _get_tag_count_by_subjectid_stmt(
    subject_id: str, network: Optional[str], groups: List[str]
):
    q = (
        select(func.count())
        .where(Tag.identifier == subject_id)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
    )

    if network is not None:
        q = q.where(Tag.network == network)

    return q


def _get_acl_groups_statement():
    return select(TagPack.acl_group).distinct()


def _get_labels_by_clusterid_stmt(cluster_id: str, network: str, groups: List[str]):
    AddressClusterMap, _, _, cluster_id = _cluster_relations_for(cluster_id)
    return (
        select(Tag.label)
        # gs_cluster_id is only unique per network (PK is (address, network),
        # index is (network, gs_cluster_id)), so the cluster->address resolution
        # MUST be scoped to the requested network — otherwise cluster #N on every
        # other chain is matched too and its addresses' labels leak in. Tag
        # matching itself stays network-agnostic by design (a tag on a matching
        # identifier applies regardless of the tag's own network).
        .where(AddressClusterMap.gs_cluster_id == cluster_id)
        .where(AddressClusterMap.network == network)
        .where(AddressClusterMap.address == Tag.identifier)
        .where(Tag.tagpack_id == TagPack.id)
        .where(TagPack.acl_group.in_(groups))
        .order_by(asc(Tag.label))
        .distinct()
    )


# Facades
def _inject_session(f):
    @wraps(f)
    async def inner_f(self, *args, **kwargs):
        session = kwargs.get("session", None)

        if session is not None:
            return await f(self, *args, **kwargs)
        else:
            async with AsyncSession(self.engine) as session:
                kwargs["session"] = session
                return await f(self, *args, **kwargs)

    return inner_f


class TagstoreDbAsync:
    engine = None

    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    def from_url(db_url):
        return TagstoreDbAsync(get_db_engine_async(db_url))

    # get Tag by id

    # Get Tags by subject id
    @_inject_session
    async def _get_tag_by_id(
        self,
        tag_id: int,
        groups: List[str],
        session=None,
    ) -> Optional[Tag]:
        return await session.exec(_get_tag_by_id_stmt(tag_id, groups)).first()

    @_inject_session
    async def get_tag_by_id(
        self,
        tag_id: int,
        groups: List[str],
        session=None,
    ) -> Optional[TagPublic]:
        result = await self._get_tag_by_id(tag_id, groups, session=session)
        if result is not None:
            t, tp = result
            return TagPublic.fromDB(t, tp)

        return None

    @_inject_session
    async def get_acl_groups(
        self,
        session=None,
    ) -> List[str]:
        return await session.exec(_get_acl_groups_statement())

    # Get Tags by subject id
    @_inject_session
    async def _get_tags_by_subjectid(
        self,
        identifier: str,
        offset: int,
        page_size: int,
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> List[Tag]:
        return (
            await session.exec(
                _get_tags_by_subjectid_stmt(
                    identifier, offset, page_size, groups, network=network
                )
            )
        ).unique()

    @_inject_session
    async def get_tags_by_subjectid(
        self,
        subject_id: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> List[TagPublic]:
        results = await self._get_tags_by_subjectid(
            subject_id.strip(),
            offset,
            page_size,
            groups,
            network=network,
            session=session,
        )
        return [TagPublic.fromDB(t, tp) for t, tp, _ in results]

    @_inject_session
    async def get_tags_by_subjectids(
        self,
        subject_ids: List[str],
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> Dict[str, List[TagPublic]]:
        # Single-query batch lookup. Avoids the per-call session checkout
        # pattern that amplified the 2026-05-04 pool-exhaustion incident.
        if not subject_ids:
            return {}
        cleaned = [sid.strip() for sid in subject_ids]
        results = (
            await session.exec(
                _get_tags_by_subjectids_stmt(cleaned, groups, network=network)
            )
        ).unique()
        grouped: Dict[str, List[TagPublic]] = {sid: [] for sid in cleaned}
        for t, tp, _ in results:
            grouped.setdefault(t.identifier, []).append(TagPublic.fromDB(t, tp))
        return grouped

    @_inject_session
    async def get_actors_by_subjectid(
        self, subject_id: str, groups: List[str], session=None
    ) -> List[HumanReadableId]:
        results = await session.exec(
            _get_actors_for_subject_stmt(subject_id.strip(), groups)
        )
        return [HumanReadableId(id=idt, label=lbl) for idt, lbl in results]

    @_inject_session
    async def get_labels_by_subjectid(
        self, subject_id: str, groups: List[str], session=None
    ) -> List[str]:
        results = await session.exec(
            _get_labels_by_subjectid_stmt(subject_id.strip(), groups)
        )
        return [x for x in results]

    @_inject_session
    async def get_tag_count_by_subjectid(
        self, subject_id: str, network: Optional[str], groups: List[str], session=None
    ) -> int:
        results = await session.exec(
            _get_tag_count_by_subjectid_stmt(subject_id, network, groups)
        )

        return sum(x for x in results)

    @_inject_session
    async def get_labels_by_clusterid(
        self, cluster_id: str, network: str, groups: List[str], session=None
    ) -> List[str]:
        results = await session.exec(
            _get_labels_by_clusterid_stmt(cluster_id, network, groups)
        )
        return [x for x in results]

    # Get Tags by Label
    @_inject_session
    async def _get_tags_by_label(
        self,
        label: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> List[Tag]:
        return (
            await session.exec(
                _get_tags_by_label_stmt(
                    label.strip(), offset, page_size, groups, network=network
                )
            )
        ).unique()

    @_inject_session
    async def get_tags_by_label(
        self,
        label: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> List[TagPublic]:
        results = await self._get_tags_by_label(
            label.strip(), offset, page_size, groups, network=network, session=session
        )
        return [TagPublic.fromDB(t, tp) for t, tp in results]

    # Cluster

    @_inject_session
    async def _get_tags_by_clusterid(
        self,
        cluster_id: int,
        network: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        exclude_identifiers: Optional[List[str]],
        session=None,
    ) -> List[Tag]:
        return (
            await session.exec(
                _get_tags_by_clusterid_stmt(
                    cluster_id, network, offset, page_size, groups, exclude_identifiers
                )
            )
        ).unique()

    @_inject_session
    async def get_tags_by_clusterid(
        self,
        cluster_id: int,
        network: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        exclude_identifiers: Optional[List[str]] = None,
        session=None,
    ) -> List[TagPublic]:
        results = await self._get_tags_by_clusterid(
            cluster_id,
            network,
            offset,
            page_size,
            groups,
            exclude_identifiers=exclude_identifiers,
            session=session,
        )
        return [TagPublic.fromDB(t, tp) for t, tp, _, _ in results]

    @_inject_session
    async def get_nr_tags_by_clusterid(
        self, cluster_id: int, network: str, groups: List[str], session=None
    ) -> int:
        results = await session.exec(
            _get_count_by_cluster_stmt(cluster_id, network, groups)
        )

        return sum(x.count for x in results)

    @_inject_session
    async def get_actors_by_clusterid(
        self, cluster_id: int, network: str, groups: List[str], session=None
    ) -> List[HumanReadableId]:
        results = await session.exec(
            _get_actors_for_clusterid_stmt(cluster_id, network, groups)
        )
        return [HumanReadableId(id=idt, label=lbl) for idt, lbl in results]

    # Batched variants: collapse N per-cluster lookups into a single
    # session/query each. Used by list_entity_neighbors to avoid the
    # N×3 fan-out that triggered the 2026-05-04 pool exhaustion.
    @_inject_session
    async def get_best_cluster_tags_for_clusters(
        self,
        cluster_ids: List[int],
        network: str,
        groups: List[str],
        session=None,
    ) -> Dict[int, TagPublic]:
        if not cluster_ids:
            return {}
        # Step 1: pick (cluster_id -> winning tag_id) at the DB level. No
        # joinedloads here — the result set scales with len(cluster_ids),
        # not with any single cluster's tag count. Ids route per regime and
        # results are keyed by the public id the caller passed in.
        cid_to_tag_id: Dict[int, int] = {}
        for raw_ids, fresh, shift in _routed_id_batches(cluster_ids):
            if not raw_ids:
                continue
            winners = await session.exec(
                _get_best_cluster_tag_winners_stmt(raw_ids, network, groups, fresh)
            )
            cid_to_tag_id.update({cid + shift: tid for cid, tid in winners})
        if not cid_to_tag_id:
            return {}

        # Step 2: hydrate Tag (with collection joinedloads) for the
        # winning tag_ids only. `.unique()` is required because the
        # joinedload on Tag.concepts is a collection.
        rows = (
            await session.exec(
                _get_tags_with_joinedloads_by_ids_stmt(list(cid_to_tag_id.values()))
            )
        ).unique()
        tag_by_id = {t.id: (t, tp) for t, tp, _c in rows}

        result: Dict[int, TagPublic] = {}
        for cid, tid in cid_to_tag_id.items():
            pair = tag_by_id.get(tid)
            if pair is None:
                continue
            t, tp = pair
            result[cid] = TagPublic.fromDB(t, tp, inherited_from=InheritedFrom.CLUSTER)
        return result

    @_inject_session
    async def get_clusters_with_concept(
        self,
        cluster_ids: List[int],
        network: str,
        groups: List[str],
        concept_id: str,
        session=None,
    ) -> Set[int]:
        """Return the subset of ``cluster_ids`` that have at least one
        cluster-definer tag with ``concept_id``. Single batched query,
        cliff-free (cost is independent of per-cluster tag count)."""
        if not cluster_ids:
            return set()
        rows = await session.exec(
            _get_clusters_with_concept_stmt(cluster_ids, network, groups, concept_id)
        )
        # Raw text() result rows are Row tuples even for a single column.
        return {cid for (cid,) in rows}

    @_inject_session
    async def get_nr_tags_for_clusters(
        self,
        cluster_ids: List[int],
        network: str,
        groups: List[str],
        session=None,
    ) -> Dict[int, int]:
        if not cluster_ids:
            return {}
        out: Dict[int, int] = {}
        for raw_ids, fresh, shift in _routed_id_batches(cluster_ids):
            if not raw_ids:
                continue
            results = await session.exec(
                _get_count_by_clusters_batch_stmt(raw_ids, network, groups, fresh)
            )
            out.update({cid + shift: int(total or 0) for cid, total in results})
        return out

    @_inject_session
    async def get_actors_for_clusters(
        self,
        cluster_ids: List[int],
        network: str,
        groups: List[str],
        session=None,
    ) -> Dict[int, List[HumanReadableId]]:
        if not cluster_ids:
            return {}
        out: Dict[int, List[HumanReadableId]] = {}
        for raw_ids, fresh, shift in _routed_id_batches(cluster_ids):
            if not raw_ids:
                continue
            results = await session.exec(
                _get_actors_for_clusterids_batch_stmt(raw_ids, network, groups, fresh)
            )
            for cid, idt, lbl in results:
                out.setdefault(cid + shift, []).append(
                    HumanReadableId(id=idt, label=lbl)
                )
        return out

    # Actor

    @_inject_session
    async def get_actor_by_id(
        self, identifier: str, include_tag_count: bool, session=None
    ) -> Optional[ActorPublic]:
        actor = (await session.exec(_get_actor_by_id_stmt(identifier))).first()

        tag_count = None
        if include_tag_count:
            tag_count = (
                await session.exec(_get_actor_tag_count_stmt(identifier.strip()))
            ).first()

        if actor is not None:
            return ActorPublic.fromDB(actor, tag_count=tag_count)

        return None

    @_inject_session
    async def _get_tags_by_actorid(
        self,
        actor: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> List[Tag]:
        return (
            await session.exec(
                _get_tags_by_actorid_stmt(
                    actor.strip(), offset, page_size, groups, network=network
                )
            )
        ).unique()

    @_inject_session
    async def get_tags_by_actorid(
        self,
        actor: str,
        offset: Optional[int],
        page_size: Optional[int],
        groups: List[str],
        network: Optional[str] = None,
        session=None,
    ) -> List[TagPublic]:
        results = await self._get_tags_by_actorid(
            actor.strip(), offset, page_size, groups, network=network, session=session
        )
        return [TagPublic.fromDB(t, tp) for t, tp, _ in results]

    # Other tag stuff

    @_inject_session
    async def get_best_cluster_tag(
        self, cluster_id: int, network: str, groups: List[str], session=None
    ) -> Optional[TagPublic]:
        result = (
            await session.exec(_get_best_cluster_tag_stmt(cluster_id, network, groups))
        ).first()
        if result is not None:
            t, tp, _ = result
            return TagPublic.fromDB(t, tp, inherited_from=InheritedFrom.CLUSTER)

        return None

    # Other
    @_inject_session
    async def get_taxonomies(
        self, include: Set[Taxonomies] = _ALL_TAXONOMIES, session=None
    ) -> TaxonomiesPublic:
        return TaxonomiesPublic(
            confidence=(
                None
                if Taxonomies.CONFIDENCE not in include
                else (
                    [
                        ConfidencePublic(**{"source": None, **(x.model_dump())})
                        for x in (await session.exec(select(Confidence)))
                    ]
                )
            ),
            country=(
                None
                if Taxonomies.COUNTRY not in include
                else (
                    [
                        ItemDescriptionPublic(**(x.model_dump()))
                        for x in (await session.exec(select(Country)))
                    ]
                )
            ),
            tag_subject=(
                None
                if Taxonomies.TAG_SUBJECT not in include
                else (
                    [
                        ItemDescriptionPublic(**(x.model_dump()))
                        for x in (await session.exec(select(TagSubject)))
                    ]
                )
            ),
            tag_type=(
                None
                if Taxonomies.TAG_TYPE not in include
                else (
                    [
                        ItemDescriptionPublic(**(x.model_dump()))
                        for x in (await session.exec(select(TagType)))
                    ]
                )
            ),
            concept=(
                None
                if Taxonomies.CONCEPT not in include
                else (
                    [
                        ConceptsPublic(**(x.model_dump()))
                        for x in (await session.exec(select(Concept)))
                    ]
                )
            ),
        )

    @_inject_session
    async def get_network_statistics(self, session=None) -> TagstoreStatisticsPublic:
        results = await session.exec(_get_per_network_statistics_stmt())
        return TagstoreStatisticsPublic(
            by_network={
                net.upper(): NetworkStatisticsPublic(
                    nr_tags=nr_tags,
                    nr_identifiers_explicit=nr_identifiers,
                    nr_labels=nr_labels,
                    nr_identifiers_implicit=None,
                )
                for net, nr_tags, nr_identifiers, nr_labels in results
            }
        )

    @_inject_session
    async def get_network_statistics_cached(
        self, session=None
    ) -> TagstoreStatisticsPublic:
        results = await session.exec(_get_per_network_statistics_cached_stmt())
        return TagstoreStatisticsPublic(
            by_network={
                net.upper(): NetworkStatisticsPublic(
                    nr_tags=nr_tags,
                    nr_identifiers_explicit=nr_i_explicit,
                    nr_identifiers_implicit=nr_i_impicit,
                    nr_labels=nr_labels,
                )
                for net, nr_labels, nr_tags, nr_i_explicit, nr_i_impicit in results
            }
        )

    @_inject_session
    async def search_tag_labels(
        self, label: str, limit: int, groups: List[str], session=None
    ) -> List[str]:
        results = await session.exec(
            _get_similar_tag_labels_stmt(label.strip(), limit, groups)
        )
        return [a for a, _ in results]

    @_inject_session
    async def search_actor_labels(
        self, label: str, limit: int, session=None
    ) -> List[HumanReadableId]:
        results = await session.exec(_get_similar_actors_stmt(label.strip(), limit))
        return [HumanReadableId(id=itm, label=lbl) for lbl, itm, _ in results]

    @_inject_session
    async def search_labels(
        self,
        label: str,
        limit: int,
        groups: List[str],
        query_actors: bool = True,
        query_labels: bool = True,
        session=None,
    ) -> LabelSearchResultPublic:
        return LabelSearchResultPublic(
            actor_labels=(
                await self.search_actor_labels(label.strip(), limit, session=session)
            )
            if query_actors
            else [],
            tag_labels=(
                [
                    HumanReadableId(id=x, label=x)
                    for x in (
                        await self.search_tag_labels(
                            label, limit, groups, session=session
                        )
                    )
                ]
            )
            if query_labels
            else [],
        )

    @_inject_session
    async def add_user_reported_tag(
        self, tag: UserReportedAddressTag, acl_group: str = "public", session=None
    ) -> str:
        IDUserReportedTagpack = f"manual-user-reported-tagpack-{acl_group}"
        q = select(TagPack).where(TagPack.id == IDUserReportedTagpack)
        tp = (await session.exec(q)).one_or_none()

        if tp is None:
            tpN = TagPack(
                id=IDUserReportedTagpack,
                title="User Reported Tags",
                description="Tagpack of tags reported by end-users via the dashboard UI",
                creator="The Graphsense Community",
                acl_group=acl_group,
            )

            session.add(tpN)
            await session.commit()

        actor = await self.get_actor_by_id(tag.actor, False)

        unique_id = str(uuid.uuid4())

        context = {"user": tag.user, "uuid": unique_id}

        tagN = Tag(
            label=tag.label,
            identifier=tag.address,
            network=tag.network.upper(),
            tag_subject_id="address",
            tag_type_id="actor",
            confidence_id="unknown",
            source=tag.description,
            tagpack_id=IDUserReportedTagpack,
            concepts=[],
            context=json.dumps(context),
        )

        if actor is not None:
            tagN.actor_id = actor.id
            tagN.concepts = [TagConcept(concept_id=c) for c in actor.concepts]

        session.add(tagN)

        try:
            await session.commit()
        except IntegrityError as e:
            if (
                hasattr(e, "orig")
                and hasattr(e.orig, "pgcode")
                and e.orig.pgcode == "23505"
            ):
                # 23505 is UNIQUE KEY VIOLATION
                # https://stackoverflow.com/questions/58740043/how-do-i-catch-a-psycopg2-errors-uniqueviolation-error-in-a-python-flask-app
                raise TagAlreadyExistsException()
            else:
                raise e

        return unique_id
