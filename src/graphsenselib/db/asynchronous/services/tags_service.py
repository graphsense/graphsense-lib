import asyncio
import logging
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union

from graphsenselib.config.config import SlackTopic
from graphsenselib.errors import FeatureNotAvailableException, NotFoundException
from graphsenselib.utils.address import address_to_user_format
from graphsenselib.utils.slack import send_message_to_slack

# TODO : add proper type hints for the tagstore once tagstore is migrated to gslib

from .common import (
    cannonicalize_address,
    try_get_cluster_id,
)
from ....utils.rest_utils import is_eth_like
from .models import (
    Actor,
    ActorContext,
    AddressTag,
    AddressTagResult,
    Concept,
    LabeledItemRef,
    LabelSummary,
    TagCloudEntry,
    TagSummary,
    Taxonomy,
)

logger = logging.getLogger(__name__)


class TagInsertConfigProtocol(Protocol):
    enable_user_tag_reporting: bool
    slack_info_hook: Dict[str, SlackTopic]


class TagstoreProtocol(Protocol):
    async def get_actor_by_id(
        self, actor_id: str, include_tag_count: bool = True
    ) -> Optional[Any]: ...
    async def get_tags_by_actorid(
        self, actor_id: str, offset: int, page_size: Optional[int], groups: List[str]
    ) -> List[Any]: ...
    async def get_tags_by_label(
        self, label: str, offset: int, page_size: Optional[int], groups: List[str]
    ) -> List[Any]: ...
    async def get_taxonomies(
        self, taxonomies: Optional[Any] = None
    ) -> List[Tuple[Any, Any]]: ...
    async def add_user_reported_tag(self, tag: Any, acl_group: str) -> None: ...
    async def get_actors_by_subjectid(
        self, subject_id: str, groups: List[str]
    ) -> List[LabeledItemRef]: ...
    async def get_tags_by_subjectid(
        self, address: str, offset: int, limit: Optional[int], groups: List[str]
    ) -> List["TagPublic"]: ...  # noqa: F821
    async def get_best_cluster_tag(
        self, cluster_id: int, currency: str, groups: List[str]
    ) -> Optional[Any]: ...


class ConceptProtocol(Protocol):
    def get_is_abuse(self, concept_id: str) -> bool: ...
    def get_taxonomy_concept_label(self, taxonomy: Any, concept_id: str) -> str: ...


class TagsService:
    def __init__(
        self,
        db: Any,
        tagstore: TagstoreProtocol,
        concepts_cache_service: ConceptProtocol,
        logger: Any,
    ):
        self.tagstore = tagstore
        self.db = db
        self.concepts_service = concepts_cache_service
        self.logger = logger

    def _address_tag_from_public_tag(
        self,
        pt: "TagPublic",  # noqa: F821
        entity: Optional[int],
    ) -> AddressTag:
        abuse = next(
            (x for x in pt.concepts if self.concepts_service.get_is_abuse(x)), None
        )

        return AddressTag(
            address=pt.identifier,
            entity=entity,
            label=pt.label,
            category=pt.primary_concept,
            concepts=pt.additional_concepts,
            actor=pt.actor,
            tag_type=pt.tag_type,
            abuse=abuse,
            source=pt.source,
            lastmod=pt.lastmod,
            tagpack_is_public=pt.group == "public",
            tagpack_uri=pt.tagpack_uri,
            tagpack_creator=pt.creator,
            tagpack_title=pt.tagpack_title,
            confidence=pt.confidence,
            confidence_level=pt.confidence_level,
            is_cluster_definer=pt.is_cluster_definer,
            inherited_from=pt.inherited_from.name.lower()
            if pt.inherited_from
            else None,
            currency=pt.network.upper(),
        )

    async def list_tags_by_address_raw(
        self,
        currency: str,
        address: Union[str, bytes],
        tagstore_groups: List[str],
        page: Optional[int] = None,
        pagesize: Optional[int] = None,
        include_best_cluster_tag: bool = False,
        cache: Optional[Dict[str, Any]] = None,
    ) -> List["TagPublic"]:  # noqa: F821
        address = address_to_user_format(currency, address)

        page = page or 0

        assert page is None or isinstance(page, int)

        tags = list(
            await self.tagstore.get_tags_by_subjectid(
                address,
                page * (pagesize or 0),
                pagesize,
                tagstore_groups,
            )
        )

        if include_best_cluster_tag and not is_eth_like(currency):
            cluster_id = await try_get_cluster_id(self.db, currency, address, cache)
            if cluster_id:
                _, best_cluster_tag = await self._get_best_cluster_tag_raw(
                    currency, address, cluster_id, tagstore_groups, cache or {}
                )
                if best_cluster_tag is not None:
                    is_direct_tag = best_cluster_tag.identifier == address
                    if not is_direct_tag:
                        tags.append(best_cluster_tag)

        return tags

    def _tag_summary_from_tag_digest(self, td: "TagDigest") -> TagSummary:  # noqa: F821
        return TagSummary(
            broad_category=td.broad_concept,
            tag_count=td.nr_tags,
            tag_count_indirect=td.nr_tags_indirect,
            best_actor=td.best_actor,
            best_label=td.best_label,
            concept_tag_cloud={
                k: TagCloudEntry(cnt=v.count, weighted=v.weighted)
                for k, v in td.concept_tag_cloud.items()
            },
            label_summary={
                key: LabelSummary(
                    label=v.label,
                    count=v.count,
                    confidence=v.confidence,
                    relevance=v.relevance,
                    creators=v.creators,
                    sources=v.sources,
                    concepts=v.concepts,
                    lastmod=v.lastmod,
                    inherited_from=v.inherited_from,
                )
                for (key, v) in td.label_digest.items()
            },
        )

    async def get_tag_summary_by_address(
        self,
        currency: str,
        address: str,
        tagstore_groups: List[str],
        include_best_cluster_tag: bool = False,
    ) -> TagSummary:
        try:
            from graphsenselib.tagstore.algorithms.tag_digest import compute_tag_digest
        except ImportError as e:
            raise ImportError(
                "tagstore is required for tag digest computation. "
                "Please install it with: uv pip install tagstore"
            ) from e

        address_canonical = cannonicalize_address(currency, address)

        tags = await self.list_tags_by_address_raw(
            currency,
            address_canonical,
            tagstore_groups,
            page=None,
            pagesize=None,
            include_best_cluster_tag=include_best_cluster_tag,
        )

        digest = compute_tag_digest(tags)
        return self._tag_summary_from_tag_digest(digest)

    async def _get_best_cluster_tag_raw(
        self,
        currency: str,
        address: str,
        cluster_id: int,
        tagstore_groups: List[str],
        cache: Dict[str, Any],
    ) -> Tuple[int, Optional[Any]]:
        key = f"best_cluster_tag_{cluster_id}_{currency.upper()}_" + "_".join(
            tagstore_groups
        )

        if key in cache:
            return cluster_id, cache[key]
        else:
            data = await self.tagstore.get_best_cluster_tag(
                cluster_id, currency.upper(), tagstore_groups
            )
            cache[key] = data
            return cluster_id, data

    def _get_address_tag_result(
        self, current_page: int, page_size: int, tags: List[AddressTag]
    ) -> AddressTagResult:
        tcnt = len(tags)
        current_page = int(current_page)
        np = current_page + 1 if (tcnt > 0 and tcnt == page_size) else None
        return AddressTagResult(
            next_page=str(np) if np is not None else None, address_tags=tags
        )

    async def _get_entities_dict(
        self,
        db: Any,
        tags: List["TagPublic"],  # noqa: F821
    ) -> Dict[Tuple[str, str], Any]:
        queryItems = list({(t.identifier, t.network) for t in tags})
        entityQueries = [try_get_cluster_id(db, n, i) for i, n in queryItems]
        return {q: d for q, d in zip(queryItems, await asyncio.gather(*entityQueries))}

    def _actor_from_actor_public(self, ap: "ActorPublic") -> Actor:  # noqa: F821
        try:
            from graphsenselib.tagstore.db import Taxonomies
        except ImportError as e:
            raise ImportError(
                "tagstore is required for actor operations. "
                "Please install it with: uv pip install tagstore"
            ) from e

        has_context = (
            ap.additional_uris
            or ap.image_links
            or ap.online_references
            or ap.coingecko_ids
            or ap.defilama_ids
            or ap.twitter_handles
            or ap.github_organisations
            or ap.legal_name
        )
        return Actor(
            id=ap.id,
            uri=ap.primary_uri,
            label=ap.label,
            jurisdictions=[
                LabeledItemRef(
                    id=x,
                    label=self.concepts_service.get_taxonomy_concept_label(
                        Taxonomies.COUNTRY, x
                    ),
                )
                for x in ap.jurisdictions
            ],
            categories=[
                LabeledItemRef(
                    id=x,
                    label=self.concepts_service.get_taxonomy_concept_label(
                        Taxonomies.CONCEPT, x
                    ),
                )
                for x in ap.concepts
            ],
            nr_tags=ap.nr_tags,
            context=ActorContext(
                uris=ap.additional_uris,
                images=ap.image_links,
                refs=ap.online_references,
                coingecko_ids=ap.coingecko_ids,
                defilama_ids=ap.defilama_ids,
                twitter_handle=",".join(ap.twitter_handles),
                github_organisation=",".join(ap.github_organisations),
                legal_name=ap.legal_name,
            )
            if has_context
            else None,
        )

    async def get_actor(self, actor_id: str) -> Actor:
        a = await self.tagstore.get_actor_by_id(actor_id, include_tag_count=True)

        if a is None:
            raise NotFoundException(f"Actor {actor_id} not found.")
        else:
            return self._actor_from_actor_public(a)

    async def get_actor_tags(
        self,
        actor_id: str,
        tagstore_groups: List[str],
        page: Optional[int] = None,
        pagesize: Optional[int] = None,
    ) -> AddressTagResult:
        page = page or 0

        assert page is None or isinstance(page, int)

        tags = await self.tagstore.get_tags_by_actorid(
            actor_id,
            offset=page * (pagesize or 0),
            page_size=pagesize,
            groups=tagstore_groups,
        )

        tag_entities = await self._get_entities_dict(self.db, tags)

        return self._get_address_tag_result(
            page,
            pagesize,
            [
                self._address_tag_from_public_tag(
                    t, tag_entities[(t.identifier, t.network)]
                )
                for t in tags
            ],
        )

    async def list_address_tags_by_label(
        self,
        label: str,
        tagstore_groups: List[str],
        page: Optional[int] = None,
        pagesize: Optional[int] = None,
    ) -> AddressTagResult:
        page = page or 0

        assert page is None or isinstance(page, int)

        tags = await self.tagstore.get_tags_by_label(
            label,
            offset=page * (pagesize or 0),
            page_size=pagesize,
            groups=tagstore_groups,
        )

        tag_entities = await self._get_entities_dict(self.db, tags)

        return self._get_address_tag_result(
            page,
            pagesize,
            [
                self._address_tag_from_public_tag(
                    t, tag_entities[(t.identifier, t.network)]
                )
                for t in tags
            ],
        )

    async def get_actors_by_subjectid(
        self,
        subject_id: str,
        tagstore_groups: List[str],
    ) -> List[LabeledItemRef]:
        actors = await self.tagstore.get_actors_by_subjectid(
            subject_id, tagstore_groups
        )
        return (
            [LabeledItemRef(id=a.id, label=a.label) for a in actors] if actors else []
        )

    async def list_concepts(self, taxonomy: str) -> List[Concept]:
        try:
            from graphsenselib.tagstore.db import Taxonomies
        except ImportError as e:
            raise ImportError(
                "tagstore is required for taxonomy operations. "
                "Please install it with: uv pip install tagstore"
            ) from e

        taxonomy = taxonomy.lower().strip()

        # for backwards comp.
        only_abuses = False
        if taxonomy == "entity":
            taxonomy = "concept"

        if taxonomy == "abuse":
            only_abuses = True
            taxonomy = "concept"

        if taxonomy.lower() not in [x.name.lower() for x in Taxonomies]:
            raise NotFoundException(f"Taxonomy {taxonomy} does not exist.")

        taxs = await self.tagstore.get_taxonomies({Taxonomies[taxonomy.upper()]})

        result = []

        for k, v in taxs:
            if v is None:
                continue

            for x in v:
                if only_abuses and not x.is_abuse:
                    continue
                result.append(
                    Concept(
                        id=x.id,
                        label=x.label,
                        description=x.description,
                        taxonomy=x.taxonomy,
                        uri=x.source,
                    )
                )

        return result

    async def list_taxonomies(self) -> List[Taxonomy]:
        taxs = await self.tagstore.get_taxonomies()

        return [
            Taxonomy(
                taxonomy=k,
                uri=(
                    "https://github.com/graphsense/"
                    "graphsense-tagpack-tool/tree/master/src/tagpack/db"
                ),
            )
            for k, v in taxs
        ]

    async def report_tag(
        self, data: Any, config: TagInsertConfigProtocol, tag_acl_group: str
    ) -> Optional[str]:
        try:
            from graphsenselib.tagstore.db import TagAlreadyExistsException
        except ImportError as e:
            raise ImportError(
                "tagstore is required for tag reporting. "
                "Please install it with: uv pip install tagstore"
            ) from e

        reporting_enabled = config.enable_user_tag_reporting

        insert_id = None
        if reporting_enabled:
            nt = data

            try:
                insert_id = await self.tagstore.add_user_reported_tag(
                    nt, acl_group=tag_acl_group
                )
            except TagAlreadyExistsException:
                logger.info("Tag already exists, ignoring insert.")

            info_hook = config.slack_info_hook

            if info_hook is not None:
                for h in info_hook.hooks:
                    try:
                        send_message_to_slack(
                            f"User Reported new Tag: {str(nt)} to ACL Group {tag_acl_group}",
                            h,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send tag reported slack info: {e}")

            return insert_id

        else:
            raise FeatureNotAvailableException(
                "The report tag feature is disabled on this endpoint."
            )
