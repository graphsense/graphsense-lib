import time
from typing import Any, Optional, Tuple

from graphsenselib.db.asynchronous.services.addresses_service import AddressesService
from graphsenselib.db.asynchronous.services.blocks_service import BlocksService
from graphsenselib.db.asynchronous.services.entities_service import EntitiesService
from graphsenselib.db.asynchronous.services.general_service import GeneralService
from graphsenselib.db.asynchronous.services.rates_service import RatesService
from graphsenselib.db.asynchronous.services.stats_service import StatsService
from graphsenselib.db.asynchronous.services.tags_service import (
    ConceptProtocol,
    TagsService,
)
from graphsenselib.db.asynchronous.services.tokens_service import TokensService
from graphsenselib.db.asynchronous.services.txs_service import TxsService
from graphsenselib.tagstore.db import TagstoreDbAsync, Taxonomies
from graphsenselib.tagstore.db.queries import TagPublic

from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
    GROUPS_HEADER_NAME,
    OBFUSCATION_MARKER_GROUP,
)
from graphsenselib.web.config import GSRestConfig


class ConceptsCacheService(ConceptProtocol):
    def __init__(self, app: Any, logger: Any):
        self.logger = logger
        self.app = app

    def get_is_abuse(self, concept: str) -> bool:
        return concept in self.app["taxonomy-cache"]["abuse"]

    def get_taxonomy_concept_label(self, taxonomy: Any, concept_id: str) -> str:
        return self.app["taxonomy-cache"]["labels"][taxonomy].get(concept_id, None)

    @classmethod
    async def setup_cache(cls, db_engine: Any, app: Any):
        tagstore_db = TagstoreDbAsync(db_engine)
        taxs = await tagstore_db.get_taxonomies(
            {Taxonomies.CONCEPT, Taxonomies.COUNTRY}
        )
        app["taxonomy-cache"] = {
            "labels": {
                Taxonomies.CONCEPT: {x.id: x.label for x in taxs.concept},
                Taxonomies.COUNTRY: {x.id: x.label for x in taxs.country},
            },
            "abuse": {x.id for x in taxs.concept if x.is_abuse},
        }


class TagAccessLoggerTagstoreProxy:
    """Adds logging for which tags are accessed from the tagstore
    it intercepts calls to the tagstore DB
    and logs returned tags to redis.
    """

    def __init__(
        self, tagstore_db: TagstoreDbAsync, redis_client: Any, key_prefix: str
    ):
        self.tagstore_db = tagstore_db
        self.redis_client = redis_client
        self.key_prefix = key_prefix

    def __getattr__(self, name):
        """Proxy all method calls to the underlying tagstore_db"""
        attr = getattr(self.tagstore_db, name)

        if callable(attr):

            async def wrapper(*args, **kwargs):
                # Call the original method
                result = await attr(*args, **kwargs)

                # Log tag access if this method returns TagPublic objects
                should_log, is_list = self._should_log_result(result)
                if self.redis_client and should_log:
                    if is_list:
                        for tag in result:
                            await self._log_tag_access(name, tag, *args, **kwargs)
                    else:
                        await self._log_tag_access(name, result, *args, **kwargs)

                return result

            return wrapper
        else:
            return attr

    def _should_log_result(self, result: Any) -> Tuple[bool, bool]:
        """Determine if this result should be logged based on data type"""

        if not result:
            return False, False

        # Check if result is a PublicTag
        if isinstance(result, TagPublic):
            return True, False

        # Check if result is a list of TagPublic objects
        if hasattr(result, "__iter__") and not isinstance(result, str):
            try:
                # Check if all items in the iterable are TagPublic objects
                for item in result:
                    if isinstance(item, TagPublic):
                        return True, True
                    break  # Only check first item for performance
            except (TypeError, StopIteration):
                pass

        return False, False

    async def _log_tag_access(self, method_name: str, tag: TagPublic, *args, **kwargs):
        """Log tag access information to Redis"""

        current_time = time.localtime()
        timestamp = time.strftime("%Y-%m-%d", current_time)
        key = "|".join(
            (self.key_prefix, timestamp, tag.creator, tag.network, tag.identifier)
        )
        await self.redis_client.incr(key)


class ServiceContainer:
    def __init__(
        self,
        config: GSRestConfig,
        db: any,
        tagstore_engine: any,
        concepts_cache_service: ConceptsCacheService,
        logger: any,
        redis_client: Optional[Any] = None,
        log_tag_access_prefix: Optional[str] = None,
    ):
        tsdb = TagstoreDbAsync(tagstore_engine)
        self.config = config
        self.db = db
        self.tagstore_db = (
            TagAccessLoggerTagstoreProxy(tsdb, redis_client, log_tag_access_prefix)
            if log_tag_access_prefix
            else tsdb
        )
        self.logger = logger
        self.category_cache_service = concepts_cache_service

        # Initialize services with dependencies
        self._stats_service = StatsService(
            db=db, tagstore=self.tagstore_db, logger=logger
        )
        self._rates_service = RatesService(
            db=db, stats_service=self._stats_service, logger=logger
        )
        self._tokens_service = TokensService(db=db, logger=logger)

        self._tags_service = TagsService(
            db=db,
            tagstore=self.tagstore_db,
            concepts_cache_service=self.category_cache_service,
            logger=logger,
        )
        self._txs_service = TxsService(
            db=db, rates_service=self._rates_service, logger=logger
        )
        self._blocks_service = BlocksService(
            db=db,
            rates_service=self._rates_service,
            config=config,
            logger=logger,
        )
        self._general_service = GeneralService(
            db=db,
            tagstore=self.tagstore_db,
            stats_service=self._stats_service,
            logger=logger,
        )
        self._entities_service = EntitiesService(
            db=db,
            tags_service=self._tags_service,
            blocks_service=self._blocks_service,
            rates_service=self._rates_service,
            logger=logger,
        )

        self._addresses_service = AddressesService(
            db=db,
            tags_service=self._tags_service,
            entities_service=self._entities_service,
            blocks_service=self._blocks_service,
            rates_service=self._rates_service,
            logger=logger,
        )

    @property
    def tags_service(self) -> TagsService:
        return self._tags_service

    @property
    def rates_service(self) -> RatesService:
        return self._rates_service

    @property
    def blocks_service(self) -> BlocksService:
        return self._blocks_service

    @property
    def tokens_service(self) -> TokensService:
        return self._tokens_service

    @property
    def stats_service(self) -> StatsService:
        return self._stats_service

    @property
    def txs_service(self) -> TxsService:
        return self._txs_service

    @property
    def general_service(self) -> GeneralService:
        return self._general_service

    @property
    def addresses_service(self) -> AddressesService:
        return self._addresses_service

    @property
    def entities_service(self) -> EntitiesService:
        return self._entities_service


def get_service_container(request) -> ServiceContainer:
    """Extract service container from request"""
    return request.app["services"]


def get_request_cache(request):
    if not hasattr(request, "_cache"):
        request._cache = {}
    return request._cache


def get_user_tags_acl_group(request) -> str:
    return request.app["config"].user_tag_reporting_acl_group


def get_tagstore_access_groups(request):
    return (
        ["public"]
        if not request.app["request_config"]["show_private_tags"]
        else ["public", "private"]
    ) + [get_user_tags_acl_group(request)]


def get_username(request) -> Optional[str]:
    """Extract username from request, if available"""
    return request.headers.get("X-Consumer-Username", None)


def should_obfuscate_private_tags(request) -> bool:
    # Check header modifications from plugin middleware first
    state = getattr(request, "state", None)
    if state is not None:
        header_mods = getattr(state, "header_modifications", {})
        if header_mods.get(GROUPS_HEADER_NAME) == OBFUSCATION_MARKER_GROUP:
            return True
    # Fall back to checking actual headers
    return request.headers.get(GROUPS_HEADER_NAME, "") == OBFUSCATION_MARKER_GROUP
