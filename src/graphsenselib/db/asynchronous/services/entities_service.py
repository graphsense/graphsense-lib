import asyncio
from typing import Any, Dict, List, Optional, Protocol

from graphsenselib.datatypes.common import NodeType
from graphsenselib.errors import ClusterNotFoundException
from graphsenselib.utils.address import address_to_user_format

from .blocks_service import BlocksService
from .common import (
    address_from_row,
    convert_token_values_map,
    convert_value,
    links_response,
    list_neighbors,
    to_values,
    to_values_tokens,
    txs_from_rows,
)
from .models import (
    AddressTagResult,
    AddressTxs,
    Entity,
    EntityAddresses,
    LabeledItemRef,
    Links,
    NeighborEntities,
    NeighborEntity,
    TxSummary,
)
from .rates_service import RatesService
from .tags_service import TagsService


class DatabaseProtocol(Protocol):
    async def get_entity(
        self, currency: str, entity_id: int
    ) -> Optional[Dict[str, Any]]: ...
    async def list_entity_addresses(
        self,
        currency: str,
        entity: int,
        page: Optional[str],
        pagesize: Optional[int],
    ) -> tuple: ...
    async def list_entity_links(
        self,
        currency: str,
        entity: int,
        neighbor_id: int,
        min_height: Optional[int],
        max_height: Optional[int],
        order: str,
        token_currency: Optional[str],
        page: Optional[str],
        pagesize: Optional[int],
    ) -> tuple: ...
    async def list_entity_txs(
        self,
        currency: str,
        entity: int,
        direction: Optional[str],
        min_height: Optional[int],
        max_height: Optional[int],
        order: str,
        token_currency: Optional[str],
        page: Optional[str],
        pagesize: Optional[int],
    ) -> tuple: ...
    def get_token_configuration(self, currency: str) -> Dict[str, Any]: ...


class TagstoreProtocol(Protocol):
    async def get_tags_by_clusterid(
        self,
        cluster_id: int,
        currency: str,
        offset: int,
        limit: Optional[int],
        groups: List[str],
    ) -> List[Any]: ...
    async def get_best_cluster_tag(
        self, cluster_id: int, currency: str, groups: List[str]
    ) -> Optional[Any]: ...
    async def get_nr_tags_by_clusterid(
        self, cluster_id: int, currency: str, groups: List[str]
    ) -> int: ...
    async def get_actors_by_clusterid(
        self, cluster_id: int, currency: str, groups: List[str]
    ) -> List[Any]: ...


class EntitiesService:
    def __init__(
        self,
        db: DatabaseProtocol,
        tagstore: TagstoreProtocol,
        tags_service: TagsService,
        blocks_service: BlocksService,
        rates_service: RatesService,
        logger: Any,
    ):
        self.db = db
        self.tagstore = tagstore
        self.tags_service = tags_service
        self.blocks_service = blocks_service
        self.rates_service = rates_service
        self.logger = logger

    def _from_row(
        self,
        currency: str,
        row: Dict[str, Any],
        rates: Dict[str, float],
        token_config: Dict[str, Any],
        best_tag=None,
        count=0,
        actors=None,
    ) -> Entity:
        return Entity(
            currency=currency,
            entity=row["cluster_id"],
            root_address=address_to_user_format(currency, row["root_address"]),
            first_tx=TxSummary(
                height=row["first_tx"].height,
                timestamp=row["first_tx"].timestamp,
                tx_hash=row["first_tx"].tx_hash.hex(),
            ),
            last_tx=TxSummary(
                height=row["last_tx"].height,
                timestamp=row["last_tx"].timestamp,
                tx_hash=row["last_tx"].tx_hash.hex(),
            ),
            no_addresses=row["no_addresses"],
            no_incoming_txs=row["no_incoming_txs"],
            no_outgoing_txs=row["no_outgoing_txs"],
            total_received=to_values(row["total_received"]),
            total_tokens_received=to_values_tokens(row.get("total_tokens_received")),
            total_spent=to_values(row["total_spent"]),
            total_tokens_spent=to_values_tokens(row.get("total_tokens_spent")),
            in_degree=row["in_degree"],
            out_degree=row["out_degree"],
            balance=convert_value(currency, row["balance"], rates),
            token_balances=convert_token_values_map(
                currency, row.get("token_balances"), rates, token_config
            ),
            best_address_tag=best_tag,
            no_address_tags=count,
            actors=actors,
        )

    async def get_entity(
        self,
        currency: str,
        entity_id: int,
        exclude_best_address_tag: bool = False,
        include_actors: bool = True,
        tagstore_groups: List[str] = [],
    ) -> Entity:
        result = await self.db.get_entity(currency, entity_id)
        if not result:
            raise ClusterNotFoundException(currency, entity_id)

        rates = await self.rates_service.get_rates(currency)

        best_tag = None
        count = 0
        if not exclude_best_address_tag:
            tag = await self.tagstore.get_best_cluster_tag(
                int(entity_id), currency.upper(), tagstore_groups
            )

            if tag is not None:
                best_tag = self.tags_service._address_tag_from_public_tag(
                    tag, int(entity_id)
                )

        count = await self.tagstore.get_nr_tags_by_clusterid(
            int(entity_id), currency.upper(), tagstore_groups
        )

        actors = None
        if include_actors:
            actor_res = await self.tagstore.get_actors_by_clusterid(
                int(entity_id), currency.upper(), tagstore_groups
            )
            actors = [LabeledItemRef(id=a.id, label=a.label) for a in actor_res]

        self.logger.debug(f"result address {result}")

        return self._from_row(
            currency,
            result,
            rates,
            self.db.get_token_configuration(currency),
            best_tag,
            count,
            actors,
        )

    async def list_entity_addresses(
        self,
        currency: str,
        entity_id: int,
        tagstore_groups: List[str],
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
    ) -> EntityAddresses:
        results, paging_state = await self.db.list_entity_addresses(
            currency, entity_id, page, pagesize
        )

        if results is None:
            return EntityAddresses(addresses=[])

        rates = await self.rates_service.get_rates(currency)
        # Convert each address result to Address objects
        addresses = []
        for row in results:
            # Get actors for this address
            actors = await self.tags_service.get_actors_by_subjectid(
                address_to_user_format(currency, row["address"]),
                tagstore_groups,
            )

            address = address_from_row(
                currency,
                row,
                rates.rates,
                self.db.get_token_configuration(currency),
                actors=actors,
            )
            addresses.append(address)

        return EntityAddresses(next_page=paging_state, addresses=addresses)

    async def list_entity_neighbors(
        self,
        currency: str,
        entity_id: int,
        direction: str,
        tagstore_groups: List[str],
        only_ids: Optional[List[int]] = None,
        include_labels: bool = False,
        include_actors: bool = False,
        relations_only=False,
        exclude_best_address_tag=False,
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
    ) -> NeighborEntities:
        results, paging_state = await list_neighbors(
            self.db,
            currency,
            entity_id,
            direction,
            NodeType.CLUSTER,
            ids=[id for id in only_ids] if only_ids else None,
            include_labels=include_labels,
            page=page,
            pagesize=pagesize,
            tagstore=self.tagstore if include_labels else None,
            tagstore_groups=tagstore_groups if include_labels else None,
        )

        is_outgoing = "out" in direction
        dst = "dst" if is_outgoing else "src"
        relations = []

        if results is None:
            return NeighborEntities(neighbors=[])

        if not relations_only:
            aws = [
                self.get_entity(
                    currency,
                    row[dst + "_cluster_id"],
                    exclude_best_address_tag=exclude_best_address_tag,
                    include_actors=include_actors,
                    tagstore_groups=tagstore_groups,
                )
                for row in results
            ]
            nodes = await asyncio.gather(*aws)

        else:
            nodes = [r[dst + "_cluster_id"] for r in results]

        for row, node in zip(results, nodes):
            nb = NeighborEntity(
                labels=row["labels"],
                value=row["value"],
                token_values=row.get("token_values", None),
                no_txs=row["no_transactions"],
                entity=node,
            )
            relations.append(nb)

        return NeighborEntities(next_page=paging_state, neighbors=relations)

    async def list_entity_links(
        self,
        currency: str,
        entity_id: int,
        neighbor_id: int,
        min_height: Optional[int] = None,
        max_height: Optional[int] = None,
        min_date: Optional[Any] = None,
        max_date: Optional[Any] = None,
        order: str = "desc",
        token_currency: Optional[str] = None,
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
    ) -> Links:
        min_b, max_b = await self.blocks_service.get_min_max_height(
            currency, min_height, max_height, min_date, max_date
        )

        # Get entity links from database
        result = await self.db.list_entity_links(
            currency,
            entity_id,
            neighbor_id,
            min_height=min_b,
            max_height=max_b,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

        return await links_response(
            currency,
            result,
            self.rates_service,
            self.db.get_token_configuration(currency),
        )

    async def list_entity_txs(
        self,
        currency: str,
        entity_id: int,
        min_height: Optional[int] = None,
        max_height: Optional[int] = None,
        min_date: Optional[Any] = None,
        max_date: Optional[Any] = None,
        direction: Optional[str] = None,
        order: str = "desc",
        token_currency: Optional[str] = None,
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
    ) -> AddressTxs:
        min_b, max_b = await self.blocks_service.get_min_max_height(
            currency, min_height, max_height, min_date, max_date
        )

        results, paging_state = await self.db.list_entity_txs(
            currency=currency,
            entity=entity_id,
            direction=direction,
            min_height=min_b,
            max_height=max_b,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

        entity_txs = await txs_from_rows(
            currency,
            results,
            self.rates_service,
            self.db.get_token_configuration(currency),
        )

        return AddressTxs(next_page=paging_state, address_txs=entity_txs)

    async def list_address_tags_by_entity(
        self,
        currency: str,
        entity_id: int,
        tagstore_groups: List[str],
        page: Optional[int] = None,
        pagesize: Optional[int] = None,
    ) -> AddressTagResult:
        page = page or 0

        assert page is None or isinstance(page, int)

        tags = await self.tagstore.get_tags_by_clusterid(
            entity_id,
            currency.upper(),
            page * (pagesize or 0),
            pagesize,
            tagstore_groups,
        )

        # Convert to AddressTag objects using tags service
        address_tags = []
        for pt in tags:
            tag = self.tags_service._address_tag_from_public_tag(pt, entity_id)
            address_tags.append(tag)

        return self.tags_service._get_address_tag_result(page, pagesize, address_tags)
