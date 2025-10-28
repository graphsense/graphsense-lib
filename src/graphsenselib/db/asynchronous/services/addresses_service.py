import asyncio
from typing import Any, Dict, List, Optional, Protocol, Tuple, Callable

from graphsenselib.datatypes.common import NodeType
from graphsenselib.errors import (
    AddressNotFoundException,
    NetworkNotFoundException,
    ClusterNotFoundException,
    DBInconsistencyException,
)
from graphsenselib.utils.address import address_to_user_format
from graphsenselib.utils.tron import tron_address_to_evm_string
from graphsenselib.tagstore.db import TagPublic

from .blocks_service import BlocksService
from .common import (
    cannonicalize_address,
    get_address,
    links_response,
    list_neighbors,
    try_get_cluster_id,
    txs_from_rows,
)
from .entities_service import EntitiesService
from .models import (
    Address,
    AddressTagResult,
    AddressTxs,
    CrossChainPubkeyRelatedAddresses,
    Entity,
    Links,
    NeighborAddress,
    NeighborAddresses,
    CrossChainPubkeyRelatedAddress,
    TagSummary,
    AddressTagQueryInput,
)
from .rates_service import RatesService
from .tags_service import TagsService


class DatabaseProtocol(Protocol):
    async def get_address_id(self, currency: str, address: str) -> Optional[int]: ...
    async def list_address_txs(
        self,
        currency: str,
        address: str,
        direction: Optional[str],
        min_height: Optional[int],
        max_height: Optional[int],
        order: str,
        token_currency: Optional[str],
        page: Optional[str],
        pagesize: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]: ...
    async def list_address_links(
        self,
        currency: str,
        address: str,
        neighbor: str,
        min_height: Optional[int],
        max_height: Optional[int],
        order: str,
        token_currency: Optional[str],
        page: Optional[str],
        pagesize: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]: ...
    async def get_address_entity_id(self, currency: str, address: str) -> int: ...
    async def new_entity(self, currency: str, address: str) -> Dict[str, Any]: ...
    def get_token_configuration(self, currency: str) -> Dict[str, Any]: ...


FORK_TUPLES = [("btc", "bch")]


class AddressesService:
    def __init__(
        self,
        db: DatabaseProtocol,
        tags_service: TagsService,
        entities_service: EntitiesService,
        blocks_service: BlocksService,
        rates_service: RatesService,
        logger: Any,
    ):
        self.db = db
        self.tagstore = tags_service.tagstore if tags_service is not None else None
        self.tags_service = tags_service
        self.entities_service = entities_service
        self.blocks_service = blocks_service
        self.rates_service = rates_service
        self.logger = logger

    async def _handle_fork_address_overlap(
        self,
        raw_query_results: List[Dict[str, Any]],
        address: str,
        network: Optional[str] = None,
    ) -> List[CrossChainPubkeyRelatedAddress]:
        results = []
        for b, f in FORK_TUPLES:
            included_nets = {addr["currency"] for addr in raw_query_results}

            if network is not None:
                included_nets.add(network)

            if b in included_nets and f not in included_nets:
                base_network = b
                fork_network = f

            elif f in included_nets and b not in included_nets:
                base_network = f
                fork_network = b
            else:
                continue

            core_addresses = [
                addr for addr in raw_query_results if addr["currency"] == base_network
            ]
            try:
                fork_address = await self.get_address(
                    fork_network, address, tagstore_groups=[], include_actors=False
                )
            except (AddressNotFoundException, NetworkNotFoundException):
                fork_address = None

            if fork_address is not None and len(core_addresses) > 0:
                core_address = core_addresses[0]
                results.append(
                    CrossChainPubkeyRelatedAddress(
                        currency=fork_address.currency,
                        address=fork_address.address,
                        type=core_address["type"],
                        pubkey=core_address["pubkey"],
                    )
                )
            elif fork_address is not None:
                results.append(
                    CrossChainPubkeyRelatedAddress(
                        currency=fork_address.currency,
                        address=fork_address.address,
                        type="fork_only",
                        pubkey=None,
                    )
                )
        return results

    async def get_cross_chain_pubkey_related_addresses(
        self,
        address: str,
        network: Optional[str] = None,
        page: Optional[int] = None,
        pagesize: Optional[int] = None,
    ) -> CrossChainPubkeyRelatedAddresses:
        if page is not None and page < 1:
            raise ValueError("Page number must be at least 1")

        try:
            raw_query_results = await self.db.get_cross_chain_pubkey_related_addresses(
                address, network
            )
        except NetworkNotFoundException:
            raw_query_results = []

        data = [
            CrossChainPubkeyRelatedAddress.model_validate(addr)
            for addr in raw_query_results
            if addr["address"] != address
            or (
                network is None or (network is not None and addr["currency"] != network)
            )
        ]

        if network is not None and network == "trx" and len(data) == 0:
            # Special case for TRX, if trx is not found in the address_to_pubkey table, retry with
            # the EVM equivalent address
            # This is because TRX addresses can be derived from EVM addresses
            # and we want to ensure we find related addresses even if the TRX address is not
            # directly in the address_to_pubkey table.
            evm_address = tron_address_to_evm_string(address)

            try:
                results_trx = await self.db.get_cross_chain_pubkey_related_addresses(
                    evm_address, "eth"
                )
            except NetworkNotFoundException:
                results_trx = []

            data = [
                CrossChainPubkeyRelatedAddress.model_validate(addr)
                for addr in results_trx
                if addr["address"] != address
                or (
                    network is None
                    or (network is not None and addr["currency"] != network)
                )
            ]

        forked_addresses = await self._handle_fork_address_overlap(
            raw_query_results, address, network=network
        )

        data.extend(forked_addresses)

        # Note: we always produce all results and then simulate pages if needed.
        next_page = None
        if page is not None and pagesize is not None:
            page = page - 1
            start = page * pagesize
            end = start + pagesize
            if end < len(data):
                next_page = page + 2
            data = data[start:end]

        return CrossChainPubkeyRelatedAddresses(addresses=data, next_page=next_page)

    async def get_address(
        self,
        currency: str,
        address: str,
        tagstore_groups: List[str],
        include_actors: bool = True,
    ) -> Address:
        return await get_address(
            self.db,
            self.tagstore,
            self.rates_service,
            currency,
            address,
            tagstore_groups,
            include_actors,
        )

    async def list_tags_by_address(
        self,
        currency: str,
        address: str,
        tagstore_groups: List[str],
        cache: Dict[str, Any],
        page: Optional[int] = None,
        pagesize: Optional[int] = None,
        include_best_cluster_tag: bool = False,
        include_pubkey_derived_tags: bool = False,
    ) -> AddressTagResult:
        page = page or 0

        assert page is None or isinstance(page, int)

        cluster_id = await try_get_cluster_id(self.db, currency, address, cache=cache)

        if include_pubkey_derived_tags:
            additional_tags = await self.get_cross_chain_pubkey_related_addresses(
                address,
                network=currency,
            )

            addresses = [AddressTagQueryInput(network=currency, address=address)] + [
                AddressTagQueryInput(
                    network=pk.network,
                    address=pk.address,
                    inherited_from_marker="pubkey",
                )
                for pk in additional_tags.addresses
            ]
        else:
            addresses = [AddressTagQueryInput(network=currency, address=address)]

        tags, is_last_page = await self.tags_service.list_tags_by_addresses_raw(
            addresses,
            tagstore_groups,
            page=page,
            pagesize=pagesize,
            include_best_cluster_tag=include_best_cluster_tag,
            cache=cache,
        )

        self.logger.info(
            f"Fetched {len(tags)} tags for address {address} on network {currency}"
        )

        # Convert to AddressTag objects using tags service
        address_tags = []
        for pt in tags:
            tag = self.tags_service._address_tag_from_public_tag(pt, cluster_id)
            # Handle foreign network clusters
            if tag.currency and tag.currency.upper() != currency.upper():
                tag.entity = await try_get_cluster_id(
                    self.db, tag.currency, address, cache=cache
                )
            address_tags.append(tag)

        return self.tags_service._get_address_tag_result(
            current_page=page,
            page_size=pagesize,
            tags=address_tags,
            is_last_page=is_last_page,
        )

    async def list_address_txs(
        self,
        currency: str,
        address: str,
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

        address = cannonicalize_address(currency, address)
        results, paging_state = await self.db.list_address_txs(
            currency=currency,
            address=address,
            direction=direction,
            min_height=min_b,
            max_height=max_b,
            order=order,
            token_currency=token_currency,
            page=page,
            pagesize=pagesize,
        )

        address_txs = await txs_from_rows(
            currency,
            results,
            self.rates_service,
            self.db.get_token_configuration(currency),
        )
        return AddressTxs(next_page=paging_state, address_txs=address_txs)

    async def list_address_neighbors(
        self,
        currency: str,
        address: str,
        direction: str,
        tagstore_groups: List[str],
        only_ids: Optional[List[str]] = None,
        include_labels: bool = False,
        include_actors: bool = True,
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
    ) -> NeighborAddresses:
        address = cannonicalize_address(currency, address)

        if isinstance(only_ids, list):
            aws = [
                self.db.get_address_id(currency, cannonicalize_address(currency, id))
                for id in only_ids
            ]
            only_ids = await asyncio.gather(*aws)
            only_ids = [id for id in only_ids if id is not None]

        results, paging_state = await list_neighbors(
            self.db,
            currency,
            address,
            direction,
            NodeType.ADDRESS,
            ids=only_ids,
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
            return NeighborAddresses(neighbors=[])

        aws = [
            self.get_address(
                currency,
                address_to_user_format(currency, row[dst + "_address"]),
                tagstore_groups,
                include_actors=include_actors,
            )
            for row in results
        ]

        nodes = await asyncio.gather(*aws)

        for row, node in zip(results, nodes):
            nb = NeighborAddress(
                labels=row["labels"],
                value=row["value"],
                no_txs=row["no_transactions"],
                token_values=row["token_values"],
                address=node,
            )
            relations.append(nb)

        return NeighborAddresses(next_page=paging_state, neighbors=relations)

    async def list_address_links(
        self,
        currency: str,
        address: str,
        neighbor: str,
        min_height: Optional[int] = None,
        max_height: Optional[int] = None,
        min_date: Optional[Any] = None,
        max_date: Optional[Any] = None,
        order: str = "desc",
        token_currency: Optional[str] = None,
        page: Optional[str] = None,
        pagesize: Optional[int] = None,
        request_timeout: Optional[float] = None,
    ) -> Links:
        min_b, max_b = await self.blocks_service.get_min_max_height(
            currency, min_height, max_height, min_date, max_date
        )

        address = cannonicalize_address(currency, address)
        neighbor = cannonicalize_address(currency, neighbor)

        try:
            result = await asyncio.wait_for(
                self.db.list_address_links(
                    currency,
                    address,
                    neighbor,
                    min_height=min_b,
                    max_height=max_b,
                    order=order,
                    token_currency=token_currency,
                    page=page,
                    pagesize=pagesize,
                ),
                timeout=request_timeout,
            )
        except asyncio.TimeoutError:
            raise Exception(
                f"Timeout while fetching links for {currency}/{address} to {neighbor}"
            )

        return await links_response(
            currency,
            result,
            self.rates_service,
            self.db.get_token_configuration(currency),
        )

    async def get_address_entity(
        self,
        currency: str,
        address: str,
        include_actors: bool = True,
        tagstore_groups: List[str] = [],
    ) -> Entity:
        address_canonical = cannonicalize_address(currency, address)

        try:
            entity_id = await self.db.get_address_entity_id(currency, address_canonical)
        except AddressNotFoundException:
            rates = await self.rates_service.get_rates(currency)
            entity_data = await self.db.new_entity(currency, address_canonical)
            return self.entities_service._from_row(
                currency,
                entity_data,
                rates.rates,
                self.db.get_token_configuration(currency),
            )

        try:
            entity = await self.entities_service.get_entity(
                currency,
                entity_id,
                include_actors=include_actors,
                tagstore_groups=tagstore_groups,
            )
            # Remove inherited indicator from tag if it's the same address
            if (
                entity is not None
                and entity.best_address_tag is not None
                and entity.best_address_tag.address == address
            ):
                entity.best_address_tag.inherited_from = None
            return entity
        except ClusterNotFoundException:
            raise DBInconsistencyException(
                f"entity referenced by {address} in {currency} not found"
            )

    async def get_tag_summary_by_address(
        self,
        currency: str,
        address: str,
        tagstore_groups: List[str],
        include_best_cluster_tag: bool = False,
        include_pubkey_derived_tags: bool = False,
        only_propagate_high_confidence_actors: bool = True,
        tag_transformer: Callable[["TagPublic"], "TagPublic"] = None,
    ) -> TagSummary:
        if include_pubkey_derived_tags:
            additional_tags = await self.get_cross_chain_pubkey_related_addresses(
                address,
                network=currency,
            )

            addresses = [AddressTagQueryInput(network=currency, address=address)] + [
                AddressTagQueryInput(
                    network=pk.network,
                    address=pk.address,
                    inherited_from_marker="pubkey",
                )
                for pk in additional_tags.addresses
            ]
        else:
            addresses = [AddressTagQueryInput(network=currency, address=address)]

        return await self.tags_service.get_tag_summary_by_addresses(
            addresses,
            tagstore_groups,
            only_propagate_high_confidence_actors=only_propagate_high_confidence_actors,
            include_best_cluster_tag=include_best_cluster_tag,
            tag_transformer=tag_transformer,
        )
