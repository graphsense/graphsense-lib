import asyncio
from datetime import datetime
from typing import Any, List, Optional, Protocol

from ....utils.rest_utils import alphanumeric_lower_identifier
from .models import (
    GeneralStats,
    LabeledItemRef,
    SearchResult,
    SearchResultByCurrency,
    Stats,
    SearchRequestConfig,
)


class DatabaseProtocol(Protocol):
    def get_supported_currencies(self) -> List[str]: ...
    async def list_matching_txs(
        self, currency: str, q: str, limit: int, include_sub_tx_identifiers: bool
    ) -> List[str]: ...
    async def list_matching_addresses(
        self, currency: str, q: str, limit: int
    ) -> List[str]: ...


class TagstoreProtocol(Protocol):
    async def search_labels(
        self,
        expression: str,
        limit: int,
        groups: List[str],
        query_actors: bool,
        query_labels: bool,
    ) -> Any: ...


class StatsServiceProtocol(Protocol):
    async def get_currency_statistics(self, currency: str) -> Any: ...


class GeneralService:
    def __init__(
        self,
        db: DatabaseProtocol,
        tagstore: TagstoreProtocol,
        stats_service: StatsServiceProtocol,
        logger: Any,
    ):
        self.db = db
        self.tagstore = tagstore
        self.stats_service = stats_service
        self.logger = logger

    async def get_statistics(self, version: str) -> Stats:
        """Returns summary statistics on all available currencies"""
        currency_stats = []

        aws = [
            self.stats_service.get_currency_statistics(currency)
            for currency in self.db.get_supported_currencies()
        ]
        currency_stats = await asyncio.gather(*aws)

        tstamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return Stats(
            currencies=currency_stats, version=version, request_timestamp=tstamp
        )

    async def search_by_currency(
        self,
        currency: str,
        q: str,
        limit: int = 10,
        config: SearchRequestConfig = SearchRequestConfig(),
    ) -> SearchResultByCurrency:
        r = SearchResultByCurrency(currency=currency, addresses=[], txs=[])

        include_sub_tx_identifiers = config.include_sub_tx_identifiers

        if len(q) >= 3:
            if config.include_txs and config.include_addresses:
                [txs, addresses] = await asyncio.gather(
                    self.db.list_matching_txs(
                        currency,
                        q,
                        limit,
                        include_sub_tx_identifiers=include_sub_tx_identifiers,
                    ),
                    self.db.list_matching_addresses(currency, q, limit=limit),
                )
            elif config.include_txs:
                txs = await self.db.list_matching_txs(
                    currency,
                    q,
                    limit,
                    include_sub_tx_identifiers=include_sub_tx_identifiers,
                )
                addresses = []
            elif config.include_addresses:
                addresses = await self.db.list_matching_addresses(
                    currency, q, limit=limit
                )
                txs = []
            else:
                txs = []
                addresses = []
        else:
            txs = []
            addresses = []

        r.txs = txs
        r.addresses = addresses
        return r

    async def search(
        self,
        q: str,
        tagstore_groups: List[str],
        currency: Optional[str] = None,
        limit: int = 10,
        config: SearchRequestConfig = SearchRequestConfig(),
    ) -> SearchResult:
        currencies = self.db.get_supported_currencies()

        q = q.strip()
        result = SearchResult(currencies=[], labels=[], actors=[])

        currs = [
            curr
            for curr in currencies
            if currency is None or currency.lower() == curr.lower()
        ]

        expression_norm = alphanumeric_lower_identifier(q)

        tagstore_search = self.tagstore.search_labels(
            expression_norm,
            limit,
            groups=tagstore_groups,
            query_actors=config.include_actors,
            query_labels=config.include_labels,
        )

        aws1 = [
            self.search_by_currency(
                curr,
                q,
                limit=limit,
                config=config,
            )
            for curr in currs
        ]
        aw1 = asyncio.gather(*aws1)

        [r1, r2] = await asyncio.gather(aw1, tagstore_search)

        result.currencies = r1
        result.labels = [x.label for x in r2.tag_labels]
        result.actors = [
            LabeledItemRef(id=x.id, label=x.label) for x in r2.actor_labels
        ]

        return result

    def get_general_stats(self) -> GeneralStats:
        """Get general statistics including supported currencies"""
        currencies = self.db.get_supported_currencies()
        return GeneralStats(currencies=currencies)
