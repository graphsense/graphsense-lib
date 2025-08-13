from typing import Any, Dict, Optional, Protocol

from .models import CurrencyStats


class DatabaseProtocol(Protocol):
    async def get_currency_statistics(
        self, currency: str
    ) -> Optional[Dict[str, Any]]: ...


class TagstoreProtocol(Protocol):
    async def get_network_statistics_cached(self) -> Any: ...


class StatsService:
    def __init__(self, db: DatabaseProtocol, tagstore: TagstoreProtocol, logger: Any):
        self.db = db
        self.tagstore = tagstore
        self.logger = logger

    async def get_currency_statistics(self, currency: str) -> CurrencyStats:
        from graphsenselib.tagstore.db import NetworkStatisticsPublic

        tag_stats = await self.tagstore.get_network_statistics_cached()

        result = await self.db.get_currency_statistics(currency)
        if result is None:
            raise SystemError("statistics for currency {} not found".format(currency))

        network_stats = tag_stats.by_network.get(
            currency.upper(), NetworkStatisticsPublic.zero()
        )
        no_labels = network_stats.nr_labels
        no_tagged_addresses = network_stats.nr_identifiers_implicit

        return CurrencyStats(
            name=currency,
            no_blocks=result["no_blocks"],
            no_address_relations=result["no_address_relations"],
            no_addresses=result["no_addresses"],
            no_entities=result["no_clusters"],
            no_txs=result["no_transactions"],
            no_labels=int(no_labels),
            no_tagged_addresses=int(no_tagged_addresses),
            timestamp=result["timestamp"],
        )

    async def get_no_blocks(self, currency: str) -> int:
        result = await self.db.get_currency_statistics(currency)
        if result is None:
            raise SystemError("statistics for currency {} not found".format(currency))
        return result["no_blocks"]
