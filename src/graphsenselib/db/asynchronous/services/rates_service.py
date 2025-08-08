from typing import Any, Dict, List, Optional, Protocol

from graphsenselib.errors import BlockNotFoundException

from .common import map_rates_for_peged_tokens
from .models import RatesResponse


class DatabaseProtocol(Protocol):
    async def get_rates(
        self, currency: str, height: int
    ) -> Optional[Dict[str, Any]]: ...
    async def list_rates(
        self, currency: str, heights: List[int]
    ) -> List[Dict[str, Any]]: ...
    def get_token_configuration(self, currency: str) -> Optional[Dict[str, Any]]: ...


class StatsServiceProtocol(Protocol):
    async def get_no_blocks(self, currency: str) -> int: ...


class RatesService:
    def __init__(
        self, db: DatabaseProtocol, stats_service: StatsServiceProtocol, logger: Any
    ):
        self.db = db
        self.logger = logger
        self._stats_service = stats_service

    async def get_rates(
        self, currency: str, height: Optional[int] = None
    ) -> RatesResponse:
        if height is None:
            height = (await self._stats_service.get_no_blocks(currency)) - 1

        if ":" in currency:
            network, currency, *rest = currency.split(":")
        else:
            network, currency = (currency, currency)

        token_config = self.db.get_token_configuration(network)
        if token_config is not None and currency.upper() in token_config:
            # create pseudo rates for eth stable coin tokens.
            r = await self.db.get_rates(network, height)
            # this avoids changing original rates if cached
            # otherwise results are wrong.
            r = r.copy()
            r["rates"] = map_rates_for_peged_tokens(
                r["rates"], token_config[currency.upper()]
            )
        else:
            r = await self.db.get_rates(currency, height)

        if r is None:
            raise BlockNotFoundException(currency, height)

        # Handle the case where rates might be a list or already a dict
        rates_data = r["rates"]
        # if isinstance(rates_data, list):
        #     return RatesResponse.from_rate_list(rates_data)
        # else:
        return RatesResponse(height=height, rates=rates_data)

    async def list_rates(
        self, currency: str, heights: List[int]
    ) -> Dict[int, Dict[str, float]]:
        rates = await self.db.list_rates(currency, heights)

        height_rates = dict()  # key: height, value: {'eur': 0, 'usd':0}
        for rate in rates:
            height_rates[rate["block_id"]] = rate["rates"]
        return height_rates
