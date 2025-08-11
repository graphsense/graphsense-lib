from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Tuple

from async_lru import alru_cache
from graphsenselib.errors import BadUserInputException, BlockNotFoundException

from .common import std_tx_from_row
from ....utils.rest_utils import is_eth_like
from .models import Block, BlockAtDate


class DatabaseProtocol(Protocol):
    async def get_block(
        self, currency: str, height: int
    ) -> Optional[Dict[str, Any]]: ...
    async def list_block_txs(
        self, currency: str, height: int
    ) -> Optional[List[Dict[str, Any]]]: ...
    async def get_block_by_date_allow_filtering(
        self, currency: str, ts: int
    ) -> Optional[Dict[str, Any]]: ...
    async def get_block_below_block_allow_filtering(
        self, currency: str, block: int
    ) -> Optional[Dict[str, Any]]: ...
    async def get_block_timestamp(
        self, currency: str, block: int
    ) -> Optional[Dict[str, Any]]: ...
    async def get_currency_statistics(
        self, currency: str
    ) -> Optional[Dict[str, Any]]: ...
    def get_token_configuration(self, currency: str) -> Dict[str, Any]: ...


class RatesServiceProtocol(Protocol):
    async def get_rates(self, currency: str, height: Optional[int] = None) -> Any: ...


class ConfigProtocol(Protocol):
    block_by_date_use_linear_search: bool


@alru_cache(maxsize=1000)
async def find_block_by_ts(get_timestamp, currency, ts, start, end):
    return await find_insertion_point_async(get_timestamp, ts, low=start, high=end)


async def find_insertion_point_async(accessor, x, low: int, high: int):
    mid = 0

    while low <= high:
        mid = (high + low) // 2
        mid_item = await accessor(mid)

        if mid_item < x:
            low = mid + 1
        elif mid_item > x:
            high = mid - 1
        else:
            return mid

    return mid - 1 if (mid_item > x) else mid


class BlocksService:
    def __init__(
        self,
        db: DatabaseProtocol,
        rates_service: RatesServiceProtocol,
        config: ConfigProtocol,
        logger: Any,
    ):
        self.db = db
        self.rates_service = rates_service
        self.config = config
        self.logger = logger

    def _block_from_row(self, currency: str, row: Dict[str, Any]) -> Block:
        if is_eth_like(currency):
            return Block(
                currency=currency,
                height=row["block_id"],
                block_hash=row["block_hash"].hex(),
                no_txs=row["transaction_count"],
                timestamp=row["timestamp"],
            )
        return Block(
            currency=currency,
            height=row["block_id"],
            block_hash=row["block_hash"].hex(),
            no_txs=row["no_transactions"],
            timestamp=row["timestamp"],
        )

    async def get_block(self, currency: str, height: int) -> Block:
        row = await self.db.get_block(currency, height)
        if not row:
            raise BlockNotFoundException(currency, height)
        return self._block_from_row(currency, row)

    async def list_block_txs(self, currency: str, height: int) -> List[Any]:
        txs = await self.db.list_block_txs(currency, height)

        if txs is None:
            raise BlockNotFoundException(currency, height)

        rates = await self.rates_service.get_rates(currency, height)

        tx_results = []
        for tx in txs:
            tx_result = await std_tx_from_row(
                currency,
                tx,
                rates.rates,
                self.db.get_token_configuration(currency),
                include_io=True,
            )
            tx_results.append(tx_result)

        return tx_results

    async def get_min_max_height(
        self,
        network: str,
        min_height: Optional[int],
        max_height: Optional[int],
        min_date: Optional[datetime],
        max_date: Optional[datetime],
    ) -> Tuple[Optional[int], Optional[int]]:
        if min_date is not None and min_height is not None:
            raise BadUserInputException(
                "Both min_height and min_date parameters are specified. Please chose one."
            )

        if max_date is not None and max_height is not None:
            raise BadUserInputException(
                "Both max_height and max_date parameters are specified. Please chose one."
            )

        if min_height is None and min_date is not None:
            bspec_min = await self.get_block_by_date(network, min_date)
            min_height = bspec_min.before_block

        if max_height is None and max_date is not None:
            bspec_max = await self.get_block_by_date(network, max_date)
            max_height = bspec_max.before_block

        return (min_height, max_height)

    async def get_block_by_date(self, currency: str, date: datetime) -> BlockAtDate:
        use_linear_search = self.config.block_by_date_use_linear_search
        ts = int(date.timestamp())

        if use_linear_search:
            x = await self.db.get_block_by_date_allow_filtering(currency, ts)

            if x:
                block = x["block_id"]
                block_before = await self.db.get_block_below_block_allow_filtering(
                    currency, block
                )

                return BlockAtDate(
                    before_block=block_before["block_id"],
                    before_timestamp=block_before["timestamp"],
                    after_block=block,
                    after_timestamp=x["timestamp"],
                )
            else:
                return BlockAtDate(
                    before_block=None,
                    before_timestamp=None,
                    after_block=None,
                    after_timestamp=None,
                )
        else:
            hb = (await self.db.get_currency_statistics(currency))["no_blocks"] - 1
            start = 0

            async def get_timestamp(blk):
                bts = await self.db.get_block_timestamp(currency, blk)
                if bts is None:
                    return None
                else:
                    return int(bts.get("timestamp", None))

            r = await find_block_by_ts(get_timestamp, currency, ts, start, hb)

            if r == -1 or r > hb:
                r = None
                at = None
                bt = None
            elif r == hb:
                bt = await get_timestamp(r)
                at = None
            else:
                bt = await get_timestamp(r)
                at = await get_timestamp(r + 1)

            return BlockAtDate(
                before_block=r,
                before_timestamp=bt,
                after_block=r + 1 if at is not None else None,
                after_timestamp=at,
            )
