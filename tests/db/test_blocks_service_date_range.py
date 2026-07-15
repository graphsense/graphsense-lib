"""Regression tests for issue #52: address/entity txs by date range.

``get_min_max_height`` translates a ``min_date``/``max_date`` window into an
inclusive block-height range. The reported bug was that the lower bound used
``before_block`` (the last block with ts <= min_date), so a whole block of
transactions timestamped *before* min_date leaked into the result. The lower
bound must instead be the first block with ts >= min_date.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from graphsenselib.db.asynchronous.services.blocks_service import BlocksService
from graphsenselib.db.asynchronous.services.models import BlockAtDate

MIN_DATE = datetime(2017, 6, 8, 22, 0, 0, tzinfo=timezone.utc)
MIN_TS = int(MIN_DATE.timestamp())


def _service_returning(block_at_date: BlockAtDate) -> BlocksService:
    svc = BlocksService(
        db=MagicMock(),
        rates_service=MagicMock(),
        config=MagicMock(),
        logger=MagicMock(),
    )
    svc.get_block_by_date = AsyncMock(return_value=block_at_date)
    return svc


@pytest.mark.asyncio
async def test_min_date_between_blocks_uses_after_block():
    # min_date falls strictly between block 100 (older) and 101 (newer): block
    # 100's txs predate min_date and must be excluded -> lower bound is 101.
    svc = _service_returning(
        BlockAtDate(
            before_block=100,
            before_timestamp=MIN_TS - 100,
            after_block=101,
            after_timestamp=MIN_TS + 50,
        )
    )
    min_h, max_h = await svc.get_min_max_height("btc", None, None, MIN_DATE, None)
    assert min_h == 101
    assert max_h is None


@pytest.mark.asyncio
async def test_min_date_exact_boundary_keeps_before_block():
    # A block sits exactly on min_date: it is >= min_date and must be included.
    svc = _service_returning(
        BlockAtDate(
            before_block=100,
            before_timestamp=MIN_TS,
            after_block=101,
            after_timestamp=MIN_TS + 50,
        )
    )
    min_h, _ = await svc.get_min_max_height("btc", None, None, MIN_DATE, None)
    assert min_h == 100


@pytest.mark.asyncio
async def test_min_date_past_tip_yields_empty_range():
    # min_date is beyond the chain tip (only before_block, no after_block):
    # nothing qualifies, so the lower bound steps past the tip.
    svc = _service_returning(
        BlockAtDate(
            before_block=200,
            before_timestamp=MIN_TS - 500,
            after_block=None,
            after_timestamp=None,
        )
    )
    min_h, _ = await svc.get_min_max_height("btc", None, None, MIN_DATE, None)
    assert min_h == 201


@pytest.mark.asyncio
async def test_min_date_before_genesis_has_no_lower_bound():
    # min_date is at/before the first block: no lower bound needed.
    svc = _service_returning(
        BlockAtDate(
            before_block=None,
            before_timestamp=None,
            after_block=None,
            after_timestamp=None,
        )
    )
    min_h, _ = await svc.get_min_max_height("btc", None, None, MIN_DATE, None)
    assert min_h is None


@pytest.mark.asyncio
async def test_max_date_still_uses_before_block():
    # Upper bound is inclusive of blocks up to and including max_date, so it
    # keeps using before_block (last block with ts <= max_date). Guards against
    # the min-side fix accidentally changing max-side behavior.
    svc = _service_returning(
        BlockAtDate(
            before_block=150,
            before_timestamp=MIN_TS,
            after_block=151,
            after_timestamp=MIN_TS + 50,
        )
    )
    min_h, max_h = await svc.get_min_max_height("btc", None, None, None, MIN_DATE)
    assert max_h == 150
    assert min_h is None
