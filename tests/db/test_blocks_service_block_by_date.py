"""Regression tests for the binary-search path of ``get_block_by_date``.

The search assumes every height in ``[start, no_blocks - 1]`` has a row in the
raw ``block`` table. TRX has no block 0 (ingest starts at block 1), so a date
below the first stored block's timestamp made the search probe height 0,
``get_block_timestamp`` returned ``None``, and the comparison ``None < ts``
raised ``TypeError`` (unhandled 500). The lower search bound must be clamped
to the first stored block.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from graphsenselib.db.asynchronous.services.blocks_service import BlocksService


def _service_with_blocks(timestamps: dict) -> BlocksService:
    db = MagicMock()

    async def get_block_timestamp(currency, height):
        ts = timestamps.get(height)
        return None if ts is None else {"timestamp": ts}

    db.get_block_timestamp = get_block_timestamp
    db.get_currency_statistics = AsyncMock(
        return_value={"no_blocks": max(timestamps) + 1}
    )
    config = MagicMock()
    config.block_by_date_use_linear_search = False
    return BlocksService(
        db=db, rates_service=MagicMock(), config=config, logger=MagicMock()
    )


def _date(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# Chain without block 0, like TRX: blocks 1..5 at timestamps 1000..5000.
TRX_LIKE = {b: b * 1000 for b in range(1, 6)}
# Chain with block 0, like BTC: blocks 0..5 at timestamps 1000..6000.
BTC_LIKE = {b: (b + 1) * 1000 for b in range(0, 6)}


@pytest.mark.asyncio
async def test_pre_genesis_date_without_block_zero_returns_empty():
    # Date before the first stored block on a chain lacking block 0: must
    # yield "no block at/before this date", not a TypeError on None.
    svc = _service_with_blocks(TRX_LIKE)
    r = await svc.get_block_by_date("trx", _date(500))
    assert r.before_block is None
    assert r.before_timestamp is None
    assert r.after_block is None
    assert r.after_timestamp is None


@pytest.mark.asyncio
async def test_date_between_blocks_without_block_zero_resolves():
    # Normal lookup still works when the search range is clamped to block 1.
    svc = _service_with_blocks(TRX_LIKE)
    r = await svc.get_block_by_date("trx", _date(2500))
    assert r.before_block == 2
    assert r.before_timestamp == 2000
    assert r.after_block == 3
    assert r.after_timestamp == 3000


@pytest.mark.asyncio
async def test_date_on_first_block_without_block_zero_resolves():
    # Date exactly on the first stored block's timestamp.
    svc = _service_with_blocks(TRX_LIKE)
    r = await svc.get_block_by_date("trx", _date(1000))
    assert r.before_block == 1
    assert r.before_timestamp == 1000
    assert r.after_block == 2


@pytest.mark.asyncio
async def test_pre_genesis_date_with_block_zero_returns_empty():
    # Existing behavior on chains that do have block 0 stays unchanged.
    svc = _service_with_blocks(BTC_LIKE)
    r = await svc.get_block_by_date("btc", _date(500))
    assert r.before_block is None
    assert r.after_block is None


@pytest.mark.asyncio
async def test_date_between_blocks_with_block_zero_resolves():
    svc = _service_with_blocks(BTC_LIKE)
    r = await svc.get_block_by_date("btc", _date(3500))
    assert r.before_block == 2
    assert r.before_timestamp == 3000
    assert r.after_block == 3
    assert r.after_timestamp == 4000
