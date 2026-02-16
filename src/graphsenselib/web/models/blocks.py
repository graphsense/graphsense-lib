"""Block-related API models."""

from typing import Optional

from graphsenselib.web.models.base import APIModel, api_model_config

BLOCK_EXAMPLE = {
    "block_hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
    "currency": "btc",
    "height": 47,
    "no_txs": 11,
    "timestamp": 1231614698,
}

BLOCK_AT_DATE_EXAMPLE = {
    "before_block": 100,
    "before_timestamp": 1231614698,
    "after_block": 101,
    "after_timestamp": 1231614700,
}


class Block(APIModel):
    """Block model."""

    model_config = api_model_config(BLOCK_EXAMPLE)

    block_hash: str
    currency: str
    height: int
    no_txs: int
    timestamp: int


class BlockAtDate(APIModel):
    """Block at date model."""

    model_config = api_model_config(BLOCK_AT_DATE_EXAMPLE)

    before_block: Optional[int] = None
    before_timestamp: Optional[int] = None
    after_block: Optional[int] = None
    after_timestamp: Optional[int] = None
