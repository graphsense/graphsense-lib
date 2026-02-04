"""Block-related API models."""

from typing import Optional

from graphsenselib.web.models.base import APIModel


class Block(APIModel):
    """Block model."""

    block_hash: str
    currency: str
    height: int
    no_txs: int
    timestamp: int


class BlockAtDate(APIModel):
    """Block at date model."""

    before_block: Optional[int] = None
    before_timestamp: Optional[int] = None
    after_block: Optional[int] = None
    after_timestamp: Optional[int] = None
