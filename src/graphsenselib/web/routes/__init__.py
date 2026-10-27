"""FastAPI routers for gsrest API endpoints"""

from graphsenselib.web.routes import (
    addresses,
    blocks,
    bulk,
    general,
    rates,
    tags,
    tokens,
    txs,
)

__all__ = [
    "addresses",
    "blocks",
    "bulk",
    "general",
    "rates",
    "tags",
    "tokens",
    "txs",
]
