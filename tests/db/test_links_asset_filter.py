"""list_links restricts the candidate scan to the requested assets.

DB-free, on the harness of test_links_side_selection: the candidate fetch
raises as soon as it is called, carrying the assets it was asked for.
"""

import asyncio

import pytest

from graphsenselib.datatypes.common import NodeType
from graphsenselib.db.asynchronous.cassandra import Cassandra

from .test_links_side_selection import SMALL, USDT, _make_self, _node


class _StopAtFetch(Exception):
    def __init__(self, include_assets):
        self.include_assets = include_assets


def _scanned_assets(token_currency):
    nodes = {SMALL: _node(3168, 5359), USDT: _node(0, 97_956_203)}
    s = _make_self(nodes)

    async def list_address_txs_ordered(**kwargs):
        raise _StopAtFetch(kwargs["include_assets"])

    s.list_address_txs_ordered = list_address_txs_ordered
    with pytest.raises(_StopAtFetch) as e:
        asyncio.run(
            Cassandra.list_links(
                s,
                "trx",
                NodeType.ADDRESS,
                SMALL,
                USDT,
                order="asc",
                token_currency=token_currency,
                pagesize=25,
            )
        )
    return e.value.include_assets


def test_no_filter_scans_used_tokens_and_native():
    assert _scanned_assets(None) == ["USDT", "TRX"]
    assert _scanned_assets("") == ["USDT", "TRX"]


def test_single_asset_filter():
    assert _scanned_assets("usdt") == ["USDT"]


def test_several_assets_filter():
    assert _scanned_assets("usdt, TRX,usdt") == ["USDT", "TRX"]
