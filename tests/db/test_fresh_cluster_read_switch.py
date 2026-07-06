"""Self-describing entity ids route between legacy and fresh tables.

There is no REST-side switch: fresh cluster ids are published shifted by
``FRESH_CLUSTER_ID_OFFSET``, so any entity id declares its own source —
``id >= offset`` is served from the fresh tables (keyed by ``id - offset``),
anything below from the legacy ``cluster`` table. Both id spaces work on the
same keyspace at the same time.

These assert the id-space helpers and the table routing only — no live
Cassandra.
"""

import asyncio
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.errors.errors import ClusterNotFoundException
from graphsenselib.utils.constants import (
    FRESH_CLUSTER_ID_OFFSET,
    is_fresh_cluster_id,
    to_public_fresh_cluster_id,
    to_raw_fresh_cluster_id,
)


def test_id_space_helpers_roundtrip():
    assert not is_fresh_cluster_id(FRESH_CLUSTER_ID_OFFSET - 1)
    assert is_fresh_cluster_id(FRESH_CLUSTER_ID_OFFSET)
    assert to_public_fresh_cluster_id(99) == FRESH_CLUSTER_ID_OFFSET + 99
    assert to_raw_fresh_cluster_id(to_public_fresh_cluster_id(99)) == 99


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(row, queries):
    async def execute_async(currency, keyspace, query, params):
        queries.append((query, list(params)))
        return _Result(dict(row) if row else None)

    async def finish_entities(currency, rows, with_txs=True):
        return list(rows)

    ns = SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        finish_entities=finish_entities,
    )
    ns._get_fresh_entity = Cassandra._get_fresh_entity.__get__(ns)
    return ns


def test_legacy_id_reads_cluster_table():
    queries = []
    s = _make_self({"cluster_id": 7, "no_addresses": 5}, queries)
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 7))
    assert "FROM cluster " in queries[0][0]
    assert queries[0][1] == [0, 7]
    assert entity["cluster_id"] == 7


def test_fresh_id_reads_fresh_table_with_raw_id():
    queries = []
    s = _make_self({"cluster_id": 7, "no_addresses": 5}, queries)
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", to_public_fresh_cluster_id(7)))
    assert "FROM fresh_cluster_stats " in queries[0][0]
    assert queries[0][1] == [0, 7]
    # the served entity carries the public (shifted) id again
    assert entity["cluster_id"] == to_public_fresh_cluster_id(7)


def test_legacy_id_misses_without_fresh_synthesis():
    # a legacy id absent from the cluster table is simply not found — no
    # singleton synthesis happens in the legacy id space
    s = _make_self(None, [])
    with pytest.raises(ClusterNotFoundException):
        asyncio.run(Cassandra.get_entity(s, "ltc", 7))
