"""Fresh-clustering singleton entities.

Fresh clustering persists only multi-member clusters, so a cluster id absent
from ``fresh_cluster_stats`` is a singleton (``cluster_id == address_id``, one
member). ``get_entity`` must synthesize that one-address entity from the
``address`` row instead of raising ``ClusterNotFoundException`` — otherwise the
REST 500s for every singleton when the fresh read switch is on.

DB-free: the real ``Cassandra.get_entity`` is bound to a fake self that stands
in for its db dependencies. ``finish_entities`` is stubbed to a pass-through so
the test asserts the row that would be finished.
"""

import asyncio
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.errors.errors import ClusterNotFoundException

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_CURRENCIES"

_ADDR_ROW = {
    "address_id": 99,
    "address": "Laddr",
    "no_incoming_txs": 3,
    "no_outgoing_txs": 4,
    "in_degree": 1,
    "out_degree": 2,
    "first_tx_id": 10,
    "last_tx_id": 20,
    "total_received": {"value": 500},
    "total_spent": {"value": 300},
}


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(cluster_stats_row, address_row):
    async def execute_async(currency, keyspace, query, params):
        # The address fallback query targets the `address` table; the cluster
        # stats query targets fresh_cluster_stats.
        if "FROM address " in query:
            return _Result(address_row)
        return _Result(cluster_stats_row)

    async def finish_entities(currency, rows, with_txs=True):
        return list(rows)

    ns = SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        finish_entities=finish_entities,
    )
    # Exercise the real singleton-synthesis method, bound to this fake self.
    ns._fresh_singleton_entity = lambda currency, cid: (
        Cassandra._fresh_singleton_entity(ns, currency, cid)
    )
    return ns


def test_get_entity_synthesizes_singleton_when_fresh(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    s = _make_self(cluster_stats_row=None, address_row=_ADDR_ROW)
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 99))
    assert entity["cluster_id"] == 99
    assert entity["no_addresses"] == 1
    assert entity["min_address_id"] == 99
    assert entity["total_received"] == {"value": 500}
    assert entity["total_spent"] == {"value": 300}
    assert entity["in_degree"] == 1
    assert entity["out_degree"] == 2
    assert entity["first_tx_id"] == 10
    assert entity["last_tx_id"] == 20


def test_get_entity_raises_when_no_cluster_and_no_address(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    s = _make_self(cluster_stats_row=None, address_row=None)
    with pytest.raises(ClusterNotFoundException):
        asyncio.run(Cassandra.get_entity(s, "ltc", 99))


def test_get_entity_uses_cluster_stats_for_multi_member(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    cluster_stats = {"cluster_id": 7, "no_addresses": 5}
    s = _make_self(cluster_stats_row=cluster_stats, address_row=_ADDR_ROW)
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 7))
    assert entity["no_addresses"] == 5  # served from stats, not synthesized
