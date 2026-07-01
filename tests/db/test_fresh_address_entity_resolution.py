"""Address->entity resolution must honor the fresh-clustering read switch.

``get_address_entity_id`` resolves an address to its entity (cluster) id, which
the REST then looks up as an entity. With ``GRAPHSENSE_FRESH_CLUSTERING_ENABLED``
on, entity stats are served from ``fresh_cluster_stats``; the address->entity
resolution must therefore also return the *fresh* cluster id, not the legacy
``address.cluster_id``. When the two disagree (fresh re-clustered the address
into a different min-id cluster) the legacy id is absent from
``fresh_cluster_stats`` and the entity lookup 500s.

DB-free: the real ``Cassandra.get_address_entity_id`` is bound to a fake self
that stands in for its db dependencies.
"""

import asyncio
from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import Cassandra

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_ENABLED"


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(fresh_cluster_id, legacy_cluster_id, address_id=42):
    async def get_address_id_id_group(currency, address):
        return address_id, 0

    async def get_fresh_cluster_id(currency, aid):
        return fresh_cluster_id

    async def execute_async(currency, keyspace, query, params):
        return _Result({"cluster_id": legacy_cluster_id})

    return SimpleNamespace(
        get_address_id_id_group=get_address_id_id_group,
        get_fresh_cluster_id=get_fresh_cluster_id,
        execute_async=execute_async,
        get_id_group=lambda keyspace, id_: 0,
    )


def test_fresh_enabled_multi_member_returns_fresh_cluster_id(monkeypatch):
    # Fresh re-clustered the address into cluster 1353379; legacy says 1396178.
    monkeypatch.setenv(_ENV, "true")
    s = _make_self(fresh_cluster_id=1353379, legacy_cluster_id=1396178)
    result = asyncio.run(Cassandra.get_address_entity_id(s, "ltc", "Laddr"))
    assert result == 1353379


def test_fresh_enabled_singleton_falls_back_to_address_id(monkeypatch):
    # Singletons aren't stored in fresh_address_cluster -> cluster id == address id.
    monkeypatch.setenv(_ENV, "true")
    s = _make_self(fresh_cluster_id=None, legacy_cluster_id=1396178, address_id=42)
    result = asyncio.run(Cassandra.get_address_entity_id(s, "ltc", "Laddr"))
    assert result == 42


def test_fresh_disabled_returns_legacy_cluster_id(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    s = _make_self(fresh_cluster_id=1353379, legacy_cluster_id=1396178)
    result = asyncio.run(Cassandra.get_address_entity_id(s, "ltc", "Laddr"))
    assert result == 1396178
