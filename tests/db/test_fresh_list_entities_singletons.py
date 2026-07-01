"""Bulk entity listing must honor fresh-clustering singletons.

``list_entities`` resolves a list of cluster ids to entity rows. Legacy stored
every cluster (incl. singletons) in the ``cluster`` table, so a bulk lookup
returned one row per requested id. Fresh clustering persists only multi-member
clusters, so singleton ids have no ``fresh_cluster_stats`` row and
``concurrent_with_args`` silently drops them — the bulk response would then be
missing entities that legacy returned.

With ``GRAPHSENSE_FRESH_CLUSTERING_ENABLED`` on, ``list_entities`` must
synthesize the one-address entity for each singleton id (mirroring
``get_entity``), preserving input order and dropping only genuinely unknown ids.

DB-free: the real ``Cassandra.list_entities`` is bound to a fake self standing
in for its db dependencies.
"""

import asyncio
from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import Cassandra

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_ENABLED"


def _addr_row(address_id):
    return {
        "address_id": address_id,
        "no_incoming_txs": 1,
        "no_outgoing_txs": 1,
        "in_degree": 1,
        "out_degree": 1,
        "first_tx_id": 1,
        "last_tx_id": 2,
        "total_received": {"value": 10},
        "total_spent": {"value": 5},
    }


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(multi_member_ids, known_address_ids):
    """multi_member_ids -> have fresh_cluster_stats rows; known_address_ids ->
    have an `address` row (so singletons can be synthesized)."""

    async def concurrent_with_args(currency, keyspace, query, params):
        rows = []
        for _group, cid in params:
            if cid in multi_member_ids:
                rows.append({"cluster_id": cid, "no_addresses": 9})
        return rows

    async def execute_async(currency, keyspace, query, params):
        # Only the address fallback query reaches here (singleton synthesis).
        _group, cid = params
        if cid in known_address_ids:
            return _Result(_addr_row(cid))
        return _Result(None)

    async def finish_entities(currency, rows, with_txs=True):
        return list(rows)

    ns = SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        concurrent_with_args=concurrent_with_args,
        execute_async=execute_async,
        finish_entities=finish_entities,
    )
    ns._fresh_singleton_entity = lambda currency, cid: (
        Cassandra._fresh_singleton_entity(ns, currency, cid)
    )
    ns._fresh_fill_singleton_entities = lambda currency, ids, rows: (
        Cassandra._fresh_fill_singleton_entities(ns, currency, ids, rows)
    )
    return ns


def test_fresh_fills_singletons_in_input_order(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    # 7 is a real multi-member cluster; 3 and 5 are singletons (address rows
    # exist); 99 is genuinely unknown (no cluster, no address).
    s = _make_self(multi_member_ids={7}, known_address_ids={3, 5})
    entities, _page = asyncio.run(Cassandra.list_entities(s, "ltc", [3, 7, 5, 99]))
    ids = [e["cluster_id"] for e in entities]
    # input order preserved; unknown 99 dropped (as legacy dropped absent ids)
    assert ids == [3, 7, 5]
    singleton = next(e for e in entities if e["cluster_id"] == 3)
    assert singleton["no_addresses"] == 1
    multi = next(e for e in entities if e["cluster_id"] == 7)
    assert multi["no_addresses"] == 9


def test_disabled_drops_singletons_like_legacy(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    s = _make_self(multi_member_ids={7}, known_address_ids={3, 5})
    entities, _page = asyncio.run(Cassandra.list_entities(s, "ltc", [3, 7, 5]))
    # switch off: no synthesis, only the stored cluster row comes back
    assert [e["cluster_id"] for e in entities] == [7]
