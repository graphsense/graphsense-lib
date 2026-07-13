"""Contract of ``get_fresh_cluster_id``.

Fresh clustering stores only multi-member clusters, so the lookup applies the
absent->self singleton convention itself: a row resolves to its cluster id, a
missing row resolves to the address id (singleton) — both published shifted
into the public id space (``+ FRESH_CLUSTER_ID_OFFSET``) so they route back to
the fresh tables when handed to the entity endpoints. ``None`` is reserved for
"fresh clustering not active" (bootstrap marker absent, or tables missing),
so callers like the address response can tell singletons apart from missing
data.

DB-free: the real ``Cassandra.get_fresh_cluster_id`` is bound to a fake self.
"""

import asyncio
from types import SimpleNamespace

from cassandra import InvalidRequest

from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET as _OFF


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(row=None, raise_invalid=False, active=True):
    async def execute_async(currency, keyspace, query, params):
        if raise_invalid:
            raise InvalidRequest("unconfigured table fresh_address_cluster")
        return _Result(row)

    async def _fresh_clustering_active(currency):
        return active

    return SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        _fresh_clustering_active=_fresh_clustering_active,
    )


def _lookup(s, address_id):
    return asyncio.run(Cassandra.get_fresh_cluster_id(s, "ltc", address_id))


def test_row_resolves_to_its_cluster_id():
    s = _make_self(row={"cluster_id": 1353379})
    assert _lookup(s, 99) == _OFF + 1353379


def test_missing_row_is_a_singleton_resolving_to_address_id():
    s = _make_self(row=None)
    assert _lookup(s, 99) == _OFF + 99


def test_missing_table_returns_none():
    s = _make_self(raise_invalid=True)
    assert _lookup(s, 99) is None


def test_inactive_keyspace_returns_none():
    # bootstrap marker absent: no fresh id claims, even with tables present
    s = _make_self(row={"cluster_id": 1353379}, active=False)
    assert _lookup(s, 99) is None
