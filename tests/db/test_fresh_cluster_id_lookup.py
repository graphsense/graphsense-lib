"""Contract of ``get_fresh_cluster_id``.

Fresh clustering stores only multi-member clusters, so the lookup applies the
absent->self singleton convention itself: a row resolves to its cluster id, a
missing row resolves to the address id (singleton). ``None`` is reserved for
"fresh tables unavailable" (clustering never ran on the keyspace), so callers
like the address response can tell singletons apart from missing data.

DB-free: the real ``Cassandra.get_fresh_cluster_id`` is bound to a fake self.
"""

import asyncio
from types import SimpleNamespace

from cassandra import InvalidRequest

from graphsenselib.db.asynchronous.cassandra import Cassandra


class _Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _make_self(row=None, raise_invalid=False):
    async def execute_async(currency, keyspace, query, params):
        if raise_invalid:
            raise InvalidRequest("unconfigured table fresh_address_cluster")
        return _Result(row)

    return SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
    )


def _lookup(s, address_id):
    return asyncio.run(Cassandra.get_fresh_cluster_id(s, "ltc", address_id))


def test_row_resolves_to_its_cluster_id():
    s = _make_self(row={"cluster_id": 1353379})
    assert _lookup(s, 99) == 1353379


def test_missing_row_is_a_singleton_resolving_to_address_id():
    s = _make_self(row=None)
    assert _lookup(s, 99) == 99


def test_missing_table_returns_none():
    s = _make_self(raise_invalid=True)
    assert _lookup(s, 99) is None
