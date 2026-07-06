"""Fresh-clustering singleton entity address listing.

Fresh clustering persists only multi-member clusters, so a singleton entity
(``cluster_id == address_id``) has no ``fresh_cluster_addresses`` rows.
``list_entity_addresses`` must fall back to serving the one address itself —
legacy ``cluster_addresses`` had a membership row even for singletons, so
without the fallback the endpoint returns an empty list where legacy returned
the address.

DB-free: the real ``Cassandra.list_entity_addresses`` is bound to a fake self;
``finish_addresses`` is stubbed to a pass-through so the tests assert the rows
that would be finished.
"""

import asyncio
from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import Cassandra

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_CURRENCIES"


class _MembershipResult:
    def __init__(self, rows):
        self.current_rows = rows
        self.paging_state = None


def _make_self(membership_rows, address_rows_by_id):
    calls = {"concurrent_params": None}

    async def execute_async(
        currency, keyspace, query, params, paging_state=None, fetch_size=None
    ):
        return _MembershipResult(membership_rows)

    async def concurrent_with_args(currency, keyspace, query, params):
        calls["concurrent_params"] = list(params)
        return [
            address_rows_by_id[address_id]
            for _, address_id in params
            if address_id in address_rows_by_id
        ]

    async def finish_addresses(currency, rows):
        return list(rows)

    ns = SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        concurrent_with_args=concurrent_with_args,
        finish_addresses=finish_addresses,
        _calls=calls,
    )
    return ns


def _list(s, entity, page=None):
    return asyncio.run(Cassandra.list_entity_addresses(s, "ltc", entity, page=page))


def test_singleton_serves_own_address_when_fresh(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    s = _make_self(membership_rows=[], address_rows_by_id={99: {"address_id": 99}})
    addresses, paging = _list(s, 99)
    assert addresses == [{"address_id": 99}]
    assert s._calls["concurrent_params"] == [(0, 99)]
    assert paging is None


def test_unknown_id_stays_empty_when_fresh(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    s = _make_self(membership_rows=[], address_rows_by_id={})
    addresses, _ = _list(s, 12345)
    assert addresses == []


def test_multi_member_cluster_uses_membership_rows(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    s = _make_self(
        membership_rows=[{"address_id": 5}, {"address_id": 8}],
        address_rows_by_id={5: {"address_id": 5}, 8: {"address_id": 8}},
    )
    addresses, _ = _list(s, 5)
    assert addresses == [{"address_id": 5}, {"address_id": 8}]
    assert s._calls["concurrent_params"] == [(0, 5), (0, 8)]


def test_no_fallback_when_fresh_disabled(monkeypatch):
    monkeypatch.setenv(_ENV, "")
    s = _make_self(membership_rows=[], address_rows_by_id={99: {"address_id": 99}})
    addresses, _ = _list(s, 99)
    assert addresses == []


def test_no_fallback_on_continuation_page(monkeypatch):
    monkeypatch.setenv(_ENV, "ltc")
    s = _make_self(membership_rows=[], address_rows_by_id={99: {"address_id": 99}})
    addresses, _ = _list(s, 99, page="00ff")
    assert addresses == []
