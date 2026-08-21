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
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET as _OFF


class _MembershipResult:
    def __init__(self, rows):
        self.current_rows = rows
        self.paging_state = None


class _AddressResult:
    def __init__(self, rows):
        self.current_rows = rows


def _make_self(membership_rows, address_rows_by_id):
    calls = {"address_query_params": []}

    async def execute_async(
        currency, keyspace, query, params, paging_state=None, fetch_size=None
    ):
        if query.startswith("SELECT address_id FROM"):
            return _MembershipResult(membership_rows)
        # Grouped address-row IN query: params = [group, ids].
        group, ids = params
        calls["address_query_params"].append((group, list(ids)))
        return _AddressResult(
            [address_rows_by_id[aid] for aid in ids if aid in address_rows_by_id]
        )

    async def finish_addresses(currency, rows):
        return list(rows)

    ns = SimpleNamespace(
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        finish_addresses=finish_addresses,
        _calls=calls,
    )
    return ns


def _list(s, entity, page=None):
    return asyncio.run(Cassandra.list_entity_addresses(s, "ltc", entity, page=page))


def test_singleton_serves_own_address_when_fresh():
    s = _make_self(membership_rows=[], address_rows_by_id={99: {"address_id": 99}})
    addresses, paging = _list(s, _OFF + 99)
    assert addresses == [{"address_id": 99}]
    assert s._calls["address_query_params"] == [(0, [99])]
    assert paging is None


def test_unknown_id_stays_empty_when_fresh():
    s = _make_self(membership_rows=[], address_rows_by_id={})
    addresses, _ = _list(s, _OFF + 12345)
    assert addresses == []


def test_multi_member_cluster_uses_membership_rows():
    s = _make_self(
        membership_rows=[{"address_id": 5}, {"address_id": 8}],
        address_rows_by_id={5: {"address_id": 5}, 8: {"address_id": 8}},
    )
    addresses, _ = _list(s, _OFF + 5)
    assert addresses == [{"address_id": 5}, {"address_id": 8}]
    assert s._calls["address_query_params"] == [(0, [5, 8])]


def test_no_fallback_for_legacy_ids():
    # legacy id space: cluster_addresses always stored singleton rows, so an
    # empty membership genuinely means "unknown id" — no synthesis
    s = _make_self(membership_rows=[], address_rows_by_id={99: {"address_id": 99}})
    addresses, _ = _list(s, 99)
    assert addresses == []


def test_no_fallback_on_continuation_page():
    s = _make_self(membership_rows=[], address_rows_by_id={99: {"address_id": 99}})
    addresses, _ = _list(s, _OFF + 99, page="00ff")
    assert addresses == []
