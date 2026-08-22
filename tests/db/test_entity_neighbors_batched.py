"""Batched cluster-row read for the /entities/{id}/neighbors page.

list_entity_neighbors used to resolve every neighbor with its own independent
get_entity chain (cluster row + root-address point read + first/last tx
summaries), throttled by the unrelated tagstore concurrency semaphore.
get_entities_by_ids replaces that with grouped single-partition IN queries for
the whole page, and finish_entities now batches the root-address lookup the
same way it already batches tx summaries.

DB-free: the real Cassandra methods are bound to a fake self; finish_entities
is stubbed to a pass-through where the query-grouping behavior itself is
under test (mirrors test_fresh_entity_addresses_singleton.py), and left real
where the root-address batching is under test.
"""

import asyncio
from types import SimpleNamespace

import pytest

from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.errors import ClusterNotFoundException


class _Result:
    def __init__(self, rows):
        self.current_rows = rows


def _make_cluster_self(cluster_rows_by_id, bucket_size=10):
    calls = {"cluster_query_params": []}

    async def execute_async(currency, keyspace, query, params, **kwargs):
        assert keyspace == "transformed"
        assert query.startswith("SELECT * FROM cluster")
        group, ids = params
        calls["cluster_query_params"].append((group, list(ids)))
        return _Result([cluster_rows_by_id[i] for i in ids if i in cluster_rows_by_id])

    async def finish_entities(currency, rows, with_txs=True):
        return list(rows)

    s = SimpleNamespace(
        get_id_group=lambda keyspace, id_: id_ // bucket_size,
        execute_async=execute_async,
        finish_entities=finish_entities,
        _calls=calls,
    )
    # get_entities_by_ids calls the real (unbound) _get_cluster_rows_by_ids,
    # which in turn drives the mocked execute_async above.
    s._get_cluster_rows_by_ids = lambda currency, cluster_ids: (
        Cassandra._get_cluster_rows_by_ids(s, currency, cluster_ids)
    )
    return s


def _get_entities(s, ids, currency="btc"):
    return asyncio.run(Cassandra.get_entities_by_ids(s, currency, ids))


def test_single_group_uses_one_grouped_in_query():
    s = _make_cluster_self({5: {"cluster_id": 5}, 8: {"cluster_id": 8}})
    result = _get_entities(s, [5, 8])
    assert result == {5: {"cluster_id": 5}, 8: {"cluster_id": 8}}
    # One round trip for the whole page: both ids share a group.
    assert s._calls["cluster_query_params"] == [(0, [5, 8])]


def test_ids_across_groups_fan_out_by_group_not_by_id():
    s = _make_cluster_self({5: {"cluster_id": 5}, 15: {"cluster_id": 15}})
    result = _get_entities(s, [5, 15])
    assert result == {5: {"cluster_id": 5}, 15: {"cluster_id": 15}}
    # Two groups -> two round trips: O(number of id-groups), not O(neighbors).
    assert sorted(s._calls["cluster_query_params"]) == [(0, [5]), (1, [15])]


def test_duplicate_neighbor_ids_only_fetched_once():
    s = _make_cluster_self({5: {"cluster_id": 5}})
    result = _get_entities(s, [5, 5, 5])
    assert result == {5: {"cluster_id": 5}}
    assert s._calls["cluster_query_params"] == [(0, [5])]


def test_missing_cluster_row_raises_cluster_not_found():
    s = _make_cluster_self({5: {"cluster_id": 5}})
    with pytest.raises(ClusterNotFoundException):
        _get_entities(s, [5, 999])


def test_empty_input_short_circuits_without_a_query():
    s = _make_cluster_self({})
    result = _get_entities(s, [])
    assert result == {}
    assert s._calls["cluster_query_params"] == []


def _make_eth_self(entity_rows_by_id):
    # get_entities_by_ids_eth is not itself @eth-decorated (it's a dispatch
    # target), so it's exercised directly here instead of through
    # get_entities_by_ids's currency-based dispatch. It calls get_entity_eth
    # per id, so that's the seam to mock.
    calls = {"get_entity_eth": []}

    async def get_entity_eth(currency, entity_id):
        calls["get_entity_eth"].append(entity_id)
        return entity_rows_by_id.get(entity_id)

    return SimpleNamespace(get_entity_eth=get_entity_eth, _calls=calls)


def _get_entities_eth(s, ids):
    return asyncio.run(Cassandra.get_entities_by_ids_eth(s, "eth", ids))


def test_eth_branch_dedups_repeated_neighbor_ids():
    s = _make_eth_self(
        {5: {"address_id": 5, "address": "a"}, 8: {"address_id": 8, "address": "b"}}
    )
    result = _get_entities_eth(s, [5, 8, 5])
    assert set(result) == {5, 8}
    # Deduplicated: id 5 only fetched once despite appearing twice on the page.
    assert sorted(s._calls["get_entity_eth"]) == [5, 8]


def test_eth_branch_raises_cluster_not_found_for_missing_id():
    s = _make_eth_self({5: {"address_id": 5, "address": "a"}})
    with pytest.raises(ClusterNotFoundException):
        _get_entities_eth(s, [5, 999])


def _make_root_address_self(address_rows_by_id, bucket_size=10):
    calls = {"address_query_params": []}

    async def execute_async(currency, keyspace, query, params, **kwargs):
        assert keyspace == "transformed"
        assert query.startswith("SELECT address, address_id FROM address")
        group, ids = params
        calls["address_query_params"].append((group, list(ids)))
        return _Result([address_rows_by_id[i] for i in ids if i in address_rows_by_id])

    return SimpleNamespace(
        get_id_group=lambda keyspace, id_: id_ // bucket_size,
        execute_async=execute_async,
        _calls=calls,
    )


def test_get_root_addresses_by_ids_groups_and_chunks():
    s = _make_root_address_self(
        {
            5: {"address_id": 5, "address": "root5"},
            15: {"address_id": 15, "address": "root15"},
        }
    )
    result = asyncio.run(Cassandra._get_root_addresses_by_ids(s, "btc", [5, 15]))
    assert result == {5: "root5", 15: "root15"}
    assert sorted(s._calls["address_query_params"]) == [(0, [5]), (1, [15])]


def test_finish_entity_uses_prefetched_root_address_without_a_point_read():
    calls = {"get_addresses_by_ids": 0}

    async def get_addresses_by_ids(currency, ids, address_only=False):
        calls["get_addresses_by_ids"] += 1
        return [{"address": "should-not-be-used"}]

    async def finish_address(currency, row, with_txs=True, tx_summaries=None):
        return row

    s = SimpleNamespace(
        get_addresses_by_ids=get_addresses_by_ids,
        finish_address=finish_address,
    )
    row = {"cluster_id": 5}
    result = asyncio.run(
        Cassandra.finish_entity(
            s, "btc", row, root_addresses={5: "root5"}, tx_summaries=None
        )
    )
    assert result["root_address"] == "root5"
    assert calls["get_addresses_by_ids"] == 0
