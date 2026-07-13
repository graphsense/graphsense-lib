"""Marker-gated dual-write of the tagpack cluster-mapping feeder.

There is no configuration switch: each network's regime comes from the
``fresh_clustering_active`` marker in its transformed keyspace. Unmarked
networks map legacy clusters into ``address_cluster_mapping`` exactly as
before; marked networks are additionally mapped against the fresh membership
into ``address_cluster_mapping_v2``, so legacy-id readers (v1) and fresh-id
readers (v2) both stay current until the legacy tables are retired.

DB-free: workers run against monkeypatched TagStore/GraphSense, the marker
check binds the real method to a fake self.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from cassandra import InvalidRequest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack import cli as tp_cli
from graphsenselib.tagpack import graphsense as gs_mod
from graphsenselib.tagpack import tagstore as ts_mod
from graphsenselib.tagpack.graphsense import GraphSense


def test_queries_and_tables_select_by_regime():
    assert "FROM address " in gs_mod._cluster_id_query(False)
    assert "fresh_address_cluster" in gs_mod._cluster_id_query(True)
    assert "FROM cluster " in gs_mod._cluster_stats_query(False)
    assert "fresh_cluster_stats" in gs_mod._cluster_stats_query(True)
    # both regimes use the same (group, id) lookup key — only the table moves
    assert "address_id_group=? and address_id=?" in gs_mod._cluster_id_query(True)
    assert "cluster_id_group=? and cluster_id=?" in gs_mod._cluster_stats_query(True)

    assert ts_mod._acm_table(False) == "address_cluster_mapping"
    assert ts_mod._acm_table(True) == "address_cluster_mapping_v2"
    # both MV pairs are refreshed — with per-network migration there is no
    # single active pair
    assert set(ts_mod._CLUSTER_MVS) == {
        "tag_count_by_cluster",
        "best_cluster_tag",
        "tag_count_by_cluster_v2",
        "best_cluster_tag_v2",
    }


class _RecordingTagStore:
    def __init__(self, *a, **kw):
        self.inserts = []

    def insert_cluster_mappings(self, clusters, fresh=False):
        self.inserts.append((clusters, fresh))


class _RegimeGraphSense:
    def __init__(self, *a, **kw):
        self.calls = []

    def keyspace_for_network_exists(self, network):
        return True

    def get_address_clusters(self, batch, network, fresh=False):
        self.calls.append(fresh)
        return pd.DataFrame(
            {
                "address": ["A"],
                "address_id": [10],
                "cluster_id": [99 if fresh else 7],
                "no_addresses": [1],
                "cluster_defining_address": ["A"],
            }
        )


def _run_wp(fresh):
    args = SimpleNamespace(
        url="postgresql://x",
        schema="tagstore",
        db_nodes=["x"],
        cassandra_username=None,
        cassandra_password=None,
    )
    store = _RecordingTagStore()
    gs = _RegimeGraphSense()
    with (
        patch("graphsenselib.tagpack.cli.TagStore", return_value=store),
        patch("graphsenselib.tagpack.cli.GraphSense", return_value=gs),
    ):
        tp_cli.insert_cluster_mapping_wp(
            "LTC", {}, args, pd.DataFrame({"address": ["A"]}), fresh
        )
    return store, gs


def test_worker_dual_writes_marked_network():
    store, gs = _run_wp(fresh=True)
    assert gs.calls == [False, True]
    assert [(f, c["cluster_id"].iloc[0]) for c, f in store.inserts] == [
        (False, 7),
        (True, 99),
    ]


def test_worker_single_writes_unmarked_network():
    store, gs = _run_wp(fresh=False)
    assert gs.calls == [False]
    assert [f for _, f in store.inserts] == [False]


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _marker_ns(row=None, raise_invalid=False):
    executed = []

    def execute(stmt, params=None):
        executed.append(stmt)
        if raise_invalid:
            raise InvalidRequest("unconfigured table state")
        return _FakeResult(row)

    ns = SimpleNamespace(
        session=SimpleNamespace(execute=execute),
        ks_map={"LTC": {"transformed": "ltc_transformed"}},
        _fresh_network_cache={},
        _executed=executed,
    )
    ns.contains_keyspace_mapping = lambda n: n in ns.ks_map
    ns.is_fresh_network = GraphSense.is_fresh_network.__get__(ns)
    return ns


def test_marker_present_means_fresh():
    ns = _marker_ns(row={"key": "fresh_clustering_active"})
    assert ns.is_fresh_network("LTC") is True
    assert "ltc_transformed.state" in ns._executed[0]


def test_no_marker_row_means_legacy():
    assert _marker_ns(row=None).is_fresh_network("LTC") is False


def test_missing_state_table_means_legacy():
    assert _marker_ns(raise_invalid=True).is_fresh_network("LTC") is False


def test_eth_like_and_unmapped_networks_never_fresh():
    ns = _marker_ns(row={"key": "fresh_clustering_active"})
    assert ns.is_fresh_network("ETH") is False
    assert ns.is_fresh_network("BTC") is False
    assert ns._executed == []


def test_marker_check_is_cached_per_network():
    ns = _marker_ns(row={"key": "fresh_clustering_active"})
    assert ns.is_fresh_network("LTC") is True
    assert ns.is_fresh_network("LTC") is True
    assert len(ns._executed) == 1
