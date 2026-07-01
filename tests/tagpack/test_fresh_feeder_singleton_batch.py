"""All-singleton batches must still be mapped by the v2 cluster feeder.

Under GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS the feeder reads cluster membership
from ``fresh_address_cluster``, which stores only multi-member clusters. A batch
in which every address is a singleton therefore resolves to an *empty*
``get_cluster_ids`` result. The early ``return DataFrame()`` then dropped the
whole batch — but singletons are the majority under fresh clustering, so those
addresses must instead be mapped to themselves (cluster_id == address_id).

DB-free: the real ``GraphSense.get_address_clusters`` is bound to a fake self;
the DB-touching sub-methods are stubbed to return canned frames (an empty,
column-less frame mirrors ``_execute_query`` on no rows).
"""

from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.graphsense import GraphSense

_ENV = "GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS"


def _fake_gs(address_ids_df, cluster_ids_df, clusters_df, definers_df):
    ns = SimpleNamespace(
        _check_passed_params=lambda df, network, col: None,
        get_address_ids=lambda addresses, network: address_ids_df,
        get_cluster_ids=lambda df, network: cluster_ids_df,
        get_clusters=lambda df, network: clusters_df,
        _get_cluster_definers=lambda df, network: definers_df,
    )
    ns._as_singleton_clusters = lambda df: GraphSense._as_singleton_clusters(ns, df)
    return ns


def test_all_singleton_batch_maps_to_self(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    address_ids = pd.DataFrame({"address_id": [10, 20], "address": ["A", "B"]})
    empty = pd.DataFrame()  # column-less, like _execute_query on no rows
    gs = _fake_gs(address_ids, empty, empty, empty)

    result = GraphSense.get_address_clusters(
        gs, pd.DataFrame({"address": ["A", "B"]}), "BTC"
    )

    assert not result.empty
    assert sorted(result["cluster_id"]) == [10, 20]
    assert (result["no_addresses"] == 1).all()
    by_addr = result.set_index("address")
    assert by_addr.loc["A", "cluster_defining_address"] == "A"
    assert by_addr.loc["B", "cluster_defining_address"] == "B"


def test_all_singleton_batch_dropped_when_switch_off(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    address_ids = pd.DataFrame({"address_id": [10], "address": ["A"]})
    empty = pd.DataFrame()
    gs = _fake_gs(address_ids, empty, empty, empty)

    result = GraphSense.get_address_clusters(
        gs, pd.DataFrame({"address": ["A"]}), "BTC"
    )
    # legacy: every address has a cluster, so an empty result means "not found"
    assert result.empty
