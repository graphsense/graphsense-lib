"""All-singleton batches must still be mapped by the fresh cluster feeder.

In fresh mode (``get_address_clusters(..., fresh=True)``) the feeder reads
cluster membership from ``fresh_address_cluster``, which stores only
multi-member clusters. A batch in which every address is a singleton therefore
resolves to an *empty* ``get_cluster_ids`` result. The early ``return
DataFrame()`` then dropped the whole batch — but singletons are the majority
under fresh clustering, so those addresses must instead be mapped to
themselves (cluster_id == address_id). In legacy mode an empty result
genuinely means "not found" and the batch is dropped as before.

DB-free: the real ``GraphSense.get_address_clusters`` is bound to a fake self;
the DB-touching sub-methods are stubbed to return canned frames (an empty,
column-less frame mirrors ``_execute_query`` on no rows).
"""

from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.graphsense import GraphSense


def _fake_gs(address_ids_df, cluster_ids_df, clusters_df, definers_df):
    ns = SimpleNamespace(
        _check_passed_params=lambda df, network, col: None,
        get_address_ids=lambda addresses, network: address_ids_df,
        get_cluster_ids=lambda df, network, fresh=False: cluster_ids_df,
        get_clusters=lambda df, network, fresh=False: clusters_df,
        _get_cluster_definers=lambda df, network: definers_df,
    )
    ns._as_singleton_clusters = lambda df: GraphSense._as_singleton_clusters(ns, df)
    return ns


def test_all_singleton_batch_maps_to_self():
    address_ids = pd.DataFrame({"address_id": [10, 20], "address": ["A", "B"]})
    empty = pd.DataFrame()  # column-less, like _execute_query on no rows
    gs = _fake_gs(address_ids, empty, empty, empty)

    result = GraphSense.get_address_clusters(
        gs, pd.DataFrame({"address": ["A", "B"]}), "BTC", fresh=True
    )

    assert not result.empty
    assert sorted(result["cluster_id"]) == [10, 20]
    assert all(result["no_addresses"] == 1)
    assert sorted(result["cluster_defining_address"]) == ["A", "B"]


def test_legacy_empty_result_still_drops_batch():
    address_ids = pd.DataFrame({"address_id": [10], "address": ["A"]})
    empty = pd.DataFrame()
    gs = _fake_gs(address_ids, empty, empty, empty)

    result = GraphSense.get_address_clusters(
        gs, pd.DataFrame({"address": ["A"]}), "BTC", fresh=False
    )

    assert result.empty
