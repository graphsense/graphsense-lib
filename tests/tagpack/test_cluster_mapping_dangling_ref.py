"""A resolved cluster_id whose stats row is absent must not crash the batch.

``get_address_clusters`` reads address -> cluster_id (from ``address`` /
``fresh_address_cluster``) and then cluster_id -> no_addresses (from ``cluster``
/ ``fresh_cluster_stats``). Those two reads can disagree: an address can carry a
cluster_id whose stats row is missing (a dangling reference — e.g. a tip cluster
whose stats write lagged, or a not-yet-written ``fresh_cluster_stats`` row). The
left-merge then leaves ``no_addresses`` NaN. A single such row anywhere in a
multiprocess batch used to abort the whole run on ``.astype(int)``
(IntCastingNaNError). It must instead keep the real cluster id and store the
unknown size as NULL.

DB-free: the real ``get_address_clusters`` is bound to a fake self whose four
sub-reads return controlled frames.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.graphsense import GraphSense


def _dangling_self():
    """Fake GraphSense: address B resolves to cluster 8, which has no stats row."""

    def get_address_ids(addresses, network):
        return pd.DataFrame({"address": ["A", "B"], "address_id": [10, 20]})

    def get_cluster_ids(df, network, fresh=False):
        # both addresses carry a cluster_id ...
        return pd.DataFrame({"address_id": [10, 20], "cluster_id": [7, 8]})

    def get_clusters(df, network, fresh=False):
        # ... but only cluster 7 has a stats row; 8 is dangling
        return pd.DataFrame({"cluster_id": [7], "no_addresses": [3]})

    def _get_cluster_definers(df, network):
        # the definer read hits the address table, independent of cluster stats,
        # so it can resolve even for the dangling cluster
        return pd.DataFrame(
            {"cluster_id": [7, 8], "cluster_defining_address": ["A", "B"]}
        )

    ns = SimpleNamespace(
        get_address_ids=get_address_ids,
        get_cluster_ids=get_cluster_ids,
        get_clusters=get_clusters,
        _get_cluster_definers=_get_cluster_definers,
        _check_passed_params=lambda df, network, col: None,
    )
    ns.get_address_clusters = GraphSense.get_address_clusters.__get__(ns)
    return ns


def test_dangling_cluster_ref_maps_with_null_size():
    ns = _dangling_self()
    df = pd.DataFrame({"address": ["A", "B"]})

    # must not raise IntCastingNaNError
    result = ns.get_address_clusters(df, "LTC", fresh=False)

    by_addr = {r.address_id: r for r in result.itertuples(index=False)}

    # the healthy row is untouched
    assert by_addr[10].cluster_id == 7
    assert by_addr[10].no_addresses == 3

    # the dangling row keeps its real cluster id, size stored as NULL (not 1,
    # which would misroute the cluster into the singleton view branch)
    assert by_addr[20].cluster_id == 8
    assert by_addr[20].no_addresses is None
