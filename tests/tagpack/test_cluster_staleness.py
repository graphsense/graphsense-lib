from unittest.mock import patch

import pandas as pd
import pytest

pytest.importorskip("yamlinclude", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.cli import check_cluster_mapping_staleness


class _FakeTagStore:
    def __init__(self, sample):
        self._sample = sample

    def get_cluster_mapping_sample(self, limit, networks=None):
        if networks is not None:
            return [r for r in self._sample if r[1] in networks][:limit]
        return self._sample[:limit]


class _FakeGS:
    def __init__(self, current_by_network, missing_keyspaces=None):
        self._current = current_by_network
        self._missing = set(missing_keyspaces or [])

    def keyspace_for_network_exists(self, network):
        return network not in self._missing

    def get_address_clusters(self, df, network):
        rows = self._current.get(network, [])
        return pd.DataFrame(rows, columns=["address", "cluster_id"])


def _patch(tagstore, gs, ks_mapping):
    return {
        "TagStore": patch("graphsenselib.tagpack.cli.TagStore", return_value=tagstore),
        "GraphSense": patch("graphsenselib.tagpack.cli.GraphSense", return_value=gs),
        "load_ks_mapping": patch(
            "graphsenselib.tagpack.cli.load_ks_mapping", return_value=ks_mapping
        ),
    }


def _run(tagstore, gs, ks_mapping, sample_size=10):
    patches = _patch(tagstore, gs, ks_mapping)
    with patches["TagStore"], patches["GraphSense"], patches["load_ks_mapping"]:
        return check_cluster_mapping_staleness(
            url="postgresql://x",
            schema="tagstore",
            db_nodes=["x"],
            cassandra_username=None,
            cassandra_password=None,
            ks_file=None,
            use_gs_lib_config_env="prod",
            sample_size=sample_size,
        )


def test_no_divergence():
    sample = [("a1", "BTC", 100), ("a2", "BTC", 200)]
    current = {"BTC": [("a1", 100), ("a2", 200)]}
    overall, per_net = _run(_FakeTagStore(sample), _FakeGS(current), {"BTC": {}})
    assert overall == 0.0
    assert per_net["BTC"]["diverged"] == 0
    assert per_net["BTC"]["checked"] == 2


def test_partial_divergence():
    sample = [
        ("a1", "BTC", 100),
        ("a2", "BTC", 200),
        ("a3", "BTC", 300),
        ("a4", "BTC", 400),
    ]
    current = {
        "BTC": [("a1", 100), ("a2", 999), ("a3", 300), ("a4", 888)],
    }
    overall, per_net = _run(_FakeTagStore(sample), _FakeGS(current), {"BTC": {}})
    assert overall == 0.5
    assert per_net["BTC"]["diverged"] == 2
    assert per_net["BTC"]["checked"] == 4
    assert per_net["BTC"]["rate"] == 0.5


def test_eth_like_skipped():
    # ETH/TRX have no real clustering — must be filtered out before sampling.
    sample = [("0xabc", "ETH", 1)]
    current = {"ETH": [("0xabc", 999)]}
    overall, per_net = _run(
        _FakeTagStore(sample), _FakeGS(current), {"ETH": {}, "TRX": {}}
    )
    assert overall == 0.0
    assert per_net == {}


def test_empty_sample():
    overall, per_net = _run(_FakeTagStore([]), _FakeGS({}), {"BTC": {}})
    assert overall == 0.0
    assert per_net == {}


def test_missing_keyspace_skipped():
    sample = [("a1", "BTC", 100), ("a2", "LTC", 200)]
    current = {"BTC": [("a1", 100)]}
    overall, per_net = _run(
        _FakeTagStore(sample),
        _FakeGS(current, missing_keyspaces=["LTC"]),
        {"BTC": {}, "LTC": {}},
    )
    assert overall == 0.0
    assert "LTC" not in per_net
    assert per_net["BTC"]["checked"] == 1


def test_address_not_found_in_graph():
    # Inner-merge drops rows the graph datastore doesn't know about — they
    # are not counted as diverged.
    sample = [("a1", "BTC", 100), ("a2", "BTC", 200)]
    current = {"BTC": [("a1", 100)]}  # a2 missing
    overall, per_net = _run(_FakeTagStore(sample), _FakeGS(current), {"BTC": {}})
    assert overall == 0.0
    assert per_net["BTC"]["checked"] == 1
    assert per_net["BTC"]["diverged"] == 0
