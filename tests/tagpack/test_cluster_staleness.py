from unittest.mock import patch

import pandas as pd
import pytest

pytest.importorskip("yaml_include", reason="PyYAML is required for tagpack tests")

from graphsenselib.tagpack.cli import check_cluster_mapping_staleness


class _FakeTagStore:
    def __init__(self, sample, fresh_sample=None, v2_exists=True):
        self._sample = {False: sample, True: fresh_sample or []}
        self._v2_exists = v2_exists

    def cluster_mapping_table_exists(self, fresh=False):
        return self._v2_exists if fresh else True

    def get_cluster_mapping_sample(self, limit, networks=None, fresh=False):
        rows = self._sample[fresh]
        if networks is not None:
            return [r for r in rows if r[1] in networks][:limit]
        return rows[:limit]


class _FakeGS:
    def __init__(
        self,
        current_by_network,
        missing_keyspaces=None,
        fresh_networks=None,
        fresh_current_by_network=None,
    ):
        self._current = {
            False: current_by_network,
            True: fresh_current_by_network or {},
        }
        self._missing = set(missing_keyspaces or [])
        self._fresh = set(fresh_networks or [])

    def keyspace_for_network_exists(self, network):
        return network not in self._missing

    def is_fresh_network(self, network):
        return network in self._fresh

    def get_address_clusters(self, df, network, fresh=False):
        rows = self._current[fresh].get(network, [])
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


def test_fresh_marked_network_checks_both_regimes():
    # v1 rows compare against legacy clustering (clean here); the *_v2 rows of
    # a fresh-marked network compare against the fresh membership, where one
    # of two sampled addresses drifted (delta-updater merge).
    sample = [("a1", "LTC", 100), ("a2", "LTC", 200)]
    fresh_sample = [("a1", "LTC", 100), ("a2", "LTC", 200)]
    current = {"LTC": [("a1", 100), ("a2", 200)]}
    fresh_current = {"LTC": [("a1", 100), ("a2", 150)]}
    overall, per_net = _run(
        _FakeTagStore(sample, fresh_sample=fresh_sample),
        _FakeGS(
            current, fresh_networks=["LTC"], fresh_current_by_network=fresh_current
        ),
        {"LTC": {}},
    )
    assert per_net["LTC"]["diverged"] == 0
    assert per_net["LTC[fresh]"]["diverged"] == 1
    assert per_net["LTC[fresh]"]["checked"] == 2
    assert overall == 0.25


def test_fresh_leg_skipped_without_v2_relations():
    sample = [("a1", "LTC", 100)]
    fresh_sample = [("a1", "LTC", 100)]
    current = {"LTC": [("a1", 100)]}
    overall, per_net = _run(
        _FakeTagStore(sample, fresh_sample=fresh_sample, v2_exists=False),
        _FakeGS(current, fresh_networks=["LTC"], fresh_current_by_network={}),
        {"LTC": {}},
    )
    assert "LTC" in per_net
    assert "LTC[fresh]" not in per_net
