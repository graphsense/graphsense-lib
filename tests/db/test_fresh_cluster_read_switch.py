"""The GRAPHSENSE_FRESH_CLUSTERING_ENABLED REST/Cassandra read switch.

The same env var that drives the Cassandra write side (delta-updater clustering)
also flips the REST read path's cluster sources: entity/cluster stats come from
``fresh_cluster_stats`` instead of the Scala ``cluster`` table, and cluster
membership from ``fresh_cluster_addresses`` instead of ``cluster_addresses``.
Resolved per-call so the switch needs no restart.

These assert the table selection only — no live Cassandra.
"""

import pytest

from graphsenselib.config import is_fresh_clustering_enabled
from graphsenselib.db.asynchronous import cassandra as ca

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_ENABLED"


def test_defaults_off_select_legacy(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert is_fresh_clustering_enabled() is False
    assert ca._cluster_stats_table() == "cluster"
    assert ca._cluster_addresses_table() == "cluster_addresses"


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes"])
def test_on_select_fresh(monkeypatch, val):
    monkeypatch.setenv(_ENV, val)
    assert is_fresh_clustering_enabled() is True
    assert ca._cluster_stats_table() == "fresh_cluster_stats"
    assert ca._cluster_addresses_table() == "fresh_cluster_addresses"
