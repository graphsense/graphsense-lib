"""The GRAPHSENSE_FRESH_CLUSTERING_CURRENCIES REST/Cassandra read switch.

The same env var that drives the Cassandra write side (delta-updater clustering)
also flips the REST read path's cluster sources per currency: entity/cluster
stats come from ``fresh_cluster_stats`` instead of the Scala ``cluster`` table,
and cluster membership from ``fresh_cluster_addresses`` instead of
``cluster_addresses``. Resolved per-call so the switch needs no restart, and
per-currency so networks can be cut over one at a time.

These assert the table selection only — no live Cassandra.
"""

import pytest

from graphsenselib.config import is_fresh_clustering_enabled
from graphsenselib.db.asynchronous import cassandra as ca

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_CURRENCIES"


def test_defaults_off_select_legacy(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert is_fresh_clustering_enabled("ltc") is False
    assert ca._cluster_stats_table("ltc") == "cluster"
    assert ca._cluster_addresses_table("ltc") == "cluster_addresses"


@pytest.mark.parametrize("val", ["ltc", "LTC", "btc,ltc", " btc , ltc "])
def test_listed_currency_selects_fresh(monkeypatch, val):
    monkeypatch.setenv(_ENV, val)
    assert is_fresh_clustering_enabled("ltc") is True
    assert ca._cluster_stats_table("ltc") == "fresh_cluster_stats"
    assert ca._cluster_addresses_table("ltc") == "fresh_cluster_addresses"


@pytest.mark.parametrize("val", ["btc", "", "bt,c"])
def test_unlisted_currency_stays_legacy(monkeypatch, val):
    monkeypatch.setenv(_ENV, val)
    assert is_fresh_clustering_enabled("ltc") is False
    assert ca._cluster_stats_table("ltc") == "cluster"
    assert ca._cluster_addresses_table("ltc") == "cluster_addresses"
