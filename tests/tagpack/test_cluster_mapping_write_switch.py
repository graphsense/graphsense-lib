"""The GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS feeder (write-side) switch.

The same env var that flips the REST read path also flips the tagpack ``sync``
cluster-mapping feeder: its Cassandra read source (``fresh_address_cluster`` /
``fresh_cluster_stats`` instead of ``address`` / ``cluster``), its Postgres write
target (``address_cluster_mapping_v2``), the staleness sample source, and the
``*_v2`` MV refresh. All resolved per-call so the switch needs no restart.

These assert the SQL/table selection only — no live DB or Cassandra.
"""

import pytest

from graphsenselib.config import is_tagstore_fresh_clusters_enabled
from graphsenselib.tagpack import graphsense as gs
from graphsenselib.tagpack import tagstore as ts

_ENV = "GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS"


def test_defaults_off_select_legacy(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert is_tagstore_fresh_clusters_enabled() is False
    # Cassandra read source
    assert "FROM address " in gs._cluster_id_query()
    assert "fresh_address_cluster" not in gs._cluster_id_query()
    assert "FROM cluster " in gs._cluster_stats_query()
    assert "fresh_cluster_stats" not in gs._cluster_stats_query()
    # Postgres write target + MVs
    assert ts._acm_table() == "address_cluster_mapping"
    assert ts._cluster_mv_names() == ("tag_count_by_cluster", "best_cluster_tag")


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes"])
def test_on_select_v2(monkeypatch, val):
    monkeypatch.setenv(_ENV, val)
    assert is_tagstore_fresh_clusters_enabled() is True
    # Cassandra read source repointed at the fresh tables
    assert "fresh_address_cluster" in gs._cluster_id_query()
    assert "FROM address " not in gs._cluster_id_query()
    assert "fresh_cluster_stats" in gs._cluster_stats_query()
    assert "FROM cluster " not in gs._cluster_stats_query()
    # Postgres write target + MVs repointed at v2
    assert ts._acm_table() == "address_cluster_mapping_v2"
    assert ts._cluster_mv_names() == (
        "tag_count_by_cluster_v2",
        "best_cluster_tag_v2",
    )


def test_read_source_keeps_lookup_key(monkeypatch):
    """Both schemes use the same (group, id) lookup key — only the table moves."""
    monkeypatch.setenv(_ENV, "true")
    assert "address_id_group=? and address_id=?" in gs._cluster_id_query()
    assert "cluster_id_group=? and cluster_id=?" in gs._cluster_stats_query()
