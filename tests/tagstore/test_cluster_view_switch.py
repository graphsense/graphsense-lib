"""The GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS read-side switch.

When the flag is off (default) the cluster-tag read path targets the legacy
relations; when on it targets the parallel ``*_v2`` fresh-clustering relations.
The switch is resolved per-call (no restart) via ``queries._cluster_models``.
"""

import pytest
from sqlalchemy.dialects import postgresql

from graphsenselib.config import is_tagstore_fresh_clusters_enabled
from graphsenselib.tagstore.db import queries as q
from graphsenselib.tagstore.db.models import (
    AddressClusterMapping,
    AddressClusterMappingV2,
    BestClusterTagView,
    BestClusterTagViewV2,
    TagCountByClusterView,
    TagCountByClusterViewV2,
)

_ENV = "GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS"


def _sql(stmt):
    return str(stmt.compile(dialect=postgresql.dialect()))


def test_flag_defaults_off(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert is_tagstore_fresh_clusters_enabled() is False
    assert q._cluster_models() == (
        AddressClusterMapping,
        BestClusterTagView,
        TagCountByClusterView,
    )


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes"])
def test_flag_truthy_values_select_v2(monkeypatch, val):
    monkeypatch.setenv(_ENV, val)
    assert is_tagstore_fresh_clusters_enabled() is True
    assert q._cluster_models() == (
        AddressClusterMappingV2,
        BestClusterTagViewV2,
        TagCountByClusterViewV2,
    )


def test_builders_target_legacy_when_off(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert "best_cluster_tag_v2" not in _sql(
        q._get_best_cluster_tag_stmt(1, "BTC", ["public"])
    )
    assert "tag_count_by_cluster_v2" not in _sql(
        q._get_count_by_cluster_stmt(1, "BTC", ["public"])
    )
    assert "address_cluster_mapping_v2" not in _sql(
        q._get_tags_by_clusterid_stmt(1, "BTC", 0, 10, ["public"], None)
    )
    assert "address_cluster_mapping_v2" not in _sql(
        q._get_actors_for_clusterid_stmt(1, "BTC", ["public"])
    )
    assert "address_cluster_mapping_v2" not in _sql(
        q._get_labels_by_clusterid_stmt(1, ["public"])
    )


def test_builders_target_v2_when_on(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    assert "best_cluster_tag_v2" in _sql(
        q._get_best_cluster_tag_stmt(1, "BTC", ["public"])
    )
    assert "best_cluster_tag_v2" in _sql(
        q._get_best_cluster_tag_winners_stmt([1, 2], "BTC", ["public"])
    )
    assert "tag_count_by_cluster_v2" in _sql(
        q._get_count_by_cluster_stmt(1, "BTC", ["public"])
    )
    assert "tag_count_by_cluster_v2" in _sql(
        q._get_count_by_clusters_batch_stmt([1, 2], "BTC", ["public"])
    )
    assert "address_cluster_mapping_v2" in _sql(
        q._get_tags_by_clusterid_stmt(1, "BTC", 0, 10, ["public"], None)
    )
    assert "address_cluster_mapping_v2" in _sql(
        q._get_actors_for_clusterid_stmt(1, "BTC", ["public"])
    )
    assert "address_cluster_mapping_v2" in _sql(
        q._get_actors_for_clusterids_batch_stmt([1, 2], "BTC", ["public"])
    )
    assert "address_cluster_mapping_v2" in _sql(
        q._get_labels_by_clusterid_stmt(1, ["public"])
    )
