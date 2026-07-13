"""Cluster-tag reads route on the public id, not on configuration.

Public entity ids are self-describing: fresh-clustering ids are published
shifted by ``FRESH_CLUSTER_ID_OFFSET`` and their tag mappings live in the
parallel ``*_v2`` relations (keyed by the raw fresh id); ids below the offset
are legacy ids keyed in the legacy relations. The read layer derives the
relations set from each id — mixed batches split per regime and results come
back keyed by the public ids the caller passed in.
"""

import pytest
from sqlalchemy.dialects import postgresql

from graphsenselib.tagstore.db import queries as q
from graphsenselib.tagstore.db.models import (
    AddressClusterMapping,
    AddressClusterMappingV2,
    BestClusterTagView,
    BestClusterTagViewV2,
    TagCountByClusterView,
    TagCountByClusterViewV2,
)
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET

LEGACY_ID = 7648699
FRESH_RAW_ID = 408665187
FRESH_PUBLIC_ID = FRESH_RAW_ID + FRESH_CLUSTER_ID_OFFSET


def _sql(stmt):
    return str(
        stmt.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def test_relations_for_legacy_id():
    assert q._cluster_relations_for(LEGACY_ID) == (
        AddressClusterMapping,
        BestClusterTagView,
        TagCountByClusterView,
        LEGACY_ID,
    )


def test_relations_for_fresh_id_unshifts():
    assert q._cluster_relations_for(FRESH_PUBLIC_ID) == (
        AddressClusterMappingV2,
        BestClusterTagViewV2,
        TagCountByClusterViewV2,
        FRESH_RAW_ID,
    )


def test_routed_id_batches_partitions_mixed_lists():
    (legacy, l_fresh, l_shift), (fresh_raw, f_fresh, f_shift) = q._routed_id_batches(
        [LEGACY_ID, FRESH_PUBLIC_ID, 1, FRESH_CLUSTER_ID_OFFSET + 2]
    )
    assert (legacy, l_fresh, l_shift) == ([LEGACY_ID, 1], False, 0)
    assert (fresh_raw, f_fresh, f_shift) == (
        [FRESH_RAW_ID, 2],
        True,
        FRESH_CLUSTER_ID_OFFSET,
    )


@pytest.mark.parametrize(
    "build",
    [
        lambda cid: q._get_best_cluster_tag_stmt(cid, "LTC", ["public"]),
        lambda cid: q._get_count_by_cluster_stmt(cid, "LTC", ["public"]),
        lambda cid: q._get_tags_by_clusterid_stmt(cid, "LTC", 0, 10, ["public"], None),
        lambda cid: q._get_actors_for_clusterid_stmt(cid, "LTC", ["public"]),
        lambda cid: q._get_labels_by_clusterid_stmt(cid, "LTC", ["public"]),
    ],
)
def test_single_id_builders_route_per_id(build):
    legacy_sql = _sql(build(LEGACY_ID))
    assert "_v2" not in legacy_sql
    assert str(LEGACY_ID) in legacy_sql

    fresh_sql = _sql(build(FRESH_PUBLIC_ID))
    assert "_v2" in fresh_sql
    # the query must carry the raw id, never the shifted public one
    assert str(FRESH_RAW_ID) in fresh_sql
    assert str(FRESH_PUBLIC_ID) not in fresh_sql


def test_batch_builders_take_regime_flag():
    assert "best_cluster_tag_v2" in _sql(
        q._get_best_cluster_tag_winners_stmt([FRESH_RAW_ID], "LTC", ["public"], True)
    )
    assert "best_cluster_tag_v2" not in _sql(
        q._get_best_cluster_tag_winners_stmt([LEGACY_ID], "LTC", ["public"], False)
    )
    assert "tag_count_by_cluster_v2" in _sql(
        q._get_count_by_clusters_batch_stmt([FRESH_RAW_ID], "LTC", ["public"], True)
    )
    assert "address_cluster_mapping_v2" in _sql(
        q._get_actors_for_clusterids_batch_stmt([FRESH_RAW_ID], "LTC", ["public"], True)
    )


class _FakeResults:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    """Answers exec() from a per-regime canned row set and records the SQL."""

    def __init__(self, legacy_rows, fresh_rows):
        self.legacy_rows = legacy_rows
        self.fresh_rows = fresh_rows
        self.executed = []

    async def exec(self, stmt):
        sql = _sql(stmt)
        self.executed.append(sql)
        return _FakeResults(self.fresh_rows if "_v2" in sql else self.legacy_rows)


@pytest.mark.asyncio
async def test_bulk_counts_merge_mixed_regimes_on_public_ids():
    db = q.TagstoreDbAsync(None)
    session = _FakeSession(legacy_rows=[(LEGACY_ID, 3)], fresh_rows=[(FRESH_RAW_ID, 5)])
    out = await db.get_nr_tags_for_clusters(
        [LEGACY_ID, FRESH_PUBLIC_ID], "LTC", ["public"], session=session
    )
    assert out == {LEGACY_ID: 3, FRESH_PUBLIC_ID: 5}
    assert len(session.executed) == 2
    legacy_sql = next(s for s in session.executed if "_v2" not in s)
    fresh_sql = next(s for s in session.executed if "_v2" in s)
    assert str(LEGACY_ID) in legacy_sql
    assert str(FRESH_RAW_ID) in fresh_sql
    assert str(FRESH_PUBLIC_ID) not in fresh_sql


@pytest.mark.asyncio
async def test_bulk_actors_merge_mixed_regimes_on_public_ids():
    db = q.TagstoreDbAsync(None)
    session = _FakeSession(
        legacy_rows=[(LEGACY_ID, "actor-a", "Actor A")],
        fresh_rows=[(FRESH_RAW_ID, "actor-b", "Actor B")],
    )
    out = await db.get_actors_for_clusters(
        [LEGACY_ID, FRESH_PUBLIC_ID], "LTC", ["public"], session=session
    )
    assert set(out.keys()) == {LEGACY_ID, FRESH_PUBLIC_ID}
    assert out[FRESH_PUBLIC_ID][0].id == "actor-b"


@pytest.mark.asyncio
async def test_bulk_single_regime_issues_one_query():
    db = q.TagstoreDbAsync(None)
    session = _FakeSession(legacy_rows=[(LEGACY_ID, 1)], fresh_rows=[])
    out = await db.get_nr_tags_for_clusters(
        [LEGACY_ID], "LTC", ["public"], session=session
    )
    assert out == {LEGACY_ID: 1}
    assert len(session.executed) == 1
