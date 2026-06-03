"""Unit tests for the v2 incremental fresh-clustering classifier.

``_plan_clustering_changes`` is a pure function (no DB / no Rust): it takes
pre-grouped components plus a cluster-stats dict and a member-reader callback,
and plans the writes. These tests pin the new/join/merge classification, the
size-chosen survivor rule, and the guarantee that membership is read only for
the absorbed side of a merge.
"""

import pytest

from graphsenselib.datatypes import DbChangeType
from graphsenselib.db import DbChange
from graphsenselib.deltaupdate.update.utxo.update import (
    ClusteringChanges,
    _clustering_changes_to_db,
    _components_via_rust,
    _plan_clustering_changes,
)


class _FakeMembers:
    """Stand-in for tdb.get_fresh_cluster_members; records its calls so tests
    can assert membership is read only for the absorbed clusters."""

    def __init__(self, members_by_cluster):
        self.members_by_cluster = members_by_cluster
        self.calls = []

    def __call__(self, cluster_ids):
        self.calls.append(list(cluster_ids))
        return [(c, a) for c in cluster_ids for a in self.members_by_cluster.get(c, [])]


def _never(_cluster_ids):
    raise AssertionError("get_members must not be called for new/join components")


# --------------------------------------------------------------------------- #
# NEW cluster
# --------------------------------------------------------------------------- #
def test_new_cluster_two_addresses():
    cc = _plan_clustering_changes(
        components=[[20, 10]],
        addr_to_cluster={},
        stats={},
        get_members=_never,
    )
    # cluster_id = min address (canonical)
    assert sorted(cc.address_assignments) == [(10, 10), (20, 10)]
    assert cc.stats_upserts == [(10, 2, 10)]
    assert cc.member_deletes == []
    assert cc.stats_deletes == []


def test_new_cluster_singleton_skipped():
    cc = _plan_clustering_changes(
        components=[[42]],
        addr_to_cluster={},
        stats={},
        get_members=_never,
    )
    assert cc.is_empty


# --------------------------------------------------------------------------- #
# JOIN (no membership read)
# --------------------------------------------------------------------------- #
def test_join_new_address_into_existing_cluster():
    cc = _plan_clustering_changes(
        components=[[10, 5]],  # 10 new, 5 already in cluster 100
        addr_to_cluster={5: 100},
        stats={100: (3, 4)},  # size 3, min 4
        get_members=_never,  # must NOT read membership for a join
    )
    assert cc.address_assignments == [(10, 100)]  # only the new addr changes
    assert cc.stats_upserts == [(100, 4, 4)]  # size 3+1, min(4, 10)=4
    assert cc.member_deletes == []
    assert cc.stats_deletes == []


def test_join_noop_when_no_new_addresses():
    # all touched addrs already in the cluster (e.g. idempotent re-run)
    cc = _plan_clustering_changes(
        components=[[5, 6]],
        addr_to_cluster={5: 100, 6: 100},
        stats={100: (9, 1)},
        get_members=_never,
    )
    assert cc.is_empty


# --------------------------------------------------------------------------- #
# MERGE
# --------------------------------------------------------------------------- #
def test_merge_keeps_larger_reads_only_smaller():
    members = _FakeMembers({200: [6, 7]})  # cluster 200 has members 6, 7
    cc = _plan_clustering_changes(
        components=[[5, 6, 99]],  # 5->C100, 6->C200, 99 new
        addr_to_cluster={5: 100, 6: 200},
        stats={100: (10, 3), 200: (2, 6)},  # 100 is larger -> survives
        get_members=members,
    )
    # only the absorbed (smaller) cluster's membership is read
    assert members.calls == [[200]]
    # absorbed members + the new address are re-pointed to survivor 100
    assert sorted(cc.address_assignments) == [(6, 100), (7, 100), (99, 100)]
    # the old reverse-index rows of the absorbed cluster are deleted (FM3)
    assert sorted(cc.member_deletes) == [(200, 6), (200, 7)]
    assert cc.stats_deletes == [200]
    # survivor stats: size 10 + 2 + 1(new) = 13, min(3, 6, 99) = 3
    assert cc.stats_upserts == [(100, 13, 3)]


def test_merge_tie_breaks_to_smallest_cluster_id():
    members = _FakeMembers({100: [5]})
    cc = _plan_clustering_changes(
        components=[[5, 6]],  # 5->C100, 6->C50, equal size
        addr_to_cluster={5: 100, 6: 50},
        stats={100: (5, 5), 50: (5, 6)},
        get_members=members,
    )
    # equal size -> smaller cluster_id (50) survives, 100 absorbed
    assert members.calls == [[100]]
    assert cc.stats_deletes == [100]
    assert cc.stats_upserts == [(50, 10, 5)]  # 5+5, min(6,5)=5
    assert sorted(cc.address_assignments) == [(5, 50)]
    assert cc.member_deletes == [(100, 5)]


def test_merge_three_clusters_absorbs_all_but_survivor():
    members = _FakeMembers({10: [1], 30: [3]})
    cc = _plan_clustering_changes(
        components=[[1, 2, 3]],  # C10, C20(survivor), C30
        addr_to_cluster={1: 10, 2: 20, 3: 30},
        stats={10: (2, 1), 20: (50, 2), 30: (4, 3)},
        get_members=members,
    )
    # survivor is the largest (C20); both others absorbed in one read call
    assert members.calls == [[10, 30]]
    assert cc.stats_deletes == [10, 30]
    assert sorted(cc.address_assignments) == [(1, 20), (3, 20)]
    assert sorted(cc.member_deletes) == [(10, 1), (30, 3)]
    # 50 + 2 + 4 = 56, min(2,1,3)=1
    assert cc.stats_upserts == [(20, 56, 1)]


# --------------------------------------------------------------------------- #
# _clustering_changes_to_db translation
# --------------------------------------------------------------------------- #
def test_changes_to_db_inserts_and_deletes():
    cc = ClusteringChanges(
        address_assignments=[(7, 100)],
        stats_upserts=[(100, 5, 3)],
        member_deletes=[(200, 7)],
        stats_deletes=[200],
    )
    changes = _clustering_changes_to_db(cc)

    def of(table, action):
        return [c.data for c in changes if c.table == table and c.action == action]

    assert isinstance(changes[0], DbChange)
    # one assignment -> insert into both forward and reverse tables
    assert of("fresh_address_cluster", DbChangeType.NEW) == [
        {"address_id": 7, "cluster_id": 100}
    ]
    assert of("fresh_cluster_addresses", DbChangeType.NEW) == [
        {"cluster_id": 100, "address_id": 7}
    ]
    assert of("fresh_cluster_stats", DbChangeType.NEW) == [
        {"cluster_id": 100, "size": 5, "min_address_id": 3}
    ]
    assert of("fresh_cluster_addresses", DbChangeType.DELETE) == [
        {"cluster_id": 200, "address_id": 7}
    ]
    assert of("fresh_cluster_stats", DbChangeType.DELETE) == [{"cluster_id": 200}]


# --------------------------------------------------------------------------- #
# _components_via_rust grouping (needs the Rust extension)
# --------------------------------------------------------------------------- #
def test_components_chain_into_one():
    pytest.importorskip("gs_clustering")
    comps = _components_via_rust([[1, 2], [2, 3]], addr_to_cluster={})
    assert [frozenset(c) for c in comps] == [frozenset({1, 2, 3})]


def test_components_unrelated_txs_stay_separate():
    pytest.importorskip("gs_clustering")
    comps = _components_via_rust([[1, 2], [3, 4]], addr_to_cluster={})
    assert {frozenset(c) for c in comps} == {frozenset({1, 2}), frozenset({3, 4})}


def test_components_synthetic_preunion_merges_same_cluster():
    # 1 and 5 are both already in cluster 100 but appear in separate txs; the
    # synthetic per-cluster union must collapse the two components into one so
    # cluster 100's fate is decided once.
    pytest.importorskip("gs_clustering")
    comps = _components_via_rust([[1, 10], [5, 20]], addr_to_cluster={1: 100, 5: 100})
    assert {frozenset(c) for c in comps} == {frozenset({1, 5, 10, 20})}


# --------------------------------------------------------------------------- #
# run_incremental_clustering orchestration (grouping + stats guard + plan), with
# an in-memory fake of db.transformed (no Cassandra, real Rust grouping).
# --------------------------------------------------------------------------- #
class _FakeTransformed:
    def __init__(self, addr_to_cluster, stats, members_by_cluster):
        self._a2c = addr_to_cluster
        self._stats = stats
        self._members = members_by_cluster
        self.member_calls = []

    def get_fresh_clusters_for_addresses(self, address_ids):
        return [(a, self._a2c[a]) for a in address_ids if a in self._a2c]

    def get_fresh_cluster_stats(self, cluster_ids):
        return {c: self._stats[c] for c in cluster_ids if c in self._stats}

    def get_fresh_cluster_members(self, cluster_ids):
        self.member_calls.append(list(cluster_ids))
        return [(c, a) for c in cluster_ids for a in self._members.get(c, [])]


class _FakeDb:
    def __init__(self, transformed):
        self.transformed = transformed


def test_run_incremental_clustering_merge_end_to_end():
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    tdb = _FakeTransformed(
        addr_to_cluster={5: 100, 6: 200},
        stats={100: (10, 3), 200: (2, 6)},
        members_by_cluster={100: [3, 5], 200: [6, 7]},
    )
    cc = run_incremental_clustering(_FakeDb(tdb), [[5, 6]])

    # only the smaller cluster (200) is read for membership
    assert tdb.member_calls == [[200]]
    assert sorted(cc.address_assignments) == [(6, 100), (7, 100)]
    assert sorted(cc.member_deletes) == [(200, 6), (200, 7)]
    assert cc.stats_deletes == [200]
    assert cc.stats_upserts == [(100, 12, 3)]  # 10+2, min(3,6)=3


def test_run_incremental_clustering_guards_on_missing_stats():
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    # address 6 is in cluster 200 but 200 has no stats row -> must raise
    tdb = _FakeTransformed(
        addr_to_cluster={5: 100, 6: 200},
        stats={100: (10, 3)},
        members_by_cluster={},
    )
    with pytest.raises(RuntimeError, match="fresh_cluster_stats missing"):
        run_incremental_clustering(_FakeDb(tdb), [[5, 6]])
