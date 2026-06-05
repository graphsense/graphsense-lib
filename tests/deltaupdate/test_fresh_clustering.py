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
    _extract_input_address_sets,
    _plan_clustering_changes,
    _resolve_input_id_sets,
)
from graphsenselib.utils import DataObject as MutableNamedTuple


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


# --------------------------------------------------------------------------- #
# In-loop harvest: _extract_input_address_sets / _resolve_input_id_sets.
# These produce the same Union-Find edges as the one-off's
# multi_input_address_id_sets, but from the txs the delta loop already holds
# (no raw re-read).  The semantics are "multi-address" (>= 2 distinct input
# addresses), matching the canonical one-off path.
# --------------------------------------------------------------------------- #
def _inp(addresses):
    """A raw-tx input whose `address` is a list (multisig => more than one)."""
    return MutableNamedTuple(address=addresses)


def _tx(coinbase=False, inputs=None):
    return MutableNamedTuple(coinbase=coinbase, inputs=inputs)


def test_extract_multi_input():
    txs = [_tx(inputs=[_inp(["A"]), _inp(["B"]), _inp(["C"])])]
    assert _extract_input_address_sets(txs) == [{"A", "B", "C"}]


def test_extract_coinbase_excluded():
    txs = [_tx(coinbase=True, inputs=[_inp(["coinbase"]), _inp(["B"])])]
    assert _extract_input_address_sets(txs) == []


def test_extract_single_address_excluded():
    txs = [_tx(inputs=[_inp(["A"])])]
    assert _extract_input_address_sets(txs) == []


def test_extract_duplicate_addresses_dedup_to_one():
    # same address across inputs collapses to one => < 2 distinct => dropped
    txs = [_tx(inputs=[_inp(["A"]), _inp(["A"])])]
    assert _extract_input_address_sets(txs) == []


def test_extract_multisig_single_input_unions_addresses():
    # one input listing two addresses (multisig) is itself a 2-address edge
    txs = [_tx(inputs=[_inp(["A", "B"])])]
    assert _extract_input_address_sets(txs) == [{"A", "B"}]


def test_extract_skips_none_and_empty_addresses():
    txs = [_tx(inputs=[_inp(["A"]), _inp(None), _inp([]), _inp(["B"])])]
    assert _extract_input_address_sets(txs) == [{"A", "B"}]


def test_extract_inputs_none_skipped():
    txs = [_tx(coinbase=False, inputs=None)]
    assert _extract_input_address_sets(txs) == []


def test_resolve_keeps_two_distinct_ids():
    a2i = {"A": 1, "B": 2}
    out = _resolve_input_id_sets([{"A", "B"}], a2i.__getitem__)
    assert [sorted(s) for s in out] == [[1, 2]]


def test_resolve_drops_when_addresses_collapse_to_one_id():
    # two distinct addresses resolving to the same id => < 2 distinct => dropped
    a2i = {"A": 1, "C": 1}
    assert _resolve_input_id_sets([{"A", "C"}], a2i.__getitem__) == []


def test_resolve_empty_input():
    assert _resolve_input_id_sets([], lambda a: 0) == []


def test_harvest_end_to_end():
    # coinbase + singleton + a 2-id edge + a 3-id multisig edge + a same-id pair
    a2i = {"A": 10, "B": 20, "C": 30, "D": 40, "E": 10}  # E shares id 10 with A
    txs = [
        _tx(coinbase=True, inputs=[_inp(["A"]), _inp(["B"])]),  # coinbase -> drop
        _tx(inputs=[_inp(["A"])]),  # singleton -> drop
        _tx(inputs=[_inp(["A"]), _inp(["B"])]),  # {10, 20}
        _tx(inputs=[_inp(["B", "C", "D"])]),  # multisig {20, 30, 40}
        _tx(inputs=[_inp(["A"]), _inp(["E"])]),  # both id 10 -> drop
    ]
    edges = _resolve_input_id_sets(_extract_input_address_sets(txs), a2i.__getitem__)
    assert sorted(sorted(e) for e in edges) == [[10, 20], [20, 30, 40]]


# --------------------------------------------------------------------------- #
# One-off (all-at-once) vs incremental (batched) partition equivalence on a
# manufactured edge set.  The one-off side uses the same Rust clustering the
# PySpark one-off drives (process_transactions + get_mapping); the incremental
# side runs the production run_incremental_clustering planner across batches,
# with an in-memory model of the fresh_* tables standing in for Cassandra.  The
# address-set partition must be identical for any batch size (cluster_id labels
# may differ — that is why we compare memberships).
# --------------------------------------------------------------------------- #
def _full_partition(edges, max_id):
    from gs_clustering import Clustering

    c = Clustering(max_address_id=max_id)
    c.process_transactions(edges)
    mapping = c.get_mapping()
    clusters: dict = {}
    for addr, root in zip(
        mapping.column("address_id").to_pylist(),
        mapping.column("cluster_id").to_pylist(),
    ):
        clusters.setdefault(root, set()).add(addr)
    # the one-off skips singletons; keep only real (>= 2) clusters
    return {frozenset(m) for m in clusters.values() if len(m) >= 2}


class _FreshStore:
    """In-memory model of the fresh_* tables: serves the reads
    run_incremental_clustering needs and applies the planned ClusteringChanges,
    so we can drive the production planner across batches without Cassandra."""

    def __init__(self):
        self.addr_to_cluster: dict = {}
        self.stats: dict = {}
        self.members: dict = {}

    # reads used by run_incremental_clustering
    def get_fresh_clusters_for_addresses(self, address_ids):
        return [
            (a, self.addr_to_cluster[a])
            for a in address_ids
            if a in self.addr_to_cluster
        ]

    def get_fresh_cluster_stats(self, cluster_ids):
        return {c: self.stats[c] for c in cluster_ids if c in self.stats}

    def get_fresh_cluster_members(self, cluster_ids):
        return [(c, a) for c in cluster_ids for a in self.members.get(c, ())]

    # apply the planned diff (mirrors the effect of _clustering_changes_to_db)
    def apply(self, cc):
        for address_id, cluster_id in cc.address_assignments:
            old = self.addr_to_cluster.get(address_id)
            if old is not None and old != cluster_id:
                self.members.get(old, set()).discard(address_id)
            self.addr_to_cluster[address_id] = cluster_id
            self.members.setdefault(cluster_id, set()).add(address_id)
        for cluster_id, address_id in cc.member_deletes:
            self.members.get(cluster_id, set()).discard(address_id)
        for cluster_id, size, min_aid in cc.stats_upserts:
            self.stats[cluster_id] = (size, min_aid)
        for cluster_id in cc.stats_deletes:
            self.stats.pop(cluster_id, None)
            self.members.pop(cluster_id, None)

    def partition(self):
        clusters: dict = {}
        for addr, cid in self.addr_to_cluster.items():
            clusters.setdefault(cid, set()).add(addr)
        return {frozenset(m) for m in clusters.values()}


class _FreshStoreDb:
    def __init__(self, store):
        self.transformed = store


@pytest.mark.parametrize("batch_size", [1, 2, 3, 100])
def test_oneoff_vs_incremental_partition_equivalence(batch_size):
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    # manufactured edges exercising new / join / cross-batch merge
    edges = [
        [1, 2],  # new {1,2}
        [2, 3],  # join 3
        [10, 11],  # new {10,11}
        [3, 10],  # merge {1,2,3} + {10,11}
        [20, 21, 22],  # new {20,21,22}
        [5, 6],  # new {5,6}
        [6, 20],  # merge {5,6} + {20,21,22}
        [99, 1],  # join 99 into the big cluster
    ]
    max_id = max(a for e in edges for a in e)

    full = _full_partition(edges, max_id)

    store = _FreshStore()
    for i in range(0, len(edges), batch_size):
        cc = run_incremental_clustering(_FreshStoreDb(store), edges[i : i + batch_size])
        store.apply(cc)

    assert store.partition() == full
