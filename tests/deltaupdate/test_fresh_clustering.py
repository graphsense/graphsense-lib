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
from graphsenselib.utils import DataObject as MutableNamedTuple
from graphsenselib.utils.utxo import (
    multi_input_address_sets,
    resolve_address_id_sets,
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
def test_merge_smaller_root_survives_reads_only_absorbed():
    # Invariant: cluster_id == root == min(address_id). C3 (root 3) and C6
    # (root 6) merge -> the smaller root (3) survives and keeps its id; the
    # higher-root side is read and re-pointed.
    members = _FakeMembers({6: [6, 7]})  # cluster 6 has members 6, 7
    cc = _plan_clustering_changes(
        components=[[3, 6, 99]],  # 3->C3, 6->C6, 99 new
        addr_to_cluster={3: 3, 6: 6},
        stats={3: (10, 3), 6: (2, 6)},
        get_members=members,
    )
    # only the absorbed (higher-root) cluster's membership is read
    assert members.calls == [[6]]
    # absorbed members + the new address are re-pointed to survivor 3
    assert sorted(cc.address_assignments) == [(6, 3), (7, 3), (99, 3)]
    # the old reverse-index rows of the absorbed cluster are deleted (FM3)
    assert sorted(cc.member_deletes) == [(6, 6), (6, 7)]
    assert cc.stats_deletes == [6]
    # survivor stats: size 10 + 2 + 1(new) = 13, root stays 3
    assert cc.stats_upserts == [(3, 13, 3)]


def test_merge_larger_cluster_absorbed_when_root_is_higher():
    # The accepted cost of cluster_id == root: a big cluster (C6, size 100) with
    # a higher root is the side that gets rewritten, because tiny C3 has the
    # smaller root.
    members = _FakeMembers({6: [6, 7]})
    cc = _plan_clustering_changes(
        components=[[3, 6]],  # 3->C3 (small), 6->C6 (large)
        addr_to_cluster={3: 3, 6: 6},
        stats={3: (2, 3), 6: (100, 6)},
        get_members=members,
    )
    assert members.calls == [[6]]  # the large cluster is read & re-pointed
    assert cc.stats_deletes == [6]
    assert sorted(cc.address_assignments) == [(6, 3), (7, 3)]
    assert sorted(cc.member_deletes) == [(6, 6), (6, 7)]
    assert cc.stats_upserts == [(3, 102, 3)]  # 2 + 100, root 3


def test_merge_three_clusters_absorbs_all_but_survivor():
    # Smallest root (5) survives even though C15 is the largest by size.
    members = _FakeMembers({15: [15], 25: [25]})
    cc = _plan_clustering_changes(
        components=[[5, 15, 25]],  # 5->C5, 15->C15, 25->C25
        addr_to_cluster={5: 5, 15: 15, 25: 25},
        stats={5: (2, 5), 15: (50, 15), 25: (4, 25)},
        get_members=members,
    )
    # both higher-root clusters absorbed in one read call
    assert members.calls == [[15, 25]]
    assert cc.stats_deletes == [15, 25]
    assert sorted(cc.address_assignments) == [(15, 5), (25, 5)]
    assert sorted(cc.member_deletes) == [(15, 15), (25, 25)]
    # 2 + 50 + 4 = 56, root 5
    assert cc.stats_upserts == [(5, 56, 5)]


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
    # bucket_size 10 -> id_group = id // 10, so groups differ per id (0, 10, 20)
    changes = _clustering_changes_to_db(cc, 10)

    def of(table, action):
        return [c.data for c in changes if c.table == table and c.action == action]

    assert isinstance(changes[0], DbChange)
    # one assignment -> insert into both forward and reverse tables, each carrying
    # its partition-bucket group as part of the primary key
    assert of("fresh_address_cluster", DbChangeType.NEW) == [
        {"address_id_group": 0, "address_id": 7, "cluster_id": 100}
    ]
    assert of("fresh_cluster_addresses", DbChangeType.NEW) == [
        {"cluster_id_group": 10, "cluster_id": 100, "address_id": 7}
    ]
    assert of("fresh_cluster_stats", DbChangeType.NEW) == [
        {
            "cluster_id_group": 10,
            "cluster_id": 100,
            "no_addresses": 5,
            "min_address_id": 3,
        }
    ]
    assert of("fresh_cluster_addresses", DbChangeType.DELETE) == [
        {"cluster_id_group": 20, "cluster_id": 200, "address_id": 7}
    ]
    assert of("fresh_cluster_stats", DbChangeType.DELETE) == [
        {"cluster_id_group": 20, "cluster_id": 200}
    ]


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
# In-loop harvest: multi_input_address_sets / resolve_address_id_sets
# (utils.utxo, shared with the driver path and mirrored by the Spark one-off's
# multi_input_address_id_sets). Same Union-Find edges, harvested from the txs the
# delta loop already holds. Semantics: >= 2 distinct input addresses.
# --------------------------------------------------------------------------- #
def _inp(addresses):
    """A raw-tx input whose `address` is a list (multisig => more than one)."""
    return MutableNamedTuple(address=addresses)


def _tx(coinbase=False, inputs=None):
    return MutableNamedTuple(coinbase=coinbase, inputs=inputs)


def test_extract_multi_input():
    txs = [_tx(inputs=[_inp(["A"]), _inp(["B"]), _inp(["C"])])]
    assert multi_input_address_sets(txs) == [{"A", "B", "C"}]


def test_extract_coinbase_excluded():
    txs = [_tx(coinbase=True, inputs=[_inp(["coinbase"]), _inp(["B"])])]
    assert multi_input_address_sets(txs) == []


def test_extract_single_address_excluded():
    txs = [_tx(inputs=[_inp(["A"])])]
    assert multi_input_address_sets(txs) == []


def test_extract_duplicate_addresses_dedup_to_one():
    # same address across inputs collapses to one => < 2 distinct => dropped
    txs = [_tx(inputs=[_inp(["A"]), _inp(["A"])])]
    assert multi_input_address_sets(txs) == []


def test_extract_multisig_single_input_unions_addresses():
    # one input listing two addresses (multisig) is itself a 2-address edge
    txs = [_tx(inputs=[_inp(["A", "B"])])]
    assert multi_input_address_sets(txs) == [{"A", "B"}]


def test_extract_skips_none_and_empty_addresses():
    txs = [_tx(inputs=[_inp(["A"]), _inp(None), _inp([]), _inp(["B"])])]
    assert multi_input_address_sets(txs) == [{"A", "B"}]


def test_extract_inputs_none_skipped():
    txs = [_tx(coinbase=False, inputs=None)]
    assert multi_input_address_sets(txs) == []


def test_resolve_keeps_two_distinct_ids():
    a2i = {"A": 1, "B": 2}
    out = resolve_address_id_sets([{"A", "B"}], a2i.get)
    assert [sorted(s) for s in out] == [[1, 2]]


def test_resolve_drops_when_addresses_collapse_to_one_id():
    # two distinct addresses resolving to the same id => < 2 distinct => dropped
    a2i = {"A": 1, "C": 1}
    assert resolve_address_id_sets([{"A", "C"}], a2i.get) == []


def test_resolve_drops_unresolvable_addresses():
    # Regression: a multisig / multi-address input contributes addresses that the
    # rest of the pipeline never assigns an address_id (filter_inoutputs drops
    # them upstream). resolve() returns None for those; they must be dropped, not
    # raise KeyError. Here B has no id, so {A, B} collapses to a single id -> drop.
    a2i = {"A": 1}  # B unresolvable
    assert resolve_address_id_sets([{"A", "B"}], a2i.get) == []
    # and a set that still has >= 2 resolvable ids survives, minus the unresolved
    a2i2 = {"A": 1, "C": 3}  # B unresolvable, A and C resolve
    out = resolve_address_id_sets([{"A", "B", "C"}], a2i2.get)
    assert [sorted(s) for s in out] == [[1, 3]]


def test_resolve_empty_input():
    assert resolve_address_id_sets([], lambda a: 0) == []


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
    edges = resolve_address_id_sets(multi_input_address_sets(txs), a2i.get)
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
