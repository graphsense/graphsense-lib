"""Unit tests for the v2 incremental fresh-clustering classifier.

``_plan_clustering_changes`` is a pure function (no DB / no Rust): it takes
pre-grouped components, the stored :class:`_ClusterStats` of every existing
cluster, and the one-member contribution of every newly-clustered address, and
plans the writes. These tests pin the new/join/merge classification, the
smaller-root survivor rule, the guarantee that membership is read only for the
absorbed side of a merge, and — the member-sum maintenance — that every stat
column of the upserted row is the additive fold of the surviving/absorbed
clusters' aggregates plus the new addresses' contributions.
"""

import random
import time

import pytest

from graphsenselib.datatypes import DbChangeType
from graphsenselib.db import DbChange
from graphsenselib.deltaupdate.update.generic import DeltaValue, EntityDelta
from graphsenselib.deltaupdate.update.utxo.update import (
    ClusteringChanges,
    _ClusterStats,
    _clustering_changes_to_db,
    _components_via_rust,
    _plan_clustering_changes,
)
from graphsenselib.utils import DataObject as MutableNamedTuple
from graphsenselib.utils.utxo import (
    multi_input_address_sets,
    resolve_address_id_sets,
)


# --------------------------------------------------------------------------- #
# Stat helpers: a currency struct is a DeltaValue (value + per-fiat values);
# fiat_values distinct per column ([v, 2v]) so an element-wise sum is exercised.
# --------------------------------------------------------------------------- #
def _cur(v):
    return DeltaValue(value=v, fiat_values=[float(v), float(2 * v)])


def _cs(no_addresses, min_id, first, last, no_in, no_out, recv, spent):
    """An existing cluster's stored aggregate."""
    return _ClusterStats(
        no_addresses, min_id, first, last, no_in, no_out, _cur(recv), _cur(spent)
    )


def _addr_cs(addr_id, first, last, no_in, no_out, recv, spent):
    """One new address's one-member contribution."""
    return _ClusterStats(
        1, addr_id, first, last, no_in, no_out, _cur(recv), _cur(spent)
    )


def _stats_row(cid, no_addresses, min_id, first, last, no_in, no_out, recv, spent):
    """A fresh_cluster_stats row as the DB reader returns it (attribute access)."""
    return MutableNamedTuple(
        cluster_id=cid,
        no_addresses=no_addresses,
        min_address_id=min_id,
        first_tx_id=first,
        last_tx_id=last,
        no_incoming_txs=no_in,
        no_outgoing_txs=no_out,
        total_received=_cur(recv),
        total_spent=_cur(spent),
    )


def _addr_row(first, last, no_in, no_out, recv, spent):
    """An ``address`` stat row as the DB reader returns it."""
    return MutableNamedTuple(
        first_tx_id=first,
        last_tx_id=last,
        no_incoming_txs=no_in,
        no_outgoing_txs=no_out,
        total_received=_cur(recv),
        total_spent=_cur(spent),
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
        addr_stats={
            20: _addr_cs(20, first=5, last=90, no_in=1, no_out=4, recv=2000, spent=200),
            10: _addr_cs(
                10, first=10, last=100, no_in=2, no_out=3, recv=1000, spent=100
            ),
        },
        get_members=_never,
    )
    # cluster_id = min address (canonical); stats are the member sum of 10 + 20
    assert sorted(cc.address_assignments) == [(10, 10), (20, 10)]
    assert cc.stats_upserts == [
        (10, _cs(2, 10, first=5, last=100, no_in=3, no_out=7, recv=3000, spent=300))
    ]
    assert cc.member_deletes == []
    assert cc.stats_deletes == []


def test_new_cluster_singleton_skipped():
    cc = _plan_clustering_changes(
        components=[[42]],
        addr_to_cluster={},
        stats={},
        addr_stats={42: _addr_cs(42, 1, 1, 1, 1, 1, 1)},
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
        stats={
            100: _cs(3, 4, first=4, last=50, no_in=6, no_out=9, recv=5000, spent=500)
        },
        addr_stats={
            10: _addr_cs(
                10, first=10, last=60, no_in=1, no_out=2, recv=1000, spent=100
            ),
        },
        get_members=_never,  # must NOT read membership for a join
    )
    assert cc.address_assignments == [(10, 100)]  # only the new addr changes
    # size 3+1, min(4,10)=4, and every rich column += the joining address
    assert cc.stats_upserts == [
        (100, _cs(4, 4, first=4, last=60, no_in=7, no_out=11, recv=6000, spent=600))
    ]
    assert cc.member_deletes == []
    assert cc.stats_deletes == []


def test_join_noop_when_no_new_addresses():
    # all touched addrs already in the cluster (e.g. idempotent re-run)
    cc = _plan_clustering_changes(
        components=[[5, 6]],
        addr_to_cluster={5: 100, 6: 100},
        stats={100: _cs(9, 1, 1, 9, 9, 9, 900, 90)},
        addr_stats={},
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
        stats={
            3: _cs(10, 3, first=3, last=40, no_in=5, no_out=5, recv=3000, spent=300),
            6: _cs(2, 6, first=6, last=30, no_in=2, no_out=1, recv=600, spent=60),
        },
        addr_stats={
            99: _addr_cs(99, first=99, last=99, no_in=1, no_out=1, recv=99, spent=9),
        },
        get_members=members,
    )
    # only the absorbed (higher-root) cluster's membership is read
    assert members.calls == [[6]]
    # absorbed members + the new address are re-pointed to survivor 3
    assert sorted(cc.address_assignments) == [(6, 3), (7, 3), (99, 3)]
    # the old reverse-index rows of the absorbed cluster are deleted (FM3)
    assert sorted(cc.member_deletes) == [(6, 6), (6, 7)]
    assert cc.stats_deletes == [6]
    # survivor stats: C3 + C6 + the new address, folded member-sum
    assert cc.stats_upserts == [
        (3, _cs(13, 3, first=3, last=99, no_in=8, no_out=7, recv=3699, spent=369))
    ]


def test_merge_larger_cluster_absorbed_when_root_is_higher():
    # The accepted cost of cluster_id == root: a big cluster (C6, size 100) with
    # a higher root is the side that gets rewritten, because tiny C3 has the
    # smaller root.
    members = _FakeMembers({6: [6, 7]})
    cc = _plan_clustering_changes(
        components=[[3, 6]],  # 3->C3 (small), 6->C6 (large)
        addr_to_cluster={3: 3, 6: 6},
        stats={
            3: _cs(2, 3, first=3, last=8, no_in=1, no_out=1, recv=30, spent=3),
            6: _cs(100, 6, first=1, last=99, no_in=50, no_out=40, recv=9000, spent=900),
        },
        addr_stats={},
        get_members=members,
    )
    assert members.calls == [[6]]  # the large cluster is read & re-pointed
    assert cc.stats_deletes == [6]
    assert sorted(cc.address_assignments) == [(6, 3), (7, 3)]
    assert sorted(cc.member_deletes) == [(6, 6), (6, 7)]
    # 2 + 100 members, root 3; first=min(3,1)=1, last=max(8,99)=99
    assert cc.stats_upserts == [
        (3, _cs(102, 3, first=1, last=99, no_in=51, no_out=41, recv=9030, spent=903))
    ]


def test_merge_three_clusters_absorbs_all_but_survivor():
    # Smallest root (5) survives even though C15 is the largest by size.
    members = _FakeMembers({15: [15], 25: [25]})
    cc = _plan_clustering_changes(
        components=[[5, 15, 25]],  # 5->C5, 15->C15, 25->C25
        addr_to_cluster={5: 5, 15: 15, 25: 25},
        stats={
            5: _cs(2, 5, first=5, last=6, no_in=1, no_out=1, recv=20, spent=2),
            15: _cs(
                50, 15, first=2, last=80, no_in=25, no_out=20, recv=5000, spent=500
            ),
            25: _cs(4, 25, first=25, last=40, no_in=3, no_out=2, recv=400, spent=40),
        },
        addr_stats={},
        get_members=members,
    )
    # both higher-root clusters absorbed in one read call
    assert members.calls == [[15, 25]]
    assert cc.stats_deletes == [15, 25]
    assert sorted(cc.address_assignments) == [(15, 5), (25, 5)]
    assert sorted(cc.member_deletes) == [(15, 15), (25, 25)]
    # 2 + 50 + 4 = 56, root 5; first=min(5,2,25)=2, last=max(6,80,40)=80
    assert cc.stats_upserts == [
        (5, _cs(56, 5, first=2, last=80, no_in=29, no_out=23, recv=5420, spent=542))
    ]


# --------------------------------------------------------------------------- #
# _clustering_changes_to_db translation
# --------------------------------------------------------------------------- #
def test_changes_to_db_inserts_and_deletes():
    st = _cs(5, 3, first=10, last=20, no_in=7, no_out=4, recv=500, spent=100)
    cc = ClusteringChanges(
        address_assignments=[(7, 100)],
        stats_upserts=[(100, st)],
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
    # the stats row now carries every member-sum column, all non-null
    assert of("fresh_cluster_stats", DbChangeType.NEW) == [
        {
            "cluster_id_group": 10,
            "cluster_id": 100,
            "no_addresses": 5,
            "min_address_id": 3,
            "first_tx_id": 10,
            "last_tx_id": 20,
            "no_incoming_txs": 7,
            "no_outgoing_txs": 4,
            "total_received": _cur(500),
            "total_spent": _cur(100),
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
    def __init__(self, addr_to_cluster, stats_rows, members_by_cluster, address_rows):
        self._a2c = addr_to_cluster
        self._stats = stats_rows
        self._members = members_by_cluster
        self._addr = address_rows
        self.member_calls = []

    def get_fresh_clusters_for_addresses(self, address_ids):
        return [(a, self._a2c[a]) for a in address_ids if a in self._a2c]

    def get_fresh_cluster_stats(self, cluster_ids):
        return {c: self._stats[c] for c in cluster_ids if c in self._stats}

    def get_address_stats(self, address_ids):
        return {a: self._addr[a] for a in address_ids if a in self._addr}

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
        stats_rows={
            100: _stats_row(100, 10, 3, 3, 50, 5, 5, 3000, 300),
            200: _stats_row(200, 2, 6, 6, 40, 2, 1, 600, 60),
        },
        members_by_cluster={100: [3, 5], 200: [6, 7]},
        address_rows={},  # 5 and 6 are both already clustered -> no new addresses
    )
    cc = run_incremental_clustering(_FakeDb(tdb), [[5, 6]])

    # only the smaller cluster (200) is read for membership
    assert tdb.member_calls == [[200]]
    assert sorted(cc.address_assignments) == [(6, 100), (7, 100)]
    assert sorted(cc.member_deletes) == [(200, 6), (200, 7)]
    assert cc.stats_deletes == [200]
    # 10+2 members, min(3,6)=3, folded member-sum of C100 + C200
    assert cc.stats_upserts == [
        (100, _cs(12, 3, first=3, last=50, no_in=7, no_out=6, recv=3600, spent=360))
    ]


def test_run_incremental_clustering_guards_on_missing_stats():
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    # address 6 is in cluster 200 but 200 has no stats row -> must raise
    tdb = _FakeTransformed(
        addr_to_cluster={5: 100, 6: 200},
        stats_rows={100: _stats_row(100, 10, 3, 3, 50, 5, 5, 3000, 300)},
        members_by_cluster={},
        address_rows={},
    )
    with pytest.raises(RuntimeError, match="fresh_cluster_stats missing"):
        run_incremental_clustering(_FakeDb(tdb), [[5, 6]])


# --------------------------------------------------------------------------- #
# In-loop harvest: multi_input_address_sets / resolve_address_id_sets
# (utils.utxo, shared with the driver path and mirrored by the Spark one-off's
# multi_input_address_id_sets). Same Union-Find edges, harvested from the txs the
# delta loop already holds. Semantics: >= 2 distinct input addresses of a
# non-coinjoin tx (a coinjoin's co-spend is deliberately not an ownership edge).
# --------------------------------------------------------------------------- #
def _inp(addresses):
    """A raw-tx input whose `address` is a list (multisig => more than one)."""
    return MutableNamedTuple(address=addresses)


def _tx(coinbase=False, inputs=None, coinjoin=False):
    return MutableNamedTuple(coinbase=coinbase, inputs=inputs, coinjoin=coinjoin)


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


def test_extract_coinjoin_excluded_by_default():
    # co-spending inside a coinjoin is not evidence of common ownership; the
    # legacy Scala clustering filters these and the fresh path must too
    txs = [_tx(inputs=[_inp(["A"]), _inp(["B"])], coinjoin=True)]
    assert multi_input_address_sets(txs) == []


def test_extract_coinjoin_kept_when_filtering_disabled():
    txs = [_tx(inputs=[_inp(["A"]), _inp(["B"])], coinjoin=True)]
    assert multi_input_address_sets(txs, exclude_coinjoin=False) == [{"A", "B"}]


def test_extract_coinjoin_null_counts_as_not_coinjoin():
    # raw keyspaces ingested before the flag existed carry NULL: keep the edge
    txs = [_tx(inputs=[_inp(["A"]), _inp(["B"])], coinjoin=None)]
    assert multi_input_address_sets(txs) == [{"A", "B"}]


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
        _tx(inputs=[_inp(["C"]), _inp(["D"])], coinjoin=True),  # coinjoin -> drop
    ]
    edges = resolve_address_id_sets(multi_input_address_sets(txs), a2i.get)
    assert sorted(sorted(e) for e in edges) == [[10, 20], [20, 30, 40]]


def test_get_coinjoin_filtering_reads_configuration():
    # the flag the harvest paths pass as exclude_coinjoin comes from the
    # transformed keyspace's configuration row (written by the Scala job);
    # a NULL column means unset -> the schema/legacy default of True
    from graphsenselib.db.utxo import TransformedDbUtxo

    def with_config(row):
        tdb = object.__new__(TransformedDbUtxo)
        tdb._db_config = row
        return tdb.get_coinjoin_filtering()

    assert with_config(MutableNamedTuple(coinjoin_filtering=True)) is True
    assert with_config(MutableNamedTuple(coinjoin_filtering=False)) is False
    assert with_config(MutableNamedTuple(coinjoin_filtering=None)) is True


# --------------------------------------------------------------------------- #
# One-off (all-at-once) vs incremental (batched) equivalence on a manufactured
# edge set.  The one-off side uses the same Rust clustering the PySpark one-off
# drives (process_transactions + get_mapping); the incremental side runs the
# production run_incremental_clustering planner across batches, with an in-memory
# model of the fresh_* tables standing in for Cassandra.  Both the address-set
# partition AND every stored cluster's member-sum stats must match the from-
# scratch result for any batch size (cluster_id labels may differ — that is why
# we compare memberships).
# --------------------------------------------------------------------------- #

# per-address stat rows for every address in EDGES (deliberately distinct so a
# missed / double-counted member would change a sum).
_ADDR_STATS = {
    1: _addr_row(first=1, last=101, no_in=1, no_out=2, recv=100, spent=10),
    2: _addr_row(first=2, last=102, no_in=2, no_out=3, recv=200, spent=20),
    3: _addr_row(first=3, last=103, no_in=3, no_out=4, recv=300, spent=30),
    5: _addr_row(first=5, last=105, no_in=5, no_out=6, recv=500, spent=50),
    6: _addr_row(first=6, last=106, no_in=6, no_out=7, recv=600, spent=60),
    10: _addr_row(first=10, last=110, no_in=10, no_out=11, recv=1000, spent=100),
    11: _addr_row(first=11, last=111, no_in=11, no_out=12, recv=1100, spent=110),
    20: _addr_row(first=20, last=120, no_in=20, no_out=21, recv=2000, spent=200),
    21: _addr_row(first=21, last=121, no_in=21, no_out=22, recv=2100, spent=210),
    22: _addr_row(first=22, last=122, no_in=22, no_out=23, recv=2200, spent=220),
    99: _addr_row(first=99, last=199, no_in=99, no_out=100, recv=9900, spent=990),
}

EDGES = [
    [1, 2],  # new {1,2}
    [2, 3],  # join 3
    [10, 11],  # new {10,11}
    [3, 10],  # merge {1,2,3} + {10,11}
    [20, 21, 22],  # new {20,21,22}
    [5, 6],  # new {5,6}
    [6, 20],  # merge {5,6} + {20,21,22}
    [99, 1],  # join 99 into the big cluster
]


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


def _expected_stats(members):
    """The from-scratch member-sum of a set of addresses (all rows present)."""
    it = iter(sorted(members))
    first = next(it)
    agg = _ClusterStats.from_address_row(first, _ADDR_STATS[first], 2)
    for a in it:
        agg = agg.merge(_ClusterStats.from_address_row(a, _ADDR_STATS[a], 2))
    return agg


class _FreshStore:
    """In-memory model of the fresh_* tables: serves the reads
    run_incremental_clustering needs (including per-address stats) and applies
    the planned ClusteringChanges, so we can drive the production planner across
    batches without Cassandra."""

    def __init__(self):
        self.addr_to_cluster: dict = {}
        self.stats: dict = {}  # cluster_id -> _ClusterStats
        self.members: dict = {}

    # reads used by run_incremental_clustering
    def get_fresh_clusters_for_addresses(self, address_ids):
        return [
            (a, self.addr_to_cluster[a])
            for a in address_ids
            if a in self.addr_to_cluster
        ]

    def get_fresh_cluster_stats(self, cluster_ids):
        # return the stored aggregate as a row (attribute access), as Cassandra would
        return {
            c: MutableNamedTuple(cluster_id=c, **self.stats[c]._asdict())
            for c in cluster_ids
            if c in self.stats
        }

    def get_address_stats(self, address_ids):
        return {a: _ADDR_STATS[a] for a in address_ids if a in _ADDR_STATS}

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
        for cluster_id, st in cc.stats_upserts:
            self.stats[cluster_id] = st
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
def test_oneoff_vs_incremental_partition_and_stats(batch_size):
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    max_id = max(a for e in EDGES for a in e)
    full = _full_partition(EDGES, max_id)

    store = _FreshStore()
    for i in range(0, len(EDGES), batch_size):
        cc = run_incremental_clustering(_FreshStoreDb(store), EDGES[i : i + batch_size])
        store.apply(cc)

    # membership matches the from-scratch partition ...
    assert store.partition() == full

    # ... and every stored cluster's stats are exactly the member sum of its
    # addresses, additively maintained regardless of the batch split.
    by_cid: dict = {}
    for addr, cid in store.addr_to_cluster.items():
        by_cid.setdefault(cid, set()).add(addr)
    assert set(store.stats) == set(by_cid)
    for cid, members in by_cid.items():
        assert store.stats[cid] == _expected_stats(members), (cid, sorted(members))


# --------------------------------------------------------------------------- #
# Activity propagation: members keep transacting BETWEEN structural mutations,
# and newly-clustered members transact IN the same batch they are clustered.
# The structural fold reads pre-batch `address` rows (the continuous delta
# clusters before the batch's writes commit), so `touched_activity` must carry
# each member's this-batch delta onto its cluster. Final cluster stats must equal
# the from-scratch member sum of the FINAL `address` rows — i.e. no drift.
# --------------------------------------------------------------------------- #
def _ed(aid, first, last, no_in, no_out, recv, spent):
    """One address's per-batch activity, as the delta loop's EntityDelta."""
    return EntityDelta(
        identifier=aid,
        total_received=_cur(recv),
        total_spent=_cur(spent),
        first_tx_id=first,
        last_tx_id=last,
        no_incoming_txs=no_in,
        no_outgoing_txs=no_out,
    )


class _GrowthStore(_FreshStore):
    """Fresh_* model whose `address` table GROWS: get_address_stats serves the
    accumulated per-address delta as it stood BEFORE the current batch's writes
    (mirroring the real ordering — clustering reads, then address rows commit)."""

    def __init__(self):
        super().__init__()
        self.address_table: dict = {}  # address_id -> accumulated EntityDelta

    def get_address_stats(self, address_ids):
        return {
            a: self.address_table[a] for a in address_ids if a in self.address_table
        }

    def commit_activity(self, activity):
        """Apply the batch's deferred `address`-row writes, AFTER clustering read."""
        for a, delta in activity.items():
            prev = self.address_table.get(a)
            self.address_table[a] = delta if prev is None else prev.merge(delta)


def _expected_from_rows(members, table):
    """From-scratch member sum of the FINAL address rows (a missing row counts a
    member with zero activity, exactly like the one-off's left join)."""
    it = iter(sorted(members))
    first = next(it)
    agg = _ClusterStats.from_address_row(first, table.get(first), 2)
    for a in it:
        agg = agg.merge(_ClusterStats.from_address_row(a, table.get(a), 2))
    return agg


def test_delta_matches_oneoff_with_activity_between_mutations():
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    # (co-spend edges, {address_id: this-batch EntityDelta}). Exercises: new
    # cluster w/ brand-new members' own-batch activity; an activity-only batch
    # (no co-spend); a merge with activity on both sides; a join of a brand-new
    # member alongside an existing member transacting.
    batches = [
        ([[1, 2]], {1: _ed(1, 1, 10, 1, 1, 100, 10), 2: _ed(2, 2, 12, 1, 1, 200, 20)}),
        (
            [[10, 11]],
            {10: _ed(10, 3, 13, 1, 1, 300, 30), 11: _ed(11, 4, 14, 1, 1, 400, 40)},
        ),
        ([], {1: _ed(1, 15, 25, 2, 2, 500, 50), 10: _ed(10, 16, 26, 2, 2, 600, 60)}),
        (
            [[2, 10]],
            {2: _ed(2, 27, 37, 3, 3, 700, 70), 10: _ed(10, 28, 38, 3, 3, 800, 80)},
        ),
        (
            [[1, 99]],
            {99: _ed(99, 40, 50, 4, 4, 900, 90), 11: _ed(11, 41, 51, 4, 4, 1000, 100)},
        ),
    ]

    store = _GrowthStore()
    db = _FreshStoreDb(store)
    for edges, activity in batches:
        cc = run_incremental_clustering(db, edges, touched_activity=activity)
        store.apply(cc)
        store.commit_activity(activity)  # deferred address writes land AFTER

    # everything collapses into one cluster ...
    by_cid: dict = {}
    for addr, cid in store.addr_to_cluster.items():
        by_cid.setdefault(cid, set()).add(addr)
    assert {frozenset(m) for m in by_cid.values()} == {frozenset({1, 2, 10, 11, 99})}

    # ... and its stats equal the member sum of the FINAL address rows: the
    # structural fold captured pre-cluster history, propagation captured every
    # batch's activity, and the two telescope to the from-scratch total.
    assert set(store.stats) == set(by_cid)
    for cid, members in by_cid.items():
        assert store.stats[cid] == _expected_from_rows(members, store.address_table), (
            cid,
            sorted(members),
        )


def test_activity_only_batch_updates_cluster_without_structural_change():
    """A single-input spend by an existing member (no co-spend) still refreshes
    its cluster — the case that used to drift until the weekly recompute."""
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    store = _GrowthStore()
    db = _FreshStoreDb(store)
    # form cluster {1,2} with initial activity
    a = {1: _ed(1, 1, 10, 1, 1, 100, 10), 2: _ed(2, 2, 12, 1, 1, 200, 20)}
    store.apply(run_incremental_clustering(db, [[1, 2]], touched_activity=a))
    store.commit_activity(a)
    cid = next(iter(store.stats))
    before = store.stats[cid]

    # member 1 transacts alone, no co-spend edge
    b = {1: _ed(1, 20, 30, 5, 6, 700, 70)}
    store.apply(run_incremental_clustering(db, [], touched_activity=b))
    store.commit_activity(b)

    after = store.stats[cid]
    # membership/root unchanged, activity columns advanced by exactly the delta
    assert after.no_addresses == before.no_addresses == 2
    assert after.min_address_id == before.min_address_id
    assert after.no_incoming_txs == before.no_incoming_txs + 5
    assert after.no_outgoing_txs == before.no_outgoing_txs + 6
    assert after.last_tx_id == 30
    assert after.total_received.value == before.total_received.value + 700
    # and it still equals the from-scratch member sum
    assert after == _expected_from_rows({1, 2}, store.address_table)


# --------------------------------------------------------------------------- #
# Fold math. The identity the incremental path rests on is
#
#     pre-batch structural fold + this-batch activity propagation
#         == from-scratch member sum of the FINAL address rows
#
# for every one of the eight `fresh_cluster_stats` columns. The tests above pin
# the individual moves; these pin the identity itself, on the folds that are
# easiest to get wrong: a member that is absorbed by a merge in the same batch
# it transacts, a chain merge that collapses three clusters at once, an address
# whose pre-join history propagation deliberately skipped because it was still a
# singleton. Every batch runs in production order — clustering plans against the
# pre-batch `address` rows, then the batch's address rows commit.
# --------------------------------------------------------------------------- #
def _clusters_by_id(store):
    by_cid: dict = {}
    for addr, cid in store.addr_to_cluster.items():
        by_cid.setdefault(cid, set()).add(addr)
    return by_cid


def _assert_member_sums(store, label):
    """Every stored cluster equals the from-scratch member sum of its addresses,
    and a stats row exists for exactly the clusters that have members."""
    by_cid = _clusters_by_id(store)
    assert set(store.stats) == set(by_cid), label
    for cid, members in by_cid.items():
        assert store.stats[cid] == _expected_from_rows(members, store.address_table), (
            label,
            cid,
            sorted(members),
        )


def _fold_batch(store, edges, activity):
    """One production-ordered batch: plan against the pre-batch rows, apply the
    planned diff, then land the deferred `address`-row writes."""
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    cc = run_incremental_clustering(
        _FreshStoreDb(store), edges, touched_activity=activity
    )
    store.apply(cc)
    store.commit_activity(activity)


def test_fold_new_cluster_with_same_batch_activity():
    """Both members are brand new and transact in the very batch that clusters
    them, so the structural fold finds no prior address row: the whole cluster's
    money has to arrive through activity propagation."""
    pytest.importorskip("gs_clustering")
    store = _GrowthStore()
    _fold_batch(
        store,
        [[1, 2]],
        {1: _ed(1, 1, 4, 1, 1, 100, 10), 2: _ed(2, 2, 5, 1, 1, 200, 20)},
    )
    _assert_member_sums(store, "new+activity-same-batch")


def test_fold_singleton_history_then_join_and_activity():
    """Address 3 transacts while still a singleton — propagation skips it,
    because a singleton has no cluster row to fold onto. When it later joins 4,
    that pre-join history must arrive via the structural fold of its address
    row, and the joining batch's own delta via propagation, with neither counted
    twice."""
    pytest.importorskip("gs_clustering")
    store = _GrowthStore()
    store.commit_activity({3: _ed(3, 1, 1, 1, 0, 500, 0)})  # no cluster yet
    _fold_batch(
        store,
        [[3, 4]],
        {3: _ed(3, 7, 7, 0, 1, 0, 60), 4: _ed(4, 8, 8, 1, 0, 90, 0)},
    )
    _assert_member_sums(store, "singleton-history+join+activity")


def test_fold_merge_with_activity_on_the_absorbed_member():
    """{3,4} is absorbed into {1,2} in the same batch that member 3 transacts.
    The merge folds the two *stored* aggregates and re-points 3 and 4 without
    re-adding their address rows; propagation then adds 3's delta to the
    survivor. Folding 3's address row a second time would show up here."""
    pytest.importorskip("gs_clustering")
    store = _GrowthStore()
    _fold_batch(store, [[1, 2]], {1: _ed(1, 1, 2, 1, 0, 100, 0)})
    _fold_batch(store, [[3, 4]], {3: _ed(3, 3, 4, 1, 0, 300, 0)})
    _fold_batch(store, [[2, 3]], {3: _ed(3, 9, 9, 0, 1, 0, 70)})

    assert _clusters_by_id(store) == {1: {1, 2, 3, 4}}
    _assert_member_sums(store, "merge+activity-on-absorbed")


def test_fold_chain_merge_in_one_batch_with_activity():
    """A single batch carries two co-spends that chain three existing clusters
    into one ({10,11}+{12,13}, then {12,13}+{14,15}) while two members transact.
    The survivor must land on the member sum of all six addresses."""
    pytest.importorskip("gs_clustering")
    store = _GrowthStore()
    _fold_batch(store, [[10, 11]], {10: _ed(10, 1, 1, 1, 0, 10, 0)})
    _fold_batch(store, [[12, 13]], {12: _ed(12, 2, 2, 1, 0, 20, 0)})
    _fold_batch(store, [[14, 15]], {14: _ed(14, 3, 3, 1, 0, 30, 0)})
    _fold_batch(
        store,
        [[11, 12], [13, 14]],
        {11: _ed(11, 5, 5, 0, 1, 0, 5), 13: _ed(13, 6, 6, 1, 0, 60, 0)},
    )

    assert _clusters_by_id(store) == {10: {10, 11, 12, 13, 14, 15}}
    _assert_member_sums(store, "chain-merge-one-batch+activity")


def test_fold_activity_only_batch_matches_member_sum():
    """A batch with no co-spend at all: nothing structural happens, both members
    transact, and the cluster still equals the member sum afterwards."""
    pytest.importorskip("gs_clustering")
    store = _GrowthStore()
    _fold_batch(store, [[1, 2]], {1: _ed(1, 1, 2, 1, 0, 100, 0)})
    _fold_batch(
        store, [], {1: _ed(1, 5, 6, 1, 1, 700, 70), 2: _ed(2, 7, 7, 1, 0, 5, 0)}
    )
    _assert_member_sums(store, "activity-only")


def test_fold_member_without_address_row():
    """Address 2 is clustered but never transacts, so it has no `address` row.
    ``from_address_row(None)`` must still count it as one member carrying zero
    money — which is what the one-off's left join does."""
    pytest.importorskip("gs_clustering")
    store = _GrowthStore()
    _fold_batch(store, [[1, 2]], {1: _ed(1, 1, 2, 1, 0, 100, 0)})

    assert 2 not in store.address_table
    assert store.stats[1].no_addresses == 2
    _assert_member_sums(store, "member-without-address-row")


@pytest.mark.parametrize("seed", [20260709, 1, 42])
def test_fold_identity_holds_under_random_batches(seed):
    """Seeded fuzz over random co-spends and random activity. After every batch
    — not just at the end — each stored cluster must equal the from-scratch
    member sum, so a fold error is caught in the batch that introduces it."""
    pytest.importorskip("gs_clustering")
    rng = random.Random(seed)
    addrs = list(range(1, 11))
    store = _GrowthStore()
    tx = 0

    for b in range(6):
        edges = [rng.sample(addrs, rng.randint(2, 3)) for _ in range(rng.randint(0, 2))]
        # every co-spend input moved coins, plus some unrelated addresses
        touched = {a for e in edges for a in e}
        activity = {}
        for a in sorted(touched | set(rng.sample(addrs, rng.randint(0, 3)))):
            tx += 1
            activity[a] = _ed(
                a,
                tx,
                tx + rng.randint(0, 2),
                rng.randint(0, 2),
                rng.randint(0, 2),
                rng.randint(0, 999),
                rng.randint(0, 999),
            )
        _fold_batch(store, edges, activity)
        _assert_member_sums(store, f"seed={seed} batch={b}")


# --------------------------------------------------------------------------- #
# Commit boundary. `fresh_cluster_stats` is an absolute row folded from its own
# stored value (`stored + this batch's activity`), so the batch's fresh_* writes
# must be committed together with the address rows, never ahead of them: a crash
# between the two commits replays the batch, and the fold would re-add activity
# that the stored row already carries. (The structural writes are replay-safe on
# their own — the redo finds the members assigned and the join no-ops — which is
# why this only became reachable once activity propagation was added.)
# --------------------------------------------------------------------------- #
def test_early_fresh_commit_double_counts_when_the_batch_is_replayed():
    """Guards the *reason* for staging: committing the fresh writes before the
    address rows makes a replayed batch fold the same activity twice."""
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    store = _GrowthStore()
    db = _FreshStoreDb(store)
    a = {1: _ed(1, 1, 10, 1, 1, 100, 10), 2: _ed(2, 2, 12, 1, 1, 200, 20)}
    store.apply(run_incremental_clustering(db, [[1, 2]], touched_activity=a))
    store.commit_activity(a)
    cid = next(iter(store.stats))

    # batch: activity only. Fresh rows commit ... then the process dies before
    # the address rows land, so the next run recomputes the whole batch.
    b = {1: _ed(1, 20, 30, 5, 6, 700, 70)}
    store.apply(run_incremental_clustering(db, [], touched_activity=b))
    store.apply(run_incremental_clustering(db, [], touched_activity=b))  # replay
    store.commit_activity(b)

    member_sum = _expected_from_rows({1, 2}, store.address_table)
    assert store.stats[cid].total_received.value == 1700
    assert member_sum.total_received.value == 1000  # 100 + 700 + 200


def test_replayed_batch_is_exact_when_fresh_writes_ride_the_batch_commit():
    """The staged ordering: nothing of a torn batch is committed, so the redo
    plans against untouched rows and lands on the exact member sum."""
    pytest.importorskip("gs_clustering")
    from graphsenselib.deltaupdate.update.utxo.update import run_incremental_clustering

    store = _GrowthStore()
    db = _FreshStoreDb(store)
    a = {1: _ed(1, 1, 10, 1, 1, 100, 10), 2: _ed(2, 2, 12, 1, 1, 200, 20)}
    store.apply(run_incremental_clustering(db, [[1, 2]], touched_activity=a))
    store.commit_activity(a)
    cid = next(iter(store.stats))

    # batch: planned, then the process dies before persist -> nothing committed.
    b = {1: _ed(1, 20, 30, 5, 6, 700, 70)}
    run_incremental_clustering(db, [], touched_activity=b)
    # redo: plan again against the untouched rows, then commit fresh + address
    # rows together, as persist_updater_progress does.
    store.apply(run_incremental_clustering(db, [], touched_activity=b))
    store.commit_activity(b)

    assert store.stats[cid] == _expected_from_rows({1, 2}, store.address_table)
    assert store.stats[cid].total_received.value == 1000


def _run_batch_hook(monkeypatch, cluster_inputs, activity):
    """Drive the real BATCH ``process_batch_impl_hook`` with its collaborators
    stubbed.  Returns (strategy, changes committed inline, args the hook passed
    to ``_clustering_changes_for``)."""
    import graphsenselib.deltaupdate.update.utxo.update as upd
    from graphsenselib.deltaupdate.update.generic import ApplicationStrategy

    addr_change = DbChange.new(table="address", data={"address_id": 1})
    fresh_change = DbChange.new(table="fresh_cluster_stats", data={"cluster_id": 1})

    monkeypatch.setattr(
        upd.parallelio, "fetch_block_transactions", lambda *a, **k: iter(())
    )
    monkeypatch.setattr(
        upd,
        "get_transaction_changes",
        lambda *a, **k: ([addr_change], 0, 0, 0, 0, cluster_inputs, activity),
    )
    monkeypatch.setattr(upd, "get_bookkeeping_changes", lambda *a, **k: [])
    monkeypatch.setattr(upd, "_check_gs_clustering", lambda: True)
    committed = []
    monkeypatch.setattr(upd, "apply_changes", lambda *a, **k: committed.append(a))

    s = object.__new__(upd.UpdateStrategyUtxo)
    s.application_strategy = ApplicationStrategy.BATCH
    s.crash_recoverer = MutableNamedTuple(is_in_recovery_mode=lambda: False)
    s._db = MutableNamedTuple(
        raw=MutableNamedTuple(get_block_timestamp=lambda b: 0),
        transformed=MutableNamedTuple(
            get_exchange_rates_by_block=lambda b: MutableNamedTuple(fiat_values=[1.0]),
            get_summary_statistics=lambda: None,
        ),
    )
    s._parallel_pool = None
    s._patch_mode = False
    s._statistics = None
    s._batch_start_time = time.time()
    s._highest_address_id = 0
    s._timing_cassandra_read = s._timing_transform = s._timing_persist = 0.0
    s._fresh_clustering_active = lambda: True

    seen = []

    def _plan(ci, ta):
        seen.append((ci, ta))
        return [fresh_change]

    s._clustering_changes_for = _plan
    s.process_batch_impl_hook([100])
    return s, committed, seen, addr_change, fresh_change


def test_batch_hook_stages_clustering_changes_instead_of_committing(monkeypatch):
    """The BATCH delta must append its fresh_* writes to ``self.changes`` (one
    commit with the address rows + bookkeeping, one WAL record) and must not
    write them inline."""
    activity = {7: _ed(7, 1, 2, 1, 0, 5, 0)}
    s, committed, seen, addr_change, fresh_change = _run_batch_hook(
        monkeypatch, [[1, 2]], activity
    )

    assert s.changes == [addr_change, fresh_change]
    assert committed == [], "clustering changes must be staged, not committed inline"
    # the batch's activity must reach the planner, not just the co-spend edges
    assert seen == [([[1, 2]], activity)]


def test_batch_hook_stages_activity_only_batch(monkeypatch):
    """A batch with activity but no multi-input co-spend must STILL plan and
    stage its fresh_* writes.

    This is the common case: members keep transacting between structural
    mutations. If the hook only fired on ``cluster_inputs`` the money columns
    would silently go stale again — exactly the production drift that activity
    propagation exists to eliminate — while every other test stayed green.
    """
    activity = {7: _ed(7, 1, 2, 1, 0, 5, 0)}
    s, committed, seen, addr_change, fresh_change = _run_batch_hook(
        monkeypatch, [], activity
    )

    assert s.changes == [addr_change, fresh_change]
    assert committed == []
    assert seen == [([], activity)]


def test_batch_hook_skips_clustering_when_nothing_touched(monkeypatch):
    """No co-spends and no activity: nothing to plan, nothing to stage."""
    s, committed, seen, addr_change, _ = _run_batch_hook(monkeypatch, [], {})

    assert s.changes == [addr_change]
    assert committed == []
    assert seen == [], "planner must not run for an untouched batch"
