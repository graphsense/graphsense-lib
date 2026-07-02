"""Unit tests for the fresh_cluster_stats recompute transforms.

Pure DataFrame transforms (no Cassandra), driven with synthetic frames on the
local ``spark`` fixture, mirroring the multi_input_address_id_sets test style.
Covers the additive per-member stats, the relation-derived degrees/tx-counts
(including self-edge exclusion and singleton-neighbour mapping), and the
combined frame.
"""

import pytest

from graphsenselib.transformation.clustering import (
    cluster_additive_stats,
    cluster_relation_stats,
    cluster_tx_counts,
    compute_fresh_cluster_stats,
)

pyspark = pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    FloatType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

_CURRENCY = StructType(
    [
        StructField("value", LongType()),
        StructField("fiat_values", ArrayType(FloatType())),
    ]
)

_ADDRESS_SCHEMA = StructType(
    [
        StructField("address_id", IntegerType()),
        StructField("total_received", _CURRENCY),
        StructField("total_spent", _CURRENCY),
        StructField("first_tx_id", LongType()),
        StructField("last_tx_id", LongType()),
    ]
)
_MEMBERS_SCHEMA = StructType(
    [
        StructField("address_id", IntegerType()),
        StructField("cluster_id", IntegerType()),
    ]
)
_REL_SCHEMA = StructType(
    [
        StructField("src_address_id", IntegerType()),
        StructField("dst_address_id", IntegerType()),
        StructField("no_transactions", IntegerType()),
        StructField("estimated_value", _CURRENCY),
    ]
)
_IN_REL_SCHEMA = StructType(
    [
        StructField("dst_address_id", IntegerType()),
        StructField("src_address_id", IntegerType()),
        StructField("no_transactions", IntegerType()),
        StructField("estimated_value", _CURRENCY),
    ]
)
_ADDRESS_TXS_SCHEMA = StructType(
    [
        StructField("address_id", IntegerType()),
        StructField("tx_id", LongType()),
        StructField("value", LongType()),  # SIGNED net flow (negative = spent)
    ]
)

# Cluster 1 = {1,2,3}; cluster 4 = {4,5}. Fiat values are whole numbers so sums
# are exact floats.
_MEMBERS = [(1, 1), (2, 1), (3, 1), (4, 4), (5, 4)]
_ADDRESSES = [
    (1, (100, [1.0, 2.0]), (10, [1.0, 1.0]), 10, 50),
    (2, (200, [2.0, 4.0]), (20, [2.0, 2.0]), 5, 60),
    (3, (300, [3.0, 6.0]), (30, [3.0, 3.0]), 20, 40),
    (4, (40, [4.0, 8.0]), (4, [4.0, 4.0]), 15, 55),
    (5, (50, [5.0, 10.0]), (5, [5.0, 5.0]), 25, 35),
]
# external edges + one intra-cluster (self) edge that must be dropped. The
# estimated_value of external edges feeds total_*_adj; the self edge's value
# (999) must never appear in any adjusted total.
_OUT_REL = [
    (1, 4, 2, (100, [1.0, 2.0])),  # c1 -> c4
    (2, 4, 3, (200, [2.0, 4.0])),  # c1 -> c4 (same neighbour, sums)
    (3, 1, 1, (999, [9.0, 9.0])),  # c1 -> c1 self, dropped
    (4, 99, 5, (70, [7.0, 14.0])),  # c4 -> singleton 99
]
_IN_REL = [
    (4, 1, 2, (100, [1.0, 2.0])),  # c4 <- c1
    (4, 2, 3, (200, [2.0, 4.0])),  # c4 <- c1
    (1, 3, 1, (999, [9.0, 9.0])),  # c1 <- c1 self, dropped
    (99, 4, 5, (70, [7.0, 14.0])),  # singleton 99 <- c4
]
# address_transactions: (address_id, tx_id, value) with value = SIGNED net flow.
# Two over-counts the cluster-level netting must avoid:
#   tx 1000 — multi-input co-spend of all three c1 members (one cluster tx);
#   tx 5000 — the change pattern: c1 spends from member 1 and sends change to a
#   *different* member 2 in the SAME tx. Per-address flags would count tx 5000
#   as BOTH outgoing (member 1) and incoming (member 2); the cluster net is
#   -61 < 0, so it is one OUTGOING tx. tx 6000 nets to exactly 0 -> neither.
_ADDRESS_TXS = [
    (1, 1000, -100),
    (2, 1000, -50),
    (3, 1000, -30),  # c1 multi-input tx, net -180 -> 1 outgoing
    (1, 5000, -100),
    (2, 5000, 39),  # c1 change pattern, net -61 -> 1 outgoing (not both)
    (2, 2000, 100),
    (3, 2000, 50),  # c1 receive, net +150 -> 1 incoming
    (1, 6000, -50),
    (2, 6000, 50),  # c1 net 0 -> counted in neither direction
    (4, 3000, -70),  # c4 outgoing
    (4, 4000, 40),
    (5, 4000, 30),  # c4 receive, net +70 -> 1 incoming
]


def _frames(spark):
    return (
        spark.createDataFrame(_MEMBERS, _MEMBERS_SCHEMA),
        spark.createDataFrame(_ADDRESSES, _ADDRESS_SCHEMA),
        spark.createDataFrame(_IN_REL, _IN_REL_SCHEMA),
        spark.createDataFrame(_OUT_REL, _REL_SCHEMA),
    )


def _txs_frame(spark):
    return spark.createDataFrame(_ADDRESS_TXS, _ADDRESS_TXS_SCHEMA)


def _by_cluster(rows):
    return {r["cluster_id"]: r.asDict(recursive=True) for r in rows}


def test_additive_stats(spark):
    members, address, _, _ = _frames(spark)
    out = _by_cluster(cluster_additive_stats(members, address).collect())

    assert out[1]["no_addresses"] == 3
    assert out[1]["min_address_id"] == 1
    assert out[1]["first_tx_id"] == 5
    assert out[1]["last_tx_id"] == 60
    assert out[1]["total_received"]["value"] == 600
    assert out[1]["total_received"]["fiat_values"] == pytest.approx([6.0, 12.0])
    assert out[1]["total_spent"]["value"] == 60
    assert out[1]["total_spent"]["fiat_values"] == pytest.approx([6.0, 6.0])

    assert out[4]["no_addresses"] == 2
    assert out[4]["min_address_id"] == 4
    assert out[4]["first_tx_id"] == 15
    assert out[4]["last_tx_id"] == 55
    assert out[4]["total_received"]["value"] == 90
    assert out[4]["total_received"]["fiat_values"] == pytest.approx([9.0, 18.0])


def test_relation_stats_excludes_self_and_maps_singletons(spark):
    members, _, in_rel, out_rel = _frames(spark)
    out = _by_cluster(cluster_relation_stats(members, in_rel, out_rel).collect())

    # c1 only sends externally (to c4); its only incoming edge was the self-edge.
    assert out[1]["out_degree"] == 1
    assert out[1].get("in_degree") in (None, 0)
    # total_spent_adj = external out value (100 + 200); self-edge 999 excluded.
    assert out[1]["total_spent_adj"]["value"] == 300
    assert out[1]["total_spent_adj"]["fiat_values"] == pytest.approx([3.0, 6.0])
    assert out[1].get("total_received_adj") is None  # no external incoming

    # c4 receives from c1 and sends to singleton 99.
    assert out[4]["in_degree"] == 1
    assert out[4]["out_degree"] == 1
    # relation stats no longer carry tx-counts (moved to cluster_tx_counts)
    assert "no_incoming_txs" not in out[4]
    assert "no_outgoing_txs" not in out[4]
    # total_received_adj = external in value (100 + 200); total_spent_adj = 70.
    assert out[4]["total_received_adj"]["value"] == 300
    assert out[4]["total_received_adj"]["fiat_values"] == pytest.approx([3.0, 6.0])
    assert out[4]["total_spent_adj"]["value"] == 70
    assert out[4]["total_spent_adj"]["fiat_values"] == pytest.approx([7.0, 14.0])

    # singleton 99 is only ever an edge NEIGHBOUR, never a multi-member anchor,
    # so anchor-pruning drops it from the relation-stats output — but it is still
    # counted as c4's out-neighbour (out_degree == 1 above), which is all the
    # downstream stats need (the additive base carries no singleton row anyway).
    assert 99 not in out


def test_tx_counts_are_netted_per_cluster(spark):
    members = spark.createDataFrame(_MEMBERS, _MEMBERS_SCHEMA)
    out = _by_cluster(cluster_tx_counts(members, _txs_frame(spark)).collect())

    # c1 outgoing: tx 1000 (multi-input, net<0) and tx 5000 (change pattern,
    # net<0) -> 2. The change tx must NOT also inflate incoming.
    assert out[1]["no_outgoing_txs"] == 2
    # c1 incoming: only tx 2000 (net>0). tx 5000 is outgoing-only, tx 6000 nets
    # to zero -> neither. A per-address flag count would wrongly give 2 here.
    assert out[1]["no_incoming_txs"] == 1
    assert out[4]["no_outgoing_txs"] == 1  # tx 3000
    assert out[4]["no_incoming_txs"] == 1  # tx 4000


def test_compute_full_stats_left_joins_and_zero_fills(spark):
    members, address, in_rel, out_rel = _frames(spark)
    out = _by_cluster(
        compute_fresh_cluster_stats(
            members, address, _txs_frame(spark), in_rel, out_rel
        ).collect()
    )

    # only real clusters survive (singleton 99 is not in the additive base)
    assert set(out) == {1, 4}

    # c1 has no external incoming *edges* -> in_degree zero-filled; but it does
    # have an incoming *tx* (2000), so tx-count is node-level, decoupled from degree.
    assert out[1]["in_degree"] == 0
    assert out[1]["no_incoming_txs"] == 1
    assert out[1]["out_degree"] == 1
    assert out[1]["no_outgoing_txs"] == 2  # netted {1000, 5000}
    assert out[1]["no_addresses"] == 3
    assert out[1]["total_received"]["value"] == 600
    # total_spent_adj summed; total_received_adj zero-filled (no external in).
    assert out[1]["total_spent_adj"]["value"] == 300
    assert out[1]["total_received_adj"]["value"] == 0
    assert out[1]["total_received_adj"]["fiat_values"] == []

    assert out[4]["in_degree"] == 1
    assert out[4]["out_degree"] == 1
    assert out[4]["no_incoming_txs"] == 1
    assert out[4]["no_outgoing_txs"] == 1
    assert out[4]["total_received_adj"]["value"] == 300
    assert out[4]["total_spent_adj"]["value"] == 70


def test_delete_stale_rows_groups_and_chunks():
    """The stale-row deleter issues single-partition deletes in bounded IN chunks."""
    from graphsenselib.transformation.cli import _delete_fresh_cluster_stats_rows

    class FakeDb:
        def __init__(self):
            self.cql = []

        def execute_raw_cql(self, cql):
            self.cql.append(cql)

    db = FakeDb()
    keys = [(0, i) for i in range(1200)] + [(7, 35001), (7, 35002)]
    _delete_fresh_cluster_stats_rows(db, keys)

    group0 = [c for c in db.cql if "cluster_id_group = 0 " in c]
    group7 = [c for c in db.cql if "cluster_id_group = 7 " in c]
    assert len(db.cql) == 4  # 1200 -> 500 + 500 + 200, plus one for group 7
    assert [c.count(",") + 1 for c in group0] == [500, 500, 200]
    assert group7 == [
        "DELETE FROM fresh_cluster_stats "
        "WHERE cluster_id_group = 7 AND cluster_id IN (35001,35002)"
    ]
    assert all(c.startswith("DELETE FROM fresh_cluster_stats WHERE") for c in db.cql)
