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
    BooleanType,
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
        StructField("is_outgoing", BooleanType()),
        StructField("tx_id", LongType()),
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
# address_transactions: (address_id, is_outgoing, tx_id). tx 1000 is a
# multi-input tx co-spending all three c1 members — ONE cluster tx that appears
# as three rows; distinct counting must collapse it (a plain count would say 3).
_ADDRESS_TXS = [
    (1, True, 1000),
    (2, True, 1000),
    (3, True, 1000),  # c1 multi-input tx -> 1 distinct outgoing
    (1, True, 1001),  # c1 second outgoing tx
    (2, False, 2000),
    (3, False, 2000),  # c1 multi-output receive -> 1 distinct incoming
    (4, True, 3000),  # c4 outgoing
    (4, False, 4000),
    (5, False, 4000),  # c4 receive -> 1 distinct incoming
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

    # singleton 99 appears as a neighbour-derived row (filtered out later by the
    # additive base, which only has real clusters).
    assert 99 in out


def test_tx_counts_are_distinct_per_cluster(spark):
    members = spark.createDataFrame(_MEMBERS, _MEMBERS_SCHEMA)
    out = _by_cluster(cluster_tx_counts(members, _txs_frame(spark)).collect())

    # tx 1000 co-spends all three c1 members but is ONE cluster tx: distinct
    # gives 2 outgoing ({1000, 1001}), NOT 4 (the plain-count over-count).
    assert out[1]["no_outgoing_txs"] == 2
    assert out[1]["no_incoming_txs"] == 1  # {2000}, not 2
    assert out[4]["no_outgoing_txs"] == 1  # {3000}
    assert out[4]["no_incoming_txs"] == 1  # {4000}, not 2


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
    assert out[1]["no_outgoing_txs"] == 2  # distinct {1000, 1001}
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
