"""Unit tests for the fresh_cluster_stats recompute transforms.

Pure DataFrame transforms (no Cassandra), driven with synthetic frames on the
local ``spark`` fixture, mirroring the multi_input_address_id_sets test style.
Covers the additive per-member stats, the netted tx-counts, and the combined
frame. Degrees and adjusted totals are intentionally absent — the recompute no
longer scans the relations tables; REST fills degrees from the legacy
``cluster`` table.
"""

import pytest

from graphsenselib.transformation.clustering import (
    cluster_additive_stats,
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
    )


def _txs_frame(spark):
    return spark.createDataFrame(_ADDRESS_TXS, _ADDRESS_TXS_SCHEMA)


def _by_cluster(rows):
    return {r["cluster_id"]: r.asDict(recursive=True) for r in rows}


def test_additive_stats(spark):
    members, address = _frames(spark)
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
    members, address = _frames(spark)
    out = _by_cluster(
        compute_fresh_cluster_stats(members, address, _txs_frame(spark)).collect()
    )

    # only real clusters survive (singleton 99 is not in the additive base)
    assert set(out) == {1, 4}

    assert out[1]["no_incoming_txs"] == 1
    assert out[1]["no_outgoing_txs"] == 2  # netted {1000, 5000}
    assert out[1]["no_addresses"] == 3
    assert out[1]["total_received"]["value"] == 600

    assert out[4]["no_incoming_txs"] == 1
    assert out[4]["no_outgoing_txs"] == 1
    assert out[4]["total_received"]["value"] == 90

    # degrees / adjusted totals are no longer part of the recompute output
    assert "in_degree" not in out[1]
    assert "out_degree" not in out[1]
    assert "total_received_adj" not in out[1]
    assert "total_spent_adj" not in out[1]


def test_zero_fill_when_cluster_has_no_txs(spark):
    members, address = _frames(spark)
    # only c4 has txs; c1's tx-counts must zero-fill through the left join
    c4_only = [(4, 3000, -70)]
    txs = spark.createDataFrame(c4_only, _ADDRESS_TXS_SCHEMA)
    out = _by_cluster(compute_fresh_cluster_stats(members, address, txs).collect())

    assert out[1]["no_incoming_txs"] == 0
    assert out[1]["no_outgoing_txs"] == 0
    assert out[4]["no_outgoing_txs"] == 1


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
