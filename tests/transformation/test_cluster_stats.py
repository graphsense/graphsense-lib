"""Unit tests for the fresh_cluster_stats recompute transforms.

Pure DataFrame transforms (no Cassandra), driven with synthetic frames on the
local ``spark`` fixture, mirroring the multi_input_address_id_sets test style.
Every column is a member sum / extremum of the cluster's ``address`` rows —
including ``no_incoming_txs`` / ``no_outgoing_txs``, which are plain sums of the
per-address counts (the same member-sum semantics the incremental delta path
maintains, an intentional overcount vs a true cluster-level netting). Degrees
and adjusted totals are intentionally absent — the recompute no longer scans the
relations tables; REST fills degrees from the legacy ``cluster`` table.
"""

import pytest

from graphsenselib.transformation.clustering import (
    cluster_additive_stats,
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
        StructField("no_incoming_txs", IntegerType()),
        StructField("no_outgoing_txs", IntegerType()),
    ]
)
_MEMBERS_SCHEMA = StructType(
    [
        StructField("address_id", IntegerType()),
        StructField("cluster_id", IntegerType()),
    ]
)

# Cluster 1 = {1,2,3}; cluster 4 = {4,5}. Fiat values are whole numbers so sums
# are exact floats. Per-address (no_incoming_txs, no_outgoing_txs) are the last
# two fields; the cluster value is their plain member sum:
#   c1 incoming = 2+1+4 = 7, outgoing = 3+2+0 = 5
#   c4 incoming = 1+3    = 4, outgoing = 1+2   = 3
_MEMBERS = [(1, 1), (2, 1), (3, 1), (4, 4), (5, 4)]
_ADDRESSES = [
    (1, (100, [1.0, 2.0]), (10, [1.0, 1.0]), 10, 50, 2, 3),
    (2, (200, [2.0, 4.0]), (20, [2.0, 2.0]), 5, 60, 1, 2),
    (3, (300, [3.0, 6.0]), (30, [3.0, 3.0]), 20, 40, 4, 0),
    (4, (40, [4.0, 8.0]), (4, [4.0, 4.0]), 15, 55, 1, 1),
    (5, (50, [5.0, 10.0]), (5, [5.0, 5.0]), 25, 35, 3, 2),
]


def _frames(spark):
    return (
        spark.createDataFrame(_MEMBERS, _MEMBERS_SCHEMA),
        spark.createDataFrame(_ADDRESSES, _ADDRESS_SCHEMA),
    )


def _by_cluster(rows):
    return {r["cluster_id"]: r.asDict(recursive=True) for r in rows}


def test_additive_stats(spark):
    members, address = _frames(spark)
    out = _by_cluster(cluster_additive_stats(members, address).collect())

    assert out[1]["no_addresses"] == 3
    assert out[1]["min_address_id"] == 1
    assert out[1]["first_tx_id"] == 5
    assert out[1]["last_tx_id"] == 60
    assert out[1]["no_incoming_txs"] == 7  # 2+1+4 member sum
    assert out[1]["no_outgoing_txs"] == 5  # 3+2+0 member sum
    assert out[1]["total_received"]["value"] == 600
    assert out[1]["total_received"]["fiat_values"] == pytest.approx([6.0, 12.0])
    assert out[1]["total_spent"]["value"] == 60
    assert out[1]["total_spent"]["fiat_values"] == pytest.approx([6.0, 6.0])

    assert out[4]["no_addresses"] == 2
    assert out[4]["min_address_id"] == 4
    assert out[4]["first_tx_id"] == 15
    assert out[4]["last_tx_id"] == 55
    assert out[4]["no_incoming_txs"] == 4  # 1+3
    assert out[4]["no_outgoing_txs"] == 3  # 1+2
    assert out[4]["total_received"]["value"] == 90
    assert out[4]["total_received"]["fiat_values"] == pytest.approx([9.0, 18.0])


def test_compute_full_stats(spark):
    members, address = _frames(spark)
    out = _by_cluster(compute_fresh_cluster_stats(members, address).collect())

    # only real clusters survive
    assert set(out) == {1, 4}

    assert out[1]["no_incoming_txs"] == 7
    assert out[1]["no_outgoing_txs"] == 5
    assert out[1]["no_addresses"] == 3
    assert out[1]["total_received"]["value"] == 600

    assert out[4]["no_incoming_txs"] == 4
    assert out[4]["no_outgoing_txs"] == 3
    assert out[4]["total_received"]["value"] == 90

    # degrees / adjusted totals are no longer part of the recompute output
    assert "in_degree" not in out[1]
    assert "out_degree" not in out[1]
    assert "total_received_adj" not in out[1]
    assert "total_spent_adj" not in out[1]


def test_orphan_member_contributes_zero(spark):
    # a membership row with no matching address row is counted in no_addresses
    # but contributes nothing: its null columns coalesce to 0 in the sums.
    members = spark.createDataFrame(_MEMBERS + [(6, 6)], _MEMBERS_SCHEMA)
    _, address = _frames(spark)  # address has no row for id 6

    out = _by_cluster(compute_fresh_cluster_stats(members, address).collect())

    assert out[6]["no_addresses"] == 1
    assert out[6]["no_incoming_txs"] == 0
    assert out[6]["no_outgoing_txs"] == 0
    assert out[1]["no_incoming_txs"] == 7  # unaffected


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
