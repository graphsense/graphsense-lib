"""Unit tests for the Spark multi-input edge derivation.

Pure DataFrame transform (no Cassandra), driven with synthetic frames on the
local ``spark`` fixture. Mirrors the single-tx harvest tests in
``tests.deltaupdate.test_fresh_clustering`` — the two producers must stay in
sync: >= 2 distinct resolved input address_ids of a non-coinbase, non-coinjoin
transaction form one Union-Find edge. Coinjoin filtering follows the legacy
Scala clustering (``removeCoinJoin``); a NULL coinjoin flag counts as
not-coinjoin (rows are kept, unlike Scala's ``=== false`` which drops them).
"""

import pytest

from graphsenselib.transformation.clustering import multi_input_address_id_sets

pyspark = pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_TX_SCHEMA = StructType(
    [
        StructField("tx_id", LongType()),
        StructField("block_id", IntegerType()),
        StructField("coinbase", BooleanType()),
        StructField(
            "inputs",
            ArrayType(StructType([StructField("address", ArrayType(StringType()))])),
        ),
        StructField("coinjoin", BooleanType()),
    ]
)
_ADDRESS_IDS_SCHEMA = StructType(
    [
        StructField("address", StringType()),
        StructField("address_id", IntegerType()),
    ]
)

# plain 2-address edge, a coinjoin co-spend, and a NULL-flag edge
_TXS = [
    (1, 10, False, [(["A"],), (["B"],)], False),
    (2, 11, False, [(["C"],), (["D"],)], True),
    (3, 12, False, [(["A"],), (["D"],)], None),
]
_ADDRESS_IDS = [("A", 10), ("B", 20), ("C", 30), ("D", 40)]


def _edges(spark, exclude_coinjoin, txs=_TXS):
    tx_df = spark.createDataFrame(txs, _TX_SCHEMA)
    ids_df = spark.createDataFrame(_ADDRESS_IDS, _ADDRESS_IDS_SCHEMA)
    rows = multi_input_address_id_sets(
        tx_df, ids_df, exclude_coinjoin=exclude_coinjoin
    ).collect()
    return sorted(sorted(r["ids"]) for r in rows)


def test_coinjoin_edges_excluded_by_default(spark):
    assert _edges(spark, exclude_coinjoin=True) == [[10, 20], [10, 40]]


def test_coinjoin_edges_kept_when_filtering_disabled(spark):
    assert _edges(spark, exclude_coinjoin=False) == [[10, 20], [10, 40], [30, 40]]


def test_null_coinjoin_counts_as_not_coinjoin(spark):
    # tx 3 carries NULL: it must survive the filter, not be dropped with it
    only_null = [t for t in _TXS if t[0] == 3]
    assert _edges(spark, exclude_coinjoin=True, txs=only_null) == [[10, 40]]
