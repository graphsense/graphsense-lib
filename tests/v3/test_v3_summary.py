"""The summary_statistics row.

Small, and more load-bearing than it looks: REST picks the latest derived
keyspace by reading `no_blocks` from it, and `/stats` serves the whole row.
"""

from decimal import Decimal

import pytest

from graphsense_v3.codec import tx_id
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import summary
from graphsense_v3.spark.writer import conformance_errors

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def blocks(spark):
    return spark.createDataFrame(
        [
            {"block_id": 0, "timestamp": 100},
            {"block_id": 1, "timestamp": 200},
            {"block_id": 2, "timestamp": 300},
        ],
        schema="block_id INT, timestamp BIGINT",
    )


@pytest.fixture(scope="module")
def transactions(spark):
    return spark.createDataFrame(
        [{"tx_id": tx_id(b, i)} for b, i in ((0, 0), (1, 0), (1, 1), (2, 0))],
        schema="tx_id BIGINT",
    )


def test_the_row_describes_the_range_it_covers(spark, blocks, transactions) -> None:
    """v2 recorded only `no_blocks`, a height called a count. Both ends make a
    partial keyspace say what it holds instead of implying a height it does not
    reach."""
    row = summary.raw_statistics(spark, blocks, transactions).collect()[0]
    assert (row["lowest_block"], row["highest_block"]) == (0, 2)
    assert row["timestamp"] == 300


def test_a_mid_chain_slice_says_where_it_starts(spark, transactions) -> None:
    blocks = spark.createDataFrame(
        [{"block_id": 500, "timestamp": 1}, {"block_id": 502, "timestamp": 2}],
        schema="block_id INT, timestamp BIGINT",
    )
    row = summary.raw_statistics(spark, blocks, transactions).collect()[0]
    assert (row["lowest_block"], row["highest_block"]) == (500, 502)


def test_transactions_are_counted_not_derived_from_an_id(
    spark, blocks, transactions
) -> None:
    """v2 ingest computes this as last_tx_id + 1, which was only right because
    the id was a dense counter. Under D12 a tx_id is (block_id << 32) + index
    and is sparse, so that arithmetic would give 4.3 billion here."""
    row = summary.raw_statistics(spark, blocks, transactions).collect()[0]
    assert row["no_transactions"] == 4
    highest = max(r["tx_id"] for r in transactions.collect())
    assert highest + 1 != row["no_transactions"]


def test_an_empty_range_reports_zero_not_a_crash(spark, transactions) -> None:
    empty = spark.createDataFrame([], schema="block_id INT, timestamp BIGINT")
    row = summary.raw_statistics(spark, empty, transactions).collect()[0]
    assert (row["lowest_block"], row["highest_block"], row["timestamp"]) == (0, 0, 0)


@pytest.fixture(scope="module")
def address_stats(spark):
    return spark.createDataFrame(
        [{"address": bytes([i])} for i in range(5)], schema="address BINARY"
    )


@pytest.fixture(scope="module")
def relations(spark):
    return spark.createDataFrame(
        [
            {"src_address": b"\x01", "dst_address": b"\x02", "value": Decimal(1)},
            {"src_address": b"\x02", "dst_address": b"\x03", "value": Decimal(1)},
        ],
        schema="src_address BINARY, dst_address BINARY, value DECIMAL(38,0)",
    )


def test_relations_are_counted_once_not_twice(
    spark, blocks, transactions, address_stats, relations
) -> None:
    """Relations are stored in both directions, so counting rows across both
    tables would double the figure. This counts one direction."""
    row = summary.derived_statistics(
        spark, blocks, transactions, address_stats, relations
    ).collect()[0]
    assert row["no_address_relations"] == 2
    assert row["no_addresses"] == 5


def test_the_row_describes_only_its_own_keyspace(
    spark, blocks, transactions, address_stats, relations
) -> None:
    """v2 also recorded how far the RAW keyspace had got, so the derived one
    could report the lag -- a second keyspace's fact, copied, and able to go
    stale. v3 writes both from one range in one run, so there is no lag."""
    row = summary.derived_statistics(
        spark, blocks, transactions, address_stats, relations
    ).collect()[0]
    assert "highest_block_transform" not in row.asDict()
    assert "timestamp_transform" not in row.asDict()
    assert row["highest_block"] == 2


@pytest.mark.parametrize("kind", list(Kind))
def test_the_row_conforms_to_its_table(
    spark, blocks, transactions, address_stats, relations, kind
) -> None:
    table = schema_for("btc", kind).table("summary_statistics")
    frame = (
        summary.raw_statistics(spark, blocks, transactions)
        if kind is Kind.RAW
        else summary.derived_statistics(
            spark, blocks, transactions, address_stats, relations
        )
    )
    assert conformance_errors(list(frame.columns), table) == []
