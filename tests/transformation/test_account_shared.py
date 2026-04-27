"""Cross-chain regression tests for the EVM transformation contract.

These run against both ETH (`AccountTransformation`) and TRX
(`AccountTrxTransformation`) since both inherit from
`AccountTransformationBase` — same drop/cast/rename logic for block,
transaction, and log. Chain-specific traces and tables live in the per-chain
test files.

Includes the access_list.storageKeys → storage_keys regression (b8905eb) for
both chains, since both share the `access_list_entry` UDT.
"""

import pytest

from graphsenselib.transformation.account import AccountTransformation
from graphsenselib.transformation.account_trx import AccountTrxTransformation


@pytest.fixture(
    params=[AccountTransformation, AccountTrxTransformation], ids=["eth", "trx"]
)
def transformer(request, spark, install_harness):
    return install_harness(
        request.param(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_raw",
        )
    )


def test_block_drops_delta_only_cols(spark, transformer):
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("partition", IntegerType()),
            StructField("block_id", IntegerType()),
            StructField("difficulty", BinaryType()),
            StructField("total_difficulty", BinaryType()),
            StructField("transaction_count", IntegerType()),
            StructField("withdrawals", ArrayType(IntegerType())),
            StructField("excess_blob_gas", IntegerType()),
        ]
    )
    rows = [
        (
            0,
            17_000_000,
            (12345).to_bytes(8, "big"),
            (99).to_bytes(2, "big"),
            250,
            [],
            0,
        )
    ]
    transformer._read_stub["block"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_block(start_block=17_000_000, end_block=17_000_000)
    out = transformer._captured["block"]
    cols = set(out.columns)

    assert "partition" not in cols
    assert "withdrawals" not in cols
    assert "excess_blob_gas" not in cols
    assert "block_id_group" in cols
    assert out.schema["transaction_count"].dataType.simpleString() == "smallint"
    row = out.collect()[0].asDict()
    assert row["difficulty"] == "12345"
    assert row["total_difficulty"] == "99"


def test_transaction_renames_access_list_storage_keys(spark, transformer):
    """Regression for the b8905eb fix.

    EIP-2930 access lists carry `storageKeys` in Delta but the Cassandra UDT
    `access_list_entry` defines `storage_keys`. The connector binds struct
    fields to UDT fields by name and throws IllegalArgumentException on the
    first populated access list when names don't match.
    """
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    access_entry = StructType(
        [
            StructField("address", BinaryType()),
            StructField("storageKeys", ArrayType(BinaryType())),
        ]
    )
    schema = StructType(
        [
            StructField("partition", IntegerType()),
            StructField("tx_hash_prefix", StringType()),
            StructField("tx_hash", BinaryType()),
            StructField("transaction_index", IntegerType()),
            StructField("v", IntegerType()),
            StructField("access_list", ArrayType(access_entry)),
        ]
    )
    rows = [
        (
            0,
            "ab",
            b"\xab\xcd",
            5,
            27,
            [(b"\x01" * 20, [b"\x02" * 32, b"\x03" * 32])],
        )
    ]
    transformer._read_stub["transaction"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_transaction(start_block=12_300_000, end_block=12_300_000)
    out = transformer._captured["transaction"]

    field_names = [
        f.name for f in out.schema["access_list"].dataType.elementType.fields
    ]
    assert field_names == ["address", "storage_keys"], (
        f"access_list must rename storageKeys → storage_keys, got {field_names}"
    )

    row = out.collect()[0].asDict(recursive=True)
    assert row["access_list"][0]["storage_keys"] == [b"\x02" * 32, b"\x03" * 32]


def test_transaction_pre_berlin_no_access_list(spark, transformer):
    """Legacy txs (pre-block 12,244,000) have no access_list column at all."""
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("partition", IntegerType()),
            StructField("tx_hash_prefix", StringType()),
            StructField("tx_hash", BinaryType()),
            StructField("transaction_index", IntegerType()),
            StructField("v", IntegerType()),
        ]
    )
    rows = [(0, "01", b"\x01" * 32, 0, 27)]
    transformer._read_stub["transaction"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_transaction(start_block=10_000_000, end_block=10_000_000)
    out = transformer._captured["transaction"]
    assert "access_list" not in out.columns
    assert "partition" not in out.columns
    assert out.schema["v"].dataType.simpleString() == "smallint"
    assert out.count() == 1


def test_ingest_complete_marker_writes_state_row(spark, transformer):
    """REST auto-discovery treats this row's presence as the readiness signal."""
    from datetime import datetime

    from graphsenselib.db.state import INGEST_COMPLETE_KEY

    transformer.write_ingest_complete_marker()
    out = transformer._captured["state"]

    assert set(out.columns) == {"key", "value", "updated_at"}
    assert out.schema["updated_at"].dataType.simpleString() == "timestamp"
    row = out.collect()[0].asDict()
    assert row["key"] == INGEST_COMPLETE_KEY
    # value is a tz-aware ISO string matching `updated_at` to the second.
    parsed = datetime.fromisoformat(row["value"])
    assert parsed.tzinfo is not None
    # Spark normalizes timestamps through the session timezone and returns a
    # naive datetime in local time. Compare epoch seconds for a tz-safe check.
    assert abs(parsed.timestamp() - row["updated_at"].timestamp()) < 1


def test_log_drops_partition_and_casts_indices(spark, transformer):
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("partition", IntegerType()),
            StructField("block_id", IntegerType()),
            StructField("transaction_index", IntegerType()),
            StructField("log_index", IntegerType()),
            StructField("topic0", BinaryType()),
        ]
    )
    rows = [(0, 17_000_000, 4, 12, b"\xaa" * 32)]
    transformer._read_stub["log"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_log(start_block=17_000_000, end_block=17_000_000)
    out = transformer._captured["log"]
    assert "partition" not in out.columns
    assert "block_id_group" in out.columns
    assert out.schema["transaction_index"].dataType.simpleString() == "smallint"
    assert out.schema["log_index"].dataType.simpleString() == "int"
