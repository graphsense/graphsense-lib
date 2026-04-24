"""TRX-specific transformation tests.

Cross-chain shared contract is in test_account_shared.py. Here we cover only
TRX-specific behavior: gas_limit varint on block, the divergent trace schema
(internal_index/call_info_index/call_value), the fee table (derives
tx_hash_prefix), and trc10.
"""

import pytest

from graphsenselib.transformation.account_trx import AccountTrxTransformation


@pytest.fixture
def transformer(spark, install_harness):
    return install_harness(
        AccountTrxTransformation(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_trx_raw",
        )
    )


def test_block_converts_gas_limit_varint(spark, transformer):
    """TRX block.gas_limit is varint (not int like ETH)."""
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
            StructField("difficulty", BinaryType()),
            StructField("total_difficulty", BinaryType()),
            StructField("gas_limit", BinaryType()),
            StructField("transaction_count", IntegerType()),
        ]
    )
    rows = [
        (
            0,
            50_000_000,
            (0).to_bytes(1, "big"),
            (0).to_bytes(1, "big"),
            (10_000_000_000).to_bytes(8, "big"),
            42,
        )
    ]
    transformer._read_stub["block"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_block(start_block=50_000_000, end_block=50_000_000)
    out = transformer._captured["block"]
    assert out.collect()[0].asDict()["gas_limit"] == "10000000000"


def test_trace_converts_call_value(spark, transformer):
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
            StructField("internal_index", IntegerType()),
            StructField("call_info_index", IntegerType()),
            StructField("call_value", BinaryType()),
        ]
    )
    rows = [(0, 60_000_000, 1, 2, (123).to_bytes(2, "big"))]
    transformer._read_stub["trace"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_trace(start_block=60_000_000, end_block=60_000_000)
    out = transformer._captured["trace"]
    cols = set(out.columns)
    assert "partition" not in cols
    assert "block_id_group" in cols
    assert out.schema["internal_index"].dataType.simpleString() == "smallint"
    assert out.schema["call_info_index"].dataType.simpleString() == "smallint"
    assert out.collect()[0].asDict()["call_value"] == "123"


def test_fee_derives_tx_hash_prefix(spark, transformer):
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
            StructField("tx_hash", BinaryType()),
            StructField("fee", IntegerType()),
        ]
    )
    rows = [(0, 60_000_000, bytes.fromhex("abcdef0123456789" + "00" * 24), 100)]
    transformer._read_stub["fee"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_fee(start_block=60_000_000, end_block=60_000_000)
    out = transformer._captured["fee"]
    cols = set(out.columns)
    assert "partition" not in cols
    assert "block_id" not in cols
    assert "tx_hash_prefix" in cols
    assert out.collect()[0].asDict()["tx_hash_prefix"] == "abcde"
