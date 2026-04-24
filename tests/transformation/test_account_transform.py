"""ETH-specific transformation tests.

Cross-chain shared contract is in test_account_shared.py. Here we cover only
what's ETH-specific: the trace transform (drops `creation_method`, converts
`value` varint, casts status/transaction_index to smallint).
"""

import pytest

from graphsenselib.transformation.account import AccountTransformation


@pytest.fixture
def transformer(spark, install_harness):
    return install_harness(
        AccountTransformation(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_eth_raw",
        )
    )


def test_trace_drops_creation_method_and_converts_value(spark, transformer):
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
            StructField("block_id", IntegerType()),
            StructField("transaction_index", IntegerType()),
            StructField("status", IntegerType()),
            StructField("value", BinaryType()),
            StructField("creation_method", StringType()),
        ]
    )
    rows = [(0, 17_000_000, 3, 1, (5000).to_bytes(8, "big"), "create")]
    transformer._read_stub["trace"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_trace(start_block=17_000_000, end_block=17_000_000)
    out = transformer._captured["trace"]
    cols = set(out.columns)

    assert "partition" not in cols
    assert "creation_method" not in cols
    assert "block_id_group" in cols
    assert out.schema["status"].dataType.simpleString() == "smallint"
    assert out.schema["transaction_index"].dataType.simpleString() == "smallint"
    assert out.collect()[0].asDict()["value"] == "5000"
