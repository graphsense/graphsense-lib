"""Written-DataFrame integer widths must match the CQL raw schema.

The sidecar bulk writer (cassandra-analytics) refuses to narrow: a LongType
column aimed at a CQL ``int`` fails every task with "Unsupported conversion
for INTEGER from java.lang.Long". The classic connector silently narrowed,
so these mismatches only surface with writer='sidecar'.
"""

import pytest

from graphsenselib.transformation.utxo import UtxoTransformation

pyspark = pytest.importorskip("pyspark")


@pytest.fixture
def transformer(spark, install_harness):
    return install_harness(
        UtxoTransformation(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_btc_raw",
            network="btc",
        )
    )


def _block_df(spark):
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    # Mirrors UTXO_SCHEMA_RAW["block"] as Spark reads it from Delta:
    # timestamp is int64 there, so it arrives as LongType.
    schema = StructType(
        [
            StructField("partition", IntegerType()),
            StructField("type", StringType()),
            StructField("size", IntegerType()),
            StructField("stripped_size", LongType()),
            StructField("weight", LongType()),
            StructField("version", LongType()),
            StructField("merkle_root", StringType()),
            StructField("nonce", StringType()),
            StructField("bits", StringType()),
            StructField("coinbase_param", StringType()),
            StructField("block_id", IntegerType()),
            StructField("block_hash", BinaryType()),
            StructField("timestamp", LongType()),
            StructField("no_transactions", IntegerType()),
        ]
    )
    rows = [
        (
            0,
            "block",
            285,
            285,
            1140,
            1,
            "aa",
            "1",
            "1d00ffff",
            "04",
            100,
            b"\x00" * 32,
            1_700_000_000,
            2,
        )
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_transform_block_writes_timestamp_as_int(spark, transformer):
    from pyspark.sql.types import IntegerType

    transformer._read_stub["block"] = _block_df(spark)

    transformer.transform_block(start_block=100, end_block=100)

    out = transformer._captured["block"]
    types = {f.name: f.dataType for f in out.schema.fields}
    # CQL: block(block_id_group int, block_id int, block_hash blob,
    #            timestamp int, no_transactions int)
    assert types["timestamp"] == IntegerType()
    assert types["block_id"] == IntegerType()
    assert types["block_id_group"] == IntegerType()
    assert types["no_transactions"] == IntegerType()

    row = out.collect()[0]
    assert row["timestamp"] == 1_700_000_000


def test_summary_statistics_writes_no_blocks_as_int(spark, transformer):
    from pyspark.sql.types import IntegerType, LongType

    transformer._read_stub["block"] = _block_df(spark)

    def fake_get_tx_df_with_ids(start_block, end_block):
        return _block_df(spark)  # only .count() is used

    transformer._get_tx_df_with_ids = fake_get_tx_df_with_ids

    transformer.write_summary_statistics(start_block=100, end_block=100)

    out = transformer._captured["summary_statistics"]
    types = {f.name: f.dataType for f in out.schema.fields}
    # CQL: summary_statistics(id text, no_blocks int, no_txs bigint,
    #                         timestamp int)
    assert types["no_blocks"] == IntegerType()
    assert types["no_txs"] == LongType()
    assert types["timestamp"] == IntegerType()
