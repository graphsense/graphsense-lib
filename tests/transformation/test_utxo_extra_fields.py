"""Tests for sequence/version/locktime projection in transform_transaction."""

import pytest

from graphsenselib.transformation.utxo import UtxoTransformation


@pytest.fixture
def transformer(spark, install_harness):
    """UtxoTransformation with read/write patched.

    `_get_tx_df_with_ids` reads the Delta path directly via `spark.read`
    rather than through `_read_delta`, so we override it to consume the
    stubbed `transaction` DataFrame and synthesize tx_id/tx_id_group.
    """
    from pyspark.sql import functions as F

    t = install_harness(
        UtxoTransformation(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_btc_raw",
        )
    )

    def fake_get_tx_df_with_ids(start_block, end_block):
        df = t._read_stub["transaction"]
        df = df.filter(
            (F.col("block_id") >= start_block) & (F.col("block_id") <= end_block)
        )
        df = df.withColumn("tx_id", F.col("index").cast("long"))
        df = df.withColumn(
            "tx_id_group",
            F.floor(F.col("tx_id") / t.tx_bucket_size).cast("int"),
        )
        return df

    t._get_tx_df_with_ids = fake_get_tx_df_with_ids
    return t


def _input_struct():
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("spent_transaction_hash", BinaryType()),
            StructField("spent_output_index", IntegerType()),
            StructField("index", IntegerType()),
            StructField("sequence", LongType()),
            StructField("script_hex", StringType()),
            StructField("txinwitness", ArrayType(BinaryType())),
            StructField("type", StringType()),
            StructField("addresses", ArrayType(StringType())),
            StructField("value", LongType()),
        ]
    )


def _output_struct():
    from pyspark.sql.types import (
        ArrayType,
        ByteType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("index", IntegerType()),
            StructField("script_hex", StringType()),
            StructField("addresses", ArrayType(StringType())),
            StructField("required_signatures", ByteType()),
            StructField("type", StringType()),
            StructField("value", LongType()),
        ]
    )


def _transaction_schema():
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("tx_hash", BinaryType()),
            StructField("partition", IntegerType()),
            StructField("block_id", IntegerType()),
            StructField("timestamp", IntegerType()),
            StructField("coinbase", BooleanType()),
            StructField("total_input", LongType()),
            StructField("total_output", LongType()),
            StructField("outputs", ArrayType(_output_struct())),
            StructField("inputs", ArrayType(_input_struct())),
            StructField("coinjoin", BooleanType()),
            StructField("type", StringType()),
            StructField("size", IntegerType()),
            StructField("virtual_size", IntegerType()),
            StructField("version", LongType()),
            StructField("lock_time", LongType()),
            StructField("index", IntegerType()),
            StructField("input_count", IntegerType()),
            StructField("output_count", IntegerType()),
            StructField("fee", LongType()),
        ]
    )


def test_transform_emits_sequence_version_locktime(spark, transformer):
    schema = _transaction_schema()

    rbf_inputs = [
        (
            b"\x11" * 32,  # spent_transaction_hash
            0,  # spent_output_index
            0,  # index
            0xFFFFFFFD,  # sequence: RBF-signaling
            "76a914",  # script_hex
            [b"\xaa\xbb"],  # txinwitness
            "pubkeyhash",  # type
            ["addr1"],  # addresses
            100,  # value
        ),
        (
            b"\x22" * 32,
            1,
            1,
            0xFFFFFFFE,  # sequence
            "76a915",
            [],
            "pubkeyhash",
            ["addr2"],
            200,
        ),
    ]
    rbf_outputs = [
        (0, "76a91688", ["addr_out1"], 1, "pubkeyhash", 250),
    ]

    final_inputs = [
        (
            b"\x33" * 32,
            0,
            0,
            0xFFFFFFFF,  # sequence: final
            "76a916",
            [],
            "pubkeyhash",
            ["addr3"],
            500,
        ),
    ]
    final_outputs = [
        (0, "76a91699", ["addr_out2"], 1, "pubkeyhash", 480),
    ]

    rows = [
        (
            b"\xab" * 32,  # tx_hash
            0,  # partition
            100,  # block_id
            1_700_000_000,  # timestamp
            False,  # coinbase
            300,  # total_input
            250,  # total_output
            rbf_outputs,
            rbf_inputs,
            False,  # coinjoin
            "tx",  # type
            250,  # size
            150,  # virtual_size
            2,  # version
            500_000,  # lock_time
            0,  # index (within block)
            2,  # input_count
            1,  # output_count
            50,  # fee
        ),
        (
            b"\xcd" * 32,
            0,
            100,
            1_700_000_000,
            False,
            500,
            480,
            final_outputs,
            final_inputs,
            False,
            "tx",
            200,
            120,
            1,  # version
            0,  # lock_time
            1,
            1,
            1,
            20,
        ),
    ]

    transformer._read_stub["transaction"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_transaction(start_block=100, end_block=100)

    out = transformer._captured["transaction"]
    cols = out.columns

    expected_cols = {
        "tx_id_group",
        "tx_id",
        "tx_hash",
        "block_id",
        "timestamp",
        "coinbase",
        "total_input",
        "total_output",
        "inputs",
        "outputs",
        "coinjoin",
        "version",
        "locktime",
    }
    assert expected_cols == set(cols), f"got {set(cols)}"
    assert len(cols) == 13

    # Schema field order in inputs UDT struct: sequence is last
    inputs_field = out.schema["inputs"].dataType
    inputs_struct = inputs_field.elementType
    assert inputs_struct.fields[-1].name == "sequence"

    collected = sorted(out.collect(), key=lambda r: r.tx_hash)
    by_hash = {bytes(r.tx_hash): r for r in collected}

    rbf_row = by_hash[b"\xab" * 32]
    final_row = by_hash[b"\xcd" * 32]

    assert rbf_row.version == 2
    assert rbf_row.locktime == 500_000
    assert final_row.version == 1
    assert final_row.locktime == 0

    rbf_in_seqs = [i["sequence"] for i in rbf_row.inputs]
    assert rbf_in_seqs == [0xFFFFFFFD, 0xFFFFFFFE]

    final_in_seqs = [i["sequence"] for i in final_row.inputs]
    assert final_in_seqs == [0xFFFFFFFF]

    for row in (rbf_row, final_row):
        for o in row.outputs:
            assert o["sequence"] is None
