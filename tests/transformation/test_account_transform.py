"""ETH-specific transformation tests.

Cross-chain shared contract is in test_account_shared.py. Here we cover only
what's ETH-specific: the trace transform (drops `creation_method`, converts
`value` varint, casts status/transaction_index to smallint).
"""

import pytest

from graphsenselib.transformation.account import AccountTransformation


def _sparkless_transformer():
    return AccountTransformation(
        spark=None, delta_lake_path="s3a://unused", raw_keyspace="test_eth_raw"
    )


def test_run_rejects_unknown_tables():
    transformer = _sparkless_transformer()
    with pytest.raises(ValueError, match="trace_withdrawl"):
        transformer.run(0, 1, tables=["trace_withdrawl"])


def test_run_partial_tables_skip_readiness_marker(monkeypatch):
    """A --tables subset run must not stamp configuration/ingest_complete —
    the marker is the REST auto-discovery readiness signal."""
    transformer = _sparkless_transformer()
    calls = []
    monkeypatch.setattr(
        transformer,
        "_table_methods",
        lambda: {
            name: (lambda s, e, n=name: calls.append(n)) for name in transformer.TABLES
        },
    )
    monkeypatch.setattr(
        transformer, "write_configuration", lambda: calls.append("configuration")
    )
    monkeypatch.setattr(
        transformer,
        "write_ingest_complete_marker",
        lambda: calls.append("ingest_complete"),
    )

    transformer.run(0, 1, tables=["trace_withdrawal"])
    assert calls == ["trace_withdrawal"]

    calls.clear()
    transformer.run(0, 1)
    assert calls == list(transformer.TABLES) + ["configuration", "ingest_complete"]


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


def test_trace_withdrawal_synthesizes_reward_traces(spark, transformer):
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    from graphsenselib.ingest.account import (
        GWEI_TO_WEI,
        WITHDRAWAL_TRACE_INDEX_OFFSET,
    )

    withdrawal_type = StructType(
        [
            StructField("index", LongType()),
            StructField("validator_index", LongType()),
            StructField("address", StringType()),
            StructField("amount", BinaryType()),
        ]
    )
    schema = StructType(
        [
            StructField("block_id", IntegerType()),
            StructField("withdrawals", ArrayType(withdrawal_type)),
        ]
    )
    rows = [
        (15_000_000, None),  # pre-Shanghai
        (
            17_100_000,
            [
                (
                    1041981,
                    340674,
                    "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f",
                    (12210183).to_bytes(4, "big"),
                ),
                (
                    1041982,
                    340675,
                    "0x1f9090aae28b8a3dceadf281b0f12828e676c326",
                    (12122076).to_bytes(4, "big"),
                ),
            ],
        ),
    ]
    transformer._read_stub["block"] = spark.createDataFrame(rows, schema=schema)

    transformer.transform_trace_withdrawal(start_block=15_000_000, end_block=17_100_000)
    out = sorted(
        (r.asDict() for r in transformer._captured["trace"].collect()),
        key=lambda r: r["trace_index"],
    )

    assert len(out) == 2
    first = out[0]
    assert first["block_id"] == 17_100_000
    assert first["block_id_group"] == 17_100
    assert first["trace_index"] == WITHDRAWAL_TRACE_INDEX_OFFSET
    assert first["trace_id"] == f"reward_17100000_{WITHDRAWAL_TRACE_INDEX_OFFSET}"
    assert first["trace_type"] == "reward"
    assert first["reward_type"] == "withdrawal"
    assert first["to_address"] == bytes.fromhex(
        "b9d7934878b5fb9610b3fe8a5e441e8fad7e293f"
    )
    assert first["value"] == str(12210183 * GWEI_TO_WEI)
    assert first["status"] == 1
    assert out[1]["value"] == str(12122076 * GWEI_TO_WEI)
