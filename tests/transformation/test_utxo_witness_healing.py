"""delta-to-raw must heal hex-ASCII txinwitness back to raw bytes.

The Delta lake's parquet prep never unhexed witness, so every stored element
is the UTF-8 bytes of the node's hex string (b"3045..." instead of
b"\\x30\\x45...). Healing is list-level all-or-nothing: a list is decoded
only when every element is valid even-length lowercase hex-ASCII — a real
raw-bytes signature can't pass that test, so already-healed rows (a future
clean lake) pass through untouched.
"""

from graphsenselib.transformation.utxo import UtxoTransformation
from tests.transformation.test_utxo_address_normalization import (
    _inputs,
    _outputs,
    _tx_row,
)
from tests.transformation.test_utxo_extra_fields import _transaction_schema

SIG_RAW = bytes.fromhex("3045022100" + "ab" * 66)
PUBKEY_RAW = bytes.fromhex("03" + "cd" * 32)
SIG_HEXFORM = SIG_RAW.hex().encode()  # what the Delta lake actually holds
PUBKEY_HEXFORM = PUBKEY_RAW.hex().encode()
# raw bytes that are NOT pure hex-ASCII (contains 0xff), non-UTF8 on purpose
RAW_NON_ASCII = b"\x30\x45\xff\x00\x12"


def _make_transformer(spark, install_harness):
    from pyspark.sql import functions as F

    t = install_harness(
        UtxoTransformation(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_raw",
            network="btc",
        )
    )

    def fake_get_tx_df_with_ids(start_block, end_block):
        df = t._read_stub["transaction"]
        df = df.withColumn("tx_id", F.col("index").cast("long"))
        df = df.withColumn(
            "tx_id_group",
            F.floor(F.col("tx_id") / t.tx_bucket_size).cast("int"),
        )
        return df

    t._get_tx_df_with_ids = fake_get_tx_df_with_ids
    return t


def _run(spark, transformer, witness):
    inputs = _inputs([["bc1qsomeaddress"]])
    inputs[0] = inputs[0][:5] + (witness,) + inputs[0][6:]
    rows = [_tx_row(inputs=inputs, outputs=_outputs([["bc1qother"]]))]
    transformer._read_stub["transaction"] = spark.createDataFrame(
        rows, schema=_transaction_schema()
    )
    transformer.transform_transaction(start_block=100, end_block=100)
    return transformer._captured["transaction"].collect()[0]


def test_hexform_witness_is_healed_to_raw_bytes(spark, install_harness):
    t = _make_transformer(spark, install_harness)
    row = _run(spark, t, [SIG_HEXFORM, PUBKEY_HEXFORM])
    assert list(row.inputs[0]["txinwitness"]) == [SIG_RAW, PUBKEY_RAW]


def test_raw_bytes_witness_stays_untouched(spark, install_harness):
    t = _make_transformer(spark, install_harness)
    row = _run(spark, t, [RAW_NON_ASCII, PUBKEY_RAW])
    assert list(row.inputs[0]["txinwitness"]) == [RAW_NON_ASCII, PUBKEY_RAW]


def test_mixed_list_is_left_alone(spark, install_harness):
    # one element not hex-ASCII -> the whole list is presumed raw already
    t = _make_transformer(spark, install_harness)
    row = _run(spark, t, [SIG_HEXFORM, RAW_NON_ASCII])
    assert list(row.inputs[0]["txinwitness"]) == [SIG_HEXFORM, RAW_NON_ASCII]


def test_null_witness_stays_null(spark, install_harness):
    t = _make_transformer(spark, install_harness)
    row = _run(spark, t, None)
    assert row.inputs[0]["txinwitness"] is None


def test_empty_elements_survive_healing(spark, install_harness):
    # CHECKMULTISIG dummy: hex-form lists carry b"" elements too
    t = _make_transformer(spark, install_harness)
    row = _run(spark, t, [b"", SIG_HEXFORM])
    assert list(row.inputs[0]["txinwitness"]) == [b"", SIG_RAW]
