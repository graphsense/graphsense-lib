"""delta-to-raw must heal BTC-form (1…) base58 P2PKH addresses.

Pre-2.14.4 ingests derived fallback addresses with BTC's version byte on
every network; the Delta lake still carries those strings, so the
delta → raw transformation has to run the same normalizer as node ingest
(ingest.utxo.normalize_base58_p2pkh) or a rebuild re-imports the corruption.
"""

import pytest

from graphsenselib.transformation.utxo import UtxoTransformation
from graphsenselib.utils.pubkey_to_address import base58check_encode
from tests.transformation.test_utxo_extra_fields import _transaction_schema

HASH160 = bytes.fromhex("ab" * 20)
BTC_FORM = base58check_encode(b"\x00", HASH160)  # corrupted 1…
LTC_FORM = base58check_encode(b"\x30", HASH160)  # healed L…
CLEAN_BECH32 = "ltc1qw508d6qejxtdg4y5r3zarvary0c5xw7kgmn4n9"


def _make_transformer(spark, install_harness, network):
    from pyspark.sql import functions as F

    t = install_harness(
        UtxoTransformation(
            spark=spark,
            delta_lake_path="s3a://unused",
            raw_keyspace="test_raw",
            network=network,
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


def _tx_row(inputs, outputs):
    return (
        b"\xab" * 32,  # tx_hash
        0,  # partition
        100,  # block_id
        1_700_000_000,  # timestamp
        False,  # coinbase
        300,  # total_input
        250,  # total_output
        outputs,
        inputs,
        False,  # coinjoin
        "tx",  # type
        250,  # size
        150,  # virtual_size
        2,  # version
        0,  # lock_time
        0,  # index (within block)
        len(inputs),  # input_count
        len(outputs),  # output_count
        50,  # fee
    )


def _inputs(addresses_per_input):
    return [
        (
            bytes([17 + i]) * 32,  # spent_transaction_hash
            0,  # spent_output_index
            i,  # index
            0xFFFFFFFE,  # sequence
            "76a914",  # script_hex
            [],  # txinwitness
            "pubkeyhash",  # type
            addrs,  # addresses
            100,  # value
        )
        for i, addrs in enumerate(addresses_per_input)
    ]


def _outputs(addresses_per_output, out_type="pubkey"):
    return [
        (i, "76a91688", addrs, 1, out_type, 250)
        for i, addrs in enumerate(addresses_per_output)
    ]


def test_ltc_rewrites_btc_form_addresses(spark, install_harness):
    transformer = _make_transformer(spark, install_harness, network="ltc")
    rows = [
        _tx_row(
            inputs=_inputs([[BTC_FORM], [CLEAN_BECH32, BTC_FORM]]),
            outputs=_outputs([[BTC_FORM], [LTC_FORM]]),
        )
    ]
    transformer._read_stub["transaction"] = spark.createDataFrame(
        rows, schema=_transaction_schema()
    )

    transformer.transform_transaction(start_block=100, end_block=100)

    out = transformer._captured["transaction"].collect()
    assert len(out) == 1
    row = out[0]

    assert [list(i["address"]) for i in row.inputs] == [
        [LTC_FORM],
        [CLEAN_BECH32, LTC_FORM],
    ]
    assert [list(o["address"]) for o in row.outputs] == [[LTC_FORM], [LTC_FORM]]


def test_btc_network_is_a_noop(spark, install_harness):
    transformer = _make_transformer(spark, install_harness, network="btc")
    rows = [
        _tx_row(
            inputs=_inputs([[BTC_FORM]]),
            outputs=_outputs([[BTC_FORM]]),
        )
    ]
    transformer._read_stub["transaction"] = spark.createDataFrame(
        rows, schema=_transaction_schema()
    )

    transformer.transform_transaction(start_block=100, end_block=100)

    row = transformer._captured["transaction"].collect()[0]
    assert [list(i["address"]) for i in row.inputs] == [[BTC_FORM]]
    assert [list(o["address"]) for o in row.outputs] == [[BTC_FORM]]


def test_null_addresses_survive_normalization(spark, install_harness):
    transformer = _make_transformer(spark, install_harness, network="ltc")
    rows = [
        _tx_row(
            inputs=_inputs([None]),
            outputs=_outputs([None], out_type="nulldata"),
        )
    ]
    transformer._read_stub["transaction"] = spark.createDataFrame(
        rows, schema=_transaction_schema()
    )

    transformer.transform_transaction(start_block=100, end_block=100)

    row = transformer._captured["transaction"].collect()[0]
    assert [i["address"] for i in row.inputs] == [None]
    assert [o["address"] for o in row.outputs] == [None]


def test_network_is_required():
    with pytest.raises(TypeError):
        UtxoTransformation(
            spark=None, delta_lake_path="s3a://unused", raw_keyspace="test_raw"
        )
