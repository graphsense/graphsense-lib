# -*- coding: utf-8 -*-
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")

import pandas as pd
import pyarrow as pa

from graphsenselib.ingest.delta.sink import DeltaTableWriter, read_table
from graphsenselib.schema.resources.parquet.utxo import UTXO_SCHEMA_RAW

tempfolder = "tests/ingest/temp"
# create folder if it doesnt exist

test_schema = {
    "partition": pa.int64(),
    "b": pa.string(),
    "pk1": pa.int64(),
    "pk2": pa.string(),
}

test_schema = pa.schema(test_schema)


def uses_ephemeral_testfolder(func):
    def wrapper(*args, **kwargs):
        Path(tempfolder).mkdir(parents=True, exist_ok=True)
        func(*args, **kwargs)
        shutil.rmtree(tempfolder)

    return wrapper


@uses_ephemeral_testfolder
def test_writing_partitionwise():
    writer_overwrite = DeltaTableWriter(
        path=tempfolder,
        table_name="test_table",
        schema=test_schema,
        partition_cols=("partition",),
        mode="overwrite",
    )

    data = [
        {"partition": 1, "b": "a", "pk1": 1, "pk2": "a"},
        {"partition": 1, "b": "b", "pk1": 2, "pk2": "b"},
        {"partition": 2, "b": "c", "pk1": 3, "pk2": "c"},
    ]

    df_original = pd.DataFrame(data)
    data_part1 = [x for x in data if x["partition"] == 1]
    data_part2 = [x for x in data if x["partition"] == 2]

    writer_overwrite.write_delta(data_part1)
    writer_overwrite.write_delta(data_part2)
    df_read = read_table(tempfolder, "test_table")

    # sort by pks
    df_original = df_original.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)

    assert df_read.equals(df_original)


@uses_ephemeral_testfolder
def test_writing_multiple_files_per_partition():
    writer_overwrite = DeltaTableWriter(
        path=tempfolder,
        table_name="test_table",
        schema=test_schema,
        partition_cols=("partition",),
        mode="overwrite",
    )

    data = [
        {"partition": 1, "b": "a", "pk1": 1, "pk2": "a"},
        {"partition": 1, "b": "b", "pk1": 2, "pk2": "b"},
        {"partition": 1, "b": "c", "pk1": 3, "pk2": "c"},
        {"partition": 2, "b": "d", "pk1": 4, "pk2": "d"},
        {"partition": 2, "b": "e", "pk1": 5, "pk2": "e"},
        {"partition": 2, "b": "f", "pk1": 6, "pk2": "f"},
    ]

    df_original = pd.DataFrame(data)

    parts = [[data[0], data[1]], [data[2]], [data[3]], [data[4], data[5]]]

    for part in parts:
        writer_overwrite.write_delta(part)

    df_read = read_table(tempfolder, "test_table")

    # sort by pks
    df_original = df_original.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)

    assert df_read.equals(df_original)

    # duplicated writing should be a problem
    writer_overwrite.write_delta(parts[0])
    df_read = read_table(tempfolder, "test_table")
    df_read = df_read.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)
    assert not df_read.equals(df_original)


@uses_ephemeral_testfolder
def test_merge_idempotent():
    writer_merge = DeltaTableWriter(
        path=tempfolder,
        table_name="test_table",
        schema=test_schema,
        partition_cols=("partition",),
        mode="overwrite",
        primary_keys=["pk1", "pk2"],
    )

    data = [
        {"partition": 1, "b": "a", "pk1": 1, "pk2": "a"},
        {"partition": 1, "b": "b", "pk1": 2, "pk2": "b"},
        {"partition": 2, "b": "c", "pk1": 3, "pk2": "c"},
    ]

    df_original = pd.DataFrame(data)
    data_part1 = [x for x in data if x["partition"] == 1]
    data_part2 = [x for x in data if x["partition"] == 2]

    writer_merge.write_delta(data_part1)
    writer_merge.write_delta(data_part2)

    # duplicated writing shouldnt be a problem
    writer_merge.write_delta(data_part1)

    df_read = read_table(tempfolder, "test_table")

    # sort by pks
    df_original = df_original.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)

    assert df_read.equals(df_original)


@uses_ephemeral_testfolder
def test_raise_on_invalid_partitioning():
    """Test that we cant write multiple different partitions in one go"""
    writer_overwrite = DeltaTableWriter(
        path=tempfolder,
        table_name="test_table",
        schema=test_schema,
        partition_cols=("partition",),
        mode="overwrite",
    )

    data = [
        {"partition": 1, "b": "a", "pk1": 1, "pk2": "a"},
        {"partition": 2, "b": "b", "pk1": 2, "pk2": "b"},
    ]
    # in the underlying code it should just be an assert
    import pytest

    with pytest.raises(AssertionError):
        writer_overwrite.write_delta(data)


@uses_ephemeral_testfolder
def test_overwrite_data():
    """
    Overwrite should actually overwrite the data in the same partition irrespective of
    keys and only if a new writer is used.
    If the same writer is used, it should append the data, the overwriting should only
    happen on the first write.
    """
    writer_overwrite = DeltaTableWriter(
        path=tempfolder,
        table_name="test_table",
        schema=test_schema,
        partition_cols=("partition",),
        mode="overwrite",
    )
    writer_overwrite2 = DeltaTableWriter(
        path=tempfolder,
        table_name="test_table",
        schema=test_schema,
        partition_cols=("partition",),
        mode="overwrite",
    )

    data_old = [
        {"partition": 1, "b": "a", "pk1": 1, "pk2": "a"},
        {"partition": 1, "b": "b", "pk1": 2, "pk2": "b"},
    ]

    writer_overwrite2.write_delta(data_old)

    data_new = [
        {"partition": 1, "b": "c", "pk1": 3, "pk2": "c"},
    ]

    writer_overwrite.write_delta(data_new)

    df_read = read_table(tempfolder, "test_table")

    df_original = pd.DataFrame(data_new)

    # sort by pks
    df_original = df_original.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)

    assert df_read.equals(df_original)

    # note that we now append the data even with the same primary keys.
    # todo There currently is no check in overwrite mode if the PK is unique.
    data_append = [
        {"partition": 1, "b": "c", "pk1": 3, "pk2": "c"},
    ]

    writer_overwrite.write_delta(data_append)

    df_read = read_table(tempfolder, "test_table")

    df_original = pd.concat([df_original, pd.DataFrame(data_append)]).reset_index(
        drop=True
    )

    # sort by pks
    df_original = df_original.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["pk1", "pk2"]).reset_index(drop=True)

    assert df_read.equals(df_original)


def check_dataframe_equality(df_original, df_read):
    """
    Check that 2 dfs have the same content
    """

    # Ensure the column structure is identical
    assert set(df_original.columns) == set(df_read.columns), (
        "Column mismatch:\n"
        f"Expected columns: {df_original.columns}\n"
        f"Read columns: {df_read.columns}"
    )

    # Check each column for pointwise equality
    for column in df_original.columns:
        try:
            pd.testing.assert_series_equal(
                df_original[column], df_read[column], check_dtype=False
            )
        except AssertionError as e:
            raise AssertionError(f"Mismatch found in column '{column}': {e}")


@uses_ephemeral_testfolder
def test_utxo_schema_data_coverage():
    writer_overwrite = DeltaTableWriter(
        path=tempfolder,
        table_name="utxo_test_table",
        schema=UTXO_SCHEMA_RAW["block"],
        partition_cols=("partition",),
        mode="overwrite",
    )

    # Sample data for "block" schema
    block_data = [
        {
            "partition": 1,
            "type": "test_type",
            "size": 1024,
            "stripped_size": 512,
            "weight": 4000,
            "version": 1,
            "merkle_root": "abcd1234",
            "nonce": "nonce_value",
            "bits": "1d00ffff",
            "coinbase_param": "test_coinbase",
            "block_id": 1,
            "block_hash": b"\x00" * 32,
            "timestamp": 1653493200,
            "no_transactions": 10,
        },
        {
            "partition": 1,
            "type": "another_type",
            "size": 2048,
            "stripped_size": 1024,
            "weight": 8000,
            "version": 2,
            "merkle_root": "dcba4321",
            "nonce": "another_nonce",
            "bits": "1d00fffe",
            "coinbase_param": "coinbase_value",
            "block_id": 2,
            "block_hash": b"\x01" * 32,
            "timestamp": 1653593200,
            "no_transactions": 20,
        },
    ]

    # Writing and testing block data
    writer_overwrite.write_delta(block_data)
    df_read = read_table(tempfolder, "utxo_test_table")
    df_original = pd.DataFrame(block_data)
    df_original = df_original.sort_values(by=["block_id"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["block_id"]).reset_index(drop=True)
    # content should be the same. datatypes might be different
    check_dataframe_equality(df_read, df_original)

    # Adding similar coverage for other schemas...
    writer_transaction = DeltaTableWriter(
        path=tempfolder,
        table_name="utxo_transaction_test_table",
        schema=UTXO_SCHEMA_RAW["transaction"],
        partition_cols=("partition",),
        mode="overwrite",
    )

    transaction_data = [
        {
            "tx_hash": b"\x00" * 32,
            "partition": 1,
            "block_id": 1,
            "timestamp": 1653493200,
            "coinbase": True,
            "total_input": 5000,
            "total_output": 4800,
            "outputs": [
                {
                    "index": 0,
                    "script_hex": "abcdef",
                    "addresses": [b"addr1", b"addr2"],
                    "required_signatures": 1,
                    "type": "p2pkh",
                    "value": 3000,
                },
                {
                    "index": 1,
                    "script_hex": "123456",
                    "addresses": [b"addr3"],
                    "required_signatures": 1,
                    "type": "p2sh",
                    "value": 1800,
                },
            ],
            "inputs": [
                {
                    "spent_transaction_hash": b"\x01" * 32,
                    "spent_output_index": 0,
                    "index": 0,
                    "sequence": 4294967295,
                },
            ],
            "coinjoin": False,
            "type": "regular",
            "size": 225,
            "virtual_size": 225,
            "version": 1,
            "lock_time": 0,
            "index": 0,
            "input_count": 1,
            "output_count": 2,
            "fee": 200,
        }
    ]

    # Writing and testing transaction data
    writer_transaction.write_delta(transaction_data)
    df_read = read_table(tempfolder, "utxo_transaction_test_table")
    df_original = pd.DataFrame(transaction_data)
    df_original = df_original.sort_values(by=["tx_hash"]).reset_index(drop=True)
    df_read = df_read.sort_values(by=["tx_hash"]).reset_index(drop=True)

    check_dataframe_equality(df_read, df_original)
