# -*- coding: utf-8 -*-
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa

from graphsenselib.ingest.delta.sink import DeltaTableWriter, read_table

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
