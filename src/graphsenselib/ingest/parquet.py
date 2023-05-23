from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import HadoopFileSystem

ACCOUNT_SCHEMA_RAW = {
    "log": pa.schema(
        [
            ("partition", pa.int16()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("address", pa.binary(20)),
            ("data", pa.large_binary()),
            ("topics", pa.list_(pa.binary(32))),
            ("topic0", pa.binary(32)),
            ("tx_hash", pa.binary(32)),
            ("log_index", pa.int32()),
            ("transaction_index", pa.int32()),
        ]
    ),
    "trace": pa.schema(
        [
            ("partition", pa.int16()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("tx_hash", pa.binary(32)),
            ("transaction_index", pa.int32()),
            ("from_address", pa.binary(20)),
            ("to_address", pa.binary(20)),
            ("value", pa.decimal128(38, 0)),
            ("input", pa.large_binary()),
            ("output", pa.large_binary()),
            ("trace_type", pa.string()),
            ("call_type", pa.string()),
            ("reward_type", pa.string()),
            ("gas", pa.int32()),
            ("gas_used", pa.decimal128(38, 0)),
            ("subtraces", pa.int32()),
            ("trace_address", pa.string()),
            ("error", pa.string()),
            ("status", pa.int16()),
            ("trace_id", pa.string()),
            ("trace_index", pa.int32()),
        ]
    ),
    "block": pa.schema(
        [
            ("partition", pa.int16()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("parent_hash", pa.binary(32)),
            ("nonce", pa.binary(8)),
            ("sha3_uncles", pa.binary(32)),
            ("logs_bloom", pa.binary(256)),
            ("transactions_root", pa.binary(32)),
            ("state_root", pa.binary(32)),
            ("receipts_root", pa.binary(32)),
            ("miner", pa.binary(20)),
            ("difficulty", pa.decimal128(38, 0)),
            ("total_difficulty", pa.decimal128(38, 0)),
            ("size", pa.int64()),
            ("extra_data", pa.large_binary()),
            ("gas_limit", pa.int32()),
            ("gas_used", pa.int32()),
            ("base_fee_per_gas", pa.decimal128(38, 0)),
            ("timestamp", pa.int32()),
            ("transaction_count", pa.int32()),
        ]
    ),
    "transaction": pa.schema(
        [
            ("partition", pa.int16()),
            ("tx_hash_prefix", pa.string()),
            ("tx_hash", pa.binary(32)),
            ("nonce", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("transaction_index", pa.int32()),
            ("from_address", pa.binary(20)),
            ("to_address", pa.binary(20)),
            ("value", pa.decimal128(38, 0)),
            ("gas", pa.int32()),
            ("gas_price", pa.decimal128(38, 0)),
            ("input", pa.large_binary()),
            ("block_timestamp", pa.int32()),
            ("max_fee_per_gas", pa.decimal128(38, 0)),
            ("max_priority_fee_per_gas", pa.decimal128(38, 0)),
            ("transaction_type", pa.decimal128(38, 0)),
            ("receipt_cumulative_gas_used", pa.decimal128(38, 0)),
            ("receipt_gas_used", pa.decimal128(38, 0)),
            ("receipt_contract_address", pa.binary(20)),
            ("receipt_root", pa.binary(32)),
            ("receipt_status", pa.decimal128(38, 0)),
            ("receipt_effective_gas_price", pa.decimal128(38, 0)),
        ]
    ),
}

SCHEMA_MAPPING = {"account": ACCOUNT_SCHEMA_RAW}


def write_parquet(
    path, table_name, parameters, schema_table, partition_cols=["partition"]
):
    if not parameters:
        return
    table = pa.Table.from_pylist(parameters, schema=schema_table[table_name])

    if path.startswith("hdfs://"):
        o = urlparse(path)
        fs = HadoopFileSystem(o.hostname, o.port)

        pq.write_to_dataset(
            table,
            root_path=o.path / table_name,
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
            filesystem=fs,
        )
    else:
        path = Path(path)
        if path.exists():
            pq.write_to_dataset(
                table,
                root_path=path / table_name,
                partition_cols=partition_cols,
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            raise Exception(f"Parquet file path not found: {path}")
