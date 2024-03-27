"""

import pyarrow as pa

ACCOUNT_SCHEMA_RAW = {
    "log": pa.schema(
        [
            ("partition", pa.int32()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("address", pa.binary(20)),
            ("data", pa.large_binary()),
            ("topics", pa.list_(pa.binary(32))),
            ("topic0", pa.binary()),  # either 32 long or 0 in rare cases
            ("tx_hash", pa.binary(32)),
            ("log_index", pa.int16()),
            ("transaction_index", pa.int32()),
        ]
    ),
    "trace": pa.schema(
        [
            ("partition", pa.int32()),
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
            ("partition", pa.int32()),
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
            ("partition", pa.int32()),
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
"""
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

ACCOUNT_SCHEMA_RAW = {
    "log": StructType(
        [
            StructField("partition", IntegerType(), True),
            StructField("block_id_group", IntegerType(), True),
            StructField("block_id", IntegerType(), True),
            StructField("block_hash", BinaryType(), True),
            StructField("address", BinaryType(), True),
            StructField(
                "data", BinaryType(), True
            ),  # pyspark does not have large_binary, using BinaryType
            StructField("topics", ArrayType(BinaryType(), True), True),
            StructField("topic0", BinaryType(), True),
            StructField("tx_hash", BinaryType(), True),
            StructField("log_index", ShortType(), True),
            StructField("transaction_index", IntegerType(), True),
        ]
    ),
    "trace": StructType(
        [
            StructField("partition", IntegerType(), True),
            StructField("block_id_group", IntegerType(), True),
            StructField("block_id", IntegerType(), True),
            StructField("tx_hash", BinaryType(), True),
            StructField("transaction_index", IntegerType(), True),
            StructField("from_address", BinaryType(), True),
            StructField("to_address", BinaryType(), True),
            StructField("value", DecimalType(38, 0), True),
            StructField(
                "input", BinaryType(), True
            ),  # pyspark does not have large_binary, using BinaryType
            StructField(
                "output", BinaryType(), True
            ),  # pyspark does not have large_binary, using BinaryType
            StructField("trace_type", StringType(), True),
            StructField("call_type", StringType(), True),
            StructField("reward_type", StringType(), True),
            StructField("gas", IntegerType(), True),
            StructField("gas_used", DecimalType(38, 0), True),
            StructField("subtraces", IntegerType(), True),
            StructField("trace_address", StringType(), True),
            StructField("error", StringType(), True),
            StructField("status", ShortType(), True),
            StructField("trace_id", StringType(), True),
            StructField("trace_index", IntegerType(), True),
        ]
    ),
    "block": StructType(
        [
            StructField("partition", IntegerType(), True),
            StructField("block_id_group", IntegerType(), True),
            StructField("block_id", IntegerType(), True),
            StructField("block_hash", BinaryType(), True),
            StructField("parent_hash", BinaryType(), True),
            StructField("nonce", BinaryType(), True),
            StructField("sha3_uncles", BinaryType(), True),
            StructField("logs_bloom", BinaryType(), True),
            StructField("transactions_root", BinaryType(), True),
            StructField("state_root", BinaryType(), True),
            StructField("receipts_root", BinaryType(), True),
            StructField("miner", BinaryType(), True),
            StructField("difficulty", DecimalType(38, 0), True),
            StructField("total_difficulty", DecimalType(38, 0), True),
            StructField("size", LongType(), True),
            StructField(
                "extra_data", BinaryType(), True
            ),  # pyspark does not have large_binary, using BinaryType
            StructField("gas_limit", IntegerType(), True),
            StructField("gas_used", IntegerType(), True),
            StructField("base_fee_per_gas", DecimalType(38, 0), True),
            StructField("timestamp", IntegerType(), True),
            StructField("transaction_count", IntegerType(), True),
        ]
    ),
    "transaction": StructType(
        [
            StructField("partition", IntegerType(), True),
            StructField("tx_hash_prefix", StringType(), True),
            StructField("tx_hash", BinaryType(), True),
            StructField("nonce", IntegerType(), True),
            StructField("block_hash", BinaryType(), True),
            StructField("block_id_group", IntegerType(), True),
            StructField("block_id", IntegerType(), True),
            StructField("transaction_index", IntegerType(), True),
            StructField("from_address", BinaryType(), True),
            StructField("to_address", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("gas", IntegerType(), True),
            StructField("gas_price", DecimalType(38, 0), True),
            StructField(
                "input", BinaryType(), True
            ),  # pyspark does not have large_binary, using BinaryType
            StructField("block_timestamp", IntegerType(), True),
            StructField("max_fee_per_gas", DecimalType(38, 0), True),
            StructField("max_priority_fee_per_gas", DecimalType(38, 0), True),
            StructField("transaction_type", DecimalType(38, 0), True),
            StructField("receipt_cumulative_gas_used", DecimalType(38, 0), True),
            StructField("receipt_gas_used", DecimalType(38, 0), True),
            StructField("receipt_contract_address", BinaryType(), True),
            StructField("receipt_root", BinaryType(), True),
            StructField("receipt_status", DecimalType(38, 0), True),
            StructField("receipt_effective_gas_price", DecimalType(38, 0), True),
        ]
    ),
}
