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
            ("value", pa.large_binary()),
            ("input", pa.large_binary()),
            ("output", pa.large_binary()),
            ("trace_type", pa.string()),
            ("call_type", pa.string()),
            ("reward_type", pa.string()),
            ("gas", pa.int32()),
            ("gas_used", pa.int64()),
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
            ("difficulty", pa.large_binary()),
            ("total_difficulty", pa.large_binary()),
            ("size", pa.int64()),
            ("extra_data", pa.large_binary()),
            ("gas_limit", pa.int32()),
            ("gas_used", pa.int32()),
            ("base_fee_per_gas", pa.int64()),
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
            ("value", pa.large_binary()),
            ("gas", pa.int32()),
            (
                "gas_price",
                pa.int64(),
            ),  # todo check, ethereumetl has this, but varint in gslib
            ("input", pa.large_binary()),
            ("block_timestamp", pa.int32()),
            ("max_fee_per_gas", pa.int64()),
            ("max_priority_fee_per_gas", pa.int64()),
            ("transaction_type", pa.int64()),
            (
                "receipt_cumulative_gas_used",
                pa.int64(),
            ),  # todo check, ethereumetl has this, but varint in gslib
            (
                "receipt_gas_used",
                pa.int64(),
            ),  # todo check, ethereumetl has this, but varint in gslib
            ("receipt_contract_address", pa.binary(20)),
            ("receipt_root", pa.binary(32)),
            ("receipt_status", pa.int64()),
            ("receipt_effective_gas_price", pa.int64()),
        ]
    ),
}

BINARY_COL_CONVERSION_MAP_ACCOUNT = {
    "transaction": ["value"],
    "trace": ["value"],
    "block": ["difficulty", "total_difficulty"],
    "log": [],
}

# todo check size
