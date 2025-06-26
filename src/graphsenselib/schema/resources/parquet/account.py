try:
    import pyarrow as pa
except ImportError:
    ACCOUNT_SCHEMA_RAW = {}
else:
    ACCOUNT_SCHEMA_RAW = {
        "log": pa.schema(
            [
                ("partition", pa.int32()),
                ("block_id", pa.int32()),
                ("block_hash", pa.binary(32)),
                ("address", pa.binary(20)),
                ("data", pa.binary()),
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
                ("block_id", pa.int32()),
                ("tx_hash", pa.binary(32)),
                ("transaction_index", pa.int32()),
                ("from_address", pa.binary(20)),
                ("to_address", pa.binary(20)),
                ("value", pa.binary()),
                ("input", pa.binary()),
                ("output", pa.binary()),
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
                ("difficulty", pa.binary()),
                ("total_difficulty", pa.binary()),
                ("size", pa.int64()),
                ("extra_data", pa.binary()),
                ("gas_limit", pa.int32()),
                ("gas_used", pa.int32()),
                ("base_fee_per_gas", pa.int64()),
                ("timestamp", pa.int32()),
                ("transaction_count", pa.int32()),
                # withdrawals', 'excess_blob_gas', 'withdrawals_root', 'blob_gas_used
                (
                    "withdrawals",
                    pa.list_(
                        pa.struct(
                            [
                                ("index", pa.int64()),  # maybe int32?
                                ("validator_index", pa.int64()),  # maybe int32?
                                ("address", pa.string()),
                                ("amount", pa.binary()),
                            ]
                        )
                    ),
                ),
                ("excess_blob_gas", pa.int64()),
                ("withdrawals_root", pa.string()),
                ("blob_gas_used", pa.int64()),
            ]
        ),
        "transaction": pa.schema(
            [
                ("partition", pa.int32()),
                ("tx_hash_prefix", pa.string()),
                ("tx_hash", pa.binary(32)),
                ("nonce", pa.int32()),
                ("block_hash", pa.binary(32)),
                ("block_id", pa.int32()),
                ("transaction_index", pa.int32()),
                ("from_address", pa.binary(20)),
                ("to_address", pa.binary(20)),
                ("value", pa.binary()),
                ("gas", pa.int32()),
                (
                    "gas_price",
                    pa.int64(),
                ),  # todo check, ethereumetl has this, but varint in gslib
                ("input", pa.binary()),
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
                ("receipt_l1_gas_used", pa.int64()),
                ("receipt_l1_fee", pa.int64()),
                ("receipt_l1_fee_scalar", pa.float32()),
                ("receipt_l1_gas_price", pa.int64()),
                ("receipt_blob_gas_used", pa.int64()),
                ("receipt_blob_gas_price", pa.int64()),
                ("max_fee_per_blob_gas", pa.int64()),
                ("blob_versioned_hashes", pa.list_(pa.binary(32))),
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
