import pyarrow as pa

UTXO_SCHEMA_RAW = {
    "block": pa.schema(
        [
            ("partition", pa.int32()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            ("block_hash", pa.binary(32)),
            ("timestamp", pa.int32()),
            ("no_transactions", pa.int32()),
        ]
    ),
    "transaction": pa.schema(
        [
            ("partition", pa.int32()),
            ("tx_id_group", pa.int32()),
            ("tx_id", pa.int64()),
            ("tx_hash", pa.binary(32)),
            ("block_id", pa.int32()),
            ("timestamp", pa.int32()),
            ("coinbase", pa.bool_()),
            ("total_input", pa.int64()),
            ("total_output", pa.int64()),
            (
                "outputs",
                pa.list_(
                    pa.struct(
                        [
                            ("address", pa.list_(pa.string())),
                            ("value", pa.int64()),
                            ("address_type", pa.int16()),
                        ]
                    )
                ),
            ),
            ("spent_transaction_hashes", pa.list_(pa.binary(32))),
            ("coinjoin", pa.bool_()),
        ]
    ),
    "transaction_spent_in": pa.schema(
        [
            ("partition", pa.int32()),
            ("spent_tx_prefix", pa.string()),
            ("spent_tx_hash", pa.binary(32)),
            ("spent_output_index", pa.int32()),
            ("spending_tx_hash", pa.binary(32)),
            ("spending_input_index", pa.int32()),
        ]
    ),
    "transaction_spending": pa.schema(
        [
            ("partition", pa.int32()),
            ("spending_tx_prefix", pa.string()),
            ("spending_tx_hash", pa.binary(32)),
            ("spending_input_index", pa.int32()),
            ("spent_tx_hash", pa.binary(32)),
            ("spent_output_index", pa.int32()),
        ]
    ),
    "transaction_by_tx_prefix": pa.schema(
        [
            ("partition", pa.int32()),
            ("tx_prefix", pa.string()),
            ("tx_hash", pa.binary(32)),
            ("tx_id", pa.int64()),
        ]
    ),
    "block_transactions": pa.schema(
        [
            ("partition", pa.int32()),
            ("block_id_group", pa.int32()),
            ("block_id", pa.int32()),
            (
                "txs",
                pa.list_(
                    pa.struct(
                        [
                            ("tx_id", pa.int64()),
                            ("no_inputs", pa.int32()),
                            ("no_outputs", pa.int32()),
                            ("total_input", pa.int64()),
                            ("total_output", pa.int64()),
                        ]
                    )
                ),
            ),
        ]
    ),
    "exchange_rates": pa.schema(
        [
            ("date", pa.string()),
            ("fiat_values", pa.map_(pa.string(), pa.float32())),
        ]
    ),
    "summary_statistics": pa.schema(
        [
            ("id", pa.string()),
            ("no_blocks", pa.int32()),
            ("no_txs", pa.int64()),
            ("timestamp", pa.int32()),
        ]
    ),
    "configuration": pa.schema(
        [
            ("id", pa.string()),
            ("block_bucket_size", pa.int32()),
            ("tx_prefix_length", pa.int32()),
            ("tx_bucket_size", pa.int32()),
        ]
    ),
}
