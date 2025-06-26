from copy import deepcopy

try:
    import pyarrow as pa
except ImportError:
    ACCOUNT_TRX_SCHEMA_RAW = {}
else:
    from .account import ACCOUNT_SCHEMA_RAW

    ACCOUNT_TRX_SCHEMA_RAW = deepcopy(ACCOUNT_SCHEMA_RAW)

    block_schema = ACCOUNT_TRX_SCHEMA_RAW["block"]

    def set_field_type(schema, field_name, new_type):
        ind = schema.get_field_index(field_name)
        if ind == -1:
            new_field = pa.field(field_name, new_type)
            return schema.append(new_field)
        else:
            new_field = pa.field(field_name, new_type)
            return schema.set(ind, new_field)

    block_schema = set_field_type(block_schema, "state_root", pa.binary())
    block_schema = set_field_type(block_schema, "gas_limit", pa.int64())
    block_schema = set_field_type(block_schema, "gas_used", pa.int64())
    ACCOUNT_TRX_SCHEMA_RAW["block"] = block_schema

    transaction_schema = ACCOUNT_TRX_SCHEMA_RAW["transaction"]
    transaction_schema = set_field_type(transaction_schema, "gas", pa.int64())
    ACCOUNT_TRX_SCHEMA_RAW["transaction"] = transaction_schema

    ACCOUNT_TRX_SCHEMA_RAW.update(
        {
            "trace": pa.schema(
                [
                    ("partition", pa.int32()),
                    ("block_id", pa.int32()),
                    ("tx_hash", pa.binary(32)),
                    ("internal_index", pa.int16()),
                    ("transferto_address", pa.binary(20)),
                    ("call_info_index", pa.int16()),
                    ("caller_address", pa.binary(20)),
                    ("call_value", pa.binary()),
                    ("rejected", pa.bool_()),
                    ("call_token_id", pa.int32()),
                    ("note", pa.string()),
                    ("trace_index", pa.int32()),
                ]
            ),
            "trc10": pa.schema(
                [
                    ("partition", pa.int32()),
                    ("owner_address", pa.binary(20)),
                    ("name", pa.string()),
                    ("abbr", pa.string()),
                    (
                        "total_supply",
                        pa.int64(),
                    ),
                    (
                        "trx_num",
                        pa.int64(),
                    ),
                    (
                        "num",
                        pa.int64(),
                    ),
                    (
                        "start_time",
                        pa.int64(),
                    ),
                    (
                        "end_time",
                        pa.int64(),
                    ),
                    ("description", pa.string()),
                    ("url", pa.string()),
                    ("id", pa.int32()),
                    (
                        "frozen_supply",
                        pa.list_(
                            pa.struct(
                                [
                                    ("frozen_amount", pa.int64()),
                                    ("frozen_days", pa.int64()),
                                ]
                            )
                        ),
                    ),
                    (
                        "public_latest_free_net_time",
                        pa.int64(),
                    ),
                    ("vote_score", pa.int16()),
                    ("free_asset_net_limit", pa.int64()),
                    ("public_free_asset_net_limit", pa.int64()),
                    ("precision", pa.int16()),
                    ("public_free_asset_net_usage", pa.int64()),
                    ("order", pa.int64()),
                ]
            ),
            "fee": pa.schema(
                [
                    ("partition", pa.int32()),
                    ("block_id", pa.int32()),
                    ("tx_hash", pa.binary(32)),
                    ("fee", pa.int64()),
                    ("energy_usage", pa.int64()),
                    ("energy_fee", pa.int64()),
                    ("origin_energy_usage", pa.int64()),
                    ("energy_usage_total", pa.int64()),
                    ("net_usage", pa.int64()),
                    ("net_fee", pa.int64()),
                    ("result", pa.int32()),
                    ("energy_penalty_total", pa.int64()),
                ]
            ),
        }
    )

BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX = {
    "transaction": ["value"],
    "trace": ["call_value"],
    "block": ["difficulty", "total_difficulty"],
    "log": [],
    "fee": [],
}
