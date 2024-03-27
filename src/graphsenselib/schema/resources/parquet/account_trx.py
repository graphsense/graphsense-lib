# flake8: noqa: F401

"""
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
ACCOUNT_TRX_SCHEMA_RAW["block"] = block_schema

trace_schema = ACCOUNT_TRX_SCHEMA_RAW["trace"]
# internal_index', 'transferto_address', 'call_info_index',
# 'caller_address', 'call_value', 'rejected', 'call_token_id', 'note
trace_schema = set_field_type(trace_schema, "internal_index", pa.int16())
trace_schema = set_field_type(trace_schema, "transferto_address", pa.binary(20))
trace_schema = set_field_type(trace_schema, "call_info_index", pa.int16())
trace_schema = set_field_type(trace_schema, "caller_address", pa.binary(20))
trace_schema = set_field_type(trace_schema, "call_value", pa.decimal128(38, 0))
trace_schema = set_field_type(trace_schema, "rejected", pa.bool_())
trace_schema = set_field_type(trace_schema, "call_token_id", pa.int32())
trace_schema = set_field_type(trace_schema, "note", pa.string())
ACCOUNT_TRX_SCHEMA_RAW["trace"] = trace_schema


ACCOUNT_SCHEMA_RAW.update(
    {
        "trc10": pa.schema(
            [
                ("partition", pa.int32()),
                ("owner_address", pa.binary(20)),
                ("name", pa.string()),
                ("abbr", pa.string()),
                ("total_supply", pa.decimal128(38, 0)),
                ("trx_num", pa.decimal128(38, 0)),
                ("num", pa.decimal128(38, 0)),
                ("start_time", pa.decimal128(38, 0)),
                ("end_time", pa.decimal128(38, 0)),
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
                ("public_latest_free_net_time", pa.decimal128(38, 0)),
                ("vote_score", pa.int16()),
                ("free_asset_net_limit", pa.int64()),
                ("public_free_asset_net_limit", pa.int64()),
                ("precision", pa.int16()),
            ]
        ),
        "fee": pa.schema(
            [
                ("partition", pa.int32()),
                ("tx_hash_prefix", pa.string()),
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
"""

from .account import ACCOUNT_SCHEMA_RAW

ACCOUNT_TRX_SCHEMA_RAW = ACCOUNT_SCHEMA_RAW
# todo restore functionality for tron
