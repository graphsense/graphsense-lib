"""Delta Lake -> v3 raw account keyspace (eth, trx).

Backfill only, as with :mod:`graphsense_v3.spark.raw_utxo`: blind inserts, no
read-back, re-runnable over the same range.

Unlike UTXO there is no id to assign -- an account ``tx_id`` is
``(block_id << 32) + transaction_index``, derivable from the transaction itself,
which is what lets the transformed keyspace drop both id-mapping tables (56% of
the TRX transformed keyspace) and with them the cross-table visibility race
behind the 2026-07-03 incident.

The one piece of real computation is the four **range pointers** on
``transaction``. A transaction's logs occupy a contiguous ``log_index`` range
because ``log_index`` is a block-scoped counter and transactions execute in
order, so ``(first_log_index, no_logs)`` turns a whole-block partition scan into
an exact clustering slice -- the same read shape at ~2% of the storage of a
duplicated per-transaction table. :func:`preflight` checks that contiguity
rather than assuming it; the doc flags ``trace_index`` in particular as
unverified, and TRON's trace model is different again.

``exchange_rates`` and ``token_exchange_rates`` are not written here: rates are
not in the lake, and the existing gslib paths already populate those tables.
"""

# NOTE: no `from __future__ import annotations` -- this module builds pandas UDFs
# through graphsense_v3.spark.columns, whose annotations pyspark reads directly.

from typing import TYPE_CHECKING, Optional

from graphsense_v3.config import (
    CONFIGURATION_SCHEMA,
    NetworkConfig,
    config_for,
)
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import writer
from graphsense_v3.spark.columns import (
    bytes_to_varint_udf,
    day_from_timestamp,
    hex_prefix,
    id_group,
)
from graphsense_v3.spark.udf import tx_id_expr

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from graphsense_v3.spark.source import LakeSource

#: Written in this order; TRON adds two more.
TABLES = (
    "block",
    "block_by_date",
    "transaction",
    "transaction_by_tx_prefix",
    "log",
    "trace",
    "configuration",
)

TRX_TABLES = ("trc10", "fee")


def tables_for(network: str) -> tuple:
    return TABLES + (TRX_TABLES if network == "trx" else ())


def _pointers(
    events: "DataFrame", index_column: str, first: str, count: str
) -> "DataFrame":
    """``tx_hash -> (first index, how many)`` for a block-scoped event index."""
    from pyspark.sql import functions as F

    return events.groupBy("tx_hash").agg(
        F.min(index_column).cast("int").alias(first),
        F.count("*").cast("int").alias(count),
    )


def _range_pointers(
    lake: "LakeSource", start_block: Optional[int], end_block: Optional[int]
) -> "DataFrame":
    """The four pointer columns, keyed by ``tx_hash``.

    Logs and traces of a transaction are always in its own block, so restricting
    both sides to the same block range gives exact counts.
    """
    logs = lake.read("log", start_block=start_block, end_block=end_block)
    traces = lake.read("trace", start_block=start_block, end_block=end_block)
    return _pointers(logs, "log_index", "first_log_index", "no_logs").join(
        _pointers(traces, "trace_index", "first_trace_index", "no_traces"),
        on="tx_hash",
        how="outer",
    )


def _block(blocks: "DataFrame", config: NetworkConfig) -> "DataFrame":
    from pyspark.sql import functions as F

    varint = bytes_to_varint_udf()
    return blocks.select(
        id_group(F.col("block_id"), config.block_bucket_size).alias("block_id_group"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("block_hash"),
        F.col("parent_hash"),
        F.col("nonce"),
        F.col("sha3_uncles"),
        F.col("logs_bloom"),
        F.col("transactions_root"),
        F.col("state_root"),
        F.col("receipts_root"),
        F.col("miner"),
        varint(F.col("difficulty")).alias("difficulty"),
        varint(F.col("total_difficulty")).alias("total_difficulty"),
        F.col("size").cast("int").alias("size"),
        F.col("extra_data"),
        # eth wrote these as int, trx as varint/bigint. Unified to bigint.
        F.col("gas_limit").cast("bigint").alias("gas_limit"),
        F.col("gas_used").cast("bigint").alias("gas_used"),
        F.col("base_fee_per_gas").cast("bigint").alias("base_fee_per_gas"),
        F.col("timestamp").cast("bigint").alias("timestamp"),
        F.col("transaction_count").cast("int").alias("no_transactions"),
    )


def _transaction(
    txs: "DataFrame", pointers: "DataFrame", config: NetworkConfig
) -> "DataFrame":
    from pyspark.sql import functions as F

    varint = bytes_to_varint_udf()
    joined = txs.join(pointers, on="tx_hash", how="left")
    return joined.select(
        id_group(F.col("block_id"), config.tx_block_bucket_size).alias(
            "block_id_group"
        ),
        tx_id_expr(F.col("block_id"), F.col("transaction_index")).alias("tx_id"),
        F.col("tx_hash"),
        F.col("nonce").cast("int").alias("nonce"),
        F.col("block_hash"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("transaction_index").cast("int").alias("transaction_index"),
        F.col("from_address"),
        F.col("to_address"),
        varint(F.col("value")).alias("value"),
        F.col("gas").cast("bigint").alias("gas"),
        F.col("gas_price").cast("decimal(38,0)").alias("gas_price"),
        F.col("input"),
        F.col("block_timestamp").cast("bigint").alias("block_timestamp"),
        F.col("max_fee_per_gas").cast("bigint").alias("max_fee_per_gas"),
        F.col("max_priority_fee_per_gas")
        .cast("bigint")
        .alias("max_priority_fee_per_gas"),
        F.col("transaction_type").cast("tinyint").alias("transaction_type"),
        F.col("receipt_cumulative_gas_used")
        .cast("bigint")
        .alias("receipt_cumulative_gas_used"),
        F.col("receipt_gas_used").cast("bigint").alias("receipt_gas_used"),
        F.col("receipt_contract_address"),
        F.col("receipt_root"),
        F.col("receipt_status").cast("tinyint").alias("receipt_status"),
        F.col("receipt_effective_gas_price")
        .cast("bigint")
        .alias("receipt_effective_gas_price"),
        F.col("max_fee_per_blob_gas").cast("bigint").alias("max_fee_per_blob_gas"),
        F.col("blob_versioned_hashes"),
        F.col("v").cast("smallint").alias("v"),
        # r and s are opaque 32-byte signature halves. v2 stored them as varint,
        # where they routinely exceed 38 decimal digits; as blobs they round-trip.
        F.col("r"),
        F.col("s"),
        F.coalesce(F.col("first_log_index"), F.lit(None).cast("int")).alias(
            "first_log_index"
        ),
        F.coalesce(F.col("no_logs"), F.lit(0)).cast("int").alias("no_logs"),
        F.coalesce(F.col("first_trace_index"), F.lit(None).cast("int")).alias(
            "first_trace_index"
        ),
        F.coalesce(F.col("no_traces"), F.lit(0)).cast("int").alias("no_traces"),
    )


def _trace(traces: "DataFrame", network: str, config: NetworkConfig) -> "DataFrame":
    from pyspark.sql import functions as F

    varint = bytes_to_varint_udf()
    shared: list[Column] = [
        id_group(F.col("block_id"), config.block_bucket_size).alias("block_id_group"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("trace_index").cast("int").alias("trace_index"),
        F.col("tx_hash"),
    ]
    if network == "trx":
        # TRON's own names for the same three things. Renamed here rather than
        # carried through, so nothing downstream branches on the chain to find
        # out who sent what to whom.
        participants = [
            F.col("caller_address").alias("from_address"),
            F.col("transferto_address").alias("to_address"),
            varint(F.col("call_value")).alias("value"),
        ]
        specific = [
            F.col("internal_index").cast("smallint").alias("internal_index"),
            F.col("call_info_index").cast("smallint").alias("call_info_index"),
            F.col("call_token_id").cast("int").alias("call_token_id"),
            F.col("note"),
            F.col("rejected"),
        ]
    else:
        participants = [
            F.col("from_address"),
            F.col("to_address"),
            varint(F.col("value")).alias("value"),
        ]
        specific = [
            F.col("transaction_index").cast("int").alias("transaction_index"),
            F.col("input"),
            F.col("output"),
            F.col("trace_type"),
            F.col("call_type"),
            F.col("reward_type"),
            F.col("gas").cast("bigint").alias("gas"),
            F.col("gas_used").cast("bigint").alias("gas_used"),
            F.col("subtraces").cast("int").alias("subtraces"),
            F.col("trace_address"),
            F.col("error"),
            F.col("status").cast("smallint").alias("status"),
            F.col("trace_id"),
        ]
    return traces.select(*shared, *participants, *specific)


def _trc10(lake: "LakeSource") -> "DataFrame":
    """TRC10 asset definitions. Block-independent, so the whole table is read."""
    from pyspark.sql import functions as F

    table = lake.read("trc10", block_column=None, partitioned=False)
    return table.select(
        F.col("id").cast("int").alias("id"),
        F.col("owner_address"),
        F.col("name"),
        F.col("abbr"),
        F.col("total_supply").cast("decimal(38,0)").alias("total_supply"),
        F.col("trx_num").cast("decimal(38,0)").alias("trx_num"),
        F.col("num").cast("decimal(38,0)").alias("num"),
        F.col("start_time").cast("decimal(38,0)").alias("start_time"),
        F.col("end_time").cast("decimal(38,0)").alias("end_time"),
        F.col("description"),
        F.col("url"),
        F.col("frozen_supply"),
        F.col("public_latest_free_net_time")
        .cast("decimal(38,0)")
        .alias("public_latest_free_net_time"),
        F.col("vote_score").cast("smallint").alias("vote_score"),
        F.col("free_asset_net_limit").cast("bigint").alias("free_asset_net_limit"),
        F.col("public_free_asset_net_limit")
        .cast("bigint")
        .alias("public_free_asset_net_limit"),
        F.col("precision").cast("smallint").alias("precision"),
    )


def _fee(
    lake: "LakeSource",
    config: NetworkConfig,
    start_block: Optional[int],
    end_block: Optional[int],
) -> "DataFrame":
    """TRON resource consumption, keyed like ``transaction`` (D13).

    The lake row has a ``tx_hash`` and a ``block_id`` but no
    ``transaction_index``, so the id costs a join against the transactions of the
    same block range. Paid once per backfill, against a third read on every
    request that wants a fee.
    """
    from pyspark.sql import functions as F

    txs = lake.read("transaction", start_block=start_block, end_block=end_block).select(
        F.col("tx_hash"),
        id_group(F.col("block_id"), config.tx_block_bucket_size).alias(
            "block_id_group"
        ),
        tx_id_expr(F.col("block_id"), F.col("transaction_index")).alias("tx_id"),
    )
    fees = lake.read("fee", start_block=start_block, end_block=end_block).drop(
        "block_id"
    )
    return fees.join(txs, on="tx_hash", how="inner").select(
        F.col("block_id_group"),
        F.col("tx_id"),
        F.col("tx_hash"),
        F.col("fee").cast("bigint").alias("fee"),
        F.col("energy_usage").cast("bigint").alias("energy_usage"),
        F.col("energy_fee").cast("bigint").alias("energy_fee"),
        F.col("origin_energy_usage").cast("bigint").alias("origin_energy_usage"),
        F.col("energy_usage_total").cast("bigint").alias("energy_usage_total"),
        F.col("net_usage").cast("bigint").alias("net_usage"),
        F.col("net_fee").cast("bigint").alias("net_fee"),
        F.col("result").cast("int").alias("result"),
        F.col("energy_penalty_total").cast("bigint").alias("energy_penalty_total"),
    )


def configuration_row(
    lake: "LakeSource", config: NetworkConfig, keyspace: str
) -> "DataFrame":
    return lake.spark.createDataFrame(
        [config.as_row(keyspace)], schema=CONFIGURATION_SCHEMA
    )


def build(
    lake: "LakeSource",
    network: str,
    keyspace: str,
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    config: Optional[NetworkConfig] = None,
) -> dict:
    """The v3 raw tables for ``network`` as DataFrames, keyed by table name."""
    from pyspark.sql import functions as F

    cfg = config or config_for(network)
    blocks = lake.read("block", start_block=start_block, end_block=end_block)
    txs = lake.read("transaction", start_block=start_block, end_block=end_block)
    logs = lake.read("log", start_block=start_block, end_block=end_block)
    traces = lake.read("trace", start_block=start_block, end_block=end_block)

    out: "dict[str, DataFrame]" = {}
    out["block"] = _block(blocks, cfg)
    out["block_by_date"] = blocks.select(
        day_from_timestamp(F.col("timestamp")).alias("day"),
        F.col("timestamp").cast("bigint").alias("timestamp"),
        F.col("block_id").cast("int").alias("block_id"),
    )
    out["transaction_by_tx_prefix"] = txs.select(
        hex_prefix(F.col("tx_hash"), cfg.tx_prefix_length).alias("tx_prefix"),
        F.col("tx_hash"),
        tx_id_expr(F.col("block_id"), F.col("transaction_index")).alias("tx_id"),
    )
    out["transaction"] = _transaction(
        txs, _range_pointers(lake, start_block, end_block), cfg
    )
    out["log"] = logs.select(
        id_group(F.col("block_id"), cfg.block_bucket_size).alias("block_id_group"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("log_index").cast("int").alias("log_index"),
        F.col("block_hash"),
        F.col("address"),
        F.col("data"),
        F.col("topics"),
        F.col("topic0"),
        F.col("tx_hash"),
        F.col("transaction_index").cast("int").alias("transaction_index"),
    )
    out["trace"] = _trace(traces, network, cfg)
    out["configuration"] = configuration_row(lake, cfg, keyspace)

    if network == "trx":
        out["trc10"] = _trc10(lake)
        out["fee"] = _fee(lake, cfg, start_block, end_block)
    return out


def preflight(
    lake: "LakeSource",
    network: str,
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
) -> list[str]:
    """Check what the range pointers assume. Empty means go.

    The doc's PRE-RUN CHECK: contiguity is certain for ETH logs, unverified for
    ``trace_index``, and TRON's trace model is different again. If traces are not
    contiguous, the fallback is a duplicated table for traces only -- so this
    must be answered before a backfill, not after.
    """
    problems: list[str] = []
    for table, index_column in (("log", "log_index"), ("trace", "trace_index")):
        events = lake.read(table, start_block=start_block, end_block=end_block)
        gaps = non_contiguous(events, index_column)
        if gaps:
            problems.append(
                f"{gaps} transactions whose {index_column} values are not "
                f"contiguous; (first, count) pointers cannot address their {table}s"
            )
    return problems


def non_contiguous(events: "DataFrame", index_column: str) -> int:
    """How many transactions hold a non-contiguous run of ``index_column``."""
    from pyspark.sql import functions as F

    return (
        events.groupBy("tx_hash")
        .agg(
            F.min(index_column).alias("lo"),
            F.max(index_column).alias("hi"),
            F.count("*").alias("n"),
        )
        .where(F.col("hi") - F.col("lo") + 1 != F.col("n"))
        .count()
    )


def load(
    lake: "LakeSource",
    network: str,
    keyspace: str,
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    tables: Optional[tuple] = None,
    config: Optional[NetworkConfig] = None,
) -> list[str]:
    """Write the raw account tables into ``keyspace``. Returns what was written."""
    cfg = config or config_for(network)
    schema = schema_for(network, Kind.RAW)
    frames = build(
        lake,
        network,
        keyspace,
        start_block=start_block,
        end_block=end_block,
        config=cfg,
    )
    selected = tables or tables_for(network)

    for name in selected:
        writer.check(frames[name], schema.table(name))

    written: list[str] = []
    for name in selected:
        writer.write(frames[name], schema.table(name), keyspace)
        written.append(name)
    return written
