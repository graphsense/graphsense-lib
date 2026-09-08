"""Delta Lake -> v3 raw account keyspace (eth, trx).

Backfill only, as with :mod:`graphsense_v3.spark.raw_utxo`: blind inserts, no
read-back, re-runnable over the same range.

Unlike UTXO there is no id to assign -- an account ``tx_id`` is
``(block_id << 32) + transaction_index``, derivable from the transaction itself,
which is what lets the derived keyspace drop both id-mapping tables (56% of
the TRX derived keyspace) and with them the cross-table visibility race
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
    NetworkConfig,
    config_for,
)
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import writer
from graphsense_v3.spark.columns import (
    bytes_to_varint_udf,
    day_key_from_timestamp,
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


def _trace(
    traces: "DataFrame",
    network: str,
    config: NetworkConfig,
    ids: "Optional[DataFrame]" = None,
) -> "DataFrame":
    """One trace row, in the shape both chains share.

    ``ids`` supplies ``tx_hash -> tx_id`` for TRON, whose trace rows carry no
    ``transaction_index``; on eth the id comes off the row itself.
    """
    from pyspark.sql import functions as F

    varint = bytes_to_varint_udf()
    if network == "trx":
        if ids is None:
            raise ValueError("a trx trace needs tx_ids_by_hash to resolve tx_id")
        traces = traces.join(ids.drop("block_id_group"), on="tx_hash", how="inner")
        tx_id = F.col("tx_id")
        # TRON has no status code, only a `rejected` flag. Mapped onto eth's
        # convention so that "did this trace succeed" is one column on both
        # chains. Derived here rather than in the source, and a NULL `rejected`
        # stays NULL rather than being read as success.
        status = (
            F.when(F.isnull(F.col("rejected")), F.lit(None))
            .when(F.col("rejected"), F.lit(0))
            .otherwise(F.lit(1))
            .cast("smallint")
        )
    else:
        tx_id = tx_id_expr(F.col("block_id"), F.col("transaction_index"))
        status = F.col("status").cast("smallint")

    shared: list[Column] = [
        id_group(F.col("block_id"), config.block_bucket_size).alias("block_id_group"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("trace_index").cast("int").alias("trace_index"),
        F.col("tx_hash"),
        tx_id.alias("tx_id"),
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
        ]
    else:
        participants = [
            F.col("from_address"),
            F.col("to_address"),
            varint(F.col("value")).alias("value"),
        ]
        specific = [
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
            F.col("trace_id"),
        ]
    return traces.select(*shared, *participants, status.alias("status"), *specific)


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


def tx_ids_by_hash(
    lake: "LakeSource",
    config: NetworkConfig,
    start_block: Optional[int],
    end_block: Optional[int],
) -> "DataFrame":
    """``tx_hash -> (block_id_group, tx_id)`` over a block range.

    Needed by the two TRON tables whose lake rows carry a ``tx_hash`` but no
    ``transaction_index`` -- ``fee`` and ``trace``. This is the one shuffle in
    the account backfill, and it is deliberate: the transform needs the same
    mapping on every run, so paying for it once per ingest is the cheaper end of
    the trade.
    """
    from pyspark.sql import functions as F

    return lake.read(
        "transaction", start_block=start_block, end_block=end_block
    ).select(
        F.col("tx_hash"),
        id_group(F.col("block_id"), config.tx_block_bucket_size).alias(
            "block_id_group"
        ),
        tx_id_expr(F.col("block_id"), F.col("transaction_index")).alias("tx_id"),
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

    ids = tx_ids_by_hash(lake, config, start_block, end_block)
    fees = lake.read("fee", start_block=start_block, end_block=end_block).drop(
        "block_id"
    )
    return fees.join(ids, on="tx_hash", how="inner").select(
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
    """The single ``configuration`` row, as a DataFrame."""
    from graphsense_v3.config import configuration_row as build_row

    return build_row(lake.spark, config, keyspace)


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
    # TRON needs the hash -> id mapping twice (trace and fee); build it once.
    ids = (
        tx_ids_by_hash(lake, cfg, start_block, end_block).cache()
        if network == "trx"
        else None
    )

    out: "dict[str, DataFrame]" = {}
    out["block"] = _block(blocks, cfg)
    out["block_by_date"] = blocks.select(
        day_key_from_timestamp(F.col("timestamp")).alias("day"),
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
        tx_id_expr(F.col("block_id"), F.col("transaction_index")).alias("tx_id"),
    )
    out["trace"] = _trace(traces, network, cfg, ids=ids)
    out["configuration"] = configuration_row(lake, cfg, keyspace)

    if network == "trx":
        out["trc10"] = _trc10(lake)
        out["fee"] = _fee(lake, cfg, start_block, end_block)
    return out


#: Every lake table this loader may read, over all account networks. This is
#: the drift guard -- a table read but absent here would be pinned late, at its
#: first read -- and NOT the set to pin: see :func:`lake_tables_for`.
LAKE_TABLES = ("block", "transaction", "trace", "log", "fee", "trc10")

#: Lake tables only TRON has. `fee` is a TRON concept (bandwidth/energy priced
#: separately); on eth a fee is gas_used * gas_price off the receipt and there
#: is no such table. `trc10` is TRON's native asset registry.
TRX_LAKE_TABLES = ("fee", "trc10")


def lake_tables_for(network: str) -> tuple:
    """The lake tables to PIN for ``network``.

    Pinning is not free of consequence: `DeltaLake.pin` resolves a version for
    every name it is given, so a name the network's lake does not have fails
    the run before a single row is read -- which is what an eth run did,
    with `PATH_NOT_FOUND: s3a://raw-data/eth/fee`. `build` already branches on
    `network == "trx"` for these two; the pin list has to make the same
    distinction rather than assuming the union.
    """
    return tuple(t for t in LAKE_TABLES if network == "trx" or t not in TRX_LAKE_TABLES)


#: The pinned tables whose tips BOUND a run. Every block has a header and at
#: least one transaction, so these two must reach the same height -- where they
#: do not, the snapshot caught ingest mid-cycle.
#:
#: Not every lake table: `log`, `trace` and `fee` are legitimately sparse at the
#: tip (a block with no logs contributes no rows), so their maxima are reported
#: but never used to cut a run. `trc10` is not block-scoped at all.
BOUND_TABLES = ("block", "transaction")


def preflight(
    lake: "LakeSource",
    network: str,
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
) -> list[str]:
    """Check what the range pointers assume. Empty means go.

    The doc's PRE-RUN CHECK. `transaction` carries (first_log_index, no_logs)
    and (first_trace_index, no_traces) INSTEAD of a per-transaction copy of the
    events -- ~46 GB against ~2 TB on ETH -- and the read they serve is

        WHERE block_id_group = ? AND block_id = ?
          AND <index> >= first AND <index> < first + count

    which returns whatever sits in that range, whoever it belongs to. So the
    property is not "each transaction's indices are contiguous" but the whole
    of :func:`contiguity_problems`: within a block, the transactions' runs must
    TILE the index space -- contiguous, disjoint, and dense from zero.

    Contiguity is certain for ETH logs, unverified for ``trace_index``, and
    TRON's trace model is different again. If traces do not tile, the fallback
    is a duplicated table for traces only, so this must be answered before a
    backfill rather than after: every failure here produces a plausible wrong
    answer, never an error.
    """
    problems: list[str] = []
    for table, index_column in (("log", "log_index"), ("trace", "trace_index")):
        events = lake.read(table, start_block=start_block, end_block=end_block)
        problems += contiguity_problems(events, index_column, table)
    return problems


def contiguity_problems(
    events: "DataFrame", index_column: str, table: str
) -> list[str]:
    """Every way ``index_column`` can break the (first, count) pointers.

    Four checks, because each fails differently and the first one alone passes
    on data that would still serve wrong rows:

    1. **Per transaction, no holes.** A gap makes (first, count) span rows that
       are not the transaction's.
    2. **Per block, no duplicate index.** This is what proves the index is
       BLOCK-scoped rather than transaction-scoped. Per-transaction numbering
       would leave every run internally contiguous -- check 1 passes -- while
       every transaction in the block claims the same range, so a read for one
       returns the logs of all of them.
    3. **Per block, dense from zero.** With 1 and 2 holding, this is what makes
       the runs TILE: disjoint contiguous runs covering 0..n-1 cannot overlap.
    4. **One block per transaction hash.** :func:`_pointers` groups by
       ``tx_hash`` alone, so a hash occurring in two blocks would take a min
       across both and point into the wrong one. Rows with no ``tx_hash`` are
       excluded here and only here: they are ONE group spanning the whole
       range, so including them fails every healthy account chain, and they are
       dropped before any pointer is written because an equi-join on tx_hash
       never matches NULL.

    Rows with no ``tx_hash`` -- ETH block-REWARD traces belong to the block,
    not to a transaction -- are one group of their own under check 1. They are
    never addressed by a pointer, but they do occupy index space, so if they
    are interleaved with transaction traces rather than appended they split a
    transaction's run and check 1 reports it. That is the honest outcome: it
    means the tiling does not hold for traces, and the fallback applies. Read a
    trace failure here as "which rows" before assuming the whole table needs
    duplicating.

    A block whose transactions emitted nothing has no rows at all and never
    reaches these aggregations, so it cannot fail check 3.
    """
    from pyspark.sql import functions as F

    problems: list[str] = []

    holed = (
        events.groupBy("block_id", "tx_hash")
        .agg(
            F.min(index_column).alias("lo"),
            F.max(index_column).alias("hi"),
            F.count("*").alias("n"),
        )
        .where(F.col("hi") - F.col("lo") + 1 != F.col("n"))
        .count()
    )
    if holed:
        problems.append(
            f"{holed} transactions whose {index_column} values are not "
            f"contiguous; (first, count) pointers cannot address their {table}s"
        )

    per_block = events.groupBy("block_id").agg(
        F.count("*").alias("n"),
        F.countDistinct(index_column).alias("distinct"),
        F.min(index_column).alias("lo"),
        F.max(index_column).alias("hi"),
    )
    shared = per_block.where(F.col("distinct") != F.col("n")).count()
    if shared:
        problems.append(
            f"{shared} blocks reuse a {index_column} within the block, so it is "
            f"not block-scoped; the pointers of every transaction in such a "
            f"block address the same {table} rows"
        )
    sparse = per_block.where(
        (F.col("lo") != 0) | (F.col("hi") - F.col("lo") + 1 != F.col("n"))
    ).count()
    if sparse:
        problems.append(
            f"{sparse} blocks whose {index_column} values are not dense from 0, "
            f"so the transactions' runs do not tile the block's {table}s"
        )

    # NOT NULL: the rows with no tx_hash are one group under this grouping, so
    # they span every block in the range and check 4 fires with a count of 1 on
    # a perfectly healthy chain -- which is what an eth dry run reported. They
    # cannot reach a pointer either way: `_pointers` groups by tx_hash and
    # `_transaction` joins that back on tx_hash, and an equi-join never matches
    # NULL, so the NULL group is dropped before a pointer column is written.
    # Check 1 still sees these rows -- per (block_id, tx_hash), where NULL is a
    # group WITHIN one block -- so an interleaved reward trace is still caught.
    split_rows = (
        # `F.isnotnull`, not `Column.isNotNull`: same thing at runtime, but ty
        # cannot resolve the method off pyspark's operator factory.
        events.where(F.isnotnull(F.col("tx_hash")))
        .groupBy("tx_hash")
        .agg(F.countDistinct("block_id").alias("blocks"))
        .where(F.col("blocks") > 1)
    )
    split = split_rows.count()
    if split:
        # Named, not just counted. A bare count cannot be acted on, and the two
        # causes need opposite responses: a real duplicate hash is a chain fact
        # to work around, while a sentinel (all-zero, say) standing in for
        # "no transaction" is this check needing to exclude that value too.
        sample = ", ".join(
            "0x" + row["hex"].lower()
            for row in split_rows.select(F.hex("tx_hash").alias("hex"))
            .limit(3)
            .collect()
        )
        problems.append(
            f"{split} transaction hashes carry {table}s in more than one block; "
            f"the pointer would take a min across both and address the wrong "
            f"one (e.g. {sample})"
        )
    return problems


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
