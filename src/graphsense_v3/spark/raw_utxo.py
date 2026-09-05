"""Delta Lake -> v3 raw UTXO keyspace.

Backfill only: every write is a blind insert, nothing is read back, and running
the same block range twice produces the same rows. Incremental updates are a
separate concern and deliberately not implemented here.

**The lake holds only ``block`` and ``transaction``** for a UTXO network
(``ingest/delta/sink.py:451``). The other four raw tables are derived here, the
same way ``ingest/utxo.py`` derives them on the Cassandra path -- all of it from
the transaction row itself, including ``tx_id``
(``(block_id << 32) + index``, see :func:`graphsense_v3.codec.tx_id`).

Two shape changes carry the rest of the work:

* ``block_transactions`` is gone: ``transaction`` is partitioned by block and
  carries every column it held, so a block's transactions are one partition read
  of it, and block-range -> tx-id-range is arithmetic (:func:`codec.tx_id_range`).
* ``transaction.inputs``/``outputs`` become rows in ``transaction_io``, so a
  20 000-input transaction is writable at all (a >16MB mutation is *rejected* by
  Cassandra, not truncated).
* Address strings become packed bytes, which is what lets the derived side
  key on the address itself rather than on a surrogate id.

``exchange_rates`` is not written here: rates are not in the lake, and the
existing gslib ``exchange-rates`` path already populates that table.
"""

# NOTE: no `from __future__ import annotations` -- this module builds pandas UDFs
# through graphsense_v3.spark.udf, and pyspark reads their annotations directly.

from typing import TYPE_CHECKING, Optional

from graphsense_v3.codec import TX_INDEX_BITS
from graphsense_v3.config import (
    NetworkConfig,
    config_for,
)
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import writer
from graphsense_v3.spark.columns import (
    ADDRESSLESS_TYPES,
    address_type,
    day_key_from_timestamp,
    hex_to_bytes,
    hex_prefix,
    id_group,
)
from graphsense_v3.spark.udf import (
    encode_address_list_udf,
    encode_address_udf,
    tx_id_expr,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from graphsense_v3.spark.source import LakeSource

TABLES = (
    "block",
    "block_by_date",
    "transaction",
    "transaction_io",
    "transaction_spent_in",
    "transaction_spending",
    "transaction_by_tx_prefix",
    "configuration",
)

#: Key under which :func:`build` returns the derived stage's spine input.
#: NOT a table -- :func:`graphsense_v3.spark.job.run` pops it before the frames
#: are checked against the schema and written. It rides along in the same dict
#: because it is built from the same cached ``transaction`` frame, and a second
#: entry point would scan the lake twice.
SPINE = "@spine"


#: Every lake table this loader reads. `DeltaLake.pin` resolves all of
#: them at one moment, so two tables cannot be pinned minutes apart and
#: disagree about where the chain ends.
LAKE_TABLES = ("block", "transaction")


def _transactions(
    lake: "LakeSource",
    config: NetworkConfig,
    start_block: Optional[int],
    end_block: Optional[int],
) -> "DataFrame":
    """The lake's transactions, carrying ``tx_id`` and both IO counts.

    Everything needed is already on the transaction row: ``tx_id`` is
    ``(block_id << 32) + index``, so nothing is counted, joined or carried in
    from an earlier block. Built once and reused by five output tables.
    """
    from pyspark.sql import functions as F

    txs = lake.read("transaction", start_block=start_block, end_block=end_block)
    return (
        txs.withColumn("tx_id", tx_id_expr(F.col("block_id"), F.col("index")))
        .withColumn(
            "block_id_group", id_group(F.col("block_id"), config.tx_block_bucket_size)
        )
        # input_count/output_count are written by ingest, but deriving them from
        # the arrays costs nothing and covers a lake written before they were.
        .withColumn(
            "no_inputs", F.coalesce(F.col("input_count"), F.size(F.col("inputs")))
        )
        .withColumn(
            "no_outputs", F.coalesce(F.col("output_count"), F.size(F.col("outputs")))
        )
    )


def _io_side(txs: "DataFrame", network: str, *, is_output: bool) -> "DataFrame":
    """One side of ``transaction_io``, exploded out of its array column."""
    from pyspark.sql import functions as F

    column = "outputs" if is_output else "inputs"
    encode = encode_address_list_udf(network)
    exploded = txs.select(
        "block_id_group", "tx_id", F.explode(F.col(column)).alias("io")
    ).select("block_id_group", "tx_id", "io.*")

    # Ingest stores no address for these script types (`address_as_string`);
    # nulling the column before the UDF keeps the branch out of the encoder.
    addresses = F.when(
        F.col("type").isin(list(ADDRESSLESS_TYPES)), F.lit(None)
    ).otherwise(F.col("addresses"))

    witness = (
        F.lit(None).cast("array<binary>")
        if is_output
        else F.col("txinwitness").cast("array<binary>")
    )
    sequence = (
        F.lit(None).cast("bigint") if is_output else F.col("sequence").cast("bigint")
    )

    return exploded.select(
        F.col("block_id_group"),
        F.col("tx_id"),
        F.lit(is_output).alias("is_output"),
        F.col("index").cast("int").alias("io_index"),
        encode(addresses).alias("address"),
        F.col("value").cast("bigint").alias("value"),
        address_type(F.col("type")).alias("address_type"),
        hex_to_bytes(F.col("script_hex")).alias("script_hex"),
        witness.alias("txinwitness"),
        sequence.alias("sequence"),
    )


def _leg_side(txs: "DataFrame", network: str, *, is_output: bool) -> "DataFrame":
    """One direction of the derived spine: single-address IOs, encoded once.

    Same rows as filtering ``size(transaction_io.address) == 1`` downstream --
    encoding is length-preserving, so the filter is equivalent on the source
    array -- but it costs one Python round trip instead of three:

    * The predicate is NATIVE here. Written the other way, Catalyst pushes it
      back below the projection that reads ``address[0]``, and the encoder then
      runs TWICE per row: two ArrowEvalPython nodes for one expression, which
      is what a real LTC plan showed. Spark does not CSE Python UDFs.
    * Multisig and addressless IOs never reach the encoder at all.
    * A scalar is encoded rather than a one-element array, so Arrow moves
      ``binary`` instead of ``array<binary>``.

    ``transaction_io`` still stores the full address list; this is only the
    derived stage's input.
    """
    from pyspark.sql import functions as F

    column = "outputs" if is_output else "inputs"
    encode = encode_address_udf(network)
    exploded = txs.select("tx_id", F.explode(F.col(column)).alias("io")).select(
        "tx_id", "io.*"
    )
    addresses = F.when(
        F.col("type").isin(list(ADDRESSLESS_TYPES)), F.lit(None)
    ).otherwise(F.col("addresses"))

    return exploded.where(F.size(addresses) == 1).select(
        F.col("tx_id"),
        encode(addresses.getItem(0)).alias("address"),
        F.lit(is_output).alias("is_output"),
        F.col("value").cast("bigint").alias("value"),
    )


def _coinbase_legs(txs: "DataFrame") -> "DataFrame":
    """The synthetic input leg of every coinbase transaction.

    graphsense-spark inserts a literal ``"coinbase"`` input on such
    transactions (`utxo/Transformation.scala:111-125`, `addCoinbaseAddress`),
    valued at the transaction's total output, and REST serves it as a
    neighbour. A coinbase transaction has NO input rows of its own, so without
    this every mined output has no incoming relation at all -- which is how a
    v2 address with one incoming neighbour reads as zero in v3.

    The address is :data:`graphsense_v3.codec.COINBASE_BYTES`, the empty-bytes
    sentinel already reserved for it: no real address encodes to empty, because
    every codec emits at least one byte for a non-empty string.
    """
    from pyspark.sql import functions as F

    from graphsense_v3.codec import COINBASE_BYTES

    return txs.where(F.col("coinbase")).select(
        F.col("tx_id"),
        F.lit(COINBASE_BYTES).cast("binary").alias("address"),
        F.lit(False).alias("is_output"),
        F.col("total_output").cast("bigint").alias("value"),
    )


def io_legs(txs: "DataFrame", network: str) -> "DataFrame":
    """Both directions of the spine input, ready for ``derived_utxo.legs``.

    ``tests/v3/test_v3_raw_loaders.py`` asserts this agrees row for row with
    running the derived side straight off ``transaction_io``, which is the
    definition it has to match -- plus the coinbase leg, which has no
    ``transaction_io`` row to come from.
    """
    return (
        _leg_side(txs, network, is_output=False)
        .unionByName(_leg_side(txs, network, is_output=True))
        .unionByName(_coinbase_legs(txs))
    )


def _spending_refs(txs: "DataFrame", prefix_length: int) -> "DataFrame":
    """Every (spending input -> spent output) reference, both prefixes attached.

    Mirrors ``get_tx_refs``: a reference with no spent hash is skipped, which is
    the ZEC shielded-transaction case.
    """
    from pyspark.sql import functions as F

    return (
        txs.select(F.col("tx_hash"), F.explode(F.col("inputs")).alias("i"))
        .where(~F.isnull(F.col("i.spent_transaction_hash")))
        .select(
            hex_prefix(F.col("tx_hash"), prefix_length).alias("spending_tx_prefix"),
            F.col("tx_hash").alias("spending_tx_hash"),
            F.col("i.index").cast("int").alias("spending_input_index"),
            hex_prefix(F.col("i.spent_transaction_hash"), prefix_length).alias(
                "spent_tx_prefix"
            ),
            F.col("i.spent_transaction_hash").alias("spent_tx_hash"),
            F.col("i.spent_output_index").cast("int").alias("spent_output_index"),
        )
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
    txs = _transactions(lake, cfg, start_block, end_block).cache()
    refs = _spending_refs(txs, cfg.tx_prefix_length)

    out: "dict[str, DataFrame]" = {}

    out["block"] = blocks.select(
        id_group(F.col("block_id"), cfg.block_bucket_size).alias("block_id_group"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("block_hash"),
        F.col("timestamp").cast("bigint").alias("timestamp"),
        F.col("no_transactions").cast("int").alias("no_transactions"),
    )

    out["block_by_date"] = blocks.select(
        day_key_from_timestamp(F.col("timestamp")).alias("day"),
        F.col("timestamp").cast("bigint").alias("timestamp"),
        F.col("block_id").cast("int").alias("block_id"),
    )

    out["transaction"] = txs.select(
        F.col("block_id_group"),
        F.col("block_id").cast("int").alias("block_id"),
        F.col("tx_id"),
        F.col("tx_hash"),
        F.col("timestamp").cast("bigint").alias("block_timestamp"),
        F.col("coinbase"),
        F.col("coinjoin"),
        F.col("total_input").cast("bigint").alias("total_input"),
        F.col("total_output").cast("bigint").alias("total_output"),
        F.col("no_inputs").cast("int").alias("no_inputs"),
        F.col("no_outputs").cast("int").alias("no_outputs"),
        F.col("version").cast("int").alias("version"),
        F.col("lock_time").cast("bigint").alias("lock_time"),
    )

    out["transaction_io"] = _io_side(txs, network, is_output=False).unionByName(
        _io_side(txs, network, is_output=True)
    )

    out["transaction_spent_in"] = refs.select(
        "spent_tx_prefix",
        "spent_tx_hash",
        "spent_output_index",
        "spending_tx_hash",
        "spending_input_index",
    )
    out["transaction_spending"] = refs.select(
        "spending_tx_prefix",
        "spending_tx_hash",
        "spending_input_index",
        "spent_tx_hash",
        "spent_output_index",
    )

    out["transaction_by_tx_prefix"] = txs.select(
        hex_prefix(F.col("tx_hash"), cfg.tx_prefix_length).alias("tx_prefix"),
        F.col("tx_hash"),
        F.col("tx_id"),
    )

    out["configuration"] = configuration_row(lake, cfg, keyspace)
    out[SPINE] = io_legs(txs, network)
    return out


def preflight(
    lake: "LakeSource",
    network: str,
    *,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
) -> list[str]:
    """Problems that would make a backfill silently wrong. Empty means go.

    Both checks cost a full pass over the range, which is why they are a separate
    call rather than part of :func:`load`: run them once before a long backfill,
    not on every resumed chunk.
    """
    from pyspark.sql import functions as F

    from graphsense_v3.spark.columns import unknown_address_types

    cfg = config_for(network)
    problems: list[str] = []
    txs = lake.read("transaction", start_block=start_block, end_block=end_block)

    # tx_id = (block_id << 32) + index is correct however sparse the indices
    # are; the one thing it needs is that `index` is unique within its block,
    # or two transactions would collide on one id.
    collisions = (
        txs.groupBy("block_id", "index").count().where(F.col("count") > 1).count()
    )
    if collisions:
        problems.append(
            f"{collisions} (block_id, index) pairs occur more than once; those "
            "transactions would collide on a single tx_id"
        )
    too_wide = txs.where(F.col("index") >= (1 << TX_INDEX_BITS)).count()
    if too_wide:
        problems.append(
            f"{too_wide} transactions have an index of {TX_INDEX_BITS} bits or "
            "more, which would carry into the block part of the id"
        )

    # `transaction` and `transaction_io` are partitioned by block, so a block is
    # a partition. v2 bounded that partition arithmetically (a dense tx_id gave
    # exactly tx_bucket_size rows); here it is bounded by consensus, which is
    # loose -- a 32 MB BCH block permits far more than its ~431-transaction
    # average. Measure the tail rather than assume it.
    fattest = biggest_block(txs)
    if fattest and fattest[1] > MAX_TRANSACTIONS_PER_BLOCK:
        problems.append(
            f"block {fattest[0]} holds {fattest[1]} transactions, over the "
            f"{MAX_TRANSACTIONS_PER_BLOCK} this layout is sized for "
            f"(tx_block_bucket_size is {cfg.tx_block_bucket_size}, so that "
            f"partition also carries its neighbours); its "
            "transaction_io partition will be correspondingly large (slow to "
            "read, but still writable -- these are rows, not a collection)"
        )

    ios = (
        txs.select(F.explode(F.col("inputs")).alias("io"))
        .select(F.col("io.type").alias("type"))
        .unionByName(
            txs.select(F.explode(F.col("outputs")).alias("io")).select(
                F.col("io.type").alias("type")
            )
        )
    )
    unknown = unknown_address_types(ios, column="type")
    if unknown:
        problems.append(
            "script types the classification table does not know, which would "
            f"store a NULL address_type: {', '.join(unknown)}"
        )
    return problems


#: Transactions in one block, above which the block's partition is worth a
#: second look. ~10x the busiest observed BTC block's share of a 100 MB
#: guideline, allowing ~5 transaction_io rows each.
MAX_TRANSACTIONS_PER_BLOCK = 50_000


def biggest_block(txs: "DataFrame") -> "Optional[tuple]":
    """``(block_id, transactions)`` for the fattest block, or None if empty."""
    from pyspark.sql import functions as F

    row = (
        txs.groupBy("block_id").agg(F.count("*").alias("n")).orderBy(F.desc("n")).head()
    )
    return (row["block_id"], row["n"]) if row else None


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
    """Write the raw UTXO tables into ``keyspace``. Returns what was written."""
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
    selected = tables or TABLES

    # Conform every frame before writing any of them: a column mismatch should
    # fail at job start, not after the first table has already landed.
    for name in selected:
        writer.check(frames[name], schema.table(name))

    written: list[str] = []
    for name in selected:
        writer.write(frames[name], schema.table(name), keyspace)
        written.append(name)
    return written
