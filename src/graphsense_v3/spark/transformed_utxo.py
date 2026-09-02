"""Raw UTXO keyspace -> v3 transformed address tables.

Backfill only. A backfill writes the **compacted base**, epoch 0, so none of the
epoch machinery in §6 runs here: `address_stats` is a group-by, not a fold, and
`address_transactions_recent` stays empty because ordinals can be assigned
directly. Both exist for the incremental path, which is deliberately not built
yet.

Semantics follow graphsense-spark's `utxo/Transformation.scala`, with one
deliberate departure -- **D7, App. B.1**: it nets an address that appears on both
sides of a transaction into a single row carrying the difference, so a 10 BTC
spend with 3 BTC of change to the same address becomes one `-7` outgoing row and
the receipt does not exist in the model. Here direction is tagged per leg
*before* the group-by, so such an address yields two rows. That moves published
numbers (see App. B.1) and is the point.
"""

# NOTE: no `from __future__ import annotations` -- this module builds pandas UDFs
# through graphsense_v3.spark.udf, whose annotations pyspark reads directly.

from typing import TYPE_CHECKING, Optional

from graphsense_v3.config import NetworkConfig, config_for
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.schema.definitions import EPOCH_BASE
from graphsense_v3.spark import writer
from graphsense_v3.spark.udf import (
    block_of_tx_id_expr,
    bucket_expr,
    search_prefix_bytes_udf,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

#: Satoshi per coin. The fiat conversion divides by this.
COIN_DECIMALS = 8

TABLES = (
    "address_transactions",
    "address_tx_pages",
    "address_stats",
    "address_by_prefix",
    "address_outgoing_relations",
    "address_incoming_relations",
    "address_link_transactions",
    "balance",
)


def legs(transaction_io: "DataFrame") -> "DataFrame":
    """One row per (transaction, address, direction): the spine.

    Two rules inherited from graphsense-spark, both load-bearing:

    * **Only single-address IOs participate** (``size(address) == 1``). A
      multisig output naming several addresses is excluded from the address
      graph entirely, not attributed to any of them.
    * **An address appearing several times on one side of one transaction is one
      leg**, with the values summed.

    And one departure, D7: direction is tagged before the group-by, so an
    address on *both* sides yields two legs rather than one netted row.
    """
    from pyspark.sql import functions as F

    single = transaction_io.where(F.size(F.col("address")) == 1).select(
        F.col("tx_id"),
        F.col("address").getItem(0).alias("address"),
        F.col("is_output"),
        F.col("value"),
    )
    return (
        single.groupBy("tx_id", "address", "is_output")
        .agg(F.sum("value").cast("bigint").alias("value"))
        # An input spends: it is the address's outgoing leg.
        .select(
            F.col("tx_id"),
            F.col("address"),
            (~F.col("is_output")).alias("is_outgoing"),
            F.col("value"),
            block_of_tx_id_expr(F.col("tx_id")).alias("block_id"),
        )
    )


def fiat_values(value: "Column", rates: "Column", decimals: int) -> "Column":
    """A value in base units -> a map of fiat currency to amount.

    Matches graphsense-spark's rounding (`utxo/Transformator.scala:59-71`):
    two decimal places, half up. v3 widens the stored type from `float` to
    `double` and the container from a positional list to a map, but the number
    is the same one v2 published.
    """
    from pyspark.sql import functions as F

    return F.transform_values(
        rates, lambda _, rate: F.round(value * rate / F.lit(10.0**decimals), 2)
    )


def _currency(value: "Column", rates: "Column", decimals: int) -> "Column":
    """The `currency` UDT: a base-unit amount and its fiat equivalents."""
    from pyspark.sql import functions as F

    return F.struct(
        value.cast("decimal(38,0)").alias("value"),
        fiat_values(value, rates, decimals).alias("fiat_values"),
    )


def address_transactions(spine: "DataFrame", config: NetworkConfig) -> "DataFrame":
    """Ordinal-paged transactions, one partition of exactly `tx_page_size`.

    The ordinal is the address's own count, so a partition is full by
    construction rather than by luck -- immune to both burst and dormancy, which
    is what the v2 block-bucketed form was not (42 rows on average, 455M in its
    worst partition).

    This window is the most expensive step in the transform: on BTC it sorts
    ~5e9 rows across ~1.5e9 partitions.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = Window.partitionBy("address", "is_outgoing").orderBy("tx_id")
    return (
        spine.withColumn("ordinal", F.row_number().over(order) - 1)
        .withColumn(
            "tx_page", (F.col("ordinal") / F.lit(config.tx_page_size)).cast("int")
        )
        .select(
            F.col("address"),
            F.col("is_outgoing"),
            F.col("tx_page"),
            F.col("tx_id"),
            F.col("value").cast("decimal(38,0)").alias("value"),
            F.col("ordinal"),
        )
    )


def address_tx_pages(paged: "DataFrame") -> "DataFrame":
    """The page index: which page holds a given `tx_id` bound.

    Ordinal pages are not tx_id-aligned, so a height or date filter cannot
    compute the page it needs. One row per page fixes that, and it is read only
    when a range filter is present.
    """
    from pyspark.sql import functions as F

    return paged.groupBy("address", "is_outgoing", "tx_page").agg(
        F.min("tx_id").alias("first_tx_id")
    )


def address_stats(
    spine: "DataFrame",
    paged: "DataFrame",
    rates: "DataFrame",
    degree: "DataFrame",
    config: NetworkConfig,
) -> "DataFrame":
    """The epoch-0 base row for every address.

    ``rates`` is ``(block_id, fiat_values map<string,double>)``; ``degree`` comes
    from :func:`degrees`. A backfill writes the compacted base directly, so this
    is a group-by rather than a fold -- the epoch machinery of §6 only runs on
    the incremental path.
    """
    from pyspark.sql import functions as F

    # Each leg is priced at its own block's rate. Summing the per-leg fiat is
    # not the same as pricing the total -- an address's transactions span years,
    # and one rate applied to the total would be an answer about no real moment.
    priced = spine.join(rates, on="block_id", how="left").withColumn(
        "fiat_values", fiat_values(F.col("value"), F.col("fiat_values"), COIN_DECIMALS)
    )

    def side(outgoing: bool, prefix: str) -> "DataFrame":
        column = "total_spent" if outgoing else "total_received"
        rows = priced.where(F.col("is_outgoing") == F.lit(outgoing))
        counts = rows.groupBy("address").agg(
            F.count("*").cast("bigint").alias(f"no_{prefix}_txs"),
            F.sum(F.when(F.col("value") == 0, 1).otherwise(0))
            .cast("bigint")
            .alias(f"no_{prefix}_txs_zero_value"),
            F.sum("value").cast("bigint").alias("_value"),
        )
        # explode drops a NULL map, so a block with no known rate contributes
        # nothing rather than zeroing the address's total.
        fiat = (
            rows.select(
                "address", F.explode("fiat_values").alias("_currency", "_amount")
            )
            .groupBy("address", "_currency")
            .agg(F.sum("_amount").alias("_amount"))
            .groupBy("address")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct("_currency", "_amount"))
                ).alias("_fiat")
            )
        )
        return counts.join(fiat, on="address", how="left").select(
            F.col("address"),
            F.col(f"no_{prefix}_txs"),
            F.col(f"no_{prefix}_txs_zero_value"),
            F.struct(
                F.col("_value").cast("decimal(38,0)").alias("value"),
                F.col("_fiat").alias("fiat_values"),
            ).alias(column),
        )

    incoming = side(False, "incoming")
    outgoing = side(True, "outgoing")

    cursors = paged.groupBy("address", "is_outgoing").agg(
        F.max("tx_page").cast("int").alias("page_max"),
        (F.max("ordinal") + 1).cast("bigint").alias("ordinal_next"),
    )
    in_cursor = cursors.where(~F.col("is_outgoing")).select(
        "address",
        F.col("page_max").alias("in_tx_page_max"),
        F.col("ordinal_next").alias("in_tx_ordinal_next"),
    )
    out_cursor = cursors.where(F.col("is_outgoing")).select(
        "address",
        F.col("page_max").alias("out_tx_page_max"),
        F.col("ordinal_next").alias("out_tx_ordinal_next"),
    )

    bounds = spine.groupBy("address").agg(
        F.min("tx_id").alias("first_tx_id"), F.max("tx_id").alias("last_tx_id")
    )
    joined = (
        bounds.join(incoming, on="address", how="left")
        .join(outgoing, on="address", how="left")
        .join(in_cursor, on="address", how="left")
        .join(out_cursor, on="address", how="left")
        .join(degree, on="address", how="left")
    )
    zero = F.lit(0).cast("bigint")
    return joined.select(
        bucket_expr(F.col("address"), config.entity_buckets).alias("address_bucket"),
        F.col("address"),
        F.lit(EPOCH_BASE).alias("epoch"),
        F.coalesce(F.col("no_incoming_txs"), zero).alias("no_incoming_txs"),
        F.coalesce(F.col("no_outgoing_txs"), zero).alias("no_outgoing_txs"),
        F.coalesce(F.col("no_incoming_txs_zero_value"), zero).alias(
            "no_incoming_txs_zero_value"
        ),
        F.coalesce(F.col("no_outgoing_txs_zero_value"), zero).alias(
            "no_outgoing_txs_zero_value"
        ),
        F.col("total_received"),
        F.col("total_spent"),
        F.col("first_tx_id"),
        F.col("last_tx_id"),
        F.coalesce(F.col("in_degree"), zero).alias("in_degree"),
        F.coalesce(F.col("out_degree"), zero).alias("out_degree"),
        F.coalesce(F.col("in_degree_zero_value"), zero).alias("in_degree_zero_value"),
        F.coalesce(F.col("out_degree_zero_value"), zero).alias("out_degree_zero_value"),
        F.col("in_tx_page_max"),
        F.col("out_tx_page_max"),
        F.col("in_tx_ordinal_next"),
        F.col("out_tx_ordinal_next"),
    )


def address_by_prefix(spine: "DataFrame", network: str, config: NetworkConfig):
    """The search index. Decoding runs over distinct addresses, not every leg."""
    from pyspark.sql import functions as F

    prefix = search_prefix_bytes_udf(network, config.address_prefix_length)
    return (
        spine.select("address")
        .distinct()
        .select(prefix(F.col("address")).alias("address_prefix"), F.col("address"))
    )


def relation_edges(spine: "DataFrame", transactions: "DataFrame") -> "DataFrame":
    """One row per (transaction, source, destination), with an attributed value.

    A UTXO transaction does not say which input paid which output, so the value
    is apportioned: a source that supplied 30% of the input is credited with 30%
    of each output. Same rule as graphsense-spark
    (`utxo/Transformator.scala:242-251`), rounded the same way.

    ``transactions`` supplies ``(tx_id, total_input)``. Note that this is the
    transaction's *whole* input, including legs no address could be attributed
    to (multi-address inputs). Shares therefore do not sum to one when part of a
    transaction is unattributable, which under-attributes rather than
    over-attributes -- deliberate, and inherited.

    v2 corrected ``total_input`` here (`Transformator.scala:206-225`) because its
    netting made an address's input smaller than what it really spent whenever
    it also received change. **D7 removes the netting, so the correction is
    identically zero** and the raw total is the right denominator again.

    Self-edges are dropped. Un-netting lets an address be both source and
    destination of one transaction, which the netted model could never produce,
    so this filter is new and required (App. B.1).
    """
    from pyspark.sql import functions as F

    outgoing = spine.where(F.col("is_outgoing")).select(
        F.col("tx_id"),
        F.col("address").alias("src_address"),
        F.col("value").alias("in_value"),
    )
    incoming = spine.where(~F.col("is_outgoing")).select(
        F.col("tx_id"),
        F.col("address").alias("dst_address"),
        F.col("value").alias("out_value"),
        F.col("block_id"),
    )
    return (
        outgoing.join(incoming, on="tx_id", how="inner")
        .where(F.col("src_address") != F.col("dst_address"))
        .join(transactions.select("tx_id", "total_input"), on="tx_id", how="inner")
        .select(
            F.col("tx_id"),
            F.col("block_id"),
            F.col("src_address"),
            F.col("dst_address"),
            F.when(
                F.col("total_input") != 0,
                F.round(F.col("in_value") / F.col("total_input") * F.col("out_value")),
            )
            .otherwise(F.lit(0))
            .cast("bigint")
            .alias("value"),
        )
    )


def address_link_transactions(edges: "DataFrame", config: NetworkConfig) -> "DataFrame":
    """The /links fix: the transaction list behind an edge, not just its count.

    Partitioned per source with the destination as a clustering prefix, which is
    the UTXO half of D10 -- 1.5e9 BTC addresses average 1.2 transactions per
    edge, so per-partition overhead dominates and partition-per-source wins by
    ~42% (935 against 1621 logical GiB).
    """
    from pyspark.sql import functions as F

    return edges.select(
        F.col("src_address"),
        bucket_expr(F.col("dst_address"), config.relation_buckets).alias("dst_bucket"),
        F.col("dst_address"),
        F.col("tx_id"),
        F.col("value").cast("decimal(38,0)").alias("value"),
    )


def _relation_side(
    edges: "DataFrame",
    rates: "DataFrame",
    config: NetworkConfig,
    *,
    near: str,
    far: str,
) -> "DataFrame":
    """One direction of the relations pair, aggregated to epoch 0.

    ``near`` is the address the partition is keyed on, ``far`` the counterparty.
    The bucket hashes the FAR side, so a read scatters over `relation_buckets`
    partitions and stops when it has collected `in_degree` rows.
    """
    from pyspark.sql import functions as F

    priced = edges.join(rates, on="block_id", how="left").withColumn(
        "fiat_values", fiat_values(F.col("value"), F.col("fiat_values"), COIN_DECIMALS)
    )
    counts = priced.groupBy(near, far).agg(
        F.count("*").cast("bigint").alias("no_transactions"),
        F.sum("value").cast("bigint").alias("_value"),
    )
    fiat = (
        priced.select(near, far, F.explode("fiat_values").alias("_currency", "_amount"))
        .groupBy(near, far, "_currency")
        .agg(F.sum("_amount").alias("_amount"))
        .groupBy(near, far)
        .agg(
            F.map_from_entries(F.collect_list(F.struct("_currency", "_amount"))).alias(
                "_fiat"
            )
        )
    )
    return counts.join(fiat, on=[near, far], how="left").select(
        F.col(near),
        bucket_expr(F.col(far), config.relation_buckets).alias("rel_bucket"),
        F.col(far),
        F.lit(EPOCH_BASE).alias("epoch"),
        F.col("no_transactions"),
        F.struct(
            F.col("_value").cast("decimal(38,0)").alias("value"),
            F.col("_fiat").alias("fiat_values"),
        ).alias("value"),
    )


def degrees(edges: "DataFrame") -> "DataFrame":
    """Distinct-counterparty counts, per address and direction.

    Not summable, which is why they live on epoch 0 and are maintained by
    compaction rather than by the ingest path (§5.2).
    """
    from pyspark.sql import functions as F

    def side(near: str, far: str, prefix: str) -> "DataFrame":
        return edges.groupBy(near).agg(
            F.countDistinct(far).cast("bigint").alias(f"{prefix}_degree"),
            F.countDistinct(F.when(F.col("value") == 0, F.col(far)))
            .cast("bigint")
            .alias(f"{prefix}_degree_zero_value"),
        )

    out = side("src_address", "dst_address", "out").withColumnRenamed(
        "src_address", "address"
    )
    incoming = side("dst_address", "src_address", "in").withColumnRenamed(
        "dst_address", "address"
    )
    return out.join(incoming, on="address", how="outer")


def balance(spine: "DataFrame", network: str, config: NetworkConfig) -> "DataFrame":
    """Received minus spent, as an epoch-0 row.

    Summable like the stats rows, so the incremental path can blind-insert a
    delta and a read sums the slice.
    """
    from pyspark.sql import functions as F

    signed = F.when(F.col("is_outgoing"), -F.col("value")).otherwise(F.col("value"))
    return (
        spine.groupBy("address")
        .agg(F.sum(signed).cast("decimal(38,0)").alias("balance"))
        .select(
            bucket_expr(F.col("address"), config.entity_buckets).alias(
                "address_bucket"
            ),
            F.col("address"),
            F.lit(network.upper()).alias("currency"),
            F.lit(EPOCH_BASE).alias("epoch"),
            F.col("balance"),
        )
    )


def build(
    transaction_io: "DataFrame",
    transactions: "DataFrame",
    rates: "DataFrame",
    network: str,
    *,
    config: Optional[NetworkConfig] = None,
) -> dict:
    """The transformed address tables, keyed by table name.

    ``transaction_io`` and ``transactions`` are the raw keyspace's tables (or the
    frames about to be written to it); ``rates`` is
    ``(block_id, fiat_values map<string,double>)``.
    """
    cfg = config or config_for(network)
    spine = legs(transaction_io).cache()
    paged = address_transactions(spine, cfg).cache()
    edges = relation_edges(spine, transactions).cache()
    return {
        "address_transactions": paged.drop("ordinal"),
        "address_tx_pages": address_tx_pages(paged),
        "address_stats": address_stats(spine, paged, rates, degrees(edges), cfg),
        "address_by_prefix": address_by_prefix(spine, network, cfg),
        "address_outgoing_relations": _relation_side(
            edges, rates, cfg, near="src_address", far="dst_address"
        ),
        "address_incoming_relations": _relation_side(
            edges, rates, cfg, near="dst_address", far="src_address"
        ),
        "address_link_transactions": address_link_transactions(edges, cfg),
        "balance": balance(spine, network, cfg),
    }


def load(
    transaction_io: "DataFrame",
    transactions: "DataFrame",
    rates: "DataFrame",
    network: str,
    keyspace: str,
    *,
    tables: Optional[tuple] = None,
    config: Optional[NetworkConfig] = None,
) -> list[str]:
    """Write the transformed address tables into ``keyspace``."""
    schema = schema_for(network, Kind.TRANSFORMED)
    frames = build(transaction_io, transactions, rates, network, config=config)
    selected = tables or TABLES
    for name in selected:
        writer.check(frames[name], schema.table(name))
    written: list[str] = []
    for name in selected:
        writer.write(frames[name], schema.table(name), keyspace)
        written.append(name)
    return written
