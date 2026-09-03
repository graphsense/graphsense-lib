"""Pieces both derived families build the same way.

Kept here rather than duplicated: the two families differ in how a transfer is
*found* -- apportioned across a UTXO transaction's inputs and outputs, or read
directly off an account trace or log -- but once found, paging, the search index
and fiat conversion are the same operation.
"""

# NOTE: no `from __future__ import annotations` -- this module is imported by
# ones that define pandas UDFs.

from typing import TYPE_CHECKING

from graphsense_v3.config import NetworkConfig
from graphsense_v3.spark.udf import bucket_expr, search_prefix_bytes_udf

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def fiat_values(value: "Column", rates: "Column", divisor: "Column") -> "Column":
    """A value in base units -> a map of fiat currency to amount.

    ``divisor`` converts base units to whole coins (1e8 for satoshi, 1e18 for
    wei, a token's own ``decimal_divisor``). Rounded to two decimal places, half
    up, as graphsense-spark rounds (`utxo/Transformator.scala:59-71`).
    """
    from pyspark.sql import functions as F

    return F.transform_values(rates, lambda _, rate: F.round(value * rate / divisor, 2))


def sum_fiat(rows: "DataFrame", keys: list, column: str = "fiat_values"):
    """Sum per-leg fiat maps per group, as a map.

    Summing the legs is not the same as pricing the total: an entity's transfers
    span years, and one rate applied to the sum would be an answer about no real
    moment. ``explode`` drops a NULL map, so a block with no known rate
    contributes nothing rather than zeroing the total.
    """
    from pyspark.sql import functions as F

    return (
        rows.select(*keys, F.explode(column).alias("_currency", "_amount"))
        .groupBy(*keys, "_currency")
        .agg(F.sum("_amount").alias("_amount"))
        .groupBy(*keys)
        .agg(
            F.map_from_entries(F.collect_list(F.struct("_currency", "_amount"))).alias(
                "_fiat"
            )
        )
    )


def currency_struct(value: "Column", fiat: "Column") -> "Column":
    """The ``currency`` UDT: a base-unit amount and its fiat equivalents."""
    from pyspark.sql import functions as F

    return F.struct(
        value.cast("decimal(38,0)").alias("value"), fiat.alias("fiat_values")
    )


def with_zero_flag(legs: "DataFrame") -> "DataFrame":
    """Tag each leg with whether it moved anything.

    A partition-key column, so it has to be computed before ordinals are
    assigned: pages are numbered within a class, not across them.
    """
    from pyspark.sql import functions as F

    return legs.withColumn("is_zero_value", F.col("value") == 0)


def with_ordinals(legs: "DataFrame", partition: list, config: NetworkConfig):
    """Number an entity's transfers in ``tx_id`` order and assign a page.

    The ordinal is the entity's own count, so a page holds exactly
    ``tx_page_size`` rows by construction rather than by luck -- immune to burst
    and to dormancy alike, which the block-bucketed form was not.

    This window is the most expensive step in either transform: on BTC it sorts
    ~5e9 rows across ~1.5e9 partitions.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = Window.partitionBy(*partition).orderBy("tx_id")
    return legs.withColumn("ordinal", F.row_number().over(order) - 1).withColumn(
        "tx_page", (F.col("ordinal") / F.lit(config.tx_page_size)).cast("int")
    )


def address_tx_pages(paged: "DataFrame") -> "DataFrame":
    """The page index: which page holds a given ``tx_id`` bound.

    Ordinal pages are not tx_id-aligned, so a height or date filter cannot
    compute the page it needs. Read only when a range filter is present. One
    index per partition class, zero-ness included, because that is how the pages
    it indexes are numbered.
    """
    from pyspark.sql import functions as F

    return paged.groupBy("address", "is_outgoing", "is_zero_value", "tx_page").agg(
        F.min("tx_id").alias("first_tx_id")
    )


def paging_cursors(paged: "DataFrame") -> "DataFrame":
    """``address -> the highest page and next ordinal of each partition class``.

    Four classes: direction x zero-ness, matching the *_transactions key.
    """
    from pyspark.sql import functions as F

    per_class = paged.groupBy("address", "is_outgoing", "is_zero_value").agg(
        F.max("tx_page").cast("int").alias("page_max"),
        (F.max("ordinal") + 1).cast("bigint").alias("ordinal_next"),
    )
    frame = None
    for outgoing in (False, True):
        for zero in (False, True):
            prefix = ("out" if outgoing else "in") + ("_zero" if zero else "")
            side = per_class.where(
                (F.col("is_outgoing") == F.lit(outgoing))
                & (F.col("is_zero_value") == F.lit(zero))
            ).select(
                "address",
                F.col("page_max").alias(f"{prefix}_tx_page_max"),
                F.col("ordinal_next").alias(f"{prefix}_tx_ordinal_next"),
            )
            frame = side if frame is None else frame.join(side, "address", "outer")
    return frame


def leg_events(legs: "DataFrame", currency: "Column") -> "DataFrame":
    """Transfer legs as balance events: one signed ``delta`` per leg.

    The uniform shape ``(address, currency, block_id, tx_id, delta)`` that
    :func:`balance`, :func:`balance_history` and :func:`running_balance` all
    consume. Transfers are only part of that stream on the account side, where
    fees move value without being a transfer -- see
    ``derived_account.fee_events``.
    """
    from pyspark.sql import functions as F

    delta = F.when(F.col("is_outgoing"), -F.col("value")).otherwise(F.col("value"))
    return legs.select(
        F.col("address"),
        currency.alias("currency"),
        F.col("block_id"),
        F.col("tx_id"),
        delta.cast("decimal(38,0)").alias("delta"),
    )


def balance(events: "DataFrame", config: NetworkConfig) -> "DataFrame":
    """Closing balance per entity and asset: the sum of every balance event."""
    from pyspark.sql import functions as F

    from graphsense_v3.schema.definitions import EPOCH_BASE

    return (
        events.groupBy("address", "currency")
        .agg(F.sum("delta").cast("decimal(38,0)").alias("balance"))
        .select(
            entity_bucket(F.col("address"), config).alias("address_bucket"),
            F.col("address"),
            F.col("currency"),
            F.lit(EPOCH_BASE).alias("epoch"),
            F.col("balance"),
        )
    )


def balance_history(events: "DataFrame", blocks: "DataFrame", config: NetworkConfig):
    """The running balance at the end of every day the entity moved.

    A cumulative sum, not a delta -- the one place this schema departs from
    summable rows, so that "balance on day D" is one row rather than a sum over
    every active day since the address was created.

    ``blocks`` supplies ``(block_id, timestamp)``; the day comes from the block,
    not from the transfer, because a transfer has no time of its own.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from graphsense_v3.spark.columns import day_key_from_timestamp

    days = blocks.select(
        F.col("block_id"), day_key_from_timestamp(F.col("timestamp")).alias("day")
    )
    daily = (
        events.join(days, on="block_id", how="inner")
        .groupBy("address", "currency", "day")
        .agg(F.sum("delta").alias("_delta"))
    )
    running = Window.partitionBy("address", "currency").orderBy("day")
    return daily.withColumn(
        "balance", F.sum("_delta").over(running).cast("decimal(38,0)")
    ).select(
        entity_bucket(F.col("address"), config).alias("address_bucket"),
        F.col("address"),
        F.col("currency"),
        F.col("day"),
        F.col("balance"),
    )


def running_balance(events: "DataFrame") -> "DataFrame":
    """``(address, currency, tx_id, balance)`` -- the balance AFTER each tx.

    A RANGE frame, not the default ROWS one, and that is the whole subtlety: an
    address can have several events in one transaction -- both sides of a
    self-change output, a transfer plus the fee that paid for it -- and a ROWS
    frame would give each of them a different running value, one of which is a
    mid-transaction state that never existed on chain. RANGE includes every peer
    sharing the row's ``tx_id``, so all of them read the post-transaction
    balance, and the rows collapse to one per transaction.

    Events whose transaction moved nothing for this address (a fee on a failed
    call) still count toward the sum; they simply have no row to be joined to.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    running = (
        Window.partitionBy("address", "currency")
        .orderBy("tx_id")
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        events.withColumn("balance", F.sum("delta").over(running).cast("decimal(38,0)"))
        .select("address", "currency", "tx_id", "balance")
        .dropDuplicates(["address", "currency", "tx_id"])
    )


def address_by_prefix(
    addresses: "DataFrame", network: str, config: NetworkConfig
) -> "DataFrame":
    """The search index. Decoding runs over distinct addresses, not every leg."""
    from pyspark.sql import functions as F

    prefix = search_prefix_bytes_udf(network, config.address_prefix_length)
    return (
        addresses.select("address")
        .distinct()
        .select(prefix(F.col("address")).alias("address_prefix"), F.col("address"))
    )


def with_running_balance(
    paged: "DataFrame", events: "DataFrame", *, per_currency: bool
) -> "DataFrame":
    """Attach the post-transaction balance to each paged transaction row.

    A LEFT join: a row whose balance is unknown keeps a NULL rather than a zero,
    which would read as "empty" instead of "not computed".

    ``per_currency`` is False for UTXO, which has one asset and no ``currency``
    column on the transaction row to join on.
    """
    balances = running_balance(events)
    on = ["address", "currency", "tx_id"]
    if not per_currency:
        balances = balances.drop("currency")
        on = ["address", "tx_id"]
    return paged.join(balances, on=on, how="left")


def entity_bucket(address: "Column", config: NetworkConfig) -> "Column":
    return bucket_expr(address, config.entity_buckets)
