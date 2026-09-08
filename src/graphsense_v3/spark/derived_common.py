"""Pieces both derived families build the same way.

Kept here rather than duplicated: the two families differ in how a transfer is
*found* -- apportioned across a UTXO transaction's inputs and outputs, or read
directly off an account trace or log -- but once found, paging, the search index
and fiat conversion are the same operation.
"""

# NOTE: no `from __future__ import annotations` -- this module is imported by
# ones that define pandas UDFs.

from typing import TYPE_CHECKING, Sequence

from graphsense_v3.config import NetworkConfig
from graphsense_v3.spark.columns import as_varint
from graphsense_v3.spark.udf import bucket_expr, search_prefix_bytes_udf

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def all_present(parts: "Sequence[Column]") -> "Column":
    """True when every one of ``parts`` is non-NULL.

    The condition behind every fiat list in this package. A Cassandra
    collection cannot hold a null ELEMENT, so a positional list is either
    complete or NULL -- there is no representation for "position 1 unknown",
    and the bulk writer rejects the row rather than storing one.
    """
    from functools import reduce

    from pyspark.sql import functions as F

    return reduce(lambda a, b: a & b, [~F.isnull(one) for one in parts])


def fiat_values(
    value: "Column", rates: "Column", divisor: "Column", currencies: Sequence[str]
) -> "Column":
    """A value in base units -> its fiat amounts, ORDERED by ``currencies``.

    ``divisor`` converts base units to whole coins (1e8 for satoshi, 1e18 for
    wei, a token's own ``decimal_divisor``). Rounded to two decimal places, half
    up, as graphsense-spark rounds (`utxo/Transformator.scala:59-71`).

    A positional list, not a map. The map carried "EUR"/"USD" as text on every
    one of ~256M relation rows -- ~27 bytes each, ~6% of the derived keyspace --
    to describe an ordering that the keyspace's own ``configuration`` row
    already holds. v2's list was unsafe because its ordering lived in a config
    FILE that could drift from the data; v3 writes the ordering into the
    keyspace, in the same run, so the coupling cannot come apart.

    ``rates`` stays a map: ``exchange_rates`` is 3 MB, and it is the one table
    read directly rather than through a reader that knows the ordering.

    ALL of the currencies or none: the list is NULL unless every configured
    currency has a rate. A Cassandra collection cannot hold a null ELEMENT, so
    "EUR known, USD unknown" has no representation here -- the bulk writer
    rejects the row outright with "Collection elements cannot be null". Between
    the two storable answers, NULL says the total is unknown and 0.0 would say
    it was worth nothing, so NULL is the one that states nothing false. A
    healthy `exchange_rates` row carries every configured currency, so this
    fires on a missing rate ROW, not on a partial one.
    """
    from pyspark.sql import functions as F

    amounts = [
        F.round(value * F.element_at(rates, code) / divisor, 2) for code in currencies
    ]
    return F.when(all_present(amounts), F.array(*amounts)).otherwise(
        F.lit(None).cast("array<double>")
    )


def sum_fiat(
    rows: "DataFrame",
    keys: list,
    currencies: Sequence[str],
    column: str = "fiat_values",
):
    """Sum per-leg fiat lists per group, positionally.

    Summing the legs is not the same as pricing the total: an entity's transfers
    span years, and one rate applied to the sum would be an answer about no real
    moment.

    One ``F.sum`` per currency, because the list length is fixed and known from
    the keyspace's configuration -- simpler than the explode-and-regroup the map
    needed. ``sum`` ignores NULLs, so a leg in a block with no known rate still
    contributes nothing rather than zeroing the total, which is the property the
    map version got from ``explode`` dropping a NULL.

    ``column`` must be the positional LIST, never the rates map: ``getItem`` on
    a map does not fail on an integer key, it returns NULL for every row.
    """
    from pyspark.sql import functions as F

    sums = [F.sum(F.col(column).getItem(index)) for index in range(len(currencies))]
    # A group where NOTHING was priced sums to NULL in every position, and
    # `F.array` of NULLs is a LIST CONTAINING NULLS -- which Cassandra forbids
    # in a collection, and which the bulk writer rejects with "Collection
    # elements cannot be null" from inside the UDT codec. NULL is also the
    # honest answer: no leg had a rate, so the total is unknown, not zero.
    return rows.groupBy(*keys).agg(
        F.when(all_present(sums), F.array(*sums))
        .otherwise(F.lit(None).cast("array<double>"))
        .alias("_fiat")
    )


def count_column(name: str) -> "Column":
    """A null-safe stats count, typed for its ``varint`` column.

    Null because the sides are outer-joined: an address that only ever received
    has no outgoing row to contribute. Zero is the count, not "unknown".
    """
    from pyspark.sql import functions as F

    return as_varint(F.coalesce(F.col(name), F.lit(0))).alias(name)


def sum_or_null(column: str = "value", alias: str = "_value") -> "Column":
    """SUM, but NULL as soon as one addend is NULL.

    `F.sum` SKIPS nulls. A token transfer whose uint256 value exceeded the
    varint ceiling is stored NULL (see `columns.bytes_to_varint_udf`), so a
    plain sum would silently UNDERSTATE that asset's total rather than say it
    is unknown -- and an understated balance is the failure mode that gets
    found by a customer. Every caller groups by currency as well as entity, so
    this nulls one asset's total, never the native one.
    """
    from pyspark.sql import functions as F

    return (
        F.when(F.max(F.isnull(F.col(column))), F.lit(None).cast("decimal(38,0)"))
        .otherwise(F.sum(F.col(column)).cast("decimal(38,0)"))
        .alias(alias)
    )


def currency_struct(value: "Column", fiat: "Column") -> "Column":
    """The ``currency`` UDT: a base-unit amount and its fiat equivalents."""
    from pyspark.sql import functions as F

    return F.struct(
        value.cast("decimal(38,0)").alias("value"), fiat.alias("fiat_values")
    )


def zero_currency(currencies: Sequence[str]) -> "Column":
    """A ``currency`` of nothing: zero, and a zero for each fiat position.

    An address that never received within the built range has no incoming row
    to aggregate, and the sides are outer-joined, so `total_received` comes out
    NULL. That is not "unknown" -- it is zero, and the difference matters at the
    boundary: the REST layer reads `.value` and `.fiat_values` off this struct,
    so a NULL arrives as an AttributeError from inside the service rather than
    as a zero.

    Reachable for any address that only spends within the range, which a
    `--start-block` run makes ordinary rather than exotic.
    """
    from pyspark.sql import functions as F

    return currency_struct(F.lit(0), F.array(*[F.lit(0.0) for _ in currencies]))


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

    **Only addresses that actually span pages are indexed.** With
    ``tx_page_size`` at 100 000 virtually every address has one page, and its
    index row says nothing but "page 0 starts at the first transaction" -- which
    a reader can assume. Written for everyone it cost 7.9% of the derived
    keyspace, nearly as much as ``address_transactions`` itself, to store a
    fact that is almost always trivial. `Dal.page_for_tx` returns page 0 when
    there is no row, which is the same answer the row would have given.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    spans = F.max("tx_page").over(
        Window.partitionBy("address", "is_outgoing", "is_zero_value")
    )
    return (
        paged.withColumn("_page_max", spans)
        .where(F.col("_page_max") > 0)
        .groupBy("address", "is_outgoing", "is_zero_value", "tx_page")
        .agg(F.min("tx_id").alias("first_tx_id"))
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
    """``address_bucket``: the partition an ENTITY's own rows live in."""
    return bucket_expr(address, config.entity_buckets)


def relation_bucket(address: "Column", config: NetworkConfig) -> "Column":
    """``rel_bucket``: the partition an edge to this COUNTERPARTY lives in.

    A different modulus from :func:`entity_bucket`, and the distinction is not
    cosmetic. `relation_buckets` is 16 and `entity_buckets` is 100 000, and a
    reader scatters a /neighbors query over 0..relation_buckets-1 -- so writing
    the entity modulus here puts every edge in a partition no reader ever
    looks at, and /neighbors returns nothing at all.

    That is not hypothetical: the account writer called `entity_bucket` for
    this, because a relation's counterparty IS an entity and the name reads as
    correct. Both families now go through this function so they cannot drift.
    """
    return bucket_expr(address, config.relation_buckets)
