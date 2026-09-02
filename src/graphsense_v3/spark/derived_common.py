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
    compute the page it needs. Read only when a range filter is present.
    """
    from pyspark.sql import functions as F

    return paged.groupBy("address", "is_outgoing", "tx_page").agg(
        F.min("tx_id").alias("first_tx_id")
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


def entity_bucket(address: "Column", config: NetworkConfig) -> "Column":
    return bucket_expr(address, config.entity_buckets)
