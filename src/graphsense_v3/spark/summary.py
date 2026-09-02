"""The ``summary_statistics`` row.

One row per keyspace, and more load-bearing than its size suggests: REST picks
the latest derived keyspace by reading ``no_blocks`` from it
(`cassandra.py:871-882`), and ``/stats`` serves the whole row
(`cassandra.py:1282`).

**Nothing here derives a count from an id.** v2 ingest computes its transaction
total as ``last_tx_id + 1`` (`ingest/utxo.py:1071`, `ingest/dump.py:490`), which
was only ever right because the id was a dense counter. Under D12 a tx_id is
``(block_id << 32) + index`` and is sparse, so a count has to be a count.
"""

# NOTE: no `from __future__ import annotations` -- imported alongside modules
# that define pandas UDFs.

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

#: `summary_statistics` holds exactly one row.
SINGLETON_ID = 0

RAW_SCHEMA = (
    "id INT, timestamp BIGINT, lowest_block BIGINT, highest_block BIGINT, "
    "no_transactions BIGINT"
)
DERIVED_SCHEMA = (
    "id INT, timestamp BIGINT, lowest_block BIGINT, highest_block BIGINT, "
    "no_transactions BIGINT, no_addresses BIGINT, no_address_relations BIGINT"
)


def _extent(blocks: "DataFrame") -> tuple:
    """``(lowest_block, highest_block, timestamp)`` of what is present.

    Both ends, so the row describes the range it actually covers. v2 recorded
    only ``no_blocks`` -- a height called a count -- which is unambiguous only
    for a keyspace that starts at block 0.
    """
    from pyspark.sql import functions as F

    row = blocks.agg(
        F.min("block_id").alias("lowest"),
        F.max("block_id").alias("highest"),
        F.max("timestamp").alias("timestamp"),
    ).collect()[0]
    if row["highest"] is None:
        return 0, 0, 0
    return int(row["lowest"]), int(row["highest"]), int(row["timestamp"] or 0)


def raw_statistics(
    spark: "SparkSession", blocks: "DataFrame", transactions: "DataFrame"
) -> "DataFrame":
    """The raw keyspace's row: what range is in here, and how recent."""
    lowest, highest, timestamp = _extent(blocks)
    return spark.createDataFrame(
        [(SINGLETON_ID, timestamp, lowest, highest, transactions.count())],
        schema=RAW_SCHEMA,
    )


def derived_statistics(
    spark: "SparkSession",
    blocks: "DataFrame",
    transactions: "DataFrame",
    address_stats: "DataFrame",
    relations: "DataFrame",
) -> "DataFrame":
    """The derived keyspace's row.

    The row describes this keyspace only. v2 also recorded how far the RAW
    keyspace had got, so the derived one could report the lag -- a second
    keyspace's fact, copied, and able to go stale. v3 writes both from one range
    in one run, so there is no lag; anything wanting to compare them reads both
    rows.

    ``no_address_relations`` counts each edge ONCE. The relations are stored
    twice, incoming and outgoing, so counting rows across both would double it;
    this counts one direction.
    """
    lowest, highest, timestamp = _extent(blocks)
    row = (
        SINGLETON_ID,
        timestamp,
        lowest,
        highest,
        transactions.count(),
        address_stats.count(),
        relations.count(),
    )
    return spark.createDataFrame([row], schema=DERIVED_SCHEMA)


def statistics_for(
    spark: "SparkSession",
    raw_frames: dict,
    derived_frames: Optional[dict] = None,
) -> "DataFrame":
    """The row for whichever keyspace is being written."""
    if derived_frames is None:
        return raw_statistics(spark, raw_frames["block"], raw_frames["transaction"])
    return derived_statistics(
        spark,
        raw_frames["block"],
        raw_frames["transaction"],
        derived_frames["address_stats"],
        derived_frames["address_outgoing_relations"],
    )
