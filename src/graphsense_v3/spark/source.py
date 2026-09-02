"""Reading the Delta Lake."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from graphsenselib.ingest.dump import PARTITIONSIZES

from graphsense_v3.settings import effective_lake_root

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class DeltaLake:
    """A Delta Lake root holding one network's ingested tables."""

    def __init__(
        self,
        spark: SparkSession,
        root: str,
        network: str,
        partition_size: int | None = None,
    ) -> None:
        self.spark = spark
        # Normally already done by `settings.effective_lake_root`, so that
        # `plan` prints the path that is really read; repeated here, and
        # idempotent, because a caller can construct this directly.
        self.root = effective_lake_root(root)
        self.network = network.lower()
        # The lake's physical Delta partition is block_id // this. Pushing a
        # predicate on `partition` is what actually prunes files; a predicate on
        # block_id alone only prunes as far as the row-group statistics allow.
        self.partition_size = partition_size or PARTITIONSIZES[self.network]

    def path(self, table: str) -> str:
        return f"{self.root}/{table}"

    def read(
        self,
        table: str,
        *,
        start_block: int | None = None,
        end_block: int | None = None,
        block_column: str | None = "block_id",
        partitioned: bool = True,
    ) -> DataFrame:
        """Read one lake table, optionally restricted to a block range.

        The range is pushed into the scan rather than applied afterwards: the
        lake is partitioned by block, so this is the difference between reading a
        window and reading a chain.

        ``block_column=None`` marks a table that carries the Delta ``partition``
        column but no block id of its own (``transaction_by_tx_prefix``, the two
        spending tables). Those can only be trimmed to whole partitions, so a
        ranged read of one returns a superset of the range -- fine for a join,
        and fine for a write, which is a blind insert of identical rows.
        """
        from pyspark.sql import functions as F

        df = self.spark.read.format("delta").load(self.path(table))
        if partitioned:
            if start_block is not None:
                df = df.filter(F.col("partition") >= start_block // self.partition_size)
            if end_block is not None:
                df = df.filter(F.col("partition") <= end_block // self.partition_size)
        if block_column is not None:
            if start_block is not None:
                df = df.filter(F.col(block_column) >= start_block)
            if end_block is not None:
                df = df.filter(F.col(block_column) <= end_block)
        return df


class LakeSource(Protocol):
    """What a raw loader needs of a lake.

    The loaders declare this rather than :class:`DeltaLake` so a test can supply
    in-memory tables without a Delta round trip, which would only be testing
    Delta. Same seam the async services already use for their DAL.
    """

    spark: SparkSession

    def read(
        self,
        table: str,
        *,
        start_block: int | None = ...,
        end_block: int | None = ...,
        block_column: str | None = ...,
        partitioned: bool = ...,
    ) -> DataFrame: ...
