"""Reading the Delta Lake."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class DeltaLake:
    """A Delta Lake root holding one network's ingested tables."""

    def __init__(self, spark: SparkSession, root: str, network: str) -> None:
        self.spark = spark
        self.root = root.rstrip("/")
        self.network = network

    def path(self, table: str) -> str:
        return f"{self.root}/{table}"

    def read(
        self,
        table: str,
        *,
        start_block: int | None = None,
        end_block: int | None = None,
        block_column: str = "block_id",
    ) -> DataFrame:
        """Read one lake table, optionally restricted to a block range.

        The range is pushed into the scan rather than applied afterwards: the
        lake is partitioned by block, so this is the difference between reading a
        window and reading a chain.
        """
        df = self.spark.read.format("delta").load(self.path(table))
        if start_block is not None:
            df = df.filter(df[block_column] >= start_block)
        if end_block is not None:
            df = df.filter(df[block_column] <= end_block)
        return df
