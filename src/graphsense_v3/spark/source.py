"""Reading the Delta Lake."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol, Sequence

from graphsenselib.ingest.dump import PARTITIONSIZES

from graphsense_v3.settings import effective_lake_root

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DeltaLake:
    """A Delta Lake root holding one network's ingested tables.

    **Every read of a table is pinned to one Delta version.** A Spark frame is
    lazy, so without a pin each action re-resolves the Delta log at its own wall
    clock: a backfill that writes `block` at 14:49 and `summary_statistics` at
    15:34 reads a lake that ingest has appended to in between, and the two
    disagree about where the chain ends. That is not only a wrong statistic --
    tables written hours apart can reference blocks the earlier ones never got,
    so the keyspace is torn at the tail.

    The version is resolved on first read of each table and reused for the whole
    run, which also makes a run reproducible: the log records what was read.
    """

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
        # table -> Delta version, resolved once. A None means "asked, and the
        # answer was unavailable"; it is cached too, so a lake that cannot be
        # pinned warns once rather than once per read.
        self.versions: dict[str, int | None] = {}

    def path(self, table: str) -> str:
        return f"{self.root}/{table}"

    def pin(self, tables: "Sequence[str]") -> dict:
        """Resolve every table's version NOW, in one pass.

        Lazy per-table resolution pins each table at the moment it is first
        read, which is not the same moment: on the first BCH run `transaction`
        was pinned during preflight at 14:13 and `block` during frame building
        at 14:28. Independent Delta logs, so the version NUMBERS are unrelated
        -- but anything ingest appended in those 15 minutes put `block` ahead of
        `transaction`, which is a block row with no transactions and a
        `highest_block` one too high. The same tear the pin exists to prevent,
        at a smaller scale.

        Called once before the first read. A table pinned here is not
        re-resolved later.
        """
        for table in tables:
            self.version(table)
        return dict(self.versions)

    def version(self, table: str) -> int | None:
        """The Delta version this run reads ``table`` at, resolved once."""
        if table not in self.versions:
            self.versions[table] = self._resolve_version(table)
        return self.versions[table]

    def _resolve_version(self, table: str) -> int | None:
        # `DESCRIBE HISTORY` rather than `delta.tables.DeltaTable`: the SQL
        # extension is already required to read the lake at all
        # (`profile.py` sets DeltaSparkSessionExtension), whereas the `delta`
        # PYTHON package ships only in the Spark image, so importing it here
        # would work on an executor and fail in a test run. Newest first.
        path = self.path(table)
        try:
            found = self.spark.sql(f"DESCRIBE HISTORY delta.`{path}`").take(1)
        except Exception as exc:  # noqa: BLE001 -- an unpinnable lake must be loud
            logger.warning(
                "could not pin %s to a Delta version (%s); this run reads it "
                "unpinned and may tear if the lake is being appended to",
                path,
                exc,
            )
            return None
        version = int(found[0]["version"]) if found else None
        logger.info("lake table %s pinned at Delta version %s", table, version)
        return version

    def read_options(self, table: str) -> dict:
        """Reader options carrying the pin, empty when it could not be had."""
        version = self.version(table)
        return {} if version is None else {"versionAsOf": version}

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

        df = (
            self.spark.read.format("delta")
            .options(**self.read_options(table))
            .load(self.path(table))
        )
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
