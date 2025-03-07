import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

import pydantic

from ..db import AnalyticsDb

INGEST_SINKS = ["parquet", "cassandra"]

CASSANDRA_INGEST_DEFAULT_CONCURRENCY = 100

logger = logging.getLogger(__name__)


def write_to_sinks(
    db: AnalyticsDb,
    sink_config: dict,
    table_name: str,
    parameters,
    concurrency: int = CASSANDRA_INGEST_DEFAULT_CONCURRENCY,
):
    for sink, config in sink_config.items():
        if sink == "cassandra":
            cassandra_ingest(db, table_name, parameters, concurrency=concurrency)
        else:
            logger.warning(f"Encountered unknown sink type {sink}, ignoring.")


def cassandra_ingest(
    db: AnalyticsDb,
    table_name: str,
    parameters,
    concurrency: int = CASSANDRA_INGEST_DEFAULT_CONCURRENCY,
) -> None:
    """Concurrent ingest into Apache Cassandra."""
    db.raw.ingest(
        table_name, parameters, concurrency=concurrency, auto_none_to_unset=True
    )


class AbstractTask:
    def run(self, ctx, data) -> List[Tuple["AbstractTask", object]]:
        pass


class AbstractETLStrategy:
    def pre_processing_tasks(self):
        return []

    def per_blockrange_tasks(self):
        return []

    def get_source_adapter(self, ctx=None):
        return None


class StoreTask(AbstractTask):
    def run(self, ctx, data):
        table, rows = data
        write_to_sinks(ctx.db, ctx.sink_config, table, rows)
        return []


class BlockRangeContent(pydantic.BaseModel):
    table_contents: Dict[str, Union[List[dict], dict]]
    start_block: Optional[int] = None  # None in the blockindependent case
    end_block: Optional[int] = None  # None in the blockindependent case

    @staticmethod
    def merge(block_range_contents: List["BlockRangeContent"]) -> "BlockRangeContent":
        # sort block_range_contents by start_block
        block_range_contents = sorted(block_range_contents, key=lambda x: x.start_block)
        # make sure that there are no gaps in the block range
        assert all(
            block_range_contents[i].end_block + 1
            == block_range_contents[i + 1].start_block
            for i in range(len(block_range_contents) - 1)
        )
        # all should have the same tables
        assert all(
            set(block_range_contents[0].table_contents.keys())
            == set(block_range_content.table_contents.keys())
            for block_range_content in block_range_contents
        )
        table_contents = {
            table_name: []
            for table_name in block_range_contents[0].table_contents.keys()
        }

        for block_range_content in block_range_contents:
            for table_name, table_content in block_range_content.table_contents.items():
                table_contents[table_name].extend(table_content)

        return BlockRangeContent(
            table_contents=table_contents,
            start_block=block_range_contents[0].start_block,
            end_block=block_range_contents[-1].end_block,
        )


class Source(ABC):
    @abstractmethod
    def read_blockrange(self, start_block, end_block) -> BlockRangeContent:
        pass

    @abstractmethod
    def read_blockindep(self) -> BlockRangeContent:
        pass

    @abstractmethod
    def get_last_synced_block(self) -> int:
        pass

    def get_last_synced_block_bo(self, backoffblocks=0):
        return self.get_last_synced_block() - backoffblocks

    def validate_blockrange(
        self, start_block: int, end_block: int, backoff: int
    ) -> Tuple[int, int]:
        last_ingestable_block = self.get_last_synced_block_bo(backoff)

        if end_block is None:
            end_block = last_ingestable_block

        assert start_block >= 0, "Start block must be greater or equal to 0"

        assert start_block <= end_block, (
            "Start block must be less or equal to end block"
        )
        assert start_block >= 0, "Start block must be greater or equal to 0"
        assert start_block <= last_ingestable_block, (
            "Start block must be less or equal to last synced block"
        )
        logger.info(f"Last synced block: {end_block:,}")

        assert end_block >= 0, "End block must be greater or equal to 0"

        if end_block > last_ingestable_block:
            logger.warning(
                f"End block {end_block:,} is greater than last synced block (-backoff) "
                f"{last_ingestable_block:,}, setting end block to this value"
            )
            end_block = last_ingestable_block

        logger.info(
            f"Validated block range "
            f"{start_block:,} - {end_block:,} ({end_block - start_block + 1:,} blks) "
        )

        return start_block, end_block


class Transformer(ABC):
    def __init__(self, partition_batch_size: int, network: str):
        self.partition_batch_size = partition_batch_size
        self.network = network

    @abstractmethod
    def transform(self, block_range_content: BlockRangeContent) -> BlockRangeContent:
        pass

    @abstractmethod
    def transform_blockindep(
        self, block_range_content: BlockRangeContent
    ) -> BlockRangeContent:
        pass


class Sink(ABC):
    @abstractmethod
    def write(self, block_range_content: BlockRangeContent):
        pass
