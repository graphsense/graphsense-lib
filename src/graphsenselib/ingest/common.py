import logging
from typing import List, Tuple

from ..db import AnalyticsDb

INGEST_SINKS = ["parquet", "cassandra", "fs-cache"]

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
        elif sink == "fs-cache":
            c = config.get("cache", None)
            kc = config.get("key_by", {"default": "block_id"})
            key = kc.get(table_name, None) or kc["default"]
            ignore_tables = config.get("ignore_tables", [])
            if table_name in ignore_tables:
                return

            if c is None:
                raise Exception("Cache not set. Error.")

            c.put_items_keyed_by(table_name, parameters, key=key)
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
