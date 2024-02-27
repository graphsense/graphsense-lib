import logging
from typing import List, Tuple

from ..db import AnalyticsDb
from .parquet import write_parquet

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
        elif sink == "parquet":
            path = config.get("output_directory", None)
            schema_table = config.get("schema", None)
            if path is None:
                raise Exception(
                    "No output_dir is set. "
                    "Please set raw_keyspace_file_sinks['parquet'].directory "
                    "in the keyspace config."
                )
            if schema_table is None:
                raise Exception(
                    "No schema_table is set. "
                    "Please provide a schema definition for the pq output data "
                    "in the keyspace config."
                )
            write_parquet(path, table_name, parameters, schema_table)
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
