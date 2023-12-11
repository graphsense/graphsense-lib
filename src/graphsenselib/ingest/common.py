import logging

from ..db import AnalyticsDb
from .parquet import write_parquet

INGEST_SINKS = ["parquet", "cassandra"]

logger = logging.getLogger(__name__)


def write_to_sinks(
    db: AnalyticsDb,
    sink_config: dict,
    table_name: str,
    parameters,
    concurrency: int = 100,
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
        else:
            logger.warning(f"Encountered unknown sink type {sink}, ignoring.")


def cassandra_ingest(
    db: AnalyticsDb, table_name: str, parameters, concurrency: int = 100
) -> None:
    """Concurrent ingest into Apache Cassandra."""
    db.raw.ingest(
        table_name, parameters, concurrency=concurrency, auto_none_to_unset=True
    )
