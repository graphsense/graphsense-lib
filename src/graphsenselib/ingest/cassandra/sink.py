import logging

from graphsenselib.db import AnalyticsDb
from graphsenselib.ingest.common import (
    CASSANDRA_INGEST_DEFAULT_CONCURRENCY,
    BlockRangeContent,
    Sink,
    cassandra_ingest,
)

logger = logging.getLogger(__name__)


class CassandraSink(Sink):
    """Sink that writes block range data to Apache Cassandra.

    Cassandra prepared statements auto-discover table columns and bind by name.
    Extra dict keys (e.g. 'partition') are silently ignored. The data must already
    have 'block_id_group' and 'tx_hash_prefix' from the prepare step.
    """

    def __init__(
        self, db: AnalyticsDb, concurrency: int = CASSANDRA_INGEST_DEFAULT_CONCURRENCY
    ):
        self.db = db
        self.concurrency = concurrency

    def lock_name(self) -> str:
        return f"{self.db.raw.get_keyspace()}_{self.db.transformed.get_keyspace()}"

    def write(self, block_range_content: BlockRangeContent):
        for table_name, rows in block_range_content.table_contents.items():
            if not rows:
                continue
            # UTXO transactions carry Cassandra-specific io fields alongside
            # the neutral parquet format. Swap them in for Cassandra's schema.
            if table_name == "transaction" and "inputs_cassandra" in rows[0]:
                rows = [dict(r) for r in rows]
                for row in rows:
                    row["inputs"] = row.pop("inputs_cassandra")
                    row["outputs"] = row.pop("outputs_cassandra")
            cassandra_ingest(self.db, table_name, rows, self.concurrency)
