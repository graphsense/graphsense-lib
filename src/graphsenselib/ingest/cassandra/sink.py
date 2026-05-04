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

    name = "cassandra"
    # Cassandra writes are UPSERTs, so re-writing an already-ingested range
    # is idempotent. This sink can be backfilled at any start_block.
    requires_monotonic_append = False

    def __init__(
        self, db: AnalyticsDb, concurrency: int = CASSANDRA_INGEST_DEFAULT_CONCURRENCY
    ):
        self.db = db
        self.concurrency = concurrency

    def lock_name(self) -> str:
        return self.db.raw.get_keyspace()

    def highest_block(self):
        return self.db.raw.get_highest_block()

    def write(self, block_range_content: BlockRangeContent):
        # Safety net: force `block` to be written LAST regardless of
        # transformer dict order. `get_highest_block()` reads MAX(block_id)
        # from the block table as the resume marker between runs; if the
        # chunk crashes mid-write with `block` already written, the height
        # advances while side tables are missing rows for that range and
        # the next run silently resumes past the gap. Transformers in
        # transform.py already emit `block` last for this reason; the sort
        # here defends against a future transformer or refactor that
        # accidentally puts it first. Stable sort preserves the relative
        # order of the other tables.
        items = sorted(
            block_range_content.table_contents.items(),
            key=lambda kv: kv[0] == "block",
        )
        for table_name, rows in items:
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
