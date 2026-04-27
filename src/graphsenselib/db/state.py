"""Ingest-complete marker writer for the per-keyspace `state` table.

REST keyspace auto-discovery
(graphsenselib.db.asynchronous.cassandra.find_latest_raw_keyspace and
find_latest_transformed_keyspace) treats the presence of an
`ingest_complete` row in this table as the "keyspace is ready to query"
signal.

Callers MUST invoke `mark_ingest_complete` as the very last write of an
ingest or transformation run. Earlier writes leave the keyspace
discoverable while data is still partial.
"""

from datetime import datetime, timezone


INGEST_COMPLETE_KEY = "ingest_complete"


def mark_ingest_complete(db, keyspace_type: str) -> None:
    now = datetime.now(timezone.utc)
    db.by_ks_type(keyspace_type).ingest(
        "state",
        [
            {
                "key": INGEST_COMPLETE_KEY,
                "value": now.isoformat(),
                "updated_at": now,
            }
        ],
    )
