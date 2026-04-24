"""Bootstrap-marker writer for the per-keyspace `state` table.

REST keyspace auto-discovery
(graphsenselib.db.asynchronous.cassandra.find_latest_raw_keyspace and
find_latest_transformed_keyspace) treats the presence of a `bootstrapped`
row in this table as the "keyspace is ready to query" signal.

Callers MUST invoke `mark_bootstrapped` as the very last write of an
ingest or transformation run. Earlier writes leave the keyspace
discoverable while data is still partial.
"""

from datetime import datetime, timezone


BOOTSTRAPPED_KEY = "bootstrapped"


def mark_bootstrapped(db, keyspace_type: str) -> None:
    now = datetime.now(timezone.utc)
    db.by_ks_type(keyspace_type).ingest(
        "state",
        [
            {
                "key": BOOTSTRAPPED_KEY,
                "value": now.isoformat(),
                "updated_at": now,
            }
        ],
    )
