"""Ingest-complete marker writer for the per-keyspace `state` table.

REST raw-keyspace auto-discovery
(graphsenselib.db.asynchronous.cassandra.find_latest_raw_keyspace) treats
the presence of an `ingest_complete` row in this table as the "raw
keyspace is ready to query" signal. Raw ingest callers MUST invoke
`mark_ingest_complete` as the very last write of an ingest run.

The marker is currently raw-only. Transformed keyspaces are produced by
the transformation pipeline (no "ingest" step) and are discovered via a
populated `summary_statistics` table — see
`find_latest_transformed_keyspace`.
"""

from datetime import datetime, timezone


STATE_TABLE = "state"
INGEST_COMPLETE_KEY = "ingest_complete"


def build_ingest_complete_row() -> dict:
    """Row shared by direct-CQL and Spark writers of the marker.

    Centralised so REST-side reads stay aligned with both writers if the
    schema ever grows a column.
    """
    now = datetime.now(timezone.utc)
    return {"key": INGEST_COMPLETE_KEY, "value": now.isoformat(), "updated_at": now}


def mark_ingest_complete(db, keyspace_type: str) -> None:
    db.by_ks_type(keyspace_type).ingest(STATE_TABLE, [build_ingest_complete_row()])
