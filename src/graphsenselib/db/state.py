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
FRESH_CLUSTERING_ACTIVE_KEY = "fresh_clustering_active"


def build_ingest_complete_row() -> dict:
    """Row shared by direct-CQL and Spark writers of the marker.

    Centralised so REST-side reads stay aligned with both writers if the
    schema ever grows a column.
    """
    now = datetime.now(timezone.utc)
    return {"key": INGEST_COMPLETE_KEY, "value": now.isoformat(), "updated_at": now}


def mark_ingest_complete(db, keyspace_type: str) -> None:
    db.by_ks_type(keyspace_type).ingest(STATE_TABLE, [build_ingest_complete_row()])


def build_fresh_clustering_active_row() -> dict:
    """Marker row: fresh clustering is bootstrapped and live on this keyspace.

    Written by the one-off ``transformation cluster`` bootstrap as its last
    step. Consumers self-detect on it — the delta updater maintains the
    fresh_* tables and REST fills ``fresh_cluster_id`` on address responses
    iff the marker is present. Mere existence of the fresh_* tables is NOT
    sufficient (migrations create them empty on every keyspace); without the
    bootstrap the incremental path would build garbage clusters from a
    mid-chain starting point.

    TODO(next major upgrade): remove this marker mechanism entirely once
    fresh clustering is the only clustering mode — drop this writer, the
    delta updater gate (deltaupdate/update/utxo/update.py
    ``_fresh_clustering_active``) and the REST gate
    (db/asynchronous/cassandra.py ``_fresh_clustering_active`` /
    analytics.py ``is_fresh_clustering_active``), and assume active on
    every transformed keyspace.
    """
    now = datetime.now(timezone.utc)
    return {
        "key": FRESH_CLUSTERING_ACTIVE_KEY,
        "value": now.isoformat(),
        "updated_at": now,
    }


def mark_fresh_clustering_active(db) -> None:
    db.by_ks_type("transformed").ingest(
        STATE_TABLE, [build_fresh_clustering_active_row()]
    )
