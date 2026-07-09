"""Process-parallel DB I/O for the UTXO delta updater.

Like the account updater, the UTXO updater's reads are client-CPU-bound:
driver row deserialization (UDT-heavy raw transaction rows, transformed
address/cluster/relation rows) runs under the GIL, so a single process
caps out far below what the Cassandra cluster can serve. Each fetch
function here either runs the existing single-process code path (pool is
None) or fans the keys out to worker processes that own their own driver
sessions and return plain picklable rows.

Worker functions run in spawn-context subprocesses: module-level state is
set up once per worker by init_worker and reused for the pool's lifetime.

fetch_address_ids and fetch_address_rows are re-exported from the account
parallelio module: the underlying async-batch methods live on the shared
TransformedDb base class, so they are schema-agnostic.
"""

from typing import Dict, List, Optional, Tuple

from graphsenselib.db.parallel import flatten_value, get_worker_db
from graphsenselib.deltaupdate.update.account.parallelio import (
    fetch_address_ids,
    fetch_address_rows,
)

__all__ = [
    "fetch_address_ids",
    "fetch_address_rows",
    "fetch_block_transactions",
    "fetch_address_incoming_relations",
    "fetch_cluster_incoming_relations",
    "fetch_cluster_rows",
    "worker_fetch_block_transactions",
    "worker_fetch_address_incoming_relations",
    "worker_fetch_cluster_incoming_relations",
    "worker_fetch_cluster_rows",
]


def worker_fetch_block_transactions(chunk: List[int]) -> List[Tuple[int, list]]:
    return [
        (block, flatten_value(get_worker_db().raw.get_transactions_in_block(block)))
        for block in chunk
    ]


def worker_fetch_address_incoming_relations(
    chunk: List[Tuple[int, int]],
) -> List[object]:
    return [
        flatten_value(qr.result_or_exc.one())
        for qr in get_worker_db().transformed.get_address_incoming_relations_async_batch(
            chunk
        )
    ]


def worker_fetch_cluster_incoming_relations(
    chunk: List[Tuple[int, int]],
) -> List[object]:
    return [
        flatten_value(qr.result_or_exc.one())
        for qr in get_worker_db().transformed.get_cluster_incoming_relations_async_batch(
            chunk
        )
    ]


def worker_fetch_cluster_rows(chunk: List[int]) -> List[Tuple[int, object]]:
    return [
        (cluster_id, flatten_value(qr.result_or_exc.one()))
        for cluster_id, qr in get_worker_db().transformed.get_cluster_async_batch(chunk)
    ]


def fetch_block_transactions(db, pool, blocks: List[int]) -> List[Tuple[int, list]]:
    """Read the raw transaction rows for each block, in block order."""
    if pool is None:
        return [(b, list(db.raw.get_transactions_in_block(b))) for b in blocks]
    return pool.map_chunked(worker_fetch_block_transactions, blocks)


def fetch_address_incoming_relations(
    tdb, pool, rel_ids: List[Tuple[int, int]]
) -> List[object]:
    """Read address_incoming_relations rows (one or None per (dst, src)
    pair), in input order."""
    if pool is None:
        return [
            qr.result_or_exc.one()
            for qr in tdb.get_address_incoming_relations_async_batch(rel_ids)
        ]
    return pool.map_chunked(worker_fetch_address_incoming_relations, rel_ids)


def fetch_cluster_incoming_relations(
    tdb, pool, rel_ids: List[Tuple[int, int]]
) -> List[object]:
    """Read cluster_incoming_relations rows (one or None per (dst, src)
    pair), in input order."""
    if pool is None:
        return [
            qr.result_or_exc.one()
            for qr in tdb.get_cluster_incoming_relations_async_batch(rel_ids)
        ]
    return pool.map_chunked(worker_fetch_cluster_incoming_relations, rel_ids)


def fetch_cluster_rows(
    tdb, pool, cluster_ids: List[int]
) -> Dict[int, Optional[object]]:
    """Read the full cluster rows for the given cluster ids."""
    if pool is None:
        return {
            cluster_id: qr.result_or_exc.one()
            for cluster_id, qr in tdb.get_cluster_async_batch(cluster_ids)
        }
    return dict(pool.map_chunked(worker_fetch_cluster_rows, cluster_ids))
