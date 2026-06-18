"""Process-parallel DB I/O for the account delta updater.

The account updater's batch reads and writes are client-CPU-bound (driver
row deserialization and statement binding under the GIL), so a single
process caps out far below what the Cassandra cluster can serve. Each
fetch function here either runs the existing single-process code path
(pool is None) or fans the keys out to worker processes that own their
own driver sessions and return plain picklable rows.

Worker functions run in spawn-context subprocesses: module-level state is
set up once per worker by init_worker and reused for the pool's lifetime.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

from graphsenselib.db.parallel import flatten_value, get_worker_db

logger = logging.getLogger(__name__)


def worker_fetch_address_ids(chunk: List[bytes]) -> List[Tuple[bytes, Optional[int]]]:
    out = []
    for address, qr in get_worker_db().transformed.get_address_id_async_batch(chunk):
        row = qr.result_or_exc.one()
        out.append((address, row.address_id if row is not None else None))
    return out


def worker_fetch_address_rows(chunk: List[int]) -> List[Tuple[int, object]]:
    return [
        (address_id, flatten_value(qr.result_or_exc.one()))
        for address_id, qr in get_worker_db().transformed.get_address_async_batch(chunk)
    ]


def worker_fetch_relations_balances(chunk: List[Tuple[str, object]]) -> List[object]:
    """Resolve a mixed chunk of ("rel", (dst, src)) and ("bal", address_id)
    items, preserving chunk order."""
    rel_positions = [i for i, (kind, _) in enumerate(chunk) if kind == "rel"]
    bal_positions = [i for i, (kind, _) in enumerate(chunk) if kind == "bal"]
    in_results, bal_results, _ = (
        get_worker_db().transformed.execute_combined_queries_account_delta_updates(
            [chunk[i][1] for i in rel_positions],
            [chunk[i][1] for i in bal_positions],
        )
    )
    out = [None] * len(chunk)
    for pos, qr in zip(rel_positions, in_results):
        out[pos] = flatten_value(qr.result_or_exc.one())
    for pos, qr in zip(bal_positions, bal_results):
        out[pos] = [flatten_value(row) for row in qr.result_or_exc.all()]
    return out


def fetch_address_ids(tdb, pool, addresses: List[bytes]) -> Dict[bytes, Optional[int]]:
    """Map each address to its existing address_id, or None if unknown."""
    if pool is None:
        out = {}
        for address, qr in tdb.get_address_id_async_batch(list(addresses)):
            row = qr.result_or_exc.one()
            out[address] = row.address_id if row is not None else None
        return out
    return dict(pool.map_chunked(worker_fetch_address_ids, list(addresses)))


def fetch_address_rows(tdb, pool, address_ids: List[int]) -> Dict[int, object]:
    """Read the full address rows for the given existing address ids."""
    if pool is None:
        return {
            address_id: qr.result_or_exc.one()
            for address_id, qr in tdb.get_address_async_batch(address_ids)
        }
    return dict(pool.map_chunked(worker_fetch_address_rows, address_ids))


def fetch_relations_and_balances(
    tdb, pool, rel_ids: List[Tuple[int, int]], address_ids: List[int]
) -> Tuple[List[object], List[List[object]], Dict[str, float]]:
    """Read incoming-relation rows (one or None per (dst, src) pair) and
    balance rows (list per address id), in input order."""
    if pool is None:
        in_results, bal_results, timing = (
            tdb.execute_combined_queries_account_delta_updates(rel_ids, address_ids)
        )
        in_rows = [qr.result_or_exc.one() for qr in in_results]
        bal_rows = [qr.result_or_exc.all() for qr in bal_results]
        return in_rows, bal_rows, timing

    tagged = [("rel", r) for r in rel_ids] + [("bal", a) for a in address_ids]
    t_start = time.time()
    results = pool.map_chunked(worker_fetch_relations_balances, tagged)
    elapsed = time.time() - t_start
    in_rows = results[: len(rel_ids)]
    bal_rows = results[len(rel_ids) :]
    total = len(tagged)
    timing = {
        "n_in": len(rel_ids),
        "n_bal": len(address_ids),
        "in_time": elapsed,
        "bal_time": elapsed,
        "in_qps": total / elapsed if elapsed > 0 else 0.0,
        "bal_qps": total / elapsed if elapsed > 0 else 0.0,
    }
    return in_rows, bal_rows, timing
