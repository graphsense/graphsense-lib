"""Process-parallel Cassandra access for the delta updater.

The cassandra-driver deserializes result rows on the client, and for the
UDT-heavy transformed tables that work is CPU-bound: a single Python
process caps at roughly 1K point reads/s regardless of request
concurrency. The helpers here let worker processes do the driver work and
hand rows back to the parent as plain picklable objects (pickle is only
used as multiprocessing's own parent<->child transport, never for
untrusted data).
"""

import multiprocessing
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor


class PlainRow:
    """Attribute-access view of a driver row or UDT value.

    Driver rows and UDT values are dynamically generated namedtuple
    classes that cannot cross process boundaries; PlainRow carries the
    same data as a plain dict. It deliberately does NOT support item
    access: the driver's UDT serializer tries ``val[i]`` first and falls
    back to ``getattr`` only on TypeError
    (cassandra.cqltypes.UserType.serialize_safe), so PlainRow values bind
    correctly to UDT columns on write.
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name):
        data = object.__getattribute__(self, "_data")
        if name in data:
            return data[name]
        raise AttributeError(name)

    def __eq__(self, other):
        return isinstance(other, PlainRow) and other._data == self._data

    def __repr__(self):
        return f"PlainRow({self._data!r})"

    def __reduce__(self):
        return (PlainRow, (self._data,))


def flatten_value(value):
    """Recursively convert driver result values into picklable equivalents.

    Rows and UDT values (anything namedtuple-like) become PlainRow, maps
    become dicts, sequences become lists; scalars pass through unchanged.
    """
    if value is None or isinstance(value, (str, bytes, bytearray)):
        return value
    if hasattr(value, "_fields"):
        return PlainRow({f: flatten_value(v) for f, v in zip(value._fields, value)})
    if isinstance(value, Mapping):
        return {flatten_value(k): flatten_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [flatten_value(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return {flatten_value(v) for v in value}
    return value


def split_even(items: list, n: int) -> list:
    """Split items into at most n contiguous chunks of near-equal size."""
    n = min(n, len(items))
    quotient, remainder = divmod(len(items), n)
    chunks = []
    start = 0
    for i in range(n):
        size = quotient + (1 if i < remainder else 0)
        chunks.append(items[start : start + size])
        start += size
    return chunks


_worker_db = None


def init_worker(env: str, currency: str):
    """Open this worker process's own database session.

    Runs once per worker via the pool initializer; the driver is not
    fork-safe, so every worker must build its own Cluster after spawn.
    """
    global _worker_db
    from graphsenselib.db import DbFactory

    _worker_db = DbFactory().from_config(env, currency)
    _worker_db.open()


def get_worker_db():
    return _worker_db


def worker_apply_changes(chunk: list) -> list:
    """Apply a shard of DbChange upserts from a worker process.

    Returns a single-element list (one ApplyChangesResult per chunk) so it
    composes with ParallelDbPool.map_chunked's list-in/list-out contract.
    """
    return [get_worker_db().transformed.apply_changes(chunk, atomic=False)]


class ParallelDbPool:
    """Pool of worker processes for client-CPU-bound Cassandra work.

    Workers are created with the spawn context because the
    cassandra-driver is not fork-safe; each worker opens its own Cluster
    session via the initializer and keeps it for the pool's lifetime.
    """

    def __init__(self, num_workers: int, initializer, initargs=()):
        self.num_workers = num_workers
        self._executor = ProcessPoolExecutor(
            max_workers=num_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=initializer,
            initargs=initargs,
        )

    def map_chunked(self, fn, items: list) -> list:
        """Run fn over near-equal chunks of items in the workers.

        fn receives a list and must return a list; the concatenated
        results preserve the order of the input items.
        """
        if not items:
            return []
        chunks = split_even(items, self.num_workers)
        futures = [self._executor.submit(fn, chunk) for chunk in chunks]
        results = []
        for future in futures:
            results.extend(future.result())
        return results

    def shutdown(self):
        self._executor.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()
        return False
