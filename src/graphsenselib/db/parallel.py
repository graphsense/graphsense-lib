"""Process-parallel Cassandra access for the delta updater.

The cassandra-driver deserializes result rows on the client, and for the
UDT-heavy transformed tables that work is CPU-bound: a single Python
process caps at roughly 1K point reads/s regardless of request
concurrency. The helpers here let worker processes do the driver work and
hand rows back to the parent as plain picklable objects (pickle is only
used as multiprocessing's own parent<->child transport, never for
untrusted data).
"""

import atexit
import multiprocessing
import pickle
import signal
import traceback
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

    def _replace(self, **kwargs):
        """Return a copy with the given fields overridden, mirroring
        namedtuple._replace so flattened rows stay drop-in replacements
        for driver rows."""
        data = object.__getattribute__(self, "_data")
        unknown = set(kwargs) - set(data)
        if unknown:
            raise ValueError(f"Got unexpected field names: {sorted(unknown)!r}")
        return PlainRow({**data, **kwargs})

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

    Workers ignore SIGINT/SIGTERM so a Ctrl-C or SIGTERM delivered to the
    whole process group cannot tear a worker down mid-chunk. The parent
    owns the graceful stop: it keeps the flag-setting signal handler,
    finishes the in-flight batch (every submitted chunk runs to completion
    under shutdown(wait=True)), and breaks at the next batch boundary.
    Shutdown then drains the pool via its sentinel queue, not a signal, so
    ignoring the signals here does not block teardown.

    The session is closed on normal worker exit via atexit, so the
    cassandra Cluster is shut down explicitly instead of relying on the
    interpreter reclaiming sockets.
    """
    global _worker_db
    from graphsenselib.db import DbFactory

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    _worker_db = DbFactory().from_config(env, currency)
    _worker_db.open()
    atexit.register(_worker_db.close)


def get_worker_db():
    return _worker_db


def worker_apply_changes(chunk: list) -> list:
    """Apply a shard of DbChange upserts from a worker process.

    Returns a single-element list (one ApplyChangesResult per chunk) so it
    composes with ParallelDbPool.map_chunked's list-in/list-out contract.
    """
    return [get_worker_db().transformed.apply_changes(chunk, atomic=False)]


class ParallelWorkerError(Exception):
    """Picklable stand-in for a worker exception that cannot cross the
    process boundary. Carries the original exception's formatted traceback
    in its message."""


def _run_task_picklable(fn, chunk):
    """Run a map_chunked task, guaranteeing any raised exception can be
    pickled back to the parent.

    cassandra-driver errors like NoHostAvailable and OperationTimedOut
    reference cassandra.pool.Host objects, each holding an RLock. The
    executor pickles a worker's exception to hand it to the parent; left
    unguarded, that pickling fails and the parent sees
    "TypeError: cannot pickle '_thread.RLock' object" instead of the real
    database error. Exceptions that survive a pickle round-trip are
    re-raised unchanged; the rest are re-raised as ParallelWorkerError
    carrying the original traceback text.
    """
    try:
        return fn(chunk)
    except Exception as exc:
        try:
            pickle.loads(pickle.dumps(exc))
        except Exception:
            detail = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ).rstrip()
            raise ParallelWorkerError(
                f"worker raised unpicklable {type(exc).__name__}; "
                f"original traceback:\n{detail}"
            ) from None
        raise


def _init_worker_signal_inert(initializer, initargs):
    """Pool-worker bootstrap: ignore termination signals, then run the
    caller's initializer.

    Terminal Ctrl-C delivers SIGINT to the whole foreground process
    group; a worker dying mid-write would turn the parent's graceful
    flag-based shutdown into a BrokenProcessPool mid-batch. Workers only
    stop via pool shutdown (or SIGKILL).
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if initializer is not None:
        initializer(*initargs)


class ParallelDbPool:
    """Pool of worker processes for client-CPU-bound Cassandra work.

    Workers are created with the spawn context because the
    cassandra-driver is not fork-safe; each worker opens its own Cluster
    session via the initializer and keeps it for the pool's lifetime.
    Workers ignore SIGINT/SIGTERM (see _init_worker_signal_inert) so
    group-delivered terminal signals cannot abort a batch mid-write.
    """

    def __init__(self, num_workers: int, initializer, initargs=()):
        self.num_workers = num_workers
        self._executor = ProcessPoolExecutor(
            max_workers=num_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_init_worker_signal_inert,
            initargs=(initializer, initargs),
        )

    def map_chunked(self, fn, items: list) -> list:
        """Run fn over near-equal chunks of items in the workers.

        fn receives a list and must return a list; the concatenated
        results preserve the order of the input items.
        """
        if not items:
            return []
        chunks = split_even(items, self.num_workers)
        futures = [
            self._executor.submit(_run_task_picklable, fn, chunk) for chunk in chunks
        ]
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
