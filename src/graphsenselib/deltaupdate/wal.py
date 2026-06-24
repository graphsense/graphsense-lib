"""Redo write-ahead log for crash-safe delta updates.

The delta updater computes aggregate columns (balances, tx counts, relation
values) as ``current_db_value + block_delta`` and writes the *absolute* result.
Such a write is idempotent only with respect to the exact value that was
computed, which is a function of the DB pre-state. Reprocessing a block whose
writes were partially applied therefore double-counts.

This module persists the *resolved* change set (absolute values) durably before
any of it is applied, and clears it only after the whole set is acknowledged. On
startup a surviving record is replayed verbatim — never recomputed — which
drives every row to the intended post-state regardless of how far the crashed
apply got.

Storage is a Cassandra table in the transformed keyspace so the log is shared
across independent container runs (a container-local file would vanish with the
container). The payload is chunked because Cassandra dislikes multi-MB cells, and
a header row written *last* commits the record (the DB analogue of an atomic
temp-file rename).
"""

import datetime as _dt
import hashlib
import logging
from dataclasses import dataclass
from typing import Callable, List, Optional

import msgpack

from graphsenselib.datatypes import DbChangeType
from graphsenselib.db.analytics import DbChange
from graphsenselib.db.parallel import PlainRow
from graphsenselib.deltaupdate.update.account.createdeltas import TxReference
from graphsenselib.deltaupdate.update.generic import DeltaValue

logger = logging.getLogger(__name__)

WAL_TABLE_NAME = "delta_updater_wal"

# Chunk size for the payload blob. Kept well under Cassandra's cell-size
# soft limits while keeping the chunk count small for typical batches.
_CHUNK_SIZE = 512 * 1024

# Clustering index of the sentinel header row. Data chunks use idx >= 0; the
# header is written last and its presence marks the record as committed.
_HEADER_IDX = -1

# DbChange.data values are msgpack-native (int, str, bytes, None, list, dict)
# except for a small, fixed set of domain types, each handled by a msgpack
# extension below so it rebinds byte-identically through the driver on replay:
#   - DeltaValue  (value: int, fiat_values: list[int]) — account + UTXO
#   - TxReference (trace_index, log_index UDT)          — account only
#   - PlainRow    (attribute-access view of a driver row/UDT) — account only:
#         the process-parallel reader returns current DB values as PlainRow
#         (db/parallel.py), and these enter DbChange.data on the account path.
#   - datetime    (bookkeeping timestamps)
#   - big int     (Python ints outside msgpack's signed/unsigned 64-bit range):
#         Cassandra varint columns hold arbitrary precision — e.g. high-decimal
#         token balances — and msgpack routes such ints here rather than packing
#         them natively. Encoded as a decimal string so the value is preserved
#         exactly and rebinds to varint identically on replay.
# numpy scalars (pandas-derived) collapse to their Python scalar via .item().
# An unrecognized type raises at encode time (loud, before any write) rather
# than being silently dropped — add a new ext code here if a new domain type
# ever enters DbChange.data.
_EXT_DATETIME = 1
_EXT_DELTAVALUE = 2
_EXT_TXREFERENCE = 3
_EXT_PLAINROW = 4
_EXT_BIGINT = 5


def _packb(obj) -> bytes:
    return msgpack.packb(obj, use_bin_type=True, default=_default)


def _default(obj):
    if isinstance(obj, _dt.datetime):
        return msgpack.ExtType(_EXT_DATETIME, obj.isoformat().encode("utf-8"))
    if isinstance(obj, DeltaValue):
        return msgpack.ExtType(
            _EXT_DELTAVALUE, _packb([obj.value, list(obj.fiat_values)])
        )
    if isinstance(obj, TxReference):
        return msgpack.ExtType(
            _EXT_TXREFERENCE, _packb([obj.trace_index, obj.log_index])
        )
    if isinstance(obj, PlainRow):
        # _data may nest further PlainRow/DeltaValue/etc.; _packb recurses
        # through _default so the whole tree rebinds byte-identically.
        return msgpack.ExtType(_EXT_PLAINROW, _packb(obj._data))
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    if isinstance(obj, int):  # int wider than msgpack's native 64-bit range
        return msgpack.ExtType(_EXT_BIGINT, str(obj).encode("ascii"))
    raise TypeError(
        f"Cannot serialize {type(obj)!r} into the WAL payload; add a msgpack "
        "ext handler in wal.py for this type."
    )


def _ext_hook(code, data):
    if code == _EXT_DATETIME:
        return _dt.datetime.fromisoformat(data.decode("utf-8"))
    if code == _EXT_DELTAVALUE:
        value, fiat_values = _unpackb(data)
        return DeltaValue(value=value, fiat_values=fiat_values)
    if code == _EXT_TXREFERENCE:
        trace_index, log_index = _unpackb(data)
        return TxReference(trace_index=trace_index, log_index=log_index)
    if code == _EXT_PLAINROW:
        return PlainRow(_unpackb(data))
    if code == _EXT_BIGINT:
        return int(data.decode("ascii"))
    return msgpack.ExtType(code, data)


def _unpackb(payload: bytes):
    return msgpack.unpackb(payload, raw=False, ext_hook=_ext_hook)


def encode_changes(changes: List[DbChange]) -> bytes:
    """Serialize a list of DbChange into a bytes payload (msgpack)."""
    encoded = []
    for c in changes:
        if c.action == DbChangeType.TRUNCATE:
            # TRUNCATE is not idempotent and must never be replayed.
            raise ValueError("TRUNCATE changes must not enter the WAL.")
        encoded.append([c.action.value, c.table, c.data])
    return _packb(encoded)


def decode_changes(payload: bytes) -> List[DbChange]:
    """Inverse of :func:`encode_changes`."""
    return [
        DbChange(action=DbChangeType(action), table=table, data=data)
        for action, table, data in _unpackb(payload)
    ]


@dataclass(frozen=True)
class WalRecord:
    run_id: str
    code_version: str
    block_lo: int
    block_hi: int
    changes: List[DbChange]
    bookkeeping: List[DbChange]


class WalVersionMismatch(Exception):
    """A pending WAL was written by an incompatible code/schema version."""


class DeltaWal:
    """DB-backed redo log. At most one record outstanding per keyspace.

    The caller supplies the apply function (see :meth:`recover`) so this class
    stays storage-only and free of import cycles with the update strategies.
    """

    def __init__(self, transformed_db, run_id: str, code_version: str):
        self._tdb = transformed_db
        self._db = transformed_db._db  # raw CassandraDb for parametrized exec
        self._keyspace = transformed_db.get_keyspace()
        self._run_id = run_id
        self._code_version = code_version

    # -- schema ----------------------------------------------------------------

    def ensure_schema(self) -> None:
        self._tdb.ensure_table_exists(
            WAL_TABLE_NAME,
            [
                "keyspace_marker text",
                "chunk_idx int",
                "payload blob",
                "run_id text",
                "code_version text",
                "block_lo bigint",
                "block_hi bigint",
                "n_data_chunks int",
                "n_book_chunks int",
                "checksum text",
            ],
            ["keyspace_marker", "chunk_idx"],
            truncate=False,
        )

    # -- low level -------------------------------------------------------------

    def _table(self) -> str:
        return f"{self._keyspace}.{WAL_TABLE_NAME}"

    def _exec(self, cql: str, params: list):
        prep = self._db.get_prepared_statement(cql)
        return self._db.execute_statement(prep.bind(params))

    def _read_header(self) -> Optional[dict]:
        rows = list(
            self._exec(
                f"SELECT run_id, code_version, block_lo, block_hi, "
                f"n_data_chunks, n_book_chunks, checksum "
                f"FROM {self._table()} WHERE keyspace_marker=? AND chunk_idx=?",
                [self._keyspace, _HEADER_IDX],
            )
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "run_id": r.run_id,
            "code_version": r.code_version,
            "block_lo": r.block_lo,
            "block_hi": r.block_hi,
            "n_data_chunks": r.n_data_chunks,
            "n_book_chunks": r.n_book_chunks,
            "checksum": r.checksum,
        }

    def _delete_partition(self) -> None:
        self._exec(
            f"DELETE FROM {self._table()} WHERE keyspace_marker=?", [self._keyspace]
        )

    # -- public API ------------------------------------------------------------

    def has_pending(self) -> bool:
        """True only if a committed (headered) record exists."""
        return self._read_header() is not None

    def stage(self, record: WalRecord) -> None:
        """Durably persist the record before any of it is applied.

        Writes data chunks first, then the header row last. A crash before the
        header lands leaves headerless chunks that :meth:`recover` sweeps; since
        staging completes before the caller applies anything, no data was
        written in that case.
        """
        # Start from a clean partition (drops any swept-but-not-cleared chunks).
        self._delete_partition()

        data_blob = encode_changes(record.changes)
        book_blob = encode_changes(record.bookkeeping)
        data_chunks = _split(data_blob)
        book_chunks = _split(book_blob)
        all_chunks = data_chunks + book_chunks
        checksum = hashlib.sha256(data_blob + book_blob).hexdigest()

        insert = (
            f"INSERT INTO {self._table()} (keyspace_marker, chunk_idx, payload) "
            f"VALUES (?, ?, ?)"
        )
        for i, chunk in enumerate(all_chunks):
            self._exec(insert, [self._keyspace, i, chunk])

        # Header row written LAST = commit point.
        self._exec(
            f"INSERT INTO {self._table()} (keyspace_marker, chunk_idx, run_id, "
            f"code_version, block_lo, block_hi, n_data_chunks, n_book_chunks, "
            f"checksum) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                self._keyspace,
                _HEADER_IDX,
                record.run_id,
                record.code_version,
                record.block_lo,
                record.block_hi,
                len(data_chunks),
                len(book_chunks),
                checksum,
            ],
        )

    def load(self, allow_version_mismatch: bool = False) -> WalRecord:
        """Load and verify the committed record. Raises on version mismatch or
        a corrupt/torn payload.

        ``allow_version_mismatch`` bypasses *only* the code-version check (the
        chunk-count and checksum verification still run, so a torn/corrupt
        payload is always rejected). The caller is then responsible for having
        confirmed that the staged absolute values are correct under the current
        code, since replay writes them verbatim without recomputing.
        """
        header = self._read_header()
        if header is None:
            raise ValueError("No committed WAL record to load.")
        if header["code_version"] != self._code_version:
            if not allow_version_mismatch:
                raise WalVersionMismatch(
                    f"Pending WAL was written by version "
                    f"{header['code_version']!r} but this binary is "
                    f"{self._code_version!r}. Refusing to replay stale writes; "
                    "manual recovery required."
                )
            logger.warning(
                "Forcing WAL replay across a code-version change: record "
                "written by %r, this binary is %r. The staged absolute values "
                "will be replayed verbatim.",
                header["code_version"],
                self._code_version,
            )

        n_data = header["n_data_chunks"]
        n_book = header["n_book_chunks"]
        total = n_data + n_book
        rows = list(
            self._exec(
                f"SELECT chunk_idx, payload FROM {self._table()} "
                f"WHERE keyspace_marker=? AND chunk_idx>=? AND chunk_idx<?",
                [self._keyspace, 0, total],
            )
        )
        by_idx = {r.chunk_idx: r.payload for r in rows}
        if len(by_idx) != total:
            raise ValueError(
                f"WAL payload is incomplete: expected {total} chunks, "
                f"found {len(by_idx)}."
            )
        blob = b"".join(by_idx[i] for i in range(total))
        if hashlib.sha256(blob).hexdigest() != header["checksum"]:
            raise ValueError("WAL payload checksum mismatch.")

        data_blob = b"".join(by_idx[i] for i in range(0, n_data))
        book_blob = b"".join(by_idx[i] for i in range(n_data, total))
        return WalRecord(
            run_id=header["run_id"],
            code_version=header["code_version"],
            block_lo=header["block_lo"],
            block_hi=header["block_hi"],
            changes=decode_changes(data_blob) if data_blob else [],
            bookkeeping=decode_changes(book_blob) if book_blob else [],
        )

    def clear(self) -> None:
        """Discard the record (the commit point). Fenced by run_id so a
        returning zombie cannot delete a newer run's WAL."""
        header = self._read_header()
        if header is not None and header["run_id"] != self._run_id:
            logger.warning(
                "WAL is owned by run %s, not this run %s; not clearing.",
                header["run_id"],
                self._run_id,
            )
            return
        self._delete_partition()

    def recover(
        self,
        apply_fn: Callable[[List[DbChange]], None],
        allow_version_mismatch: bool = False,
    ) -> bool:
        """Replay a pending record to completion, then clear it.

        ``apply_fn`` applies a list of resolved changes idempotently. Returns
        True if a record was replayed, False if there was nothing pending.
        ``allow_version_mismatch`` is forwarded to :meth:`load` to override the
        code-version fence (see its docstring for the caveat).
        """
        if not self.has_pending():
            # Sweep any torn/headerless chunks from an interrupted stage().
            self._delete_partition()
            return False
        record = self.load(allow_version_mismatch=allow_version_mismatch)
        logger.warning(
            "Replaying pending WAL for blocks %s-%s (run %s) before resuming.",
            record.block_lo,
            record.block_hi,
            record.run_id,
        )
        # Data first, bookkeeping last (mirrors normal persist ordering).
        apply_fn(record.changes)
        apply_fn(record.bookkeeping)
        # Take ownership before clearing so the run_id fence does not skip a
        # record we just replayed on behalf of a dead run.
        self._run_id = record.run_id
        self.clear()
        logger.info("WAL replay complete; keyspace is consistent.")
        return True


def _split(blob: bytes) -> List[bytes]:
    if not blob:
        return []
    return [blob[i : i + _CHUNK_SIZE] for i in range(0, len(blob), _CHUNK_SIZE)]
