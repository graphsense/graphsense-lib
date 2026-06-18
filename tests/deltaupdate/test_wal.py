"""Unit tests for the delta-update redo write-ahead log.

These exercise the WAL's storage protocol (chunking, header-last commit,
checksum, run_id fence, recovery) against an in-memory fake of the small
Cassandra surface DeltaWal uses, so they need no live database.
"""

import datetime
from types import SimpleNamespace

import numpy as np
import pytest

from graphsenselib.datatypes import DbChangeType
from graphsenselib.db.analytics import DbChange
from graphsenselib.db.parallel import PlainRow
from graphsenselib.deltaupdate.update.account.createdeltas import TxReference
from graphsenselib.deltaupdate.update.generic import DeltaValue
from graphsenselib.deltaupdate.wal import (
    DeltaWal,
    WalRecord,
    WalVersionMismatch,
    decode_changes,
    encode_changes,
)


# --------------------------------------------------------------------------
# In-memory fake of the Cassandra surface DeltaWal touches.
# --------------------------------------------------------------------------


class _Bound:
    def __init__(self, cql, params):
        self.cql = cql
        self.params = params


class _Prepared:
    def __init__(self, cql):
        self.cql = cql

    def bind(self, params):
        return _Bound(self.cql, params)


class _FakeCassandra:
    """Stores WAL rows in a dict keyed by (keyspace_marker, chunk_idx)."""

    def __init__(self):
        # (ks, idx) -> dict of columns
        self.rows = {}

    def get_prepared_statement(self, cql):
        return _Prepared(cql)

    def execute_statement(self, bound):
        cql, p = bound.cql, bound.params
        if cql.startswith("DELETE"):
            ks = p[0]
            for key in [k for k in self.rows if k[0] == ks]:
                del self.rows[key]
            return []
        if cql.startswith("INSERT") and "checksum)" in cql:
            ks, idx = p[0], p[1]
            self.rows[(ks, idx)] = {
                "run_id": p[2],
                "code_version": p[3],
                "block_lo": p[4],
                "block_hi": p[5],
                "n_data_chunks": p[6],
                "n_book_chunks": p[7],
                "checksum": p[8],
            }
            return []
        if cql.startswith("INSERT"):  # data chunk
            ks, idx, payload = p[0], p[1], p[2]
            self.rows[(ks, idx)] = {"payload": payload}
            return []
        if cql.startswith("SELECT run_id"):  # header read
            ks, idx = p[0], p[1]
            row = self.rows.get((ks, idx))
            return [SimpleNamespace(**row)] if row else []
        if cql.startswith("SELECT chunk_idx"):  # chunk range read
            ks, lo, hi = p[0], p[1], p[2]
            return [
                SimpleNamespace(chunk_idx=k[1], payload=v["payload"])
                for k, v in sorted(self.rows.items())
                if k[0] == ks and lo <= k[1] < hi
            ]
        raise AssertionError(f"unexpected statement: {cql}")


class _FakeTransformedDb:
    def __init__(self, keyspace="ks_transformed"):
        self._db = _FakeCassandra()
        self._keyspace = keyspace
        self.ensured = []

    def get_keyspace(self):
        return self._keyspace

    def ensure_table_exists(self, name, columns, pk, truncate=False):
        self.ensured.append(name)


def _make_wal(run_id="host:1:aaaa", version="9.9.9"):
    tdb = _FakeTransformedDb()
    wal = DeltaWal(tdb, run_id, version)
    wal.ensure_schema()
    return wal


def _record(run_id="host:1:aaaa", version="9.9.9", big=False):
    n = 200000 if big else 2  # big -> spans multiple 512KB chunks
    changes = [
        DbChange.new(
            table="balance",
            data={"address_id": bytes([i % 256]), "balance": 10 + i},
        )
        for i in range(n)
    ]
    bookkeeping = [
        DbChange.new(
            table="summary_statistics",
            data={"no_blocks": 5, "timestamp": datetime.datetime(2026, 6, 15, 1, 2, 3)},
        )
    ]
    return WalRecord(
        run_id=run_id,
        code_version=version,
        block_lo=100,
        block_hi=109,
        changes=changes,
        bookkeeping=bookkeeping,
    )


# --------------------------------------------------------------------------
# Codec
# --------------------------------------------------------------------------


def test_codec_roundtrip_types():
    changes = [
        DbChange.new(
            table="balance",
            data={
                "address_id": b"\x00\x01\xff",
                "balance": 12345678901234567890,
                "currency": "ETH",
                "missing": None,
                "lst": [1, 2, 3],
                "mp": {"a": 1},
            },
        ),
        DbChange.update(
            table="summary_statistics",
            data={"ts": datetime.datetime(2026, 6, 15, 12, 30, 45, 123456)},
        ),
        DbChange.delete(table="x", data={"k": 1}),
    ]
    back = decode_changes(encode_changes(changes))
    assert back == changes
    assert back[0].data["address_id"] == b"\x00\x01\xff"
    assert back[1].data["ts"] == datetime.datetime(2026, 6, 15, 12, 30, 45, 123456)
    assert back[0].action == DbChangeType.NEW
    assert back[2].action == DbChangeType.DELETE


def test_codec_roundtrip_domain_objects():
    # The real change set carries DeltaValue dataclasses, dict[str, DeltaValue],
    # and a TxReference UDT, which must round-trip with exact type fidelity so
    # the driver rebinds them to their UDT/map columns identically.
    rel = DbChange.update(
        table="address_incoming_relations",
        data={
            "no_transactions": 3,
            "value": DeltaValue(value=100, fiat_values=[1, 2]),
            "token_values": {
                "USDT": DeltaValue(value=5, fiat_values=[3, 4]),
                "WETH": DeltaValue(value=7, fiat_values=[5, 6]),
            },
        },
    )
    tx = DbChange.new(
        table="address_transactions",
        data={
            "address_id": b"\x01",
            "tx_reference": TxReference(trace_index=4, log_index=None),
        },
    )
    back = decode_changes(encode_changes([rel, tx]))
    assert back == [rel, tx]
    assert isinstance(back[0].data["value"], DeltaValue)
    assert back[0].data["value"] == DeltaValue(value=100, fiat_values=[1, 2])
    assert isinstance(back[0].data["token_values"]["USDT"], DeltaValue)
    assert isinstance(back[1].data["tx_reference"], TxReference)
    assert back[1].data["tx_reference"].trace_index == 4


def test_codec_roundtrip_plainrow():
    # The account-model delta updater reads current DB values through the
    # process-parallel reader, which returns UDT values as PlainRow (see
    # db/parallel.py). These enter DbChange.data and must round-trip with type
    # fidelity so the driver rebinds them to their UDT columns identically.
    change = DbChange.update(
        table="address",
        data={
            "balance": PlainRow(
                {"value": 16127357, "fiat_values": [13.712573, 16.127357]}
            ),
            # nested PlainRow inside a map exercises recursion through _default.
            "token_balances": {
                "USDT": PlainRow({"value": 5, "fiat_values": [3, 4]}),
            },
        },
    )
    back = decode_changes(encode_changes([change]))
    assert back == [change]
    assert isinstance(back[0].data["balance"], PlainRow)
    assert back[0].data["balance"].value == 16127357
    assert back[0].data["balance"].fiat_values == [13.712573, 16.127357]
    assert isinstance(back[0].data["token_balances"]["USDT"], PlainRow)
    assert back[0].data["token_balances"]["USDT"].value == 5


def test_codec_handles_numpy_scalars():
    changes = [DbChange.new(table="t", data={"a": np.int64(7), "b": np.float64(1.5)})]
    back = decode_changes(encode_changes(changes))
    # numpy scalars collapse to their Python scalar (the driver accepts those
    # on the normal write path too).
    assert back[0].data == {"a": 7, "b": 1.5}


def test_codec_roundtrip_big_ints():
    # Cassandra varint columns hold arbitrary precision (e.g. high-decimal token
    # balances); such ints exceed msgpack's native 64-bit range and must still
    # round-trip exactly — top-level and nested inside DeltaValue / PlainRow.
    big_pos = 2**63 + 1  # just past signed-64 max
    big_neg = -3533128827657550912722770  # the value from the field report
    huge = 10**40
    changes = [
        DbChange.new(
            table="address",
            data={
                "balance": big_neg,
                "delta": DeltaValue(value=big_pos, fiat_values=[1, 2]),
                "row": PlainRow({"value": huge, "fiat_values": [3, 4]}),
            },
        )
    ]
    back = decode_changes(encode_changes(changes))
    assert back == changes
    assert back[0].data["balance"] == big_neg
    assert isinstance(back[0].data["balance"], int)
    assert back[0].data["delta"].value == big_pos
    assert back[0].data["row"].value == huge


def test_codec_rejects_unknown_type():
    class Weird:
        pass

    with pytest.raises(TypeError, match="ext handler"):
        encode_changes([DbChange.new(table="t", data={"x": Weird()})])


def test_truncate_rejected():
    with pytest.raises(ValueError, match="TRUNCATE"):
        encode_changes([DbChange.truncate(table="t")])


# --------------------------------------------------------------------------
# DeltaWal lifecycle
# --------------------------------------------------------------------------


def test_no_pending_initially():
    wal = _make_wal()
    assert wal.has_pending() is False
    assert "delta_updater_wal" in wal._tdb.ensured


@pytest.mark.parametrize("big", [False, True])
def test_stage_load_roundtrip(big):
    wal = _make_wal()
    rec = _record(big=big)
    wal.stage(rec)
    assert wal.has_pending() is True
    loaded = wal.load()
    assert loaded.changes == rec.changes
    assert loaded.bookkeeping == rec.bookkeeping
    assert (loaded.block_lo, loaded.block_hi) == (100, 109)


def test_clear_removes_record():
    wal = _make_wal()
    wal.stage(_record())
    wal.clear()
    assert wal.has_pending() is False


def test_recover_replays_then_clears():
    wal = _make_wal()
    rec = _record()
    wal.stage(rec)

    applied = []
    assert wal.recover(lambda ch: applied.append(list(ch))) is True
    # data first, bookkeeping second
    assert applied[0] == rec.changes
    assert applied[1] == rec.bookkeeping
    assert wal.has_pending() is False


def test_recover_noop_when_empty():
    wal = _make_wal()
    applied = []
    assert wal.recover(lambda ch: applied.append(ch)) is False
    assert applied == []


def test_crash_mid_apply_then_recover_completes():
    # Stage succeeds, the first apply attempt dies -> WAL stays pending.
    wal = _make_wal()
    rec = _record()
    wal.stage(rec)

    with pytest.raises(RuntimeError):
        wal.recover(lambda ch: (_ for _ in ()).throw(RuntimeError("boom")))
    assert wal.has_pending() is True  # not cleared

    # A fresh run (different run_id) replays and clears.
    wal2 = DeltaWal(wal._tdb, "host:2:bbbb", "9.9.9")
    applied = []
    assert wal2.recover(lambda ch: applied.append(list(ch))) is True
    assert applied == [rec.changes, rec.bookkeeping]
    assert wal2.has_pending() is False


def test_torn_staging_is_swept_and_not_pending():
    # Data chunks present but no header row (stage() interrupted before commit).
    wal = _make_wal()
    tdb = wal._tdb
    ks = tdb.get_keyspace()
    tdb._db.rows[(ks, 0)] = {"payload": b"partial"}
    assert wal.has_pending() is False  # headerless -> not committed

    applied = []
    assert wal.recover(lambda ch: applied.append(ch)) is False
    assert applied == []
    # swept
    assert all(k[0] != ks for k in tdb._db.rows)


def test_version_mismatch_refuses_replay():
    wal = _make_wal(version="1.0.0")
    wal.stage(_record(version="1.0.0"))

    newer = DeltaWal(wal._tdb, "host:3:cccc", "2.0.0")
    assert newer.has_pending() is True
    with pytest.raises(WalVersionMismatch):
        newer.load()
    with pytest.raises(WalVersionMismatch):
        newer.recover(lambda ch: None)


def test_force_replay_overrides_version_mismatch():
    wal = _make_wal(version="1.0.0")
    wal.stage(_record(version="1.0.0"))

    newer = DeltaWal(wal._tdb, "host:3:cccc", "2.0.0")
    # The override bypasses the version fence and replays the staged changes.
    loaded = newer.load(allow_version_mismatch=True)
    assert loaded.block_lo == 100 and loaded.block_hi == 109

    applied = []
    assert newer.recover(lambda ch: applied.extend(ch), allow_version_mismatch=True)
    assert applied  # staged changes were replayed
    assert wal.has_pending() is False  # cleared after replay


def test_force_replay_still_rejects_corrupt_payload():
    # The override skips only the version check; checksum verification stays on.
    wal = _make_wal(version="1.0.0")
    wal.stage(_record(version="1.0.0"))
    ks = wal._tdb.get_keyspace()
    wal._tdb._db.rows[(ks, 0)]["payload"] = b"corrupted"

    newer = DeltaWal(wal._tdb, "host:3:cccc", "2.0.0")
    with pytest.raises(ValueError):
        newer.load(allow_version_mismatch=True)


def test_clear_fenced_by_run_id():
    wal = _make_wal(run_id="owner:1:aaaa")
    wal.stage(_record(run_id="owner:1:aaaa"))

    # A different run must not delete the owner's WAL.
    other = DeltaWal(wal._tdb, "intruder:9:zzzz", "9.9.9")
    other.clear()
    assert wal.has_pending() is True

    # The owner can.
    wal.clear()
    assert wal.has_pending() is False


def test_stage_overwrites_previous_partition():
    wal = _make_wal()
    wal.stage(_record(big=True))
    first_count = len([k for k in wal._tdb._db.rows])
    assert first_count > 2  # multiple chunks + header
    # A smaller record must not leave stale chunks behind.
    wal.stage(_record(big=False))
    loaded = wal.load()
    assert len(loaded.changes) == 2
