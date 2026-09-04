"""Carrying the last rate forward over an already-written keyspace.

This module WRITES, which every other part of the v3 read path does not, so the
first thing tested is that it cannot write anywhere it should not.
"""

from types import SimpleNamespace

import pytest

from graphsense_v3 import ratefill

RAW = "ltc_raw_v3_full1"
DERIVED = "ltc_derived_v3_full1"
SIZE = 100
DAY = 86400


class Session:
    """Answers the three reads and records the writes."""

    def __init__(self, *, tip, rated, stamps, fiat=None):
        self.tip = tip
        self.rated = rated
        self.stamps = stamps
        self.fiat = fiat if fiat is not None else {"EUR": 39.04, "USD": 45.95}
        self.written = []

    def execute(self, cql, params=()):
        if "summary_statistics" in cql:
            return [SimpleNamespace(highest_block=self.tip)]
        if cql.startswith("INSERT"):
            self.written.append(params)
            return []
        if "exchange_rates" in cql:
            _asset, group, upper = tuple(params)
            if self.rated is not None and self.rated // SIZE == group:
                if self.rated <= upper:
                    return [SimpleNamespace(block_id=self.rated, fiat_values=self.fiat)]
            return []
        # raw.block
        _group, low, high = tuple(params)
        return [
            SimpleNamespace(block_id=b, timestamp=t)
            for b, t in self.stamps.items()
            if low <= b <= high
        ]


def stamps_for(first, last, start=1_000_000, step=150):
    return {b: start + (b - first) * step for b in range(first, last + 1)}


def test_it_refuses_to_write_to_a_v2_keyspace() -> None:
    """The only module here that writes. A v2 keyspace name must not be
    reachable, whatever the caller passes."""
    session = Session(tip=10, rated=5, stamps=stamps_for(5, 10))
    with pytest.raises(Exception):
        ratefill.fill(session, RAW, "ltc_transformed_20260727", "LTC", size=SIZE)
    assert session.written == []


def test_the_unrated_tail_is_filled_with_the_last_real_rate() -> None:
    session = Session(tip=8, rated=5, stamps=stamps_for(5, 8))
    summary = ratefill.fill(session, RAW, DERIVED, "LTC", size=SIZE)
    assert summary["written"] == 3
    assert [p[2] for p in session.written] == [6, 7, 8]
    # Every row carries the rate that was really observed at block 5.
    assert all(p[3] == {"EUR": 39.04, "USD": 45.95} for p in session.written)


def test_the_partition_key_is_computed_the_same_way_the_reader_does() -> None:
    """A row written to the wrong partition is invisible rather than wrong,
    which is the failure mode that looks like "the fill did nothing"."""
    session = Session(tip=201, rated=200, stamps=stamps_for(200, 201))
    ratefill.fill(session, RAW, DERIVED, "LTC", size=SIZE)
    (_asset, group, block_id, _fiat) = session.written[0]
    assert (group, block_id) == (2, 201)


def test_a_rated_tip_writes_nothing() -> None:
    session = Session(tip=5, rated=5, stamps=stamps_for(5, 5))
    assert ratefill.fill(session, RAW, DERIVED, "LTC", size=SIZE)["written"] == 0
    assert session.written == []


def test_blocks_beyond_the_window_are_left_alone() -> None:
    """Past the cap the rate feed is broken rather than lagging, and a stale
    rate carried indefinitely would hide that behind plausible numbers."""
    stamps = {5: 1_000_000, 6: 1_000_000 + DAY, 7: 1_000_000 + 10 * DAY}
    session = Session(tip=7, rated=5, stamps=stamps)
    summary = ratefill.fill(
        session, RAW, DERIVED, "LTC", size=SIZE, within_seconds=2 * DAY
    )
    assert summary["written"] == 1
    assert summary["skipped"] == 1
    assert [p[2] for p in session.written] == [6]


def test_a_block_the_keyspace_does_not_hold_is_skipped() -> None:
    """Filling a rate for a missing block would invent coverage for a block
    that is not there."""
    session = Session(tip=8, rated=5, stamps={5: 1_000_000, 6: 1_000_150})
    summary = ratefill.fill(session, RAW, DERIVED, "LTC", size=SIZE)
    assert summary["written"] == 1
    assert summary["skipped"] == 2


def test_a_dry_run_writes_nothing_but_reports_the_same_count() -> None:
    session = Session(tip=8, rated=5, stamps=stamps_for(5, 8))
    summary = ratefill.fill(session, RAW, DERIVED, "LTC", size=SIZE, dry_run=True)
    assert summary["written"] == 3
    assert session.written == []


def test_nothing_to_carry_forward_is_an_error_not_a_silent_no_op() -> None:
    """No rate anywhere means the keyspace is unservable for a reason this tool
    cannot fix, and saying "wrote 0 rows" would read as success."""
    session = Session(tip=8, rated=None, stamps=stamps_for(0, 8))
    with pytest.raises(LookupError, match="nothing to carry forward"):
        ratefill.fill(session, RAW, DERIVED, "LTC", size=SIZE)


class ZeroSession(Session):
    """Adds partition listing and a prepared-statement stub for the zero fill."""

    def __init__(self, *, tip, rated, present, fiat=None):
        super().__init__(tip=tip, rated=rated, stamps={}, fiat=fiat)
        self.present = present
        self.prepared = []

    def prepare(self, cql):
        return cql

    def execute(self, cql, params=()):
        if "summary_statistics" in cql:
            return [SimpleNamespace(highest_block=self.tip)]
        if "exchange_rates" in cql and "block_id <=" not in cql:
            _asset, group = tuple(params)
            return [
                SimpleNamespace(block_id=b) for b in self.present if b // SIZE == group
            ]
        return super().execute(cql, params)


def _concurrent(monkeypatch, sink):
    import cassandra.concurrent as concurrent

    monkeypatch.setattr(
        concurrent,
        "execute_concurrent_with_args",
        lambda s, st, args, **k: sink.extend(args),
    )


def test_a_block_with_no_rate_gets_zeros(monkeypatch) -> None:
    """v2 carries [0, 0] for the early chain, because its feed starts years
    after genesis. An absent row here is a failed request, not a missing
    number."""
    written = []
    _concurrent(monkeypatch, written)
    session = ZeroSession(tip=3, rated=3, present={3})
    summary = ratefill.zero_fill(session, RAW, DERIVED, "LTC", size=SIZE)
    assert summary["written"] == 3
    assert [args[2] for args in written] == [0, 1, 2]
    assert all(args[3] == {"EUR": 0.0, "USD": 0.0} for args in written)


def test_a_block_that_already_has_a_rate_is_never_overwritten(monkeypatch) -> None:
    """The dangerous failure: blanket-inserting over the range would replace
    real rates with zeros, which is far worse than the gap it repairs."""
    written = []
    _concurrent(monkeypatch, written)
    session = ZeroSession(tip=3, rated=3, present={1, 3})
    ratefill.zero_fill(session, RAW, DERIVED, "LTC", size=SIZE)
    assert [args[2] for args in written] == [0, 2]


def test_the_zero_uses_the_currencies_the_feed_actually_carries(monkeypatch) -> None:
    written = []
    _concurrent(monkeypatch, written)
    session = ZeroSession(tip=1, rated=1, present={1}, fiat={"GBP": 3.0})
    ratefill.zero_fill(session, RAW, DERIVED, "LTC", size=SIZE)
    assert written[0][3] == {"GBP": 0.0}


def test_zero_fill_refuses_a_v2_keyspace(monkeypatch) -> None:
    written = []
    _concurrent(monkeypatch, written)
    session = ZeroSession(tip=3, rated=3, present={3})
    with pytest.raises(Exception):
        ratefill.zero_fill(session, RAW, "ltc_transformed_20260727", "LTC", size=SIZE)
    assert written == []


def test_a_zero_fill_dry_run_writes_nothing(monkeypatch) -> None:
    written = []
    _concurrent(monkeypatch, written)
    session = ZeroSession(tip=3, rated=3, present={3})
    summary = ratefill.zero_fill(session, RAW, DERIVED, "LTC", size=SIZE, dry_run=True)
    assert summary["written"] == 3
    assert written == []
