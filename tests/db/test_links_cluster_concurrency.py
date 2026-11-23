"""Bounded Cassandra fan-out for cluster /links and per process.

Prod 2026-11-23: a dozen parallel /btc/clusters/{id}/links requests between
large clusters each resolved every io address of a candidate batch (up to
links_adaptive_fetch_size_cap txs, hundreds of ios each) with one unbounded
gather. The driver's connections were reported overloaded, heartbeats failed,
every host was marked down and the whole REST process answered 500.

DB-free: the real methods are bound to a fake self.
"""

import asyncio
from types import SimpleNamespace

import pytest

from graphsenselib.config.cassandra_async_config import CassandraConfig
from graphsenselib.db.asynchronous.cassandra import (
    MAX_ROW_LOOKUP_CONCURRENCY,
    Cassandra,
)

SRC = 1  # cluster of the queried id
DST = 2  # cluster of the neighbor
OTHER = 3


def _io(address):
    return SimpleNamespace(address=[address] if address is not None else None)


def _row(tx_id, inputs, outputs):
    return {
        "tx_id": tx_id,
        "inputs": [_io(a) for a in inputs],
        "outputs": [_io(a) for a in outputs],
    }


class _Tracker:
    def __init__(self, clusters):
        self.clusters = clusters
        self.calls = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def get_address_entity_id(self, currency, address):
        self.calls.append(address)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.in_flight -= 1
        return self.clusters[address]


def _filter(tracker, rows, cache):
    fake = SimpleNamespace(get_address_entity_id=tracker.get_address_entity_id)
    return asyncio.run(
        Cassandra._filter_links_rows_by_cluster(fake, "btc", rows, SRC, DST, cache)
    )


def test_keeps_only_txs_from_src_inputs_to_dst_outputs():
    clusters = {"s1": SRC, "s2": SRC, "d1": DST, "o1": OTHER}
    rows = [
        _row(1, ["s1"], ["d1"]),  # link
        _row(2, ["o1", "s2"], ["o1", "d1"]),  # link (mixed ios)
        _row(3, ["d1"], ["s1"]),  # wrong direction
        _row(4, ["s1"], ["o1"]),  # dst not among outputs
        _row(5, ["o1"], ["d1"]),  # src not among inputs
        _row(6, [None, "s1"], ["d1", None]),  # ios without address are skipped
    ]
    kept = _filter(_Tracker(clusters), rows, {})
    assert [r["tx_id"] for r in kept] == [1, 2, 6]


def test_coinbase_inputs_none_do_not_crash():
    rows = [{"tx_id": 1, "inputs": None, "outputs": [_io("d1")]}]
    assert _filter(_Tracker({"d1": DST}), rows, {}) == []


def test_each_address_resolved_once_per_request_and_bounded():
    # 3000 txs sharing a few busy addresses plus many distinct ones: the old
    # code issued one lookup per io (12,000) all at once.
    n_distinct = 3 * MAX_ROW_LOOKUP_CONCURRENCY
    clusters = {"s": SRC, "d": DST}
    clusters.update({f"x{i}": OTHER for i in range(n_distinct)})
    rows = [
        _row(i, ["s", f"x{i % n_distinct}"], ["d", f"x{(i + 1) % n_distinct}"])
        for i in range(3000)
    ]
    tracker = _Tracker(clusters)
    cache = {}
    kept = _filter(tracker, rows, cache)

    assert len(kept) == 3000
    assert sorted(tracker.calls) == sorted(clusters)  # once per address
    assert tracker.max_in_flight <= MAX_ROW_LOOKUP_CONCURRENCY

    # the next candidate batch of the same request reuses the cache
    tracker.calls.clear()
    _filter(tracker, rows[:10], cache)
    assert tracker.calls == []


def test_next_batch_only_resolves_new_addresses():
    tracker = _Tracker({"s": SRC, "d": DST, "n": DST})
    cache = {}
    _filter(tracker, [_row(1, ["s"], ["d"])], cache)
    tracker.calls.clear()
    kept = _filter(tracker, [_row(2, ["s"], ["d", "n"])], cache)
    assert tracker.calls == ["n"]
    assert [r["tx_id"] for r in kept] == [2]


def _fake_db(limit, delay=0.01):
    state = SimpleNamespace(in_flight=0, max_in_flight=0)

    async def lowlevel(*args, **kwargs):
        state.in_flight += 1
        state.max_in_flight = max(state.max_in_flight, state.in_flight)
        await asyncio.sleep(delay)
        state.in_flight -= 1
        return SimpleNamespace(paging_state=None, current_rows=[])

    fake = SimpleNamespace(
        tconfig=SimpleNamespace(max_concurrent_statements=limit),
        execute_async_lowlevel=lowlevel,
    )
    fake._statement_slot = lambda: Cassandra._statement_slot(fake)
    return fake, state


@pytest.mark.parametrize("limit, expected_max", [(7, 7), (0, 50)])
def test_statements_in_flight_are_bounded_per_process(limit, expected_max):
    fake, state = _fake_db(limit)

    async def run():
        await asyncio.gather(
            *[
                Cassandra.execute_async(fake, "btc", "raw", "SELECT 1")
                for _ in range(50)
            ]
        )

    asyncio.run(run())
    assert state.max_in_flight == expected_max


def test_cancelled_waiter_frees_nothing_it_did_not_hold():
    fake, state = _fake_db(2, delay=0.05)

    async def run():
        tasks = [
            asyncio.ensure_future(Cassandra.execute_async(fake, "btc", "raw", "Q"))
            for _ in range(6)
        ]
        await asyncio.sleep(0.01)
        for t in tasks[2:4]:  # cancel two that are still waiting for a slot
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # the semaphore is back at its limit: two can run again at once
        assert fake._statement_sem._value == 2

    asyncio.run(run())
    assert state.max_in_flight == 2


def test_default_limit_is_on():
    assert CassandraConfig.model_fields["max_concurrent_statements"].default > 0
