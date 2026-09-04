"""The v3 DAL.

Driven through a fake session: what is worth pinning here is the SHAPE of each
query -- which partition it addresses, how many reads it costs, how the epoch
rows are folded -- and every one of those is decided before the driver is
reached. The probe covers the other half, that the shapes work against real
data.
"""

import asyncio

import pytest

from graphsense_v3.codec import bucket
from graphsense_v3.db.core import Dal

RAW = "ltc_raw_v3_test"
DERIVED = "ltc_derived_v3_test"

CONFIG = {
    "entity_buckets": 100_000,
    "tx_page_size": 100_000,
    "relation_buckets": 16,
    "block_bucket_size": 100,
    "tx_block_bucket_size": 16,
    "address_prefix_length": 4,
    "tx_prefix_length": 5,
}

ADDRESS = b"\xa1" * 21
OTHER = b"\xb0" * 21


class Row:
    """A driver row: attribute access plus `_asdict`, like a namedtuple."""

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def _asdict(self):
        return dict(self.__dict__)


class FakeFuture:
    def __init__(self, rows):
        self.rows = rows

    def add_callbacks(self, on_success, on_error):
        on_success(self.rows)


class FakeSession:
    """Records every statement and replays canned rows.

    ``rows`` is either a fixed list or a function of (cql, params), which is
    what lets one test answer address_stats and address_transactions
    differently in a single call.
    """

    def __init__(self, rows=None):
        self.seen = []
        self._rows = rows if callable(rows) else (lambda cql, params: rows or [])

    def execute_async(self, cql, params=()):
        self.seen.append((" ".join(cql.split()), params))
        return FakeFuture(self._rows(cql, params))


def make(rows=None) -> tuple:
    session = FakeSession(rows)
    return Dal(session, RAW, DERIVED, dict(CONFIG)), session


def run(coro):
    return asyncio.run(coro)


def test_the_address_bucket_is_crc32_of_the_address() -> None:
    """The single thing that must match the writer exactly. A murmur3 bucket
    addresses a partition that exists and is empty, which reads as "no such
    address" rather than as an error."""
    dal, session = make()
    run(dal.stats(ADDRESS))
    _, params = session.seen[0]
    assert params[0] == bucket(ADDRESS, CONFIG["entity_buckets"])


def test_stats_sums_epochs_but_not_the_epoch_zero_only_columns() -> None:
    """Epoch 0 is the compacted base and later epochs are deltas, so counts
    sum. Degrees are DISTINCT counts and the paging cursors are positions --
    summing either gives a plausible wrong number."""
    rows = [
        Row(
            epoch=0,
            no_incoming_txs=10,
            no_outgoing_txs=2,
            no_incoming_txs_zero_value=0,
            no_outgoing_txs_zero_value=0,
            first_tx_id=100,
            last_tx_id=500,
            in_degree=7,
            out_degree=1,
            in_tx_page_max=3,
            out_tx_page_max=0,
        ),
        Row(
            epoch=5,
            no_incoming_txs=4,
            no_outgoing_txs=1,
            no_incoming_txs_zero_value=0,
            no_outgoing_txs_zero_value=0,
            first_tx_id=900,
            last_tx_id=999,
            in_degree=None,
            out_degree=None,
        ),
    ]
    dal, _ = make(rows)
    stats = run(dal.stats(ADDRESS))
    assert stats.summed["no_incoming_txs"] == 14
    assert stats.no_transactions == 17
    # not 7 + None, and not doubled
    assert stats.epoch_zero["in_degree"] == 7
    assert stats.epoch_zero["in_tx_page_max"] == 3
    # min-merge and max-merge across the slice
    assert stats.first_tx_id == 100
    assert stats.last_tx_id == 999


def test_balance_sums_per_currency() -> None:
    rows = [
        Row(currency="LTC", balance=100),
        Row(currency="LTC", balance=-30),
        Row(currency="USDT", balance=5),
    ]
    dal, _ = make(rows)
    assert run(dal.balance(ADDRESS)) == {"LTC": 70, "USDT": 5}


def test_unbound_direction_costs_two_partition_reads() -> None:
    """is_outgoing is in the PARTITION key, so it cannot be left unrestricted:
    "all of an address's transactions" is two reads merged client-side."""
    dal, session = make()
    run(dal.transactions(ADDRESS, page=0))
    assert len(session.seen) == 2
    assert {params[1] for _, params in session.seen} == {False, True}


def test_including_zero_value_doubles_the_reads_again() -> None:
    """A fourth partition, which is why zero-value is excluded by default."""
    dal, session = make()
    run(dal.transactions(ADDRESS, page=0, include_zero_value=True))
    assert len(session.seen) == 4
    assert {params[2] for _, params in session.seen} == {False, True}


def test_one_direction_is_one_read() -> None:
    dal, session = make()
    run(dal.transactions(ADDRESS, is_outgoing=True, page=0))
    assert len(session.seen) == 1
    assert session.seen[0][1][1] is True


def test_transactions_come_back_newest_first_across_partitions() -> None:
    """Each partition is tx_id DESC on its own; merging two of them is the
    DAL's job, not Cassandra's."""
    dal, _ = make(
        lambda cql, params: [
            Row(tx_id=10, value=1, balance=None),
            Row(tx_id=30, value=3, balance=None),
        ]
    )
    txs = run(dal.transactions(ADDRESS, page=0))
    assert [tx.tx_id for tx in txs] == [30, 30, 10, 10]


def test_the_default_page_is_the_highest_not_zero() -> None:
    """Pages are numbered by ASCENDING ordinal, so page 0 holds the OLDEST
    transactions. A newest-first listing has to start at *_tx_page_max."""

    def rows(cql, params):
        if "address_stats" in cql:
            return [
                Row(
                    epoch=0,
                    no_incoming_txs=1,
                    no_outgoing_txs=0,
                    no_incoming_txs_zero_value=0,
                    no_outgoing_txs_zero_value=0,
                    first_tx_id=1,
                    last_tx_id=2,
                    in_tx_page_max=4,
                    out_tx_page_max=2,
                )
            ]
        return []

    dal, session = make(rows)
    run(dal.transactions(ADDRESS))
    pages = {params[3] for cql, params in session.seen if "address_transactions" in cql}
    assert pages == {4}


def test_neighbors_scatter_over_every_relation_bucket() -> None:
    """The bucket is derived from the FAR side, which is what we are looking
    for, so there is nothing to compute and no watermark table to stop early."""
    dal, session = make()
    run(dal.neighbors(ADDRESS, is_outgoing=True))
    assert len(session.seen) == CONFIG["relation_buckets"]
    assert {params[1] for _, params in session.seen} == set(range(16))


def test_neighbors_sum_a_counterpartys_epochs() -> None:
    dal, _ = make(
        lambda cql, params: (
            [
                Row(
                    src_address=ADDRESS,
                    dst_address=OTHER,
                    no_transactions=2,
                    value=None,
                ),
                Row(
                    src_address=ADDRESS,
                    dst_address=OTHER,
                    no_transactions=3,
                    value=None,
                ),
            ]
            if params[1] == 0
            else []
        )
    )
    neighbors = run(dal.neighbors(ADDRESS, is_outgoing=True))
    assert len(neighbors) == 1
    assert neighbors[0].no_transactions == 5


def test_a_specific_neighbor_is_a_point_read() -> None:
    """One partition, because the bucket comes from the counterparty -- this is
    what keeps "is X a neighbour of Y" off the 16-partition scatter."""
    dal, session = make(
        [Row(src_address=ADDRESS, dst_address=OTHER, no_transactions=1)]
    )
    run(dal.neighbor(ADDRESS, OTHER, is_outgoing=True))
    assert len(session.seen) == 1
    _, params = session.seen[0]
    assert params[1] == bucket(OTHER, CONFIG["relation_buckets"])


def test_link_transactions_bucket_on_the_destination() -> None:
    dal, session = make([])
    run(dal.link_transactions(ADDRESS, OTHER))
    _, params = session.seen[0]
    assert params == (ADDRESS, bucket(OTHER, CONFIG["relation_buckets"]), OTHER)


def test_the_transaction_partition_is_arithmetic_from_the_id() -> None:
    """(tx_id >> 32) // tx_block_bucket_size -- no index, and no read to find
    out which partition a transaction is in."""
    dal, session = make([])
    tx = (98514 << 32) + 0
    run(dal.transaction(tx))
    _, params = session.seen[0]
    assert params == (98514 // CONFIG["tx_block_bucket_size"], tx)


def test_block_transactions_are_a_tx_id_range() -> None:
    """The reason the block_transactions table is gone: the range falls out of
    the height, so a block's transactions are a clustering slice."""
    dal, session = make([])
    run(dal.block_transactions(98514))
    cql, params = session.seen[0]
    assert "tx_id >= %s AND tx_id <= %s" in cql
    assert params[1] == 98514 << 32
    assert params[2] == ((98515 << 32) - 1)


def test_block_uses_its_own_bucket_size() -> None:
    """block_bucket_size and tx_block_bucket_size are different numbers and are
    both in play on the block path."""
    dal, session = make([])
    run(dal.block(98514))
    _, params = session.seen[0]
    assert params == (98514 // CONFIG["block_bucket_size"], 98514)


def test_a_keyspace_outside_the_v3_pattern_is_refused() -> None:
    """Read-only, but a DAL pointed at a v2 keyspace would return rows that
    silently mean something else."""
    from graphsense_v3.settings import UnsafeKeyspace

    with pytest.raises(UnsafeKeyspace):
        run(Dal.open(["127.0.0.1:1"], "ltc_raw", "ltc_derived_v3_test"))


def test_close_is_safe_without_a_cluster() -> None:
    """The tests construct a Dal directly; close must not assume `open` ran."""
    dal, _ = make()
    run(dal.close())


def test_the_direction_survives_the_fan_out() -> None:
    """Direction is in the PARTITION KEY, not on the row, so a flattening
    gather loses it -- and a caller cannot re-derive it. v2 signs an outgoing
    value negative, so a lost direction is a wrong sign on every row of an
    unbounded listing."""

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, in_tx_page_max=0, out_tx_page_max=0)]
        # tx_id encodes the direction so the assertion can tell them apart.
        return [Row(tx_id=2 if params[1] else 1, value=10, balance=None)]

    dal = Dal(FakeSession(rows), RAW, DERIVED, dict(CONFIG))
    found = asyncio.run(dal.transactions(ADDRESS))
    assert {tx.tx_id: tx.is_outgoing for tx in found} == {1: False, 2: True}
