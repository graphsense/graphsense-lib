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
from graphsense_v3.db import core
from graphsense_v3.db.core import Dal, dal_for

RAW = "ltc_raw_v3_test"
DERIVED = "ltc_derived_v3_test"

CONFIG: dict = {
    "entity_buckets": 100_000,
    "tx_page_size": 100_000,
    "relation_buckets": 16,
    "block_bucket_size": 100,
    "tx_block_bucket_size": 16,
    "address_prefix_length": 4,
    "tx_prefix_length": 5,
    # Every real keyspace has this -- `NetworkConfig.fiat_currencies` defaults
    # to ("EUR", "USD") and is written into `configuration` by the same run.
    # Without it here, `_fiat_list` takes its empty-order branch and returns []
    # for everything, so a test asserting a fiat amount passes vacuously.
    "fiat_currencies": ["EUR", "USD"],
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
    return dal_for(session, RAW, DERIVED, dict(CONFIG)), session


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


def _stats_rows(**cursors):
    """A stats reader whose epoch-0 row carries the given paging cursors."""

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
                    **cursors,
                )
            ]
        return []

    return rows


def _pages_by_class(session) -> dict:
    """``{(is_outgoing, is_zero_value): tx_page}`` actually queried."""
    return {
        (params[1], params[2]): params[3]
        for cql, params in session.seen
        if "address_transactions" in cql
    }


def test_the_default_page_is_the_highest_not_zero() -> None:
    """Pages are numbered by ASCENDING ordinal, so page 0 holds the OLDEST
    transactions. A newest-first listing has to start at *_tx_page_max."""
    dal, session = make(_stats_rows(in_tx_page_max=4, out_tx_page_max=4))
    run(dal.transactions(ADDRESS))
    assert set(_pages_by_class(session).values()) == {4}


def test_each_direction_starts_on_its_own_highest_page() -> None:
    """The cursors are PER PARTITION CLASS and do not move together. Taking one
    number for all of them is what the first BCH backtest caught: an address
    with two outgoing pages and one incoming read the incoming class at the
    outgoing page, found nothing there, and returned an outgoing-only listing
    that looked like a complete one."""
    dal, session = make(_stats_rows(in_tx_page_max=1, out_tx_page_max=2))
    run(dal.transactions(ADDRESS))
    assert _pages_by_class(session) == {(False, False): 1, (True, False): 2}


def test_the_zero_value_classes_have_their_own_cursors_too() -> None:
    """Zero-ness is in the partition key, so its pages are numbered separately
    from the non-zero ones -- and nothing was reading its cursors at all."""
    dal, session = make(
        _stats_rows(
            in_tx_page_max=1,
            out_tx_page_max=2,
            in_zero_tx_page_max=3,
            out_zero_tx_page_max=4,
        )
    )
    run(dal.transactions(ADDRESS, include_zero_value=True))
    assert _pages_by_class(session) == {
        (False, False): 1,
        (True, False): 2,
        (False, True): 3,
        (True, True): 4,
    }


def test_an_explicit_page_overrides_every_cursor() -> None:
    """A caller walking pages passes the number it wants; the cursors are only
    the DEFAULT entry point."""
    dal, session = make(_stats_rows(in_tx_page_max=1, out_tx_page_max=2))
    run(dal.transactions(ADDRESS, page=0))
    assert set(_pages_by_class(session).values()) == {0}


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

    dal = dal_for(FakeSession(rows), RAW, DERIVED, dict(CONFIG))
    found = asyncio.run(dal.transactions(ADDRESS))
    assert {tx.tx_id: tx.is_outgoing for tx in found} == {1: False, 2: True}


def test_a_missing_page_index_row_means_page_zero() -> None:
    """The index is written only for addresses that span pages, so absence is
    the answer rather than the absence of one. Returning None would make a
    height filter on an ordinary address look unanswerable."""
    dal = dal_for(FakeSession([]), RAW, DERIVED, dict(CONFIG))
    assert asyncio.run(dal.page_for_tx(ADDRESS, True, 12345)) == 0


# --------------------------------------------------------------------------- #
# Edge amounts: summed over epochs like every other relations column           #
# --------------------------------------------------------------------------- #


def _edge(no_transactions, value=None, fiat=None, token_values=None):
    return Row(
        src_address=ADDRESS,
        dst_address=OTHER,
        no_transactions=no_transactions,
        value=None if value is None else {"value": value, "fiat_values": fiat or []},
        token_values=token_values,
    )


def _one_bucket(rows):
    return lambda cql, params: rows if params[1] == 0 else []


def test_an_edges_fiat_is_summed_across_epochs() -> None:
    """The regression: the DAL read `value.fiat_values` off the row and threw
    it away, so no neighbour edge ever carried a fiat amount. Nothing failed --
    the adapter then did `getattr(int, "fiat_values", None)`, which is None
    every time, so the field was simply always empty."""
    dal, _ = make(_one_bucket([_edge(2, 100, [1.5, 2.0]), _edge(3, 50, [0.5, 1.0])]))
    edge = run(dal.neighbors(ADDRESS, is_outgoing=True))[0]
    assert (edge.no_transactions, edge.value) == (5, 150)
    assert edge.fiat_values == (2.0, 3.0)


def test_fiat_is_summed_positionally_not_by_name() -> None:
    """The `currency` UDT is a positional list ordered by the keyspace's own
    `configuration.fiat_currencies`. Adding by index is the only correct
    merge; anything else relabels amounts rather than failing."""
    dal, _ = make(_one_bucket([_edge(1, 1, [10.0, 0.0]), _edge(1, 1, [0.0, 20.0])]))
    assert run(dal.neighbors(ADDRESS, is_outgoing=True))[0].fiat_values == (10.0, 20.0)


def test_token_values_are_merged_by_asset_not_zipped() -> None:
    """The one merge in the summable model that is not a scalar add: a map,
    so epochs union by asset and then add per asset."""
    dal, _ = make(
        _one_bucket(
            [
                _edge(1, 0, [], {"USDT": {"value": 10, "fiat_values": [1.0]}}),
                _edge(1, 0, [], {"USDT": {"value": 5, "fiat_values": [0.5]}}),
            ]
        )
    )
    tokens = run(dal.neighbors(ADDRESS, is_outgoing=True))[0].token_values
    assert tokens == {"USDT": {"value": 15, "fiat_values": [1.5]}}


def test_an_asset_carried_in_only_one_epoch_survives_the_merge() -> None:
    """A union, not an intersection: an edge that moved USDC once and USDT
    twice must report both."""
    dal, _ = make(
        _one_bucket(
            [
                _edge(1, 0, [], {"USDT": {"value": 10, "fiat_values": [1.0]}}),
                _edge(1, 0, [], {"USDC": {"value": 7, "fiat_values": [0.7]}}),
            ]
        )
    )
    tokens = run(dal.neighbors(ADDRESS, is_outgoing=True))[0].token_values
    assert sorted(tokens) == ["USDC", "USDT"]
    assert tokens["USDT"]["value"] == 10 and tokens["USDC"]["value"] == 7


def test_a_utxo_edge_has_no_token_values_at_all() -> None:
    """UTXO relations carry no token column, so the merge must leave None
    rather than inventing an empty map the service would iterate."""
    dal, _ = make(_one_bucket([_edge(2, 100, [1.0])]))
    assert run(dal.neighbors(ADDRESS, is_outgoing=True))[0].token_values is None


def test_a_single_edge_point_read_sums_its_epochs_too() -> None:
    """`neighbor()` answers "is X a neighbour of Y" from the same rows, and
    was summing only the transaction count."""
    dal, _ = make(lambda cql, params: [_edge(2, 100, [1.0]), _edge(3, 50, [0.5])])
    edge = run(dal.neighbor(ADDRESS, OTHER, is_outgoing=True))
    assert (edge.no_transactions, edge.value, edge.fiat_values) == (5, 150, (1.5,))


# --------------------------------------------------------------------------- #
# block_by_date: the table exists to make this ONE row from ONE partition      #
# --------------------------------------------------------------------------- #


def test_the_timestamp_bound_is_pushed_into_the_query() -> None:
    """The point of `block_by_date`. It used to read a LIMIT 100 slice of the
    day and filter client-side -- which truncates every chain with more than
    100 blocks a day, so a timestamp late in the day could never be found."""
    dal, session = make(lambda cql, params: [Row(block_id=7, timestamp=500)])
    assert run(dal.block_at_or_after(500)) == {"block_id": 7, "timestamp": 500}
    cql, params = session.seen[0]
    assert "timestamp >= %s" in cql and "LIMIT 1" in cql
    assert params[1] == 500


def test_the_bound_can_be_made_strict() -> None:
    """/blocks/by_date needs the block STRICTLY after the timestamp: the
    service reports it as `after_block` and the one below as `before_block`,
    so an exact match has to fall on the before side to match what v2 serves.
    Both bounds push into CQL; only the operator changes."""
    dal, session = make(lambda cql, params: [Row(block_id=7, timestamp=501)])
    run(dal.block_at_or_after(500, inclusive=False))
    cql, params = session.seen[0]
    assert "timestamp > %s" in cql and "timestamp >= %s" not in cql
    assert params[1] == 500


def test_a_day_with_no_later_block_walks_to_the_next() -> None:
    """A timestamp late in a day often has no block after it until the next
    one. Returning the day's LAST block instead -- which is BEFORE the
    timestamp -- was the old fallback, and it is a wrong answer, not a miss."""
    seen: list = []

    def rows(cql, params):
        seen.append(params[0])
        return [Row(block_id=9, timestamp=999)] if len(seen) == 3 else []

    dal, _ = make(rows)
    assert run(dal.block_at_or_after(0))["block_id"] == 9
    assert seen == [19700101, 19700102, 19700103]


def test_the_forward_walk_is_bounded() -> None:
    """Past the bound the chain has a gap the date index cannot answer around,
    and None says so rather than reading forever to prove it."""
    dal, session = make(lambda cql, params: [])
    assert run(dal.block_at_or_after(0)) is None
    assert len(session.seen) == core.BLOCK_BY_DATE_MAX_DAYS


# --------------------------------------------------------------------------- #
# The account layout has columns the UTXO one does not                         #
# --------------------------------------------------------------------------- #


def account_dal(rows=None):
    """A reader over an ACCOUNT keyspace -- the family comes from the name."""
    session = FakeSession(rows)
    return dal_for(session, "eth_raw_v3_t1", "eth_derived_v3_t1", dict(CONFIG)), session


def test_the_family_is_read_off_the_keyspace_name() -> None:
    """`v3_keyspace` builds every name as <net>_<kind>_v3[_label] and
    `assert_v3_keyspace` refuses anything else, so the prefix is the network by
    construction rather than by convention."""
    utxo, _ = make()
    account, _ = account_dal()
    assert (utxo.network, utxo.is_account) == ("ltc", False)
    assert (account.network, account.is_account) == ("eth", True)


def test_an_account_listing_selects_the_asset_it_moved() -> None:
    """`currency` and `tx_reference` are clustering columns on the account
    layout. Not selecting them serves every USDT transfer as the native coin --
    a wrong answer that looks like data, and one no UTXO run could ever catch."""
    dal, session = account_dal(
        lambda cql, params: (
            [Row(tx_id=1, value=5, balance=None, currency="USDT", tx_reference=None)]
            if "address_transactions" in cql
            else []
        )
    )
    txs = run(dal.transactions(ADDRESS, page=0))
    assert txs[0].currency == "USDT"
    assert any(
        "currency" in cql and "tx_reference" in cql
        for cql, _ in session.seen
        if "address_transactions" in cql
    )


def test_a_utxo_listing_does_not_ask_for_columns_it_has_not_got() -> None:
    """Selecting them unconditionally is a CQL error on every UTXO keyspace,
    so the family has to gate the projection rather than the reader."""
    dal, session = make(
        lambda cql, params: (
            [Row(tx_id=1, value=5, balance=None)]
            if "address_transactions" in cql
            else []
        )
    )
    txs = run(dal.transactions(ADDRESS, page=0))
    assert txs[0].currency is None
    for cql, _ in session.seen:
        if "address_transactions" in cql:
            assert "currency" not in cql and "tx_reference" not in cql


def test_the_factory_picks_the_reader_for_the_family() -> None:
    """`dal_for` is the only supported way in. The base has no `transactions`
    at all -- deliberately, because a base that guessed is what served every
    token transfer as the native coin."""
    utxo, _ = make()
    account, _ = account_dal()
    assert isinstance(utxo, core.UtxoDal)
    assert isinstance(account, core.AccountDal)
    # The base DECLARES both so the contract is visible, and raises: a base
    # that guessed is what served every account token transfer as native coin.
    bare = core.Dal(FakeSession(None), RAW, DERIVED, dict(CONFIG))
    with pytest.raises(NotImplementedError, match="dal_for"):
        run(bare.transactions(ADDRESS))
    with pytest.raises(NotImplementedError, match="dal_for"):
        run(bare.link_transactions(ADDRESS, OTHER))


def test_an_account_edge_within_one_page_is_served() -> None:
    """`tx_page` is the edge's own ordinal // tx_page_size, so an edge below
    100k transactions -- every edge but a hub-to-hub one -- is entirely in
    page 0, ordered newest-first by its clustering."""
    dal, _ = account_dal(
        lambda cql, params: (
            []
            if "tx_page = 1" in cql
            else [Row(tx_id=9, tx_reference=None, currency="USDT", value=42)]
        )
    )
    found = run(dal.link_transactions(ADDRESS, OTHER))
    assert found == [
        {"tx_id": 9, "tx_reference": None, "currency": "USDT", "value": 42}
    ]


def _link_session(page_max, pages):
    """A fake answering the cursor read and each page of the link table."""

    def rows(cql, params):
        if "address_outgoing_relations" in cql:
            return [] if page_max is None else [Row(link_page_max=page_max)]
        if "address_link_transactions" in cql:
            return pages.get(params[2], [])
        return []

    return rows


def _link_row(tx_id, currency="ETH"):
    return Row(tx_id=tx_id, tx_reference=None, currency=currency, value=tx_id)


def test_an_account_edge_is_read_from_its_NEWEST_page() -> None:
    """Ordinals ascend with tx_id, so page 0 holds the OLDEST transactions.
    Reading it would answer from the wrong end of the edge's history -- which
    is exactly what this could not do until the backfill filled
    `link_page_max`."""
    dal, session = account_dal(
        _link_session(2, {2: [_link_row(99)], 0: [_link_row(1)]})
    )
    found = run(dal.link_transactions(ADDRESS, OTHER, limit=1))
    assert [row["tx_id"] for row in found] == [99]
    pages = [p[2] for c, p in session.seen if "address_link_transactions" in c]
    assert pages == [2]


def test_the_walk_continues_down_until_the_page_is_full() -> None:
    """A page holds tx_page_size rows, so one page satisfies every real
    request -- but the newest page of an edge can be nearly empty."""
    dal, _ = account_dal(
        _link_session(2, {2: [_link_row(99)], 1: [_link_row(50)], 0: [_link_row(1)]})
    )
    found = run(dal.link_transactions(ADDRESS, OTHER, limit=3))
    assert [row["tx_id"] for row in found] == [99, 50, 1]


def test_no_relation_row_means_no_edge() -> None:
    """Not an empty page of a real edge -- no edge. Reading the link table
    anyway would be a partition read for a partition that cannot exist."""
    dal, session = account_dal(_link_session(None, {0: [_link_row(1)]}))
    assert run(dal.link_transactions(ADDRESS, OTHER)) == []
    assert not any("address_link_transactions" in c for c, _ in session.seen)


def test_a_missing_cursor_reads_page_zero() -> None:
    """An edge written before the backfill filled the cursor. Page 0 is right
    for it either way: an edge that small has only one page."""
    dal, _ = account_dal(_link_session(None, {}))
    dal_with_null, _ = account_dal(
        lambda cql, params: (
            [Row(link_page_max=None)]
            if "address_outgoing_relations" in cql
            else [_link_row(7)]
        )
    )
    assert run(dal_with_null.link_transactions(ADDRESS, OTHER))[0]["tx_id"] == 7


def test_the_cursor_re_reads_the_boundary_transaction_and_skips_what_was_sent() -> None:
    """An account transaction produces several rows, so a page can end inside
    one. An exclusive `tx_id <` bound drops the rest of it silently; an
    inclusive bound plus a skip cannot lose a row."""
    rows = [
        Row(tx_id=9, value=1, balance=None, currency="A", tx_reference=None),
        Row(tx_id=9, value=2, balance=None, currency="B", tx_reference=None),
        Row(tx_id=9, value=3, balance=None, currency="C", tx_reference=None),
    ]
    dal, session = account_dal(
        lambda cql, params: rows if "address_transactions" in cql else []
    )
    found = run(
        dal.transactions(ADDRESS, is_outgoing=True, page=0, before_row=(9, 2), limit=1)
    )
    # The first two rows of tx 9 were already delivered; the third is next.
    assert [tx.currency for tx in found] == ["C"]
    cql, _ = next(c for c in session.seen if "address_transactions" in c[0])
    assert "tx_id <= %s" in cql
    # It has to ASK for the skipped rows to be able to drop them.
    assert "LIMIT 3" in cql


def test_no_cursor_means_no_skip_and_no_extra_rows_fetched() -> None:
    """The first page must not pay for a cursor it does not have."""
    dal, session = account_dal(
        lambda cql, params: (
            [Row(tx_id=9, value=1, balance=None, currency="A", tx_reference=None)]
            if "address_transactions" in cql
            else []
        )
    )
    run(dal.transactions(ADDRESS, is_outgoing=True, page=0, limit=5))
    cql, _ = next(c for c in session.seen if "address_transactions" in c[0])
    assert "LIMIT 5" in cql and "tx_id <=" not in cql


def test_a_height_bound_stays_exclusive_and_is_not_a_cursor() -> None:
    """`max_height` is a FILTER, not a position -- it has no rows already
    delivered, so it keeps the exclusive bound and adds nothing to the limit."""
    dal, session = account_dal(lambda cql, params: [])
    run(dal.transactions(ADDRESS, is_outgoing=True, page=0, before_tx_id=99, limit=4))
    cql, _ = next(c for c in session.seen if "address_transactions" in c[0])
    assert "tx_id < %s" in cql and "tx_id <=" not in cql
    assert "LIMIT 4" in cql


def _account(rows=None):
    session = FakeSession(rows)
    return dal_for(session, "eth_raw_v3", "eth_derived_v3", dict(CONFIG)), session


def test_an_account_transaction_has_no_inputs_and_asks_for_none() -> None:
    """`transaction_io`, `transaction_spent_in` and `transaction_spending` do
    not exist in an account keyspace, and these four lived on the shared base
    querying them unconditionally -- so `get_tx` and `list_block_txs` on eth
    died with `table transaction_io does not exist`, a CQL error naming a table
    rather than the family mismatch.

    Empty, not an error: an account transaction genuinely has no inputs, which
    is a fact about the family and not a feature v3 has yet to build. And NO
    QUERY is issued -- a read that cannot succeed should not be attempted."""
    dal, session = _account()
    assert run(dal.transaction_io(1)) == []
    assert run(dal.transaction_io_many([1, 2])) == {}
    assert run(dal.spent_in(b"\xaa" * 32, "abcde")) == []
    assert run(dal.spending(b"\xaa" * 32, "abcde")) == []
    assert session.seen == [], "an account keyspace was queried for UTXO tables"


def test_the_utxo_reader_still_queries_the_io_tables() -> None:
    """The account skip must not quietly disable the UTXO path."""
    dal, session = make()
    run(dal.transaction_io(1))
    run(dal.spent_in(b"\xaa" * 32, "abcde"))
    run(dal.spending(b"\xaa" * 32, "abcde"))
    asked = " ".join(cql for cql, _ in session.seen)
    assert "transaction_io" in asked
    assert "transaction_spent_in" in asked
    assert "transaction_spending" in asked


def test_the_base_refuses_rather_than_guessing_a_family() -> None:
    """A base that guessed is what served every account token transfer as the
    native coin. Same contract as `transactions` and `link_transactions`."""
    from graphsense_v3.db.core import Dal

    bare = Dal(FakeSession(), RAW, DERIVED, dict(CONFIG))
    for call in (
        bare.transaction_io(1),
        bare.transaction_io_many([1]),
        bare.spent_in(b"\xaa", "abcde"),
        bare.spending(b"\xaa", "abcde"),
    ):
        with pytest.raises(NotImplementedError, match="dal_for"):
            run(call)


# --------------------------------------------------------------------------- #
# exchange_rates: the native coin is dense, a token is not                     #
# --------------------------------------------------------------------------- #


def test_a_token_rate_is_read_at_or_before_the_block() -> None:
    """The merged table inherited v2's density: the native coin has a row per
    block, a token only where a price was fetched. An exact match finds
    nothing on most blocks and prices the transfer at nothing."""
    dal, session = make(lambda cql, params: [Row(fiat_values={"eur": 1.5})])
    assert run(dal.rate_at_or_before("USDT", 500)) == {"eur": 1.5}
    cql, params = session.seen[0]
    assert "block_id <= %s" in cql and "LIMIT 1" in cql
    assert params[0] == "USDT" and params[2] == 500


def test_a_token_rate_walks_back_through_groups() -> None:
    """A token can go a whole bucket without a price. The bound stops helping
    once the group changes -- every earlier group ends below the block -- so
    it moves to that group's last block."""
    seen: list = []

    def rows(cql, params):
        seen.append(params[1:])
        return [Row(fiat_values={"eur": 2.0})] if len(seen) == 3 else []

    dal, _ = make(rows)
    assert run(dal.rate_at_or_before("USDT", 250)) == {"eur": 2.0}
    groups = [group for group, _block in seen]
    assert groups == [2, 1, 0]
    # Not still 250: the second read asks for the last block of group 1.
    assert seen[1][1] < 250


def test_a_token_with_no_rate_anywhere_is_none_not_an_endless_walk() -> None:
    dal, session = make(lambda cql, params: [])
    assert run(dal.rate_at_or_before("USDT", 10_000)) is None
    assert len(session.seen) == core.BLOCK_BELOW_MAX_GROUPS


# --------------------------------------------------------------------------- #
# neighbours: the read is bounded, and a row is not a neighbour               #
# --------------------------------------------------------------------------- #


def _relations(edges: dict):
    """A session serving ``{far_address: [no_transactions per epoch]}``.

    HONOURS the CQL ``LIMIT``, which a fake that ignores it cannot: the whole
    point of the bounded read is what happens when a partition comes back full.
    """
    seen: list = []

    def rows(cql, params):
        if "relations" not in cql:
            return []
        seen.append((" ".join(cql.split()), params))
        bucket_index = params[1]
        after = params[2] if len(params) > 2 else None
        out = []
        for far in sorted(edges):
            if bucket(far, CONFIG["relation_buckets"]) != bucket_index:
                continue
            if after is not None and far <= after:
                continue
            for epoch, count in enumerate(edges[far]):
                out.append(
                    Row(
                        dst_address=far,
                        rel_bucket=bucket_index,
                        epoch=epoch,
                        no_transactions=count,
                        value={"value": count, "fiat_values": []},
                        token_values=None,
                    )
                )
        limit = None
        if " LIMIT " in cql:
            limit = int(cql.rsplit(" LIMIT ", 1)[1].split()[0])
        return out[:limit] if limit else out

    dal, _ = make(rows)
    return dal, seen


def _far(index: int) -> bytes:
    return bytes([0]) + index.to_bytes(20, "big")


def test_a_bounded_neighbour_read_asks_for_a_row_limit() -> None:
    """Without one, a hub with 50 000 edges reads all of them to return 20."""
    dal, seen = _relations({_far(i): [1] for i in range(200)})
    found = run(dal.neighbors(b"\xaa" * 20, is_outgoing=True, limit=20))
    assert len(found) >= 20
    assert all(" LIMIT " in cql for cql, _p in seen)


def test_an_unbounded_neighbour_read_still_asks_for_everything() -> None:
    """`limit=None` is the probe's path and the one that must not change."""
    dal, seen = _relations({_far(i): [1] for i in range(30)})
    found = run(dal.neighbors(b"\xaa" * 20, is_outgoing=True))
    assert len(found) == 30
    assert all(" LIMIT " not in cql for cql, _p in seen)


def test_a_neighbour_split_across_epochs_is_never_half_counted() -> None:
    """THE reason a row limit is not enough. One counterparty owns one row per
    epoch and the read SUMS them, so a cap that stops inside that run does not
    raise -- it understates the neighbour's totals, silently.

    `_far(0)` sorts first in its bucket, so a budget below its epoch count
    fills the whole read with that one counterparty."""
    edges = {_far(i): [1] for i in range(40)}
    edges[_far(0)] = [1] * 60
    dal, _ = _relations(edges)
    found = run(dal.neighbors(b"\xaa" * 20, is_outgoing=True, limit=5))
    by_address = {bytes(n.address): n for n in found}
    assert _far(0) in by_address, "the counterparty the budget straddled is gone"
    assert by_address[_far(0)].no_transactions == 60, "epoch rows were cut"


def test_the_budget_doubles_rather_than_returning_a_short_page() -> None:
    """A partition that came back full may have stopped inside a group, so its
    last group is dropped -- which can leave nothing complete at all. Reading
    again with more budget is the answer; returning what survived is not.

    Wide fan-out never triggers this: 16 buckets means the first budget covers
    far more than a page. Only a DEEP counterparty does, which is why the
    fixture is one address with thirty epochs rather than many addresses."""
    edges = {_far(i): [1] for i in range(40)}
    edges[_far(0)] = [1] * 30
    dal, seen = _relations(edges)
    run(dal.neighbors(b"\xaa" * 20, is_outgoing=True, limit=5))
    budgets = sorted({int(cql.rsplit(" LIMIT ", 1)[1]) for cql, _p in seen})
    assert len(budgets) > 1, "it never had to grow, so this proves nothing"
    assert budgets[1] == budgets[0] * 2


def test_a_counterparty_wider_than_every_budget_falls_back_to_a_whole_read() -> None:
    """A group larger than any budget would loop forever. The fallback reads
    the partitions whole -- what the bound exists to avoid, and still the only
    right answer."""
    edges = {_far(i): [1] for i in range(10)}
    edges[_far(3)] = [1] * 100_000
    dal, seen = _relations(edges)
    found = run(dal.neighbors(b"\xaa" * 20, is_outgoing=True, limit=5))
    by_address = {bytes(n.address): n for n in found}
    assert by_address[_far(3)].no_transactions == 100_000
    assert any(" LIMIT " not in cql for cql, _p in seen), "it never fell back"
