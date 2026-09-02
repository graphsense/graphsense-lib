"""Raw UTXO -> transformed address tables.

The interesting assertions are the two places v3 departs from graphsense-spark:
un-netting an address that appears on both sides of a transaction (D7), and
ordinal paging.
"""

from dataclasses import replace

import pytest

from graphsense_v3.codec import encode_address, tx_id
from graphsense_v3.config import config_for
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import transformed_utxo
from graphsense_v3.spark.writer import conformance_errors

ALICE = encode_address("btc", "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
BOB = encode_address("btc", "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq")

IO_SCHEMA = (
    "block_id_group INT, tx_id BIGINT, is_output BOOLEAN, io_index INT, "
    "address ARRAY<BINARY>, value BIGINT, address_type SMALLINT"
)
RATES_SCHEMA = "block_id INT, fiat_values MAP<STRING,DOUBLE>"
TX_SCHEMA = "tx_id BIGINT, total_input BIGINT"


def _io(tx, is_output, index, address, value):
    return {
        "block_id_group": 0,
        "tx_id": tx,
        "is_output": is_output,
        "io_index": index,
        "address": address,
        "value": value,
        "address_type": 3,
    }


@pytest.fixture(scope="module")
def rates(spark):
    return spark.createDataFrame(
        [
            {"block_id": 1, "fiat_values": {"EUR": 100.0, "USD": 200.0}},
            {"block_id": 2, "fiat_values": {"EUR": 300.0, "USD": 400.0}},
        ],
        schema=RATES_SCHEMA,
    )


@pytest.fixture(scope="module")
def self_change_txs(spark):
    return spark.createDataFrame(
        [{"tx_id": tx_id(1, 0), "total_input": 10}], schema=TX_SCHEMA
    )


@pytest.fixture(scope="module")
def many_txs(spark):
    return spark.createDataFrame(
        [
            {"tx_id": tx_id(block, index), "total_input": 10}
            for block, index in ((1, 0), (1, 1), (1, 2), (2, 0), (2, 1))
        ],
        schema=TX_SCHEMA,
    )


@pytest.fixture(scope="module")
def self_change_io(spark):
    """One transaction where Alice spends 10 and takes 3 back as change.

    graphsense-spark nets this to a single -7 outgoing row and the 3 receipt
    does not exist in the model (App. B.1). v3 keeps both legs.
    """
    tx = tx_id(1, 0)
    return spark.createDataFrame(
        [
            _io(tx, False, 0, [ALICE], 10),
            _io(tx, True, 0, [ALICE], 3),
            _io(tx, True, 1, [BOB], 7),
        ],
        schema=IO_SCHEMA,
    )


def test_an_address_on_both_sides_yields_two_legs(self_change_io) -> None:
    """D7. The netted form loses the receipt entirely."""
    rows = transformed_utxo.legs(self_change_io).collect()
    alice = sorted(
        ((r["is_outgoing"], r["value"]) for r in rows if bytes(r["address"]) == ALICE)
    )
    assert alice == [(False, 3), (True, 10)]
    assert [(r["is_outgoing"], r["value"]) for r in rows if bytes(r["address"]) == BOB]


def test_legs_are_gross_not_net(self_change_io, self_change_txs, rates) -> None:
    """The visible consequence: total_received and total_spent are both real
    amounts, and a self-change transaction counts once in each direction."""
    stats = transformed_utxo.build(self_change_io, self_change_txs, rates, "btc")[
        "address_stats"
    ]
    alice = next(r for r in stats.collect() if bytes(r["address"]) == ALICE)
    assert alice["no_incoming_txs"] == 1
    assert alice["no_outgoing_txs"] == 1
    assert int(alice["total_received"]["value"]) == 3
    assert int(alice["total_spent"]["value"]) == 10


def test_multi_address_ios_are_excluded(spark, rates) -> None:
    """graphsense-spark filters `size(address) == 1`: a multisig output is not
    attributed to any of its addresses, rather than to all of them."""
    tx = tx_id(1, 0)
    io = spark.createDataFrame(
        [_io(tx, True, 0, [ALICE, BOB], 10), _io(tx, True, 1, [BOB], 5)],
        schema=IO_SCHEMA,
    )
    rows = transformed_utxo.legs(io).collect()
    assert [(bytes(r["address"]), r["value"]) for r in rows] == [(BOB, 5)]


def test_repeated_address_on_one_side_is_one_leg(spark, rates) -> None:
    """Two outputs to the same address in one transaction: one leg, values
    summed -- so no_incoming_txs counts transactions, not output entries."""
    tx = tx_id(1, 0)
    io = spark.createDataFrame(
        [_io(tx, True, 0, [ALICE], 4), _io(tx, True, 1, [ALICE], 6)],
        schema=IO_SCHEMA,
    )
    rows = transformed_utxo.legs(io).collect()
    assert [(r["is_outgoing"], r["value"]) for r in rows] == [(False, 10)]


@pytest.fixture(scope="module")
def many_io(spark):
    """Alice receives in five transactions across two blocks."""
    rows = []
    for block, index in ((1, 0), (1, 1), (1, 2), (2, 0), (2, 1)):
        rows.append(_io(tx_id(block, index), True, 0, [ALICE], 10))
    return spark.createDataFrame(rows, schema=IO_SCHEMA)


def test_ordinal_paging_fills_pages_by_construction(many_io, rates) -> None:
    """A page holds exactly tx_page_size rows because the ordinal is the
    address's own count -- immune to burst and to dormancy alike."""
    config = replace(config_for("btc"), tx_page_size=2)
    paged = transformed_utxo.address_transactions(
        transformed_utxo.legs(many_io), config
    ).collect()
    by_page: dict[int, list[int]] = {}
    for row in sorted(paged, key=lambda r: r["tx_id"]):
        by_page.setdefault(row["tx_page"], []).append(row["tx_id"])
    assert [len(v) for v in (by_page[0], by_page[1])] == [2, 2]
    assert len(by_page[2]) == 1


def test_page_index_gives_the_entry_page_for_a_bound(many_io, rates) -> None:
    """Ordinal pages are not tx_id-aligned, so a height filter needs this."""
    config = replace(config_for("btc"), tx_page_size=2)
    paged = transformed_utxo.address_transactions(
        transformed_utxo.legs(many_io), config
    )
    pages = {
        r["tx_page"]: r["first_tx_id"]
        for r in transformed_utxo.address_tx_pages(paged).collect()
    }
    assert pages == {0: tx_id(1, 0), 1: tx_id(1, 2), 2: tx_id(2, 1)}


def test_fiat_is_summed_per_leg_at_its_own_block_rate(many_io, many_txs, rates) -> None:
    """Three receipts of 10 sat in block 1 at 100 EUR/coin and two in block 2 at
    300. Pricing the total at one rate would be an answer about no real moment.
    """
    stats = transformed_utxo.build(many_io, many_txs, rates, "btc")["address_stats"]
    alice = next(r for r in stats.collect() if bytes(r["address"]) == ALICE)
    each_block1 = round(10 * 100.0 / 10**8, 2)
    each_block2 = round(10 * 300.0 / 10**8, 2)
    assert alice["total_received"]["fiat_values"]["EUR"] == pytest.approx(
        3 * each_block1 + 2 * each_block2
    )


def test_a_block_without_a_rate_contributes_no_fiat(spark, many_io, many_txs) -> None:
    """A missing rate must not zero the address's total: explode drops the NULL
    map, so those legs simply do not contribute."""
    only_block_two = spark.createDataFrame(
        [{"block_id": 2, "fiat_values": {"EUR": 300.0}}], schema=RATES_SCHEMA
    )
    stats = transformed_utxo.build(many_io, many_txs, only_block_two, "btc")[
        "address_stats"
    ]
    alice = next(r for r in stats.collect() if bytes(r["address"]) == ALICE)
    assert alice["total_received"]["fiat_values"] == {
        "EUR": pytest.approx(2 * round(10 * 300.0 / 10**8, 2))
    }
    # the base-unit total is unaffected by what we know about prices
    assert int(alice["total_received"]["value"]) == 50


def test_frames_conform_to_the_schema(self_change_io, self_change_txs, rates) -> None:
    schema = schema_for("btc", Kind.TRANSFORMED)
    frames = transformed_utxo.build(self_change_io, self_change_txs, rates, "btc")
    assert set(frames) == set(transformed_utxo.TABLES)
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []


def test_search_prefix_comes_back_out_of_the_bytes(
    self_change_io, self_change_txs, rates
) -> None:
    """The transformed side only ever holds encoded addresses."""
    rows = transformed_utxo.build(self_change_io, self_change_txs, rates, "btc")[
        "address_by_prefix"
    ].collect()
    by_address = {bytes(r["address"]): r["address_prefix"] for r in rows}
    assert by_address[ALICE] == "1a1z"
    # bech32: the dead 'bc1' + witness character are stripped, so all four
    # characters vary -- the fix that makes whole-partition prefix reads viable.
    assert by_address[BOB] == "ar0s"


# --------------------------------------------------------------------------- #
# relations, links, degrees, balance                                           #
# --------------------------------------------------------------------------- #

CAROL = encode_address("btc", "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2")


@pytest.fixture(scope="module")
def split_io(spark):
    """Alice supplies 30 and Bob 70 of a 100-unit input; Carol receives 100."""
    tx = tx_id(1, 0)
    return spark.createDataFrame(
        [
            _io(tx, False, 0, [ALICE], 30),
            _io(tx, False, 1, [BOB], 70),
            _io(tx, True, 0, [CAROL], 100),
        ],
        schema=IO_SCHEMA,
    )


@pytest.fixture(scope="module")
def split_txs(spark):
    return spark.createDataFrame(
        [{"tx_id": tx_id(1, 0), "total_input": 100}], schema=TX_SCHEMA
    )


def test_edge_value_is_apportioned_by_input_share(split_io, split_txs) -> None:
    """A UTXO transaction does not say which input paid which output, so a
    source supplying 30% of the input is credited with 30% of each output."""
    edges = transformed_utxo.relation_edges(
        transformed_utxo.legs(split_io), split_txs
    ).collect()
    by_src = {bytes(r["src_address"]): r["value"] for r in edges}
    assert by_src == {ALICE: 30, BOB: 70}
    assert {bytes(r["dst_address"]) for r in edges} == {CAROL}


def test_unattributable_input_is_not_over_attributed(spark, split_io) -> None:
    """total_input is the transaction's whole input, including legs no address
    could be attributed to. Shares then do not sum to one -- deliberate, and
    inherited: under-attribute rather than invent a source."""
    txs = spark.createDataFrame(
        [{"tx_id": tx_id(1, 0), "total_input": 200}], schema=TX_SCHEMA
    )
    edges = transformed_utxo.relation_edges(
        transformed_utxo.legs(split_io), txs
    ).collect()
    assert sum(r["value"] for r in edges) == 50  # not 100


def test_self_edges_are_dropped(self_change_io, self_change_txs) -> None:
    """Un-netting lets an address be both source and destination of one
    transaction, which the netted model could never produce. New filter,
    required by App. B.1."""
    edges = transformed_utxo.relation_edges(
        transformed_utxo.legs(self_change_io), self_change_txs
    ).collect()
    assert all(bytes(r["src_address"]) != bytes(r["dst_address"]) for r in edges)
    assert {bytes(r["dst_address"]) for r in edges} == {BOB}


def test_degrees_count_distinct_counterparties(split_io, split_txs) -> None:
    edges = transformed_utxo.relation_edges(transformed_utxo.legs(split_io), split_txs)
    by_address = {
        bytes(r["address"]): (r["in_degree"], r["out_degree"])
        for r in transformed_utxo.degrees(edges).collect()
    }
    assert by_address[CAROL][0] == 2  # Alice and Bob
    assert by_address[ALICE][1] == 1


def test_relations_bucket_the_far_side(split_io, split_txs, rates) -> None:
    """The bucket hashes the counterparty, so a /neighbors read scatters over
    relation_buckets partitions and stops once it has in_degree rows."""
    from graphsense_v3.codec import bucket

    frames = transformed_utxo.build(split_io, split_txs, rates, "btc")
    buckets = config_for("btc").relation_buckets
    for row in frames["address_incoming_relations"].collect():
        assert row["rel_bucket"] == bucket(bytes(row["src_address"]), buckets)
    for row in frames["address_outgoing_relations"].collect():
        assert row["rel_bucket"] == bucket(bytes(row["dst_address"]), buckets)


def test_link_transactions_carry_the_tx_list(split_io, split_txs, rates) -> None:
    """The /links fix: the transactions behind an edge, not just their count.
    Both writers already materialise these tuples and aggregate them away."""
    frames = transformed_utxo.build(split_io, split_txs, rates, "btc")
    links = frames["address_link_transactions"].collect()
    assert {(bytes(r["src_address"]), int(r["value"])) for r in links} == {
        (ALICE, 30),
        (BOB, 70),
    }
    relations = frames["address_outgoing_relations"].collect()
    # the payoff: no_transactions on a relation is now the row count of the
    # link table, so /links can use a point-read bound as account does.
    assert {r["no_transactions"] for r in relations} == {1}
    assert len(links) == sum(r["no_transactions"] for r in relations)


def test_balance_is_received_minus_spent(
    self_change_io, self_change_txs, rates
) -> None:
    frames = transformed_utxo.build(self_change_io, self_change_txs, rates, "btc")
    by_address = {
        bytes(r["address"]): int(r["balance"]) for r in frames["balance"].collect()
    }
    assert by_address[ALICE] == -7  # spent 10, took 3 back as change
    assert by_address[BOB] == 7
    assert {r["currency"] for r in frames["balance"].collect()} == {"BTC"}


def test_every_frame_conforms_to_its_table(split_io, split_txs, rates) -> None:
    """All eight tables, checked against the model before a single write."""
    schema = schema_for("btc", Kind.TRANSFORMED)
    frames = transformed_utxo.build(split_io, split_txs, rates, "btc")
    assert set(frames) == set(transformed_utxo.TABLES)
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []
