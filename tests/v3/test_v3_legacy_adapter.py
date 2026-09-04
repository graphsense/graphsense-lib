"""The v2-shaped adapter over a v3 keyspace.

What matters here is not that the methods return data -- `core` is tested for
that -- but that the adapter is HONEST about the three places v3 cannot answer
in v2's terms. A shim that quietly returns an empty list for a cluster, or a
zero for an id, turns a missing feature into a wrong answer and a comparison
harness would report it as agreement.
"""

import asyncio

import pytest

from graphsense_v3.codec import decode_address, encode_address
from graphsense_v3.db.core import Dal
from graphsense_v3.db.legacy import LegacyAdapter, NotAvailable, synthetic_id

from test_v3_dal import CONFIG, DERIVED, RAW, FakeSession, Row

# A real LTC address, so encode/decode is exercised rather than stubbed.
ADDRESS = "LSUjAt5oZcBWrqs4SjnWREHUAsiufiVkTd"
NEIGHBOR = "Ld2LjwjfcZQTTPsfrD1xiCzkSRMM3F61hs"


def adapter(rows=None) -> tuple:
    session = FakeSession(rows)
    dal = Dal(session, RAW, DERIVED, dict(CONFIG))
    return LegacyAdapter({"ltc": dal}), session


def run(coro):
    return asyncio.run(coro)


def test_an_unconfigured_currency_says_so() -> None:
    shim, _ = adapter()
    with pytest.raises(NotAvailable, match="no v3 keyspace configured"):
        run(shim.get_block("btc", 1))


@pytest.mark.parametrize(
    "method",
    [
        "get_entity",
        "get_entities_by_ids",
        "list_entity_txs",
        "list_entity_links",
        "list_entity_addresses",
        "get_address_entity_id",
        "get_fresh_cluster_id",
        "new_entity",
        "get_addresses_light",
    ],
)
def test_every_cluster_method_raises_rather_than_returning_empty(method) -> None:
    """The failure this prevents: an empty cluster reads as "this address has
    no cluster", which a comparison harness scores as agreement with v2."""
    shim, _ = adapter()
    with pytest.raises(NotAvailable, match="no cluster tables"):
        run(getattr(shim, method)("ltc", ADDRESS))


def test_new_address_explains_why_there_is_no_id() -> None:
    shim, _ = adapter()
    with pytest.raises(NotAvailable, match="int32"):
        run(shim.new_address("ltc", ADDRESS))


def test_the_synthetic_id_is_stable_and_not_v2s() -> None:
    """The service layer round-trips ids through paging tokens, so it needs a
    stable one; it must not be mistaken for v2's."""
    raw = encode_address("ltc", ADDRESS)
    assert synthetic_id(raw) == synthetic_id(raw)
    assert synthetic_id(raw) != synthetic_id(encode_address("ltc", NEIGHBOR))


def test_addresses_are_encoded_to_bytes_before_the_query() -> None:
    """v2 passes strings, v3 keys on packed bytes. Getting this wrong queries a
    partition that does not exist and returns nothing."""
    shim, session = adapter()
    run(shim.get_address("ltc", ADDRESS))
    _, params = session.seen[0]
    assert params[1] == encode_address("ltc", ADDRESS)


def test_get_address_omits_cluster_id_entirely() -> None:
    """Not zero: a zero would be read as cluster 0."""
    shim, _ = adapter(
        lambda cql, params: (
            [
                Row(
                    epoch=0,
                    no_incoming_txs=3,
                    no_outgoing_txs=1,
                    no_incoming_txs_zero_value=0,
                    no_outgoing_txs_zero_value=0,
                    first_tx_id=1,
                    last_tx_id=9,
                )
            ]
            if "address_stats" in cql
            else []
        )
    )
    row = run(shim.get_address("ltc", ADDRESS))
    assert "cluster_id" not in row
    assert row["no_incoming_txs"] == 3


def test_list_neighbors_refuses_a_numeric_id() -> None:
    """v2 hands `list_neighbors` the id `get_address_id` returned. Ours is
    synthetic and irreversible, so a numeric argument cannot be resolved -- and
    guessing would return an empty neighbour list for a real address."""
    shim, _ = adapter()
    with pytest.raises(NotAvailable, match="no surrogate id"):
        run(shim.list_neighbors("ltc", 12345, True))


def test_list_neighbors_accepts_an_address() -> None:
    shim, session = adapter()
    run(shim.list_neighbors("ltc", ADDRESS, True))
    assert len(session.seen) == CONFIG["relation_buckets"]


def test_prefix_search_normalises_the_expression() -> None:
    """v3 stores the prefix lowercased with the dead leading run stripped; v2
    does neither. Comparing raw would find nothing."""
    encoded = encode_address("ltc", ADDRESS)
    shim, session = adapter(
        lambda cql, params: [Row(address=encoded)] if "address_by_prefix" in cql else []
    )
    found = run(shim.list_matching_addresses("ltc", ADDRESS[:6]))
    _, params = session.seen[0]
    assert params[0] == ADDRESS[:4].lower()
    assert found == [decode_address("ltc", encoded)]


def test_prefix_search_drops_rows_the_partition_shares() -> None:
    """A prefix partition holds every address with that prefix, so a longer
    expression has to be filtered client-side."""
    other = encode_address("ltc", NEIGHBOR)
    shim, _ = adapter(
        lambda cql, params: [Row(address=other)] if "address_by_prefix" in cql else []
    )
    assert run(shim.list_matching_addresses("ltc", ADDRESS)) == []


def test_tx_hash_lookup_slices_its_own_prefix() -> None:
    """The prefix length is the keyspace's, not a constant."""
    tx_hash = bytes.fromhex("45a9ca2943a3ce54") + b"\x00" * 24
    shim, session = adapter()
    run(shim.get_tx_by_hash("ltc", tx_hash))
    _, params = session.seen[0]
    assert params[0] == tx_hash.hex()[: CONFIG["tx_prefix_length"]]
    assert params[1] == tx_hash


def test_block_timestamp_comes_back_as_a_row_not_a_bare_int() -> None:
    """The protocol declares Optional[Dict[str, Any]] and `blocks_service`
    reads `bts.get("timestamp")` off it. A bare int raises AttributeError
    inside the block-by-date binary search, nowhere near this method."""
    shim, _ = adapter(
        lambda cql, params: (
            [Row(block_id=7, timestamp=1331578610)] if "block" in cql else []
        )
    )
    row = run(shim.get_block_timestamp("ltc", 7))
    assert row.get("timestamp") == 1331578610


# --------------------------------------------------------------------------
# Return SHAPES. Every one of these was a failure in the first live run: the
# adapter returned v3's shape where the service layer reads v2's, and each
# surfaced as a TypeError or KeyError deep inside a service, far from its
# cause. Signatures alone do not catch these -- the conformance test checks how
# a method is CALLED, these check what it hands back.
# --------------------------------------------------------------------------


def test_currency_statistics_uses_v2s_key_names() -> None:
    """`StatsService` reads these off the dict directly. v3 renamed two of
    them, and a rename is a KeyError, not a difference."""
    shim, _ = adapter(
        lambda cql, params: (
            [
                Row(
                    highest_block=3171361,
                    lowest_block=0,
                    no_transactions=413004464,
                    no_addresses=7,
                    no_address_relations=9,
                    timestamp=1788442898,
                )
            ]
            if "summary_statistics" in cql
            else []
        )
    )
    stats = run(shim.get_currency_statistics("ltc"))
    assert set(stats) >= {
        "no_blocks",
        "no_transactions",
        "no_addresses",
        "no_address_relations",
        "no_clusters",
        "timestamp",
    }


def test_no_blocks_is_a_count_not_a_height() -> None:
    """v2's `no_blocks` is a height called a count. The rate lookup asks for
    `no_blocks - 1`, so returning the height reads one block too low -- and on
    a chain tip that block has no rate, which fails the whole call."""
    shim, _ = adapter(
        lambda cql, params: (
            [Row(highest_block=3171361, no_transactions=0, timestamp=0)]
            if "summary_statistics" in cql
            else []
        )
    )
    assert run(shim.get_currency_statistics("ltc"))["no_blocks"] == 3171362


def test_rates_come_back_as_an_ordered_list_not_a_map() -> None:
    """v3 stores fiat values as a map, v2 as a list aligned with the keyspace's
    `fiat_currencies`. The service's model rejects the map outright."""
    session = FakeSession(
        lambda cql, params: (
            [Row(block_id=7, fiat_values={"USD": 45.95, "EUR": 39.04})]
            if "exchange_rates" in cql
            else []
        )
    )
    dal = Dal(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["EUR", "USD"]})
    shim = LegacyAdapter({"ltc": dal})
    rates = run(shim.get_rates("ltc", 7))["rates"]
    assert rates == [
        {"code": "eur", "value": 39.04},
        {"code": "usd", "value": 45.95},
    ]


def test_rate_order_follows_the_keyspace_not_the_map() -> None:
    """The order is positional in v2's response, so taking the map's own order
    would be a different answer on a different day."""
    session = FakeSession(
        lambda cql, params: (
            [Row(block_id=7, fiat_values={"EUR": 1.0, "USD": 2.0})]
            if "exchange_rates" in cql
            else []
        )
    )
    dal = Dal(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["USD", "EUR"]})
    shim = LegacyAdapter({"ltc": dal})
    assert [r["code"] for r in run(shim.get_rates("ltc", 7))["rates"]] == ["usd", "eur"]


def test_list_address_txs_returns_rows_and_a_paging_state() -> None:
    """The service unpacks two values. A bare list raises "not enough values to
    unpack", which names neither the method nor the cause."""

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            return [Row(tx_id=8270462039621668, value=500, balance=1000)]
        if ".transaction " in cql:
            return [
                Row(
                    tx_id=8270462039621668,
                    block_id=1925,
                    block_timestamp=1788442898,
                    coinbase=False,
                    tx_hash=b"\xab\xcd",
                )
            ]
        return []

    shim, _ = adapter(rows)
    result = run(shim.list_address_txs("ltc", ADDRESS))
    assert isinstance(result, tuple) and len(result) == 2
    found, paging = result
    assert paging is None
    # The service reads exactly these off each row.
    assert set(found[0]) >= {"height", "timestamp", "coinbase", "tx_hash", "value"}
    assert found[0]["height"] == 1925
    assert found[0]["timestamp"] == 1788442898


def test_list_neighbors_returns_rows_and_a_paging_state() -> None:
    shim, _ = adapter(
        lambda cql, params: (
            [
                Row(
                    dst_address=encode_address("ltc", NEIGHBOR),
                    no_transactions=3,
                    epoch=0,
                )
            ]
            if "relations" in cql
            else []
        )
    )
    result = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert isinstance(result, tuple) and len(result) == 2


def test_a_neighbour_row_carries_the_id_the_service_subscripts() -> None:
    """`addresses_service` reads row["dst_address_id"] by SUBSCRIPT before
    anything else and feeds it to get_fresh_cluster_id. Absent, the call dies
    with a KeyError naming no cause; present, it fails honestly on "no cluster
    tables" -- which is the true state of v3."""
    encoded = encode_address("ltc", NEIGHBOR)
    shim, _ = adapter(
        lambda cql, params: (
            [Row(dst_address=encoded, no_transactions=3, epoch=0)]
            if "relations" in cql
            else []
        )
    )
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert rows[0]["dst_address_id"] == synthetic_id(encoded)


def test_the_counterparty_address_is_decoded_not_raw_bytes() -> None:
    """The service passes it to `address_to_user_format`, which leaves a UTXO
    address alone -- so raw bytes would reach the response body."""
    encoded = encode_address("ltc", NEIGHBOR)
    shim, _ = adapter(
        lambda cql, params: (
            [Row(dst_address=encoded, no_transactions=3, epoch=0)]
            if "relations" in cql
            else []
        )
    )
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert rows[0]["dst_address"] == NEIGHBOR


def test_the_counterparty_is_keyed_by_direction() -> None:
    """The service looks for `dst_address` going out and `src_address` coming
    in. One key for both directions finds nothing in one of them."""
    shim, _ = adapter(
        lambda cql, params: (
            [
                Row(
                    dst_address=encode_address("ltc", NEIGHBOR),
                    no_transactions=3,
                    epoch=0,
                )
            ]
            if "outgoing_relations" in cql
            else (
                [
                    Row(
                        src_address=encode_address("ltc", NEIGHBOR),
                        no_transactions=3,
                        epoch=0,
                    )
                ]
                if "incoming_relations" in cql
                else []
            )
        )
    )
    out, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert "dst_address" in out[0] and "dst_address_id" in out[0]
    incoming, _ = run(shim.list_neighbors("ltc", ADDRESS, False))
    assert "src_address" in incoming[0] and "src_address_id" in incoming[0]


def test_a_neighbor_value_exposes_attributes_not_keys() -> None:
    """`to_values` reads `.value` and `.fiat_values` as ATTRIBUTES -- v2 hands
    back a driver UDT. A plain dict raises AttributeError inside the service."""
    shim, _ = adapter(
        lambda cql, params: (
            [
                Row(
                    dst_address=encode_address("ltc", NEIGHBOR),
                    no_transactions=3,
                    epoch=0,
                )
            ]
            if "relations" in cql
            else []
        )
    )
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    value = rows[0]["value"]
    assert hasattr(value, "value") and hasattr(value, "fiat_values")
    assert isinstance(value.fiat_values, list)


def test_the_direction_string_is_parsed_not_coerced() -> None:
    """v2 passes "in" or "out". `bool(direction)` is True for BOTH, so an
    incoming listing silently returned outgoing rows -- no error, and a
    plausible answer that happened to be the wrong transactions."""
    asked = []

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            asked.append(params)
        return []

    shim, _ = adapter(rows)
    run(shim.list_address_txs("ltc", ADDRESS, direction="in"))
    assert all(p[1] is False for p in asked), "in must query is_outgoing = false"

    asked.clear()
    run(shim.list_address_txs("ltc", ADDRESS, direction="out"))
    assert all(p[1] is True for p in asked), "out must query is_outgoing = true"


def test_an_outgoing_value_is_signed_negative() -> None:
    """v2 signs by direction -- money leaving is negative. v3 stores the
    magnitude and the direction separately, so the sign has to be reapplied."""

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            return [Row(tx_id=8270462039621668, value=569994, balance=0)]
        if ".transaction " in cql:
            return [
                Row(
                    tx_id=8270462039621668,
                    block_id=1925,
                    block_timestamp=1,
                    coinbase=False,
                    tx_hash=b"\xab",
                )
            ]
        return []

    shim, _ = adapter(rows)
    outgoing, _ = run(shim.list_address_txs("ltc", ADDRESS, direction="out"))
    assert outgoing[0]["value"] == -569994
    incoming, _ = run(shim.list_address_txs("ltc", ADDRESS, direction="in"))
    assert incoming[0]["value"] == 569994


def _tx_session(legs):
    def rows(cql, params):
        if "transaction_by_tx_prefix" in cql:
            return [Row(tx_id=8270462039621668)]
        if "transaction_io" in cql:
            return legs
        if ".transaction " in cql:
            return [
                Row(
                    tx_id=8270462039621668,
                    block_id=1925,
                    block_timestamp=1788442898,
                    coinbase=False,
                    tx_hash=b"\xab\xcd",
                    total_input=10,
                    total_output=9,
                )
            ]
        return []

    return rows


def test_a_transaction_carries_its_inputs_and_outputs() -> None:
    """v2 stores the I/Os on the transaction row; v3 keeps them in
    `transaction_io`. `std_tx_from_row` reads row["inputs"] by SUBSCRIPT, so an
    absent key is a KeyError layers away from its cause."""
    encoded = encode_address("ltc", NEIGHBOR)
    shim, _ = adapter(
        _tx_session(
            [
                Row(is_output=False, io_index=0, address=[encoded], value=10),
                Row(is_output=True, io_index=0, address=[encoded], value=9),
            ]
        )
    )
    tx = run(shim.get_tx("ltc", b"\xab\xcd"))
    assert [io.value for io in tx["inputs"]] == [10]
    assert [io.value for io in tx["outputs"]] == [9]
    assert tx["inputs"][0].address == [NEIGHBOR]
    # v3 names it block_timestamp; v2's readers ask for `timestamp`.
    assert tx["timestamp"] == 1788442898


def test_io_is_ordered_by_index_within_each_direction() -> None:
    """Position IS the identity of an input or output -- the service indexes
    them, and a spend refers to output N."""
    encoded = encode_address("ltc", NEIGHBOR)
    shim, _ = adapter(
        _tx_session(
            [
                Row(is_output=True, io_index=1, address=[encoded], value=2),
                Row(is_output=True, io_index=0, address=[encoded], value=1),
            ]
        )
    )
    tx = run(shim.get_tx("ltc", b"\xab\xcd"))
    assert [io.value for io in tx["outputs"]] == [1, 2]


def test_an_io_with_no_address_is_none_not_an_empty_list() -> None:
    """The service treats None as a nonstandard I/O it only emits on request,
    and an empty list as a standard one paying nobody. They are different
    answers."""
    shim, _ = adapter(
        _tx_session([Row(is_output=True, io_index=0, address=[], value=0)])
    )
    tx = run(shim.get_tx("ltc", b"\xab\xcd"))
    assert tx["outputs"][0].address is None


def test_cluster_stubbing_is_opt_in() -> None:
    """The default must stay honest: an adapter that quietly reports "no
    cluster" turns a missing feature into a wrong answer."""
    shim, _ = adapter()
    with pytest.raises(NotAvailable, match="no cluster tables"):
        run(shim.get_fresh_cluster_id("ltc", 1))


def test_stubbed_clusters_report_none_rather_than_raising() -> None:
    """None is v2's own value for "no fresh cluster", so the call completes and
    the cluster FIELDS are excluded from the comparison instead of the whole
    call failing for a reason unrelated to what is under test."""
    session = FakeSession()
    shim = LegacyAdapter(
        {"ltc": Dal(session, RAW, DERIVED, dict(CONFIG))}, stub_clusters=True
    )
    assert run(shim.get_fresh_cluster_id("ltc", 1)) is None


def test_stubbing_does_not_fabricate_a_cluster_anywhere_else() -> None:
    """Only `get_fresh_cluster_id` is stubbable. The endpoints that ARE the
    cluster feature must still refuse, or the report would claim parity for
    the one thing v3 has not built."""
    session = FakeSession()
    shim = LegacyAdapter(
        {"ltc": Dal(session, RAW, DERIVED, dict(CONFIG))}, stub_clusters=True
    )
    for method in ("get_entity", "list_entity_txs", "get_address_entity_id"):
        with pytest.raises(NotAvailable, match="no cluster tables"):
            run(getattr(shim, method)("ltc", ADDRESS))


def _paging_session(count):
    """An address with `count` transactions on one ordinal page.

    Tests using it pin a DIRECTION: an unbounded listing reads both, and this
    fake answers each identically, so the merge would double the rows.
    """

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            return [
                Row(tx_id=8270462039621668 - i, value=1, balance=None)
                for i in range(count)
            ]
        if ".transaction " in cql:
            return [
                Row(
                    tx_id=params[1],
                    block_id=1925,
                    block_timestamp=1,
                    coinbase=False,
                    tx_hash=b"\xab",
                )
            ]
        return []

    return rows


def test_a_full_page_offers_a_next_page() -> None:
    """Returning None unconditionally made every address look like it had
    exactly one page: a caller would never see past the first `pagesize`
    transactions, and nothing would report an error."""
    shim, _ = adapter(_paging_session(3))
    found, token = run(
        shim.list_address_txs("ltc", ADDRESS, direction="out", pagesize=3)
    )
    assert len(found) == 3
    assert token == str(found[-1]["tx_id"])


def test_a_short_page_is_the_last_one() -> None:
    """Fewer rows than asked for means the listing is exhausted; offering a
    token there sends the caller back for an empty page."""
    shim, _ = adapter(_paging_session(2))
    _found, token = run(
        shim.list_address_txs("ltc", ADDRESS, direction="out", pagesize=3)
    )
    assert token is None


def test_an_empty_listing_offers_no_token() -> None:
    shim, _ = adapter(_paging_session(0))
    found, token = run(
        shim.list_address_txs("ltc", ADDRESS, direction="out", pagesize=3)
    )
    assert found == [] and token is None


def test_a_resume_token_becomes_an_exclusive_tx_id_bound() -> None:
    """`before_tx_id` is exclusive, so resuming from the last tx_id handed out
    continues after it rather than repeating it."""
    asked = []

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            asked.append((cql, params))
        return []

    shim, _ = adapter(rows)
    run(
        shim.list_address_txs(
            "ltc", ADDRESS, direction="out", page="8270462039621668", pagesize=3
        )
    )
    cql, params = asked[0]
    assert "tx_id < %s" in cql
    assert params[-1] == 8270462039621668


def test_links_return_rows_and_a_paging_state() -> None:
    """`links_response` unpacks two values. A bare list raises "not enough
    values to unpack", naming neither the method nor the cause."""

    def rows(cql, params):
        if "address_link_transactions" in cql:
            return [Row(tx_id=8270462039621668, input_value=10, output_value=9)]
        if ".transaction " in cql:
            return [
                Row(
                    tx_id=8270462039621668,
                    block_id=1925,
                    block_timestamp=1788442898,
                    tx_hash=b"\xab\xcd",
                )
            ]
        return []

    shim, _ = adapter(rows)
    result = run(shim.list_address_links("ltc", ADDRESS, NEIGHBOR))
    assert isinstance(result, tuple) and len(result) == 2
    found, _token = result
    # Exactly what links_response reads off each row.
    assert set(found[0]) == {
        "tx_hash",
        "block_id",
        "timestamp",
        "input_value",
        "output_value",
    }


def test_a_link_reports_the_real_amounts_not_the_apportioned_one() -> None:
    """The apportioned value is the graph EDGE weight; /links reports what each
    side actually put in and took out. Serving the apportioned share would be a
    plausible wrong number rather than an error."""

    def rows(cql, params):
        if "address_link_transactions" in cql:
            return [Row(tx_id=1, input_value=1000, output_value=250)]
        if ".transaction " in cql:
            return [Row(tx_id=1, block_id=1, block_timestamp=1, tx_hash=b"\xaa")]
        return []

    shim, _ = adapter(rows)
    found, _ = run(shim.list_address_links("ltc", ADDRESS, NEIGHBOR))
    assert (found[0]["input_value"], found[0]["output_value"]) == (1000, 250)


def test_min_height_becomes_a_lower_bound_on_the_query() -> None:
    """The bug this pins: `min_height` chose a starting PAGE and set no bound,
    so rows below the height came back anyway -- 26 of 52 sampled addresses
    returned transactions where v2 correctly returned none. Nothing errored."""
    asked = []

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            asked.append((cql, params))
        return []

    shim, _ = adapter(rows)
    run(shim.list_address_txs("ltc", ADDRESS, direction="out", min_height=1_000_000))
    cql, params = asked[0]
    assert "tx_id >= %s" in cql, "no lower bound reached the query"
    from graphsense_v3.codec import tx_id_range

    assert params[-1] == tx_id_range(1_000_000, 1_000_000)[0]


def test_a_height_range_bounds_both_ends() -> None:
    """min and max together are one clustering slice, not two queries."""
    asked = []

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            asked.append(cql)
        return []

    shim, _ = adapter(rows)
    run(
        shim.list_address_txs(
            "ltc", ADDRESS, direction="out", min_height=100, max_height=200
        )
    )
    assert "tx_id < %s" in asked[0] and "tx_id >= %s" in asked[0]


def test_a_positional_udt_is_labelled_from_the_keyspaces_own_order() -> None:
    """The `currency` UDT stores amounts positionally now. Zipping against a
    different order relabels every amount rather than failing, so the order
    must come from the keyspace that wrote them."""
    session = FakeSession()
    dal = Dal(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["EUR", "USD"]})
    shim = LegacyAdapter({"ltc": dal})
    assert shim._fiat_list("ltc", [1.5, 2.5]) == [
        {"code": "eur", "value": 1.5},
        {"code": "usd", "value": 2.5},
    ]


def test_a_rates_map_is_still_labelled_by_key() -> None:
    """`exchange_rates` keeps its map -- 3 MB, and read directly."""
    session = FakeSession()
    dal = Dal(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["EUR", "USD"]})
    shim = LegacyAdapter({"ltc": dal})
    assert shim._fiat_list("ltc", {"USD": 2.5, "EUR": 1.5}) == [
        {"code": "eur", "value": 1.5},
        {"code": "usd", "value": 2.5},
    ]


def test_an_account_neighbour_stays_bytes_for_the_service_to_format() -> None:
    """v2 stores an account address as a BLOB, and `address_to_user_format`
    keys off the type: bytes become "0x...", while a str is only lowercased.
    Decoding here would serve "742d..." -- a valid-looking address missing its
    prefix, on every neighbour of every account chain."""
    from graphsense_v3.codec import encode_address

    encoded = encode_address("eth", "0x742d35cc6634c0532925a3b844bc9e7595f0beb7")
    session = FakeSession(
        lambda cql, params: (
            [Row(dst_address=encoded, no_transactions=1, epoch=0)]
            if "relations" in cql
            else []
        )
    )
    shim = LegacyAdapter(
        {"eth": Dal(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    rows, _ = run(shim.list_neighbors("eth", encoded, True))
    assert isinstance(rows[0]["dst_address"], bytes)

    from graphsenselib.utils.address import address_to_user_format

    assert address_to_user_format("eth", rows[0]["dst_address"]).startswith("0x")


def test_a_utxo_neighbour_is_still_a_decoded_string() -> None:
    encoded = encode_address("ltc", NEIGHBOR)
    shim, _ = adapter(
        lambda cql, params: (
            [Row(dst_address=encoded, no_transactions=1, epoch=0)]
            if "relations" in cql
            else []
        )
    )
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert rows[0]["dst_address"] == NEIGHBOR


def test_account_links_refuse_rather_than_failing_on_a_column_name() -> None:
    """The account link table is keyed (src, dst, tx_page) with no dst_bucket,
    and `links_response` routes it through `txs_from_rows` instead of reporting
    two amounts. The UTXO read would die on `Undefined column name dst_bucket`
    -- a CQL error naming a column rather than the missing feature."""
    session = FakeSession()
    shim = LegacyAdapter(
        {"eth": Dal(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    with pytest.raises(NotAvailable, match="UTXO layout only"):
        run(shim.list_address_links("eth", "0xaa", "0xbb"))
