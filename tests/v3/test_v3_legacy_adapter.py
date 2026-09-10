"""The v2-shaped adapter over a v3 keyspace.

What matters here is not that the methods return data -- `core` is tested for
that -- but that the adapter is HONEST about the three places v3 cannot answer
in v2's terms. A shim that quietly returns an empty list for a cluster, or a
zero for an id, turns a missing feature into a wrong answer and a comparison
harness would report it as agreement.
"""

import asyncio
import zlib

from types import SimpleNamespace

import pytest

from graphsense_v3.codec import decode_address, encode_address
from graphsense_v3.db.core import dal_for
from graphsense_v3.db.legacy import LegacyAdapter, NotAvailable, synthetic_id

from test_v3_dal import CONFIG, DERIVED, RAW, FakeSession, Row

# A real LTC address, so encode/decode is exercised rather than stubbed.
ADDRESS = "LSUjAt5oZcBWrqs4SjnWREHUAsiufiVkTd"
NEIGHBOR = "Ld2LjwjfcZQTTPsfrD1xiCzkSRMM3F61hs"


def adapter(rows=None) -> tuple:
    session = FakeSession(rows)
    dal = dal_for(session, RAW, DERIVED, dict(CONFIG))
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
    dal = dal_for(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["EUR", "USD"]})
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
    dal = dal_for(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["USD", "EUR"]})
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
        {"ltc": dal_for(session, RAW, DERIVED, dict(CONFIG))}, stub_clusters=True
    )
    assert run(shim.get_fresh_cluster_id("ltc", 1)) is None


def test_stubbing_does_not_fabricate_a_cluster_anywhere_else() -> None:
    """Only `get_fresh_cluster_id` is stubbable. The endpoints that ARE the
    cluster feature must still refuse, or the report would claim parity for
    the one thing v3 has not built."""
    session = FakeSession()
    shim = LegacyAdapter(
        {"ltc": dal_for(session, RAW, DERIVED, dict(CONFIG))}, stub_clusters=True
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
    # "<tx_id>:<rows of it already delivered>". The count is what an account
    # listing needs: one transaction there produces several rows, so a page can
    # end mid-transaction and resuming at `tx_id <` would drop the rest of it.
    assert token == f"{found[-1]['tx_id']}:1"


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


def test_a_resume_token_becomes_an_inclusive_tx_id_bound() -> None:
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
    # INCLUSIVE, and the already-delivered rows are dropped client-side. An
    # exclusive bound cannot express "the rest of that transaction".
    assert "tx_id <= %s" in cql
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
    assert (found[0]["input_value"], found[0]["output_value"]) == (-1000, 250)


def test_the_link_input_value_is_signed_the_way_v2_signs_it() -> None:
    """v2 has no link table: it takes each side's `address_transactions.value`,
    which is already signed by direction, so the source's input arrives NEGATIVE
    (`cassandra.py:2631-2632`). v3 stores the magnitude, and the first BCH
    backtest showed the sign flipped on every one of 51 sampled links.

    The sign has to go on before the service converts to fiat, not after: v2
    rounds the negative number, and rounding the positive one then negating
    lands a cent away on half-cent values."""

    def rows(cql, params):
        if "address_link_transactions" in cql:
            return [Row(tx_id=1, input_value=1100000, output_value=1099000)]
        if ".transaction " in cql:
            return [Row(tx_id=1, block_id=1, block_timestamp=1, tx_hash=b"\xaa")]
        return []

    shim, _ = adapter(rows)
    found, _ = run(shim.list_address_links("ltc", ADDRESS, NEIGHBOR))
    assert found[0]["input_value"] == -1100000
    # The destination RECEIVED, so its side stays positive.
    assert found[0]["output_value"] == 1099000


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
    dal = dal_for(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["EUR", "USD"]})
    shim = LegacyAdapter({"ltc": dal})
    assert shim._fiat_list("ltc", [1.5, 2.5]) == [
        {"code": "eur", "value": 1.5},
        {"code": "usd", "value": 2.5},
    ]


def test_a_rates_map_is_still_labelled_by_key() -> None:
    """`exchange_rates` keeps its map -- 3 MB, and read directly."""
    session = FakeSession()
    dal = dal_for(session, RAW, DERIVED, {**CONFIG, "fiat_currencies": ["EUR", "USD"]})
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
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
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


def _account_link_session(
    currency: str = "ETH",
    trace_index=7,
    log_index=None,
    trace_address="",
    trace_type="call",
):
    """One edge with one transfer, plus the transaction and trace behind it."""
    ref = SimpleNamespace(trace_index=trace_index, log_index=log_index)
    tx_id = (12 << 32) + 1

    def rows(cql, params):
        if "link_page_max" in cql:
            return [Row(link_page_max=0)]
        if "address_link_transactions" in cql:
            return [Row(tx_id=tx_id, tx_reference=ref, currency=currency, value=5)]
        if ".transaction" in cql:
            return [
                Row(
                    tx_id=tx_id,
                    tx_hash=b"\xab" * 32,
                    block_id=12,
                    block_timestamp=1700,
                    first_trace_index=7,
                    first_log_index=3,
                    input=b"\xde\xad",
                    receipt_gas_used=21000,
                    receipt_effective_gas_price=1000,
                )
            ]
        if ".trace" in cql:
            return [
                Row(
                    block_id=12,
                    trace_index=trace_index,
                    trace_address=trace_address,
                    trace_type=trace_type,
                    input=TRACE_INPUT,
                )
            ]
        return []

    return FakeSession(rows)


def _account_links(session, **kwargs):
    shim = LegacyAdapter(
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    return run(shim.list_address_links("eth", "0xaa", "0xbb", **kwargs))


def test_an_account_link_is_one_row_per_transfer_not_two_amounts() -> None:
    """`links_response` splits on the family: a UTXO link reports input_value
    and output_value, an account link goes through `txs_from_rows` and reports
    a transaction. The two share nothing but a method name."""
    rows, _ = _account_links(_account_link_session())
    assert len(rows) == 1
    row = rows[0]
    assert "input_value" not in row and "output_value" not in row
    assert row["tx_hash"] == b"\xab" * 32
    assert row["block_id"] == 12 and row["timestamp"] == 1700
    assert row["value"] == 5


def test_an_account_link_names_the_edge_as_the_two_ends() -> None:
    """v2 fetches the trace or the log purely to learn a transfer's ends. For
    an edge the caller already named both, and taking the TRANSACTION's from/to
    would report the outer transaction's counterparty on every internal call
    and every token transfer."""
    rows, _ = _account_links(_account_link_session())
    assert rows[0]["from_address"] == encode_address("eth", "0xaa")
    assert rows[0]["to_address"] == encode_address("eth", "0xbb")


def test_a_link_row_is_external_when_its_trace_address_is_empty() -> None:
    """v2's test, on `/links` as on the listing: trace_address is the path
    down the call tree, so the root's is empty (`cassandra.py:5053`)."""
    rows, _ = _account_links(_account_link_session(trace_index=7))
    assert rows[0]["type"] == "external"

    rows, _ = _account_links(_account_link_session(trace_index=9, trace_address="0"))
    assert rows[0]["type"] == "internal"
    assert rows[0]["trace_index"] == 9


def test_a_token_transfer_is_erc20_and_carries_the_log_index() -> None:
    """`get_tx_identifier` renders `token_tx_id` into the identifier, and v2
    puts the log index there."""
    session = _account_link_session(currency="USDT", trace_index=None, log_index=4)
    rows, _ = _account_links(session)
    assert rows[0]["type"] == "erc20"
    assert rows[0]["token_tx_id"] == 4
    assert rows[0]["currency"] == "USDT"


def test_a_link_row_reads_the_trace_it_points_at_too() -> None:
    """The edge names the two ENDS, which is one of the two reasons v2 reads
    the trace; `trace_type` and `input` are the other, and they live nowhere
    else. Reported as None, `/links` lost contract_creation and the call data
    on every row."""
    rows, _ = _account_links(_account_link_session(trace_index=9, trace_address="0"))
    assert rows[0]["contract_creation"] is False
    assert rows[0]["input"] == TRACE_INPUT

    created = _account_link_session(trace_type="create")
    assert _account_links(created)[0][0]["contract_creation"] is True


def test_a_link_row_is_charged_the_fee_only_when_it_is_external() -> None:
    """The fee belongs to the transaction. v2 sets it under
    `type == "external"` (`cassandra.py:5090`), and `/links` goes through the
    same builder (`cassandra.py:2591-2616`)."""
    external, _ = _account_links(_account_link_session(trace_index=7))
    assert external[0]["fee"] == 21000 * 1000

    internal, _ = _account_links(
        _account_link_session(trace_index=9, trace_address="0")
    )
    assert "fee" not in internal[0]


def test_a_link_to_a_transaction_the_raw_keyspace_lacks_is_an_error() -> None:
    """A torn keyspace, not a row to quietly drop."""

    def rows(cql, params):
        if "link_page_max" in cql:
            return [Row(link_page_max=0)]
        if "address_link_transactions" in cql:
            return [
                Row(
                    tx_id=99,
                    tx_reference=SimpleNamespace(trace_index=1, log_index=None),
                    currency="ETH",
                    value=5,
                )
            ]
        return []

    with pytest.raises(NotAvailable, match="does not have"):
        _account_links(FakeSession(rows))


def test_an_account_link_pages_only_when_the_page_was_full() -> None:
    """A short page cannot have more behind it; returning a token anyway makes
    a caller ask for a page that does not exist."""
    rows, token = _account_links(_account_link_session(), pagesize=100)
    assert len(rows) == 1 and token is None
    rows, token = _account_links(_account_link_session(), pagesize=1)
    assert token == str((12 << 32) + 1)


def _relation_rows(**extra):
    """One edge, in ONE bucket.

    A neighbour listing scatters over every `relation_buckets` partition, so a
    fake that answers every bucket with the same row multiplies each summed
    amount by 16 -- which is invisible on the identity fields the older tests
    assert, and exactly wrong on the amounts these do.
    """
    encoded = encode_address("ltc", NEIGHBOR)
    return lambda cql, params: (
        [Row(dst_address=encoded, no_transactions=3, epoch=0, **extra)]
        if "relations" in cql and params[1] == 0
        else []
    )


def test_a_neighbour_edge_reports_its_fiat_values() -> None:
    """The regression this pins: the adapter read the fiat off the AMOUNT --
    `getattr(edge.value, "fiat_values", None)` where `edge.value` is an int --
    so it resolved to None on every edge of every call and the field was
    always empty. The relations row has held the numbers all along."""
    shim, _ = adapter(_relation_rows(value={"value": 500, "fiat_values": [1.25, 1.5]}))
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert rows[0]["value"].value == 500
    assert rows[0]["value"].fiat_values == [
        {"code": "eur", "value": 1.25},
        {"code": "usd", "value": 1.5},
    ]


def test_a_utxo_neighbour_carries_no_token_values() -> None:
    """UTXO relations have no token column. None, not an empty map: the
    service iterates this field."""
    shim, _ = adapter(_relation_rows(value={"value": 1, "fiat_values": [0.0, 0.0]}))
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    assert rows[0]["token_values"] is None


def test_token_values_reach_the_response_as_value_objects() -> None:
    """`to_values` reads `.value` and `.fiat_values` as ATTRIBUTES, so a bare
    dict per asset raises AttributeError inside the service rather than at the
    boundary -- the same trap `_Value` exists for on the native amount."""
    shim, _ = adapter(
        _relation_rows(
            value={"value": 0, "fiat_values": [0.0, 0.0]},
            token_values={"USDT": {"value": 42, "fiat_values": [40.0, 44.0]}},
        )
    )
    rows, _ = run(shim.list_neighbors("ltc", ADDRESS, True))
    token = rows[0]["token_values"]["USDT"]
    assert token.value == 42
    assert token.fiat_values == [
        {"code": "eur", "value": 40.0},
        {"code": "usd", "value": 44.0},
    ]


def test_block_by_date_returns_the_row_the_service_subscripts() -> None:
    """`blocks_service.get_block_by_date` reads BOTH `x["block_id"]` and
    `x["timestamp"]` off this. It returned a bare int until 2026-09-07, which
    raises "'int' object is not subscriptable" from inside the service -- and
    nothing caught it, because `block_by_date_use_linear_search` defaults to
    False so the backtest never took this path."""
    shim, _ = adapter(
        lambda cql, params: (
            [Row(block_id=1925, timestamp=1788442898)] if "block_by_date" in cql else []
        )
    )
    found = run(shim.get_block_by_date_allow_filtering("ltc", 1788442000))
    assert found["block_id"] == 1925
    assert found["timestamp"] == 1788442898


def test_no_block_at_or_after_is_none_not_a_crash() -> None:
    """The service checks `if x:` before subscripting, so None is the honest
    answer for a timestamp past the chain tip."""
    shim, _ = adapter(lambda cql, params: [])
    assert run(shim.get_block_by_date_allow_filtering("ltc", 1788442000)) is None


def test_the_spend_tables_answer_current_rows() -> None:
    """`txs_service` iterates `results.current_rows` on both of these, because
    v2 hands back a driver ResultSet. A plain list raises AttributeError from
    inside the service, naming neither the method nor the cause -- and nothing
    caught it, because the backtest never calls /spending or /spent_in."""
    row = Row(
        spending_tx_hash=b"\xaa" * 32,
        spent_tx_hash=b"\xbb" * 32,
        spending_input_index=0,
        spent_output_index=1,
    )
    shim, _ = adapter(lambda cql, params: [row])
    for method in ("get_spending_txs", "get_spent_in_txs"):
        found = run(getattr(shim, method)("ltc", "ab" * 32))
        assert list(found.current_rows) == list(found)
        assert len(found) == 1


def test_filtering_by_io_index_keeps_the_result_set_shape() -> None:
    """The filtered branch built a plain list, so passing io_index -- which the
    service always does -- took the AttributeError path even once the unfiltered
    one was fixed."""
    row = Row(
        spending_tx_hash=b"\xaa" * 32,
        spent_tx_hash=b"\xbb" * 32,
        spending_input_index=3,
        spent_output_index=7,
    )
    shim, _ = adapter(lambda cql, params: [row])
    assert run(shim.get_spending_txs("ltc", "ab" * 32, io_index=3)).current_rows
    assert run(shim.get_spent_in_txs("ltc", "ab" * 32, io_index=7)).current_rows
    assert run(shim.get_spending_txs("ltc", "ab" * 32, io_index=99)).current_rows == []


def test_the_token_counts_rows_carried_across_a_page_boundary() -> None:
    """Two pages ending inside the SAME transaction must accumulate. Counting
    only this page's rows would tell the next page to skip fewer than were
    actually delivered, and it would repeat them."""
    from graphsense_v3.db.legacy import decode_page_token, encode_page_token

    page = [SimpleNamespace(tx_id=9), SimpleNamespace(tx_id=9)]
    assert encode_page_token(page, None) == "9:2"
    # Resuming inside tx 9, having already been given 2 of its rows.
    assert encode_page_token(page, (9, 2)) == "9:4"
    # A boundary that moved on: the previous count does not carry.
    assert encode_page_token(page, (11, 3)) == "9:2"
    assert decode_page_token("9:4") == (9, 4)


def test_a_bare_tx_id_token_still_resumes() -> None:
    """A token minted before the count existed resumes as zero delivered, which
    RE-READS the boundary transaction rather than skipping it. Re-reading is
    the safe direction to be wrong in -- it can duplicate, never lose."""
    from graphsense_v3.db.legacy import decode_page_token

    assert decode_page_token("8270462039621668") == (8270462039621668, 0)


def test_a_block_answers_to_both_count_names() -> None:
    """`_block_from_row` subscripts `transaction_count` for an eth-like
    currency and `no_transactions` for UTXO. v3 renamed the account column to
    `no_transactions` for consistency across the raw tables, so the eth branch
    raised KeyError inside the service -- the same trap `no_blocks` already has
    a mapping for."""
    session = FakeSession(
        lambda cql, params: [Row(block_id=7, no_transactions=3, timestamp=99)]
    )
    shim = LegacyAdapter(
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    row = run(shim.get_block("eth", 7))
    assert row["transaction_count"] == 3
    # ... and the UTXO name survives, because the other branch reads it.
    assert row["no_transactions"] == 3


def test_a_missing_block_is_still_none() -> None:
    shim = LegacyAdapter(
        {
            "eth": dal_for(
                FakeSession(), "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG)
            )
        }
    )
    assert run(shim.get_block("eth", 7)) is None


def test_an_account_address_arrives_already_canonical() -> None:
    """`cannonicalize_address` runs before EVERY DAL call, and its canonical
    form is per family:

        elif currency == "eth":
            return hex_str_to_bytes(strip_0x(address))

    so the adapter is handed bytes for an account network and a string for a
    UTXO one. Assuming a string sent every account call into `strip_0x`, where
    `bytes.startswith("0x")` raises TypeError from inside gslib -- naming
    neither the family nor the adapter, and taking out get_address,
    list_address_txs, the neighbour listings and /links at once."""
    from graphsenselib.utils.address import cannonicalize_address

    shim = LegacyAdapter({})
    text = "0xc765353a888d0e5ffa105bf768c843c1d4824174"
    canonical = cannonicalize_address("eth", text)
    assert isinstance(canonical, bytes), "gslib no longer canonicalises to bytes"
    # The bytes route through unchanged, and agree with encoding the string.
    assert shim._bytes("eth", canonical) == canonical
    assert shim._bytes("eth", text) == canonical


def test_a_utxo_address_is_still_encoded_from_its_string() -> None:
    """UTXO canonicalisation returns a string, so the encode path must stay."""
    shim = LegacyAdapter({})
    assert shim._bytes("ltc", ADDRESS) == encode_address("ltc", ADDRESS)


def test_a_memoryview_is_accepted_too() -> None:
    """The driver hands blobs back in several buffer shapes; a row read out of
    one listing and fed into the next call must not depend on which."""
    shim = LegacyAdapter({})
    raw = encode_address("eth", "0xc765353a888d0e5ffa105bf768c843c1d4824174")
    assert shim._bytes("eth", memoryview(raw)) == raw
    assert shim._bytes("eth", bytearray(raw)) == raw


def _stats_rows(*rows):
    def answer(cql, params):
        if "address_stats" in cql:
            return list(rows)
        return []

    return answer


def test_the_totals_the_service_subscripts_are_present() -> None:
    """`address_from_row` reads row["total_received"] and row["total_spent"] by
    SUBSCRIPT, and `address_stats` has carried both since the schema was
    written -- the READER dropped them, because neither SUMMABLE_STATS nor
    EPOCH_ZERO_ONLY listed them. Every get_address and every neighbour listing
    died on a KeyError raised inside the service."""
    session = FakeSession(
        _stats_rows(
            Row(
                epoch=0,
                total_received={"value": 7, "fiat_values": [1.0, 2.0]},
                total_spent={"value": 3, "fiat_values": [0.5, 1.0]},
            )
        )
    )
    shim = LegacyAdapter(
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    row = run(shim.get_address("eth", "0xaa"))
    # Attributes, not keys: `to_values` reads .value/.fiat_values off a UDT.
    assert row["total_received"].value == 7
    # And LABELLED, not positional: `to_values` does r["code"] on every
    # element, so the UDT's bare list of doubles fails inside the service with
    # "'float' object is not subscriptable" -- on every address and neighbour.
    assert row["total_received"].fiat_values == [
        {"code": "eur", "value": 1.0},
        {"code": "usd", "value": 2.0},
    ]
    assert row["total_spent"].value == 3


def test_the_totals_sum_over_the_epoch_slice() -> None:
    """Epoch 0 is the compacted base and later epochs are deltas -- the same
    argument the counts already follow. Reading one row understates an address
    the incremental path has touched, and the fiat list adds POSITIONALLY."""
    session = FakeSession(
        _stats_rows(
            Row(
                epoch=0,
                total_received={"value": 7, "fiat_values": [1.0, 2.0]},
                total_spent={"value": 0, "fiat_values": []},
            ),
            Row(
                epoch=3,
                total_received={"value": 5, "fiat_values": [0.5, 1.5]},
                total_spent={"value": 0, "fiat_values": []},
            ),
        )
    )
    shim = LegacyAdapter(
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    row = run(shim.get_address("eth", "0xaa"))
    assert row["total_received"].value == 12
    assert row["total_received"].fiat_values == [
        {"code": "eur", "value": 1.5},
        {"code": "usd", "value": 3.5},
    ]


def test_an_address_that_received_nothing_reports_zero_not_a_missing_key() -> None:
    """Absent means zero. The service subscripts the key either way, so a
    missing one is a KeyError several layers from its cause."""
    session = FakeSession(_stats_rows(Row(epoch=0)))
    shim = LegacyAdapter(
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    row = run(shim.get_address("eth", "0xaa"))
    assert row["total_received"].value == 0
    assert row["total_received"].fiat_values == []
    assert row["total_tokens_received"] is None


TX_HASH = b"\xab" * 32
FROM_TX, TO_TX = b"\x01" * 20, b"\x02" * 20
FROM_TRACE, TO_TRACE = b"\x03" * 20, b"\x04" * 20
TRACE_INPUT = b"\xbe\xef"
FROM_LOG, TO_LOG = b"\x05" * 20, b"\x06" * 20


def _account_listing(
    currency="ETH",
    trace_index=7,
    log_index=None,
    trace_address="",
    trace_type="call",
):
    """One listing row, plus the transaction, trace and log behind it."""
    ref = SimpleNamespace(trace_index=trace_index, log_index=log_index)
    tx_id = (12 << 32) + 1

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            return [
                Row(
                    tx_id=tx_id,
                    value=5,
                    balance=None,
                    is_outgoing=True,
                    currency=currency,
                    tx_reference=ref,
                )
            ]
        if ".transaction" in cql:
            return [
                Row(
                    tx_id=tx_id,
                    tx_hash=TX_HASH,
                    block_id=12,
                    block_timestamp=1700,
                    first_trace_index=7,
                    from_address=FROM_TX,
                    to_address=TO_TX,
                    input=b"\xde\xad",
                    receipt_gas_used=21000,
                    receipt_effective_gas_price=1000,
                )
            ]
        if ".trace" in cql:
            return [
                Row(
                    block_id=12,
                    trace_index=trace_index,
                    from_address=FROM_TRACE,
                    to_address=TO_TRACE,
                    trace_address=trace_address,
                    trace_type=trace_type,
                    input=TRACE_INPUT,
                )
            ]
        if ".log" in cql:
            return [
                Row(
                    block_id=12,
                    log_index=log_index,
                    topics=[
                        b"\x00" * 32,
                        b"\x00" * 12 + FROM_LOG,
                        b"\x00" * 12 + TO_LOG,
                    ],
                )
            ]
        return []

    return FakeSession(rows)


def _listing(session):
    shim = LegacyAdapter(
        {"eth": dal_for(session, "eth_raw_v3_t", "eth_derived_v3_t", dict(CONFIG))}
    )
    rows, _ = run(shim.list_address_txs("eth", "0xaa"))
    return rows


def test_an_account_listing_row_carries_the_type_the_service_subscripts() -> None:
    """`_tx_account_from_row` reads row["type"] by SUBSCRIPT, and `_as_v2_txs`
    built UTXO rows for both families -- so every account listing died on a
    KeyError inside the service."""
    row = _listing(_account_listing())[0]
    assert row["type"] == "external"
    assert row["tx_hash"] == TX_HASH
    assert row["height"] == 12 and row["timestamp"] == 1700
    assert "coinbase" not in row, "a UTXO field on an account row"


def test_an_internal_call_reports_the_TRACE_ends_not_the_transaction_s() -> None:
    """The outer transaction's from/to are the wrong counterparty for an
    internal call. v2 fetches the trace for exactly this."""
    row = _listing(_account_listing(trace_index=9, trace_address="0"))[0]
    assert row["type"] == "internal"
    assert row["from_address"] == FROM_TRACE
    assert row["to_address"] == TO_TRACE
    assert row["trace_index"] == 9


def test_the_root_is_the_trace_with_an_empty_trace_address() -> None:
    """v2's test, and the one that lives ON the row (`cassandra.py:5053`):
    trace_address is the path down the call tree, so the root's is empty.
    trace_index == first_trace_index says the same thing, but by comparing two
    rows rather than reading one."""
    row = _listing(_account_listing(trace_index=9, trace_address=""))[0]
    assert row["type"] == "external"


def test_the_input_reported_is_the_TRACE_s_not_the_transaction_s() -> None:
    """They differ for every internal call, which is most of this listing.
    `raw.trace` carries `input` for eth, so reading the transaction's was a
    wrong value where a right one was available."""
    row = _listing(_account_listing(trace_index=9, trace_address="0"))[0]
    assert row["input"] == TRACE_INPUT != b"\xde\xad"


def test_a_token_transfer_reports_the_LOG_ends() -> None:
    """A token transfer's two ends are indexed topics 1 and 2, and they are
    not the transaction's from/to either."""
    session = _account_listing(currency="USDT", trace_index=None, log_index=4)
    row = _listing(session)[0]
    assert row["type"] == "erc20"
    assert row["token_tx_id"] == 4
    assert row["currency"] == "USDT"
    assert row["from_address"] == FROM_LOG
    assert row["to_address"] == TO_LOG


def test_the_fee_is_gas_used_times_the_price_actually_paid() -> None:
    """What v2 reports. v3 has the receipt fields, so unlike contract_creation
    this needs no schema change."""
    assert _listing(_account_listing())[0]["fee"] == 21000 * 1000


def test_contract_creation_comes_from_the_trace_type() -> None:
    """NOT a schema gap: `definitions._TRACE_EXTRA` gives eth's trace
    `trace_type`, and the raw loader writes it. The reader was hardcoding
    None."""
    assert _listing(_account_listing())[0]["contract_creation"] is False
    created = _account_listing(trace_type="create")
    assert _listing(created)[0]["contract_creation"] is True


def test_the_fee_is_reported_only_on_the_external_row() -> None:
    """The fee belongs to the TRANSACTION. v2 sets it only where the type is
    external (`cassandra.py:5090`); on every internal and token row it would
    bill the same gas once per transfer."""
    internal = _listing(_account_listing(trace_index=9, trace_address="0"))[0]
    assert "fee" not in internal
    token = _account_listing(currency="USDT", trace_index=None, log_index=4)
    assert "fee" not in _listing(token)[0]


def test_an_outgoing_account_row_is_signed_negative() -> None:
    """v2 signs by direction on this listing (`cassandra.py:4949`), as the
    UTXO branch already did. v3 stores the magnitude and the direction apart,
    so the account branch reported an outgoing transfer as a credit.

    An unbound listing reads both directions, and the direction is in the
    PARTITION KEY rather than on the row -- so the one stored magnitude comes
    back once signed each way."""
    assert sorted(row["value"] for row in _listing(_account_listing())) == [-5, 5]


def _block_listing(**extra):
    """One account transaction in a block."""

    def rows(cql, params):
        if ".transaction" in cql:
            fields = {
                "tx_id": (12 << 32) + 1,
                "tx_hash": TX_HASH,
                "block_id": 12,
                "block_timestamp": 1700,
                "value": 9,
                "from_address": FROM_TX,
                "to_address": TO_TX,
                "input": b"\xde\xad",
                "receipt_gas_used": 21000,
                "receipt_effective_gas_price": 1000,
                "receipt_contract_address": None,
            }
            return [Row(**{**fields, **extra})]
        return []

    shim = LegacyAdapter(
        {"eth": dal_for(FakeSession(rows), "eth_raw_v3_t", "eth_derived_v3_t", CONFIG)}
    )
    return run(shim.list_block_txs("eth", 12))


def test_a_block_listing_on_an_account_chain_carries_the_type() -> None:
    """`std_tx_from_row` reads row["type"] by SUBSCRIPT and `list_block_txs`
    built UTXO rows for both families, so every account block listing died on
    a KeyError inside the service -- the same family blindness `_as_v2_txs`
    had."""
    row = _block_listing()[0]
    assert row["type"] == "external"
    assert row["tx_hash"] == TX_HASH
    assert row["from_address"] == FROM_TX and row["to_address"] == TO_TX
    assert row["fee"] == 21000 * 1000
    assert "inputs" not in row, "a UTXO field on an account row"


def test_a_deployment_names_the_contract_it_created() -> None:
    """A deployment has no recipient; v2 reports the created contract as the
    recipient and marks the row (`cassandra.py:5249-5252`)."""
    created = b"\x07" * 20
    row = _block_listing(to_address=None, receipt_contract_address=created)[0]
    assert row["to_address"] == created
    assert row["contract_creation"] is True


USDT = b"\xda" * 20


def _block_with_a_token_transfer():
    """One transaction, one Transfer log against a configured token."""
    tx_id = (12 << 32) + 1
    transfer = bytes.fromhex(
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )

    def rows(cql, params):
        if "token_configuration" in cql:
            return [Row(currency_ticker="USDT", token_address=USDT, decimals=6)]
        if ".transaction" in cql:
            return [
                Row(
                    tx_id=tx_id,
                    tx_hash=TX_HASH,
                    block_id=12,
                    block_timestamp=1700,
                    value=9,
                    from_address=FROM_TX,
                    to_address=TO_TX,
                    input=b"",
                    receipt_gas_used=21000,
                    receipt_effective_gas_price=1000,
                    receipt_contract_address=None,
                )
            ]
        if ".log" in cql:
            return [
                Row(
                    block_id=12,
                    log_index=3,
                    tx_id=tx_id,
                    address=USDT,
                    topic0=transfer,
                    topics=[
                        transfer,
                        b"\x00" * 12 + FROM_LOG,
                        b"\x00" * 12 + TO_LOG,
                    ],
                    data=(7).to_bytes(32, "big"),
                )
            ]
        return []

    shim = LegacyAdapter(
        {"eth": dal_for(FakeSession(rows), "eth_raw_v3_t", "eth_derived_v3_t", CONFIG)}
    )
    run(shim.preload_token_configuration())
    return run(shim.list_block_txs("eth", 12))


def test_a_block_listing_interleaves_the_token_transfers() -> None:
    """v2 lists a block with include_token_txs=True, so a token transfer is a
    row of its own behind the transaction that made it. They are not stored as
    rows anywhere -- they are Transfer logs -- so the block's logs are read
    once and decoded."""
    rows = _block_with_a_token_transfer()
    assert [row["type"] for row in rows] == ["external", "erc20"]

    token = rows[1]
    assert token["currency"] == "USDT"
    assert token["value"] == 7
    assert token["from_address"] == FROM_LOG and token["to_address"] == TO_LOG
    assert token["token_tx_id"] == 3
    # The transaction paid the gas once; the transfer is not charged again.
    assert "fee" not in token
    assert token["tx_hash"] == TX_HASH and token["timestamp"] == 1700


def test_block_by_date_asks_for_the_block_strictly_after() -> None:
    """v2's own version of this query is `timestamp >= %s`, but nothing runs
    it: with the flag off v2 answers /blocks/by_date by binary search, whose
    exact-match block becomes `before_block`. The inclusive bound made it
    `after_block` and shifted BOTH heights down one. The REST answer is the
    contract, not the query."""
    session = FakeSession(
        lambda cql, params: (
            [Row(block_id=1925, timestamp=1788442898)] if "block_by_date" in cql else []
        )
    )
    shim = LegacyAdapter({"ltc": dal_for(session, RAW, DERIVED, dict(CONFIG))})
    run(shim.get_block_by_date_allow_filtering("ltc", 1788442000))
    cql, _params = session.seen[0]
    assert "timestamp > %s" in cql and "timestamp >= %s" not in cql


def _unpegged_listing(peg=None, rate_rows=None):
    """One listing row for an unpegged token, plus that token's own rate."""
    ref = SimpleNamespace(trace_index=None, log_index=4)
    tx_id = (12 << 32) + 1
    seen: list = []

    def rows(cql, params):
        if "token_configuration" in cql:
            return [
                Row(
                    currency_ticker="SHIB",
                    token_address=b"\x5c" * 20,
                    decimal_divisor=10**18,
                    peg_currency=peg,
                )
            ]
        if "address_stats" in cql:
            return [Row(epoch=0, out_tx_page_max=0, in_tx_page_max=0)]
        if "address_transactions" in cql:
            return [
                Row(
                    tx_id=tx_id,
                    value=5,
                    balance=None,
                    is_outgoing=True,
                    currency="SHIB",
                    tx_reference=ref,
                )
            ]
        if ".transaction" in cql:
            return [
                Row(tx_id=tx_id, tx_hash=TX_HASH, block_id=12, block_timestamp=1700)
            ]
        if ".log" in cql:
            return [Row(block_id=12, log_index=4, topics=[b"\x00" * 32] * 3)]
        if "exchange_rates" in cql:
            seen.append(params)
            return (
                rate_rows if rate_rows is not None else [Row(fiat_values={"usd": 0.5})]
            )
        return []

    shim = LegacyAdapter(
        {"eth": dal_for(FakeSession(rows), "eth_raw_v3_t", "eth_derived_v3_t", CONFIG)}
    )
    run(shim.preload_token_configuration())
    listed, _ = run(shim.list_address_txs("eth", "0xaa"))
    return listed, seen


def test_an_unpegged_token_row_carries_its_own_rate() -> None:
    """Without it `map_rates_for_peged_tokens` has nothing to convert with and
    returns EMPTY fiat -- not a wrong number, but no number, on every row of
    that token. v2 attaches the rate in `_attach_token_tx_rates`."""
    rows, reads = _unpegged_listing()
    assert rows[0]["type"] == "erc20"
    assert rows[0]["token_rate"] == [
        {"code": "eur", "value": 0.0},
        {"code": "usd", "value": 0.5},
    ]
    # One lookup for the distinct (asset, block), keyed by the TICKER.
    assert len(reads) == 1 and reads[0][0] == "SHIB"


def test_a_pegged_token_row_is_not_looked_up() -> None:
    """Its branch in `map_rates_for_peged_tokens` returns before `token_rate`
    is read, so the lookup would be a read per page for nothing. v2 still sets
    the key on every erc20 row, so this does too."""
    rows, reads = _unpegged_listing(peg="USD")
    assert reads == []
    # Absent, not None: with nothing to look up v2 returns before it sets the
    # key either (`cassandra.py:5166`).
    assert "token_rate" not in rows[0]


def test_a_link_row_carries_the_token_rate_too() -> None:
    """`/links` is the same builder as the listing (`cassandra.py:2591-2616`),
    so an unpegged token loses its fiat there in exactly the same way. The
    height is spelled `block_id` on this row shape, not `height`."""
    seen: list = []

    def rows(cql, params):
        if "token_configuration" in cql:
            return [
                Row(
                    currency_ticker="SHIB",
                    token_address=b"\x5c" * 20,
                    decimal_divisor=10**18,
                    peg_currency=None,
                )
            ]
        if "link_page_max" in cql:
            return [Row(link_page_max=0)]
        if "address_link_transactions" in cql:
            return [
                Row(
                    tx_id=(12 << 32) + 1,
                    tx_reference=SimpleNamespace(trace_index=None, log_index=4),
                    currency="SHIB",
                    value=5,
                )
            ]
        if ".transaction" in cql:
            return [
                Row(
                    tx_id=(12 << 32) + 1,
                    tx_hash=b"\xab" * 32,
                    block_id=12,
                    block_timestamp=1700,
                )
            ]
        if "exchange_rates" in cql:
            seen.append(params)
            return [Row(fiat_values={"usd": 0.5})]
        return []

    shim = LegacyAdapter(
        {"eth": dal_for(FakeSession(rows), "eth_raw_v3_t", "eth_derived_v3_t", CONFIG)}
    )
    run(shim.preload_token_configuration())
    links, _ = run(shim.list_address_links("eth", "0xaa", "0xbb"))
    assert links[0]["token_rate"] == [
        {"code": "eur", "value": 0.0},
        {"code": "usd", "value": 0.5},
    ]
    assert len(seen) == 1


def _many_neighbours(count: int, *, stats=True):
    """``count`` edges spread over the relation buckets, plus their stats."""
    addresses = [bytes([0]) + index.to_bytes(19, "big") for index in range(count)]
    seen: list = []

    def rows(cql, params):
        if "relations" in cql:
            seen.append((cql, params))
            bucket = params[1]
            after = params[2] if len(params) > 2 else None
            return [
                Row(dst_address=a, no_transactions=3, epoch=0)
                for a in addresses
                if zlib.crc32(a) % CONFIG["relation_buckets"] == bucket
                and (after is None or a > after)
            ]
        if "address_stats" in cql and stats:
            return [Row(epoch=0)]
        return []

    shim = LegacyAdapter(
        {"eth": dal_for(FakeSession(rows), "eth_raw_v3_t", "eth_derived_v3_t", CONFIG)}
    )
    return shim, seen


def test_a_neighbour_listing_is_paged() -> None:
    """The service builds a full address row per neighbour of the page, so an
    unpaged listing is one round trip per edge -- 3573 of them for WETH, which
    is a request that never returns rather than a slow one. pagesize was
    accepted and ignored."""
    shim, _ = _many_neighbours(50)
    rows, token = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20))
    assert len(rows) == 20
    assert token is not None


def test_a_short_neighbour_page_has_no_cursor() -> None:
    shim, _ = _many_neighbours(5)
    rows, token = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20))
    assert len(rows) == 5 and token is None


def test_the_neighbour_cursor_resumes_where_the_page_ended() -> None:
    """Ordered by the far address, which every bucket clusters on -- so the
    cursor is that address and the next page reads LESS rather than re-reading
    and discarding."""
    shim, seen = _many_neighbours(50)
    first, token = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20))
    assert token == bytes(first[-1]["dst_address"]).hex()

    second, _ = run(
        shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20, page=token)
    )
    assert [r["dst_address"] for r in second][:1] > [r["dst_address"] for r in first][
        -1:
    ]
    # The bound is in CQL, not a client-side filter.
    resumed = [cql for cql, _p in seen if "dst_address > %s" in cql]
    assert resumed


def test_a_neighbour_listing_is_ordered_by_address() -> None:
    """Arrival order is whatever `_gather` returned the buckets in: stable
    within a call, meaningless across them -- so a cursor built on it would
    skip and repeat neighbours."""
    shim, _ = _many_neighbours(50)
    rows, _ = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=50))
    addresses = [bytes(r["dst_address"]) for r in rows]
    assert addresses == sorted(addresses)


def test_each_neighbour_carries_the_address_row_the_service_would_refetch() -> None:
    """`addresses_service` reads {dst}_address_row and falls back to a per-row
    `await self.get_address(...)` when it is missing -- the N+1 its own comment
    says was removed."""
    shim, _ = _many_neighbours(3)
    rows, _ = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20))
    assert all("dst_address_row" in row for row in rows)
    assert rows[0]["dst_address_row"]["total_received"].value == 0


def test_a_neighbour_with_no_stats_row_is_left_for_the_service_to_fetch() -> None:
    """`is None` is what the service tests, so an absent key sends it down the
    fallback -- the honest answer for an edge whose far side has no row."""
    shim, _ = _many_neighbours(3, stats=False)
    rows, _ = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20))
    assert all("dst_address_row" not in row for row in rows)


def _address_with_txs(first=7, last=9, transactions=True):
    tx_ids = {first: b"\xf1" * 32, last: b"\xf9" * 32}

    def rows(cql, params):
        if "address_stats" in cql:
            return [Row(epoch=0, first_tx_id=first, last_tx_id=last)]
        if ".transaction" in cql and transactions:
            tx_id = params[1]
            return [
                Row(
                    tx_id=tx_id,
                    tx_hash=tx_ids[tx_id],
                    block_id=12,
                    block_timestamp=1700,
                )
            ]
        return []

    shim = LegacyAdapter(
        {"ltc": dal_for(FakeSession(rows), RAW, DERIVED, dict(CONFIG))}
    )
    return run(shim.get_address("ltc", ADDRESS))


def test_an_address_resolves_its_first_and_last_transaction() -> None:
    """v3's `address_stats` keeps the two tx_ids and v2 keeps them too -- but
    v2 resolves them to a height, timestamp and hash in `finish_address`
    (`cassandra.py:4077`). Reporting the ids alone left every address with
    `first_tx: null` in the REST body."""
    row = _address_with_txs()
    # Attributes, not keys: `address_from_row` reads .height/.timestamp and
    # calls .hex() on the hash.
    assert row["first_tx"].tx_hash == b"\xf1" * 32
    assert row["last_tx"].tx_hash == b"\xf9" * 32
    assert row["first_tx"].height == 12 and row["first_tx"].timestamp == 1700


def test_a_missing_transaction_leaves_the_summary_none_rather_than_raising() -> None:
    """The id came from this keyspace's own stats row, so its absence is a
    torn keyspace -- but this runs once per neighbour of a listing, and one
    bad edge should not take the page down with it."""
    row = _address_with_txs(transactions=False)
    assert row["first_tx"] is None and row["last_tx"] is None
    # The ids are still reported, so the inconsistency is visible.
    assert row["first_tx_id"] == 7 and row["last_tx_id"] == 9


def test_an_address_v3_can_answer_for_is_reported_clean() -> None:
    """v2 says "dirty" while its delta updater holds the address in flight and
    "clean" otherwise. v3 has no incremental writer, so nothing is ever in
    flight -- and None read as a missing field rather than as a settled one."""
    assert _address_with_txs()["status"] == "clean"


def test_a_neighbour_page_reads_its_transactions_in_one_batch() -> None:
    """Four point reads per neighbour -- stats, balance, first tx, last tx --
    is three dependent waves for a page, and measured 1.6x v2 on LTC hubs.
    The first/last tx ids live ON the stats row, so the page can read every
    one of its transactions in a single batch once the stats are in."""
    seen: list = []

    def rows(cql, params):
        if "relations" in cql:
            return [
                Row(dst_address=bytes([i]) + bytes(19), no_transactions=1, epoch=0)
                for i in range(3)
            ]
        if "address_stats" in cql:
            return [Row(epoch=0, first_tx_id=7, last_tx_id=9)]
        if ".transaction" in cql:
            seen.append(params)
            return [
                Row(
                    tx_id=params[1], tx_hash=b"\xf0" * 32, block_id=1, block_timestamp=5
                )
            ]
        return []

    shim = LegacyAdapter(
        {"eth": dal_for(FakeSession(rows), "eth_raw_v3_t", "eth_derived_v3_t", CONFIG)}
    )
    found, _ = run(shim.list_neighbors("eth", b"\xaa" * 20, True, pagesize=20))
    assert len(found) == 3
    assert all(r["dst_address_row"]["first_tx"].tx_hash == b"\xf0" * 32 for r in found)
    # Three neighbours sharing two tx ids: two reads, not six.
    assert len(seen) == 2, f"one read per (page, tx id), got {len(seen)}"
