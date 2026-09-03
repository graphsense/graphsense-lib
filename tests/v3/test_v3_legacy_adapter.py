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


def test_block_timestamp_comes_from_the_block_row() -> None:
    shim, _ = adapter(
        lambda cql, params: (
            [Row(block_id=7, timestamp=1331578610)] if "block" in cql else []
        )
    )
    assert run(shim.get_block_timestamp("ltc", 7)) == 1331578610
