"""Fresh cluster entities are served from complete stats rows.

The incremental updater and the recompute both write every ``fresh_cluster_stats``
stat column (member sums), so REST no longer re-derives money / tx-count columns
from member address rows on read — the ``_fresh_entity_from_members`` fallback was
removed. A stored full row is served as-is (degrees still come from the legacy
hop, tested in ``test_fresh_degree_legacy_hop``). Legacy ids are untouched.

DB-free: real ``Cassandra`` methods bound to a fake self.
"""

import asyncio
from collections import namedtuple
from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.utils.constants import FRESH_CLUSTER_ID_OFFSET as _OFF

Values = namedtuple("Values", ["value", "fiat_values"])


class _Result:
    def __init__(self, rows):
        self.current_rows = rows
        self.paging_state = None

    def one(self):
        return self.current_rows[0] if self.current_rows else None


# A stats-pending row (rich columns null) — the shape a pre-member-sum delta or a
# not-yet-recomputed keyspace can still hold. It is served as stored now, never
# synthesized; the legacy id space below asserts the same pass-through.
_PENDING_ROW = {
    "cluster_id": 100,
    "no_addresses": 2,
    "min_address_id": 100,
    "no_incoming_txs": None,
    "no_outgoing_txs": None,
    "in_degree": None,
    "out_degree": None,
    "first_tx_id": None,
    "last_tx_id": None,
    "total_received": None,
    "total_spent": None,
}

_FULL_ROW = {
    "cluster_id": 200,
    "no_addresses": 3,
    "min_address_id": 200,
    "no_incoming_txs": 7,
    "no_outgoing_txs": 5,
    "in_degree": 2,
    "out_degree": 2,
    "first_tx_id": 10,
    "last_tx_id": 20,
    "total_received": Values(500, [50.0, 5.0]),
    "total_spent": Values(100, [10.0, 1.0]),
}


def _make_self(stats_rows_by_id):
    async def execute_async(
        currency, keyspace, query, params, paging_state=None, fetch_size=None
    ):
        cluster_id = params[1]
        row = stats_rows_by_id.get(cluster_id)
        return _Result([dict(row)] if row else [])

    async def finish_entities(currency, rows, with_txs=True):
        return list(rows)

    ns = SimpleNamespace(
        parameters={"ltc": {"fiat_currencies": ["EUR", "USD"]}},
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        finish_entities=finish_entities,
        _fresh_singleton_entity=lambda currency, id_: _never(),
    )
    ns._zero_values = Cassandra._zero_values.__get__(ns)
    ns._get_fresh_entity = Cassandra._get_fresh_entity.__get__(ns)
    return ns


async def _never():
    raise AssertionError("must not be called")


def test_full_row_served_untouched():
    s = _make_self({200: _FULL_ROW})
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", _OFF + 200))
    assert entity["total_received"] == Values(500, [50.0, 5.0])
    assert entity["no_incoming_txs"] == 7


def test_no_synthesis_for_legacy_ids():
    # legacy id space serves the cluster row as stored — the fresh tables are a
    # separate id regime and are not consulted here
    s = _make_self({100: _PENDING_ROW})
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 100))
    assert entity["total_received"] is None


def test_finish_address_backstop_zero_fills():
    s = _make_self({})
    s.markup_currency = Cassandra.markup_currency.__get__(s)
    s.markup_values = Cassandra.markup_values.__get__(s)

    async def add_balance(currency, row):
        row["balance"] = 0

    s.add_balance = add_balance
    row = {"total_received": None, "total_spent": None}
    finished = asyncio.run(Cassandra.finish_address(s, "ltc", row, with_txs=False))
    assert finished["total_received"].value == 0
    assert [f["code"] for f in finished["total_received"].fiat_values] == [
        "eur",
        "usd",
    ]
