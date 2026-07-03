"""Stats-pending fresh cluster rows must be served, not 500.

Delta-update clustering writes ``fresh_cluster_stats`` rows with only
``no_addresses``/``min_address_id``; the value columns stay null until the
next recompute-cluster-stats run. REST synthesizes those entities from the
member address rows: money columns and first/last tx exactly, tx counts and
degrees as member sums (a documented over-count healed by recompute).

DB-free: real ``Cassandra`` methods bound to a fake self.
"""

import asyncio
from collections import namedtuple
from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import Cassandra

_ENV = "GRAPHSENSE_FRESH_CLUSTERING_ENABLED"

Values = namedtuple("Values", ["value", "fiat_values"])


class _Result:
    def __init__(self, rows):
        self.current_rows = rows
        self.paging_state = None

    def one(self):
        return self.current_rows[0] if self.current_rows else None


def _address_row(address_id, received, spent, first, last, txs=1, deg=1):
    return {
        "address_id": address_id,
        "no_incoming_txs": txs,
        "no_outgoing_txs": txs,
        "in_degree": deg,
        "out_degree": deg,
        "first_tx_id": first,
        "last_tx_id": last,
        "total_received": Values(received, [received / 10.0, received / 100.0]),
        "total_spent": Values(spent, [spent / 10.0, spent / 100.0]),
    }


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


def _make_self(stats_rows_by_id, membership_by_id, address_rows_by_id):
    async def execute_async(
        currency, keyspace, query, params, paging_state=None, fetch_size=None
    ):
        if "fresh_cluster_addresses" in query or "cluster_addresses" in query:
            cluster_id = params[1]
            return _Result(
                [{"address_id": a} for a in membership_by_id.get(cluster_id, [])]
            )
        cluster_id = params[1]
        row = stats_rows_by_id.get(cluster_id)
        return _Result([dict(row)] if row else [])

    async def concurrent_with_args(currency, keyspace, query, params):
        return [
            dict(address_rows_by_id[address_id])
            for _, address_id in params
            if address_id in address_rows_by_id
        ]

    async def finish_entities(currency, rows, with_txs=True):
        return list(rows)

    ns = SimpleNamespace(
        parameters={"ltc": {"fiat_currencies": ["EUR", "USD"]}},
        get_id_group=lambda keyspace, id_: 0,
        execute_async=execute_async,
        concurrent_with_args=concurrent_with_args,
        finish_entities=finish_entities,
        _fresh_singleton_entity=lambda currency, id_: _never(),
    )
    ns._sum_currency = Cassandra._sum_currency.__get__(ns)
    ns._zero_values = Cassandra._zero_values.__get__(ns)
    ns._fresh_entity_from_members = Cassandra._fresh_entity_from_members.__get__(ns)
    ns._fresh_heal_pending_entities = Cassandra._fresh_heal_pending_entities.__get__(ns)
    return ns


async def _never():
    raise AssertionError("must not be called")


_MEMBERS = {
    100: [100, 101],
}
_ADDRESSES = {
    100: _address_row(100, received=1000, spent=300, first=50, last=90, txs=4, deg=2),
    101: _address_row(101, received=200, spent=0, first=40, last=60, txs=1, deg=1),
}


def test_pending_entity_synthesized_from_members(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self({100: _PENDING_ROW}, _MEMBERS, _ADDRESSES)
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 100))
    assert entity["no_addresses"] == 2
    assert entity["total_received"] == Values(1200, [120.0, 12.0])
    assert entity["total_spent"] == Values(300, [30.0, 3.0])
    assert entity["first_tx_id"] == 40
    assert entity["last_tx_id"] == 90
    # member sums, documented over-count until recompute
    assert entity["no_incoming_txs"] == 5
    assert entity["in_degree"] == 3


def test_full_row_served_untouched(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self({200: _FULL_ROW}, {}, {})
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 200))
    assert entity["total_received"] == Values(500, [50.0, 5.0])
    assert entity["no_incoming_txs"] == 7


def test_no_synthesis_when_fresh_disabled(monkeypatch):
    monkeypatch.setenv(_ENV, "false")
    s = _make_self({100: _PENDING_ROW}, _MEMBERS, _ADDRESSES)
    entity = asyncio.run(Cassandra.get_entity(s, "ltc", 100))
    assert entity["total_received"] is None


def test_bulk_list_heals_only_pending_rows(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self({100: _PENDING_ROW}, _MEMBERS, _ADDRESSES)
    rows = [dict(_FULL_ROW), dict(_PENDING_ROW)]
    healed = asyncio.run(s._fresh_heal_pending_entities("ltc", rows))
    assert healed[0] == _FULL_ROW
    assert healed[1]["total_received"] == Values(1200, [120.0, 12.0])


def test_finish_address_backstop_zero_fills(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self({}, {}, {})
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
