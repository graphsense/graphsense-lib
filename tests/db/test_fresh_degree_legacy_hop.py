"""Cluster degrees for fresh entities come from the legacy table.

The fresh recompute no longer derives ``in_degree``/``out_degree`` (that
required scanning the address-relations tables), so recomputed
``fresh_cluster_stats`` rows carry null degrees. REST fills them via the
root-address hop: the fresh root's address row still carries its LEGACY
cluster id, whose ``cluster`` row has the degrees. Clusters without a legacy
row (root postdates the last full transform, hence small) fall back to
member-summed address degrees.

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


def _degrees_null_row(cluster_id):
    return {
        "cluster_id": cluster_id,
        "no_addresses": 3,
        "min_address_id": cluster_id,
        "no_incoming_txs": 4,
        "no_outgoing_txs": 2,
        "in_degree": None,
        "out_degree": None,
        "first_tx_id": 10,
        "last_tx_id": 20,
        "total_received": Values(600, [60.0, 6.0]),
        "total_spent": Values(100, [10.0, 1.0]),
    }


_PENDING_ROW = {
    "cluster_id": 300,
    "no_addresses": 2,
    "min_address_id": 300,
    "no_incoming_txs": None,
    "no_outgoing_txs": None,
    "in_degree": None,
    "out_degree": None,
    "first_tx_id": None,
    "last_tx_id": None,
    "total_received": None,
    "total_spent": None,
}


def _make_self(
    stats_rows_by_id,
    address_rows_by_id,
    legacy_cluster_rows_by_id,
    membership_by_id=None,
):
    membership_by_id = membership_by_id or {}

    async def execute_async(
        currency, keyspace, query, params, paging_state=None, fetch_size=None
    ):
        if "FROM address " in query or "FROM address\n" in query:
            address_id = params[1]
            row = address_rows_by_id.get(address_id)
            return _Result([dict(row)] if row else [])
        if "fresh_cluster_addresses" in query:
            cluster_id = params[1]
            return _Result(
                [{"address_id": a} for a in membership_by_id.get(cluster_id, [])]
            )
        if "FROM cluster " in query:
            cluster_id = params[1]
            row = legacy_cluster_rows_by_id.get(cluster_id)
            return _Result([dict(row)] if row else [])
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
    )
    ns._sum_currency = Cassandra._sum_currency.__get__(ns)
    ns._fresh_fill_degrees = Cassandra._fresh_fill_degrees.__get__(ns)
    ns._fresh_entity_from_members = Cassandra._fresh_entity_from_members.__get__(ns)
    ns._fresh_heal_pending_entities = Cassandra._fresh_heal_pending_entities.__get__(ns)
    return ns


def _get(s, entity):
    return asyncio.run(Cassandra.get_entity(s, "ltc", entity))


def test_degrees_filled_from_legacy_cluster(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self(
        stats_rows_by_id={100: _degrees_null_row(100)},
        address_rows_by_id={100: {"address_id": 100, "cluster_id": 555}},
        legacy_cluster_rows_by_id={555: {"in_degree": 7, "out_degree": 9}},
    )
    entity = _get(s, 100)
    assert entity["in_degree"] == 7
    assert entity["out_degree"] == 9
    # fresh stats untouched
    assert entity["total_received"] == Values(600, [60.0, 6.0])
    assert entity["no_incoming_txs"] == 4


def test_member_sum_fallback_without_legacy_row(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self(
        stats_rows_by_id={100: _degrees_null_row(100)},
        address_rows_by_id={
            100: {
                "address_id": 100,
                "cluster_id": None,
                "in_degree": 2,
                "out_degree": 1,
            },
            101: {"address_id": 101, "in_degree": 3, "out_degree": 4},
        },
        legacy_cluster_rows_by_id={},
        membership_by_id={100: [100, 101]},
    )
    entity = _get(s, 100)
    assert entity["in_degree"] == 5
    assert entity["out_degree"] == 5


def test_fully_pending_row_uses_member_synthesis(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    member = {
        "address_id": 300,
        "cluster_id": 555,
        "no_incoming_txs": 1,
        "no_outgoing_txs": 1,
        "in_degree": 1,
        "out_degree": 1,
        "first_tx_id": 5,
        "last_tx_id": 6,
        "total_received": Values(10, [1.0, 0.1]),
        "total_spent": Values(0, [0.0, 0.0]),
    }
    s = _make_self(
        stats_rows_by_id={300: _PENDING_ROW},
        address_rows_by_id={300: member},
        legacy_cluster_rows_by_id={555: {"in_degree": 99, "out_degree": 99}},
        membership_by_id={300: [300]},
    )
    entity = _get(s, 300)
    # member synthesis, not the legacy hop
    assert entity["in_degree"] == 1
    assert entity["total_received"] == Values(10, [1.0, 0.1])


def test_full_row_untouched(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    row = _degrees_null_row(100)
    row["in_degree"] = 3
    row["out_degree"] = 4
    s = _make_self(
        stats_rows_by_id={100: row},
        address_rows_by_id={},
        legacy_cluster_rows_by_id={},
    )
    entity = _get(s, 100)
    assert entity["in_degree"] == 3
    assert entity["out_degree"] == 4


def test_no_hop_when_fresh_disabled(monkeypatch):
    monkeypatch.setenv(_ENV, "false")
    # flag off: the stats source IS the legacy `cluster` table, so the
    # (hypothetical) degrees-null row lives there; no fill must run on it.
    s = _make_self(
        stats_rows_by_id={},
        address_rows_by_id={100: {"address_id": 100, "cluster_id": 555}},
        legacy_cluster_rows_by_id={100: _degrees_null_row(100)},
    )
    entity = _get(s, 100)
    assert entity["in_degree"] is None


def test_bulk_heal_mixes_synthesis_and_degree_fill(monkeypatch):
    monkeypatch.setenv(_ENV, "true")
    s = _make_self(
        stats_rows_by_id={},
        address_rows_by_id={100: {"address_id": 100, "cluster_id": 555}},
        legacy_cluster_rows_by_id={555: {"in_degree": 7, "out_degree": 9}},
    )
    full = _degrees_null_row(200)
    full["in_degree"] = 1
    full["out_degree"] = 1
    rows = [full, _degrees_null_row(100)]
    healed = asyncio.run(s._fresh_heal_pending_entities("ltc", rows))
    assert healed[0]["in_degree"] == 1
    assert healed[1]["in_degree"] == 7
    assert healed[1]["out_degree"] == 9
