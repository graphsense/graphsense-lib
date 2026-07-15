"""Driver-side exchange_rates copy for delta-to-raw rebuilds.

The Delta lake carries no exchange rates, so a rebuild into a fresh raw
keyspace copies them from the previous raw keyspace.
"""

from collections import namedtuple

from graphsenselib.transformation.cli import copy_exchange_rates

_Row = namedtuple("_Row", ["date", "fiat_values"])


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.selects = []
        self.prepared = []
        self.inserts = []

    def execute(self, statement, params=None):
        if isinstance(statement, str):
            self.selects.append(statement)
            return list(self._rows)
        self.inserts.append((statement, params))
        return []

    def prepare(self, cql):
        self.prepared.append(cql)
        return object()


def test_copy_exchange_rates_copies_all_rows():
    rows = [
        _Row("2024-01-01", {"EUR": 60.0, "USD": 65.0}),
        _Row("2024-01-02", {"EUR": 61.0, "USD": 66.0}),
    ]
    session = _FakeSession(rows)

    count = copy_exchange_rates(session, "ltc_raw_old", "ltc_raw_new")

    assert count == 2
    assert session.selects == [
        "SELECT date, fiat_values FROM ltc_raw_old.exchange_rates"
    ]
    assert len(session.prepared) == 1
    assert "INSERT INTO ltc_raw_new.exchange_rates" in session.prepared[0]
    assert [params for _, params in session.inserts] == [
        ("2024-01-01", {"EUR": 60.0, "USD": 65.0}),
        ("2024-01-02", {"EUR": 61.0, "USD": 66.0}),
    ]


def test_copy_exchange_rates_empty_source():
    session = _FakeSession([])
    assert copy_exchange_rates(session, "ltc_raw_old", "ltc_raw_new") == 0
    assert session.inserts == []
