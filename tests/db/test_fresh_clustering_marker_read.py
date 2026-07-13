"""Regression: the fresh-clustering marker check must issue parameterized CQL.

``is_fresh_clustering_active`` reads the ``state`` table with a *string*
where-value. The plain ``select_one`` path interpolates where-values verbatim
into the statement (placeholder callers like ``?`` / ``%(k)s`` depend on
that), so a string value yields unquoted — syntactically invalid — CQL:
``WHERE key=fresh_clustering_active`` crashed the delta updater with a
Cassandra ``SyntaxException`` on its first marker check against prod. The
check must therefore go through the ``_safe`` (parameterized) path, where the
statement carries a ``%(key)s`` placeholder and the value travels as a bound
parameter.

These bind the real ``select_stmt`` / ``select_safe`` / ``select_one_safe`` /
``is_fresh_clustering_active`` methods to a fake self whose ``_db`` captures
what would be sent to Cassandra — only the driver itself is faked.
"""

from types import SimpleNamespace

from cassandra import InvalidRequest

from graphsenselib.db.analytics import DbReaderMixin, TransformedDb, WithinKeyspace
from graphsenselib.db.state import FRESH_CLUSTERING_ACTIVE_KEY


class _Result:
    def __init__(self, rows):
        self.current_rows = rows


def _make_tdb(rows, raise_invalid=False):
    calls = {}

    class _FakeDriver:
        def execute_safe(self, stmt, params, fetch_size=None):
            if raise_invalid:
                raise InvalidRequest("unconfigured table state")
            calls["stmt"] = stmt
            calls["params"] = params
            return _Result(rows)

    ns = SimpleNamespace(_db=_FakeDriver(), _keyspace="ltc_transformed")
    for owner, name in [
        (WithinKeyspace, "select_stmt"),
        (WithinKeyspace, "get_keyspace"),
        (DbReaderMixin, "select_safe"),
        (DbReaderMixin, "select_one_safe"),
        (DbReaderMixin, "_at_most_one_result"),
        (TransformedDb, "is_fresh_clustering_active"),
    ]:
        setattr(ns, name, getattr(owner, name).__get__(ns))
    return ns, calls


def test_marker_value_is_bound_not_interpolated():
    tdb, calls = _make_tdb(rows=[{"key": FRESH_CLUSTERING_ACTIVE_KEY}])
    assert tdb.is_fresh_clustering_active() is True
    # the statement must carry a placeholder, never the raw (unquoted) string
    assert "%(key)s" in calls["stmt"]
    assert FRESH_CLUSTERING_ACTIVE_KEY not in calls["stmt"]
    assert calls["params"] == {"key": FRESH_CLUSTERING_ACTIVE_KEY}


def test_no_marker_row_is_inactive():
    tdb, _ = _make_tdb(rows=[])
    assert tdb.is_fresh_clustering_active() is False


def test_missing_state_table_is_inactive():
    tdb, _ = _make_tdb(rows=[], raise_invalid=True)
    assert tdb.is_fresh_clustering_active() is False
