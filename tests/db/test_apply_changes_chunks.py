from cassandra.cluster import OperationTimedOut

from graphsenselib.datatypes import DbChangeType
from graphsenselib.db import DbChange
from graphsenselib.db.analytics import DbWriterMixin


class _Prepared:
    def __init__(self, log):
        self._log = log

    def bind(self, data):
        self._log.append(data["id"])
        return ("bound", data["id"])


class _FakeCassandra:
    def __init__(self, fail_first_call_containing=None):
        self.binds = []
        self.calls = []
        self._fail_on = fail_first_call_containing
        self._failed = False

    def get_prepared_statement(self, stmt):
        return _Prepared(self.binds)

    def execute_statements(self, stmts, concurrency):
        ids = [i for _, i in stmts]
        self.calls.append(("concurrent", ids))
        if self._fail_on in ids and not self._failed:
            self._failed = True
            raise OperationTimedOut("timeout")

    def execute_statements_atomic(self, stmts):
        self.calls.append(("atomic", [i for _, i in stmts]))


class _Writer(DbWriterMixin):
    def __init__(self, db):
        self._db = db

    def get_keyspace(self):
        return "ks"


def _changes(n):
    return [DbChange(DbChangeType.NEW, "t", {"id": i}) for i in range(n)]


def test_non_atomic_changes_are_bound_and_written_per_chunk():
    # Binding a whole batch up front held a few KB per change at once;
    # each chunk is now bound right before it is written.
    db = _FakeCassandra()
    _Writer(db).apply_changes(_changes(7), atomic=False, chunk_size=3)
    assert db.calls == [
        ("concurrent", [0, 1, 2]),
        ("concurrent", [3, 4, 5]),
        ("concurrent", [6]),
    ]
    assert db.binds == list(range(7))


def test_atomic_changes_go_out_as_one_batch():
    db = _FakeCassandra()
    _Writer(db).apply_changes(_changes(7), atomic=True, chunk_size=3)
    assert db.calls == [("atomic", list(range(7)))]


def test_failed_chunk_is_retried_alone(monkeypatch):
    import graphsenselib.db.analytics as analytics

    monkeypatch.setattr(analytics, "wait_exponential", lambda **kw: lambda rs: 0)
    db = _FakeCassandra(fail_first_call_containing=4)
    result = _Writer(db).apply_changes(_changes(7), atomic=False, chunk_size=3)
    assert db.calls == [
        ("concurrent", [0, 1, 2]),
        ("concurrent", [3, 4, 5]),
        ("concurrent", [3, 4, 5]),
        ("concurrent", [6]),
    ]
    # The retry re-sends the statements bound for that chunk, no re-binding.
    assert db.binds == list(range(7))
    assert result.attempts_made == 2


def test_empty_change_list_writes_nothing():
    db = _FakeCassandra()
    result = _Writer(db).apply_changes([], atomic=False)
    assert db.calls == []
    assert result.attempts_made == 0
