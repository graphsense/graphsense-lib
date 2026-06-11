from collections import namedtuple

from graphsenselib.deltaupdate.update.account import parallelio

IdRow = namedtuple("IdRow", ["address_id", "address", "address_prefix"])
AddressRow = namedtuple("AddressRow", ["address_id", "in_degree"])
RelRow = namedtuple("RelRow", ["no_transactions"])
BalRow = namedtuple("BalRow", ["currency", "balance"])


class FakeResult:
    """Mimics the driver's ExecutionResult -> ResultSet chain."""

    def __init__(self, rows):
        self._rows = rows

    @property
    def result_or_exc(self):
        return self

    def one(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class FakeTdb:
    def get_address_id_async_batch(self, addresses):
        return [
            (
                a,
                FakeResult([] if a == b"missing" else [IdRow(100 + i, a, "prefix")]),
            )
            for i, a in enumerate(addresses)
        ]

    def get_address_async_batch(self, address_ids):
        return [(i, FakeResult([AddressRow(i, 1)])) for i in address_ids]

    def execute_combined_queries_account_delta_updates(self, rel_ids, address_ids):
        in_results = [FakeResult([RelRow(3)]) for _ in rel_ids]
        bal_results = [FakeResult([BalRow("TRX", 7)]) for _ in address_ids]
        timing = {
            "n_in": len(rel_ids),
            "n_bal": len(address_ids),
            "in_time": 0.0,
            "bal_time": 0.0,
            "in_qps": 0.0,
            "bal_qps": 0.0,
        }
        return in_results, bal_results, timing


class FakePool:
    """Stands in for ParallelDbPool; records dispatch and returns canned
    worker-shaped results."""

    def __init__(self, result_fn):
        self.result_fn = result_fn
        self.dispatched = []

    def map_chunked(self, fn, items):
        self.dispatched.append((fn, list(items)))
        return [self.result_fn(item) for item in items]


def test_fetch_address_ids_serial_returns_id_or_none():
    result = parallelio.fetch_address_ids(FakeTdb(), None, [b"a", b"missing", b"c"])
    assert result == {b"a": 100, b"missing": None, b"c": 102}


def test_fetch_address_ids_pool_dispatches_worker_fn():
    pool = FakePool(lambda addr: (addr, 55))
    result = parallelio.fetch_address_ids(FakeTdb(), pool, [b"a", b"b"])
    assert result == {b"a": 55, b"b": 55}
    fn, items = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_address_ids
    assert items == [b"a", b"b"]


def test_fetch_address_rows_serial_returns_rows_by_id():
    result = parallelio.fetch_address_rows(FakeTdb(), None, [4, 9])
    assert result[4].address_id == 4
    assert result[9].in_degree == 1


def test_fetch_address_rows_pool_dispatches_worker_fn():
    pool = FakePool(lambda aid: (aid, f"row-{aid}"))
    result = parallelio.fetch_address_rows(FakeTdb(), pool, [4, 9])
    assert result == {4: "row-4", 9: "row-9"}
    fn, _ = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_address_rows


def test_fetch_relations_and_balances_serial_unwraps_rows():
    in_rows, bal_rows, timing = parallelio.fetch_relations_and_balances(
        FakeTdb(), None, [(1, 2), (3, 4)], [7]
    )
    assert [r.no_transactions for r in in_rows] == [3, 3]
    assert [[b.balance for b in rows] for rows in bal_rows] == [[7]]
    assert timing["n_in"] == 2
    assert timing["n_bal"] == 1


def test_fetch_relations_and_balances_pool_splits_tagged_results():
    def canned(item):
        kind, key = item
        return f"{kind}-result" if kind == "rel" else [f"{kind}-row"]

    pool = FakePool(canned)
    in_rows, bal_rows, timing = parallelio.fetch_relations_and_balances(
        FakeTdb(), pool, [(1, 2), (3, 4)], [7, 8, 9]
    )
    assert in_rows == ["rel-result", "rel-result"]
    assert bal_rows == [["bal-row"], ["bal-row"], ["bal-row"]]
    assert timing["n_in"] == 2
    assert timing["n_bal"] == 3
    fn, items = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_relations_balances
    assert items == [
        ("rel", (1, 2)),
        ("rel", (3, 4)),
        ("bal", 7),
        ("bal", 8),
        ("bal", 9),
    ]


def test_apply_changes_dispatches_data_changes_to_pool():
    from graphsenselib.db.analytics import ApplyChangesResult, DbChange
    from graphsenselib.db.parallel import worker_apply_changes
    from graphsenselib.deltaupdate.update.utxo.update import apply_changes

    ok = ApplyChangesResult(
        attempts_made=1,
        total_retry_wait_seconds=0.0,
        warning_threshold=None,
        warning_text=None,
    )
    pool = FakePool(lambda change: ok)
    changes = [
        DbChange.update(table="address", data={"address_id": i}) for i in range(5)
    ]
    apply_changes(None, changes, pedantic=False, try_atomic_writes=False, pool=pool)
    fn, items = pool.dispatched[0]
    assert fn is worker_apply_changes
    assert items == changes


def test_delta_update_cli_exposes_parallel_workers_option():
    from click.testing import CliRunner

    from graphsenselib.deltaupdate.cli import deltaupdate_cli

    result = CliRunner().invoke(deltaupdate_cli, ["delta-update", "update", "--help"])
    assert result.exit_code == 0
    assert "--parallel-workers" in result.output
