from collections import namedtuple

from graphsenselib.deltaupdate.update.generic import ApplicationStrategy
from graphsenselib.deltaupdate.update.utxo import parallelio
from graphsenselib.deltaupdate.update.utxo import update as utxo_update

RelRow = namedtuple("RelRow", ["no_transactions"])
ClusterRow = namedtuple("ClusterRow", ["cluster_id", "in_degree"])
TxRow = namedtuple("TxRow", ["tx_id", "block_id"])


class FakeResult:
    """Mimics the driver's ExecutionResult -> ResultSet chain."""

    def __init__(self, rows):
        self._rows = rows

    @property
    def result_or_exc(self):
        return self

    def one(self):
        return self._rows[0] if self._rows else None


class FakeTdb:
    def get_address_incoming_relations_async_batch(self, rel_ids):
        return [
            FakeResult([] if dst is None else [RelRow(dst * 10 + src)])
            for dst, src in rel_ids
        ]

    def get_cluster_incoming_relations_async_batch(self, rel_ids):
        return [
            FakeResult([] if dst is None else [RelRow(dst * 100 + src)])
            for dst, src in rel_ids
        ]

    def get_cluster_async_batch(self, cluster_ids):
        return [
            (cid, FakeResult([] if cid == 404 else [ClusterRow(cid, 1)]))
            for cid in cluster_ids
        ]


class FakeRawDb:
    def get_transactions_in_block(self, block):
        return [TxRow(tx_id=block * 1000 + i, block_id=block) for i in range(2)]


class FakeDb:
    raw = FakeRawDb()


class FakePool:
    """Stands in for ParallelDbPool; records dispatch and returns canned
    worker-shaped results."""

    def __init__(self, result_fn):
        self.result_fn = result_fn
        self.dispatched = []

    def map_chunked(self, fn, items):
        self.dispatched.append((fn, list(items)))
        return [self.result_fn(item) for item in items]


def test_fetch_block_transactions_serial_returns_block_tx_pairs_in_order():
    result = parallelio.fetch_block_transactions(FakeDb(), None, [5, 3])
    assert [block for block, _ in result] == [5, 3]
    assert [tx.tx_id for _, txs in result for tx in txs] == [5000, 5001, 3000, 3001]


def test_fetch_block_transactions_pool_dispatches_worker_fn():
    pool = FakePool(lambda block: (block, [f"tx-{block}"]))
    result = parallelio.fetch_block_transactions(FakeDb(), pool, [1, 2])
    assert result == [(1, ["tx-1"]), (2, ["tx-2"])]
    fn, items = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_block_transactions
    assert items == [1, 2]


def test_fetch_address_incoming_relations_serial_resolves_rows_in_order():
    rows = parallelio.fetch_address_incoming_relations(
        FakeTdb(), None, [(1, 2), (None, 0), (3, 4)]
    )
    assert [r.no_transactions if r else None for r in rows] == [12, None, 34]


def test_fetch_address_incoming_relations_pool_dispatches_worker_fn():
    pool = FakePool(lambda rel: f"row-{rel}")
    rows = parallelio.fetch_address_incoming_relations(FakeTdb(), pool, [(1, 2)])
    assert rows == ["row-(1, 2)"]
    fn, items = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_address_incoming_relations
    assert items == [(1, 2)]


def test_fetch_cluster_incoming_relations_serial_resolves_rows_in_order():
    rows = parallelio.fetch_cluster_incoming_relations(
        FakeTdb(), None, [(1, 2), (None, 0)]
    )
    assert [r.no_transactions if r else None for r in rows] == [102, None]


def test_fetch_cluster_incoming_relations_pool_dispatches_worker_fn():
    pool = FakePool(lambda rel: f"crow-{rel}")
    rows = parallelio.fetch_cluster_incoming_relations(FakeTdb(), pool, [(7, 8)])
    assert rows == ["crow-(7, 8)"]
    fn, _ = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_cluster_incoming_relations


def test_fetch_cluster_rows_serial_returns_rows_by_id():
    result = parallelio.fetch_cluster_rows(FakeTdb(), None, [4, 404, 4])
    assert result[4].cluster_id == 4
    assert result[404] is None
    assert len(result) == 2


def test_fetch_cluster_rows_pool_dispatches_worker_fn():
    pool = FakePool(lambda cid: (cid, f"cluster-{cid}"))
    result = parallelio.fetch_cluster_rows(FakeTdb(), pool, [4, 9])
    assert result == {4: "cluster-4", 9: "cluster-9"}
    fn, _ = pool.dispatched[0]
    assert fn is parallelio.worker_fetch_cluster_rows


def test_reexports_generic_address_fetchers():
    from graphsenselib.deltaupdate.update.account.parallelio import (
        fetch_address_ids,
        fetch_address_rows,
    )

    assert parallelio.fetch_address_ids is fetch_address_ids
    assert parallelio.fetch_address_rows is fetch_address_rows


# --- persist ordering with a pool ----------------------------------------


class _StubTransformed:
    def get_keyspace(self):
        return "tks"

    def get_summary_statistics(self):
        return None

    def get_highest_address_id(self, sanity_check=True):
        return 0

    def get_highest_cluster_id(self, sanity_check=True):
        return 1


class _StubRaw:
    def get_keyspace(self):
        return "rks"


class _StubDb:
    raw = _StubRaw()
    transformed = _StubTransformed()


def _make_strategy(application_strategy, pool):
    strategy = utxo_update.UpdateStrategyUtxo(
        _StubDb(),
        "btc",
        pedantic=False,
        application_strategy=application_strategy,
        parallel_pool=pool,
    )
    strategy._batch_start_time = 0.0
    return strategy


def test_persist_pool_writes_data_shards_before_bookkeeping(monkeypatch):
    calls = []

    def record(db, changes, pedantic, try_atomic_writes, pool=None):
        calls.append((list(changes), try_atomic_writes, pool))

    monkeypatch.setattr(utxo_update, "apply_changes", record)
    pool = object()
    strategy = _make_strategy(ApplicationStrategy.BATCH, pool)
    strategy.changes = ["data1", "data2"]
    strategy.bookkeeping_changes = ["bookkeeping"]
    strategy.persist_updater_progress()

    assert calls == [
        (["data1", "data2"], False, pool),
        (["bookkeeping"], False, None),
    ]
    assert strategy.changes is None
    assert strategy.bookkeeping_changes is None


def test_persist_without_pool_combines_data_and_bookkeeping(monkeypatch):
    calls = []

    def record(db, changes, pedantic, try_atomic_writes, pool=None):
        calls.append((list(changes), try_atomic_writes, pool))

    monkeypatch.setattr(utxo_update, "apply_changes", record)
    strategy = _make_strategy(ApplicationStrategy.BATCH, None)
    strategy.changes = ["data1"]
    strategy.bookkeeping_changes = ["bookkeeping"]
    strategy.persist_updater_progress()

    assert calls == [(["data1", "bookkeeping"], False, None)]


def test_persist_pool_ignored_in_tx_mode(monkeypatch):
    calls = []

    def record(db, changes, pedantic, try_atomic_writes, pool=None):
        calls.append((list(changes), try_atomic_writes, pool))

    monkeypatch.setattr(utxo_update, "apply_changes", record)
    strategy = _make_strategy(ApplicationStrategy.TX, object())
    strategy.changes = ["data1"]
    strategy.bookkeeping_changes = ["bookkeeping"]
    strategy.persist_updater_progress()

    assert calls == [(["data1", "bookkeeping"], True, None)]


def test_factory_passes_pool_to_utxo_v2():
    from graphsenselib.deltaupdate.update.factory import UpdaterFactory

    class _DuConfig:
        currency = "btc"

    pool = object()
    updater = UpdaterFactory().get_updater(
        _DuConfig(),
        _StubDb(),
        version=2,
        write_new=False,
        write_dirty=False,
        pedantic=False,
        write_batch=10,
        patch_mode=False,
        parallel_pool=pool,
    )
    assert isinstance(updater, utxo_update.UpdateStrategyUtxo)
    assert updater._parallel_pool is pool


def test_factory_drops_pool_for_utxo_tx_mode():
    # TX mode (write_batch == 1) is the conservative per-transaction path
    # and stays fully single-process; without this gate the block-read
    # phase (which runs before the mode branch) would still use the pool.
    from graphsenselib.deltaupdate.update.factory import UpdaterFactory

    class _DuConfig:
        currency = "btc"

    updater = UpdaterFactory().get_updater(
        _DuConfig(),
        _StubDb(),
        version=2,
        write_new=False,
        write_dirty=False,
        pedantic=False,
        write_batch=1,
        patch_mode=False,
        parallel_pool=object(),
    )
    assert isinstance(updater, utxo_update.UpdateStrategyUtxo)
    assert updater.application_strategy == ApplicationStrategy.TX
    assert updater._parallel_pool is None
