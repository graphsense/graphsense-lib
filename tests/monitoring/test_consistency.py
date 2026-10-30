"""Tests for the read-only `monitoring check-consistency` checker.

The lake side runs against a real local Delta table (duckdb + deltalake, no
S3); the Cassandra side is a small in-memory fake of the reader surface the
checker uses, so no testcontainer is needed.
"""

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from graphsenselib.monitoring import consistency
from graphsenselib.monitoring.consistency import (
    ACCOUNT_BLOCK_TX_FILTER,
    AddressSample,
    ConsistencyChecker,
    Status,
    check_address_samples,
    compare_per_block,
)
from graphsenselib.utils.DeltaTableConnector import DeltaTableConnector

# -- pure comparisons ----------------------------------------------------------


def test_compare_per_block_agree():
    f = compare_per_block("c", [1, 2], {"a": {1: 3, 2: 4}, "b": {1: 3, 2: 4}})
    assert f.status == Status.OK


def test_compare_per_block_reports_mismatch_and_missing():
    f = compare_per_block("c", [1, 2, 3], {"a": {1: 3, 2: 4, 3: 1}, "b": {1: 3, 2: 5}})
    assert f.status == Status.FAIL
    assert "2/3 blocks differ" in f.detail
    assert "2: a=4, b=5" in f.detail
    assert "3: a=1, b=missing" in f.detail


def _sample(**kw):
    base = dict(
        address="a1",
        address_id=7,
        no_incoming_txs=1,
        no_outgoing_txs=1,
        rows=[(False, 10, 5), (True, 11, -3), (False, 12, 0)],
        expected_flows={11: -3},
    )
    base.update(kw)
    return AddressSample(**base)


def test_address_sample_consistent():
    # The net-zero row (tx 12) counts on neither side, as in Spark.
    assert check_address_samples([_sample()]).status == Status.OK


def test_address_sample_double_counted_counter():
    f = check_address_samples([_sample(no_incoming_txs=2)])
    assert f.status == Status.FAIL
    assert "counters in/out 2/1 but address_transactions has 1/1" in f.detail


def test_address_sample_missing_row_and_id():
    f = check_address_samples(
        [_sample(expected_flows={13: 4}), _sample(address="a2", address_id=None)]
    )
    assert f.status == Status.FAIL
    assert "tx 13: expected flow 4, stored no row" in f.detail
    assert "a2: missing from address_ids_by_address_prefix" in f.detail


def test_address_sample_empty_is_skip():
    assert check_address_samples([]).status == Status.SKIP


# -- local lake ------------------------------------------------------------------


def _write(base: Path, table: str, columns: dict, schema: pa.Schema):
    t = pa.Table.from_pydict(columns, schema=schema)
    write_deltalake(str(base / table), t, partition_by=["partition"])


BLOCKS = [100, 101, 102]


@pytest.fixture
def eth_lake(tmp_path):
    """ETH lake with 3 blocks. Block 101 has a failed tx, block 102 has two
    validator withdrawals (which raw keeps as extra trace rows)."""
    base = tmp_path / "eth"
    _write(
        base,
        "block",
        {
            "block_id": BLOCKS,
            "transaction_count": [2, 2, 1],
            "withdrawals": [None, [], [{"index": 1}, {"index": 2}]],
            "partition": [0, 0, 0],
        },
        pa.schema(
            [
                ("block_id", pa.int64()),
                ("transaction_count", pa.int64()),
                ("withdrawals", pa.list_(pa.struct([("index", pa.int64())]))),
                ("partition", pa.int64()),
            ]
        ),
    )
    _write(
        base,
        "transaction",
        {
            "block_id": [100, 100, 101, 101, 102],
            "tx_hash": [bytes([i]) * 32 for i in range(5)],
            "receipt_status": [1, None, 1, 0, 1],
            "partition": [0] * 5,
        },
        pa.schema(
            [
                ("block_id", pa.int64()),
                ("tx_hash", pa.binary()),
                ("receipt_status", pa.int64()),
                ("partition", pa.int64()),
            ]
        ),
    )
    ids = pa.schema([("block_id", pa.int64()), ("partition", pa.int64())])
    _write(base, "trace", {"block_id": [100, 100, 101, 102], "partition": [0] * 4}, ids)
    _write(base, "log", {"block_id": [100, 102], "partition": [0, 0]}, ids)
    return DeltaTableConnector(str(base), None)


def test_aggregate_per_block_and_highest_block(eth_lake):
    counts = eth_lake.aggregate_per_block("trace", BLOCKS, {"n": "count(*)"})
    assert counts == {100: {"n": 2}, 101: {"n": 1}, 102: {"n": 1}}
    assert eth_lake.highest_block() == 102


def test_eth_block_tx_filter_keeps_null_status_drops_failed(eth_lake):
    expected = eth_lake.aggregate_per_block(
        "transaction",
        BLOCKS,
        {"n": f"count(*) FILTER (WHERE {ACCOUNT_BLOCK_TX_FILTER['eth']})"},
    )
    # pre-Byzantium null receipt_status is not "failed"; status 0 is.
    assert {b: v["n"] for b, v in expected.items()} == {100: 2, 101: 1, 102: 1}


def test_trx_block_tx_filter(tmp_path):
    base = tmp_path / "trx"
    _write(
        base,
        "transaction",
        {
            "block_id": [5, 5, 5, 5],
            "receipt_status": [1, 1, 0, 1],
            "to_address": [b"a", None, b"b", None],
            "receipt_contract_address": [None, b"c", None, None],
            "partition": [0] * 4,
        },
        pa.schema(
            [
                ("block_id", pa.int64()),
                ("receipt_status", pa.int64()),
                ("to_address", pa.binary()),
                ("receipt_contract_address", pa.binary()),
                ("partition", pa.int64()),
            ]
        ),
    )
    dtc = DeltaTableConnector(str(base), None)
    expected = dtc.aggregate_per_block(
        "transaction",
        [5],
        {"n": f"count(*) FILTER (WHERE {ACCOUNT_BLOCK_TX_FILTER['trx']})"},
    )
    # kept: plain transfer and contract creation; dropped: failed, no target.
    assert expected == {5: {"n": 2}}


# -- end to end with fake Cassandra ----------------------------------------------

# raw account `transaction` rows by hash, matching the eth_lake fixture
ETH_RAW_TXS = {bytes([i]) * 32: b for i, b in enumerate([100, 100, 101, 101, 102])}


class FakeKeyspace:
    """Answers the select() shapes the checker issues from in-memory tables:
    ``block`` by group, ``COUNT(*) AS n`` per (group, block)."""

    def __init__(self, tables, bucket_size=1000):
        self.tables = tables
        self.bucket_size = bucket_size

    def get_id_group(self, id_, bucket_size):
        return id_ // bucket_size

    def get_block_bucket_size(self):
        return self.bucket_size

    get_block_id_bucket_size = get_block_bucket_size

    def select(self, table, columns, where):
        rows = [
            r
            for r in self.tables[table]
            if r["block_id"] // self.bucket_size == where["block_id_group"]
            and ("block_id" not in where or r["block_id"] == where["block_id"])
        ]
        if columns == ["COUNT(*) AS n"]:
            return [SimpleNamespace(n=len(rows))]
        return [SimpleNamespace(**r) for r in rows]


class FakeRawAccountTxs:
    """raw._db.execute_batch for the by-hash tx reads."""

    def __init__(self, txs, prefix_len):
        self.txs = txs
        self.prefix_len = prefix_len

    def execute_batch(self, stmt, params):
        out = []
        for key, (prefix, tx_hash) in params:
            assert prefix == tx_hash.hex()[: self.prefix_len]
            b = self.txs.get(tx_hash)
            row = SimpleNamespace(block_id=b) if b is not None else None
            out.append((key, SimpleNamespace(one=lambda row=row: row)))
        return out


def _fake_db(
    raw_tables, transformed_tables, hb_du, has_history=True, wal=False, raw_txs=None
):
    raw = FakeKeyspace(raw_tables)
    raw.get_highest_block = lambda: max(r["block_id"] for r in raw_tables["block"])
    raw.get_tx_prefix_length = lambda: 5
    raw.select_stmt = lambda table, columns, where, limit: "stmt"
    raw._db = FakeRawAccountTxs(ETH_RAW_TXS if raw_txs is None else raw_txs, 5)

    tdb = FakeKeyspace(transformed_tables)
    tdb._db = SimpleNamespace(has_table=lambda ks, t: wal)
    tdb.get_keyspace = lambda: "eth_transformed"
    tdb.get_highest_block_delta_updater = lambda: hb_du
    tdb.is_first_delta_update_run = lambda: False
    tdb.delta_updater_history_has_block = lambda b: has_history
    tdb.get_exchange_rates_by_block = lambda b: SimpleNamespace(fiat_values=[1.0, 2.0])
    return SimpleNamespace(raw=raw, transformed=tdb)


def _consistent_tables():
    raw = {
        "block": [
            {"block_id": 100, "transaction_count": 2},
            {"block_id": 101, "transaction_count": 2},
            {"block_id": 102, "transaction_count": 1},
        ],
        # 102: one lake trace + two withdrawal traces
        "trace": [{"block_id": b} for b in [100, 100, 101, 102, 102, 102]],
        "log": [{"block_id": 100}, {"block_id": 102}],
    }
    transformed = {
        "block_transactions": [{"block_id": b} for b in [100, 100, 101, 102]],
    }
    return raw, transformed


def _by_check(findings):
    return {f.check: f for f in findings}


RAW_TX_CHECK = "raw transaction rows (newest 3 blocks, by hash)"


def test_eth_end_to_end_consistent(eth_lake):
    raw, transformed = _consistent_tables()
    findings = ConsistencyChecker(
        _fake_db(raw, transformed, hb_du=102), "eth", dtc=eth_lake, n_blocks=3
    ).run()
    assert [f for f in findings if f.status in (Status.FAIL, Status.WARN)] == []
    checks = _by_check(findings)
    assert checks["trace counts"].status == Status.OK
    assert checks[RAW_TX_CHECK].status == Status.OK
    assert checks["transformed block_transactions"].status == Status.OK
    assert checks["raw rows above highest block"].status == Status.OK
    assert checks["lake rows above highest block"].status == Status.OK


def test_eth_end_to_end_detects_torn_and_missing(eth_lake, monkeypatch):
    raw, transformed = _consistent_tables()
    raw["log"].append({"block_id": 101})  # raw has a log the lake lacks
    transformed["block_transactions"].pop()  # block 102 tx missing in transformed
    raw_txs = dict(ETH_RAW_TXS)
    del raw_txs[bytes([2]) * 32]  # a raw tx row of block 101 never written
    monkeypatch.setattr(
        consistency.DeltaWal,
        "pending_header",
        lambda self: {
            "block_lo": 103,
            "block_hi": 110,
            "run_id": "r1",
            "code_version": "2.16.4",
        },
    )
    findings = ConsistencyChecker(
        _fake_db(
            raw, transformed, hb_du=102, has_history=False, wal=True, raw_txs=raw_txs
        ),
        "eth",
        dtc=eth_lake,
        n_blocks=3,
    ).run()
    checks = _by_check(findings)
    assert checks["delta updater WAL"].status == Status.FAIL
    assert "103-110" in checks["delta updater WAL"].detail
    assert checks["delta updater bookkeeping"].status == Status.FAIL
    assert checks["log counts"].status == Status.FAIL
    assert "101: raw log rows=1, lake log rows=0" in checks["log counts"].detail
    assert checks["transformed block_transactions"].status == Status.FAIL
    assert checks[RAW_TX_CHECK].status == Status.FAIL
    assert "101: lake transaction rows=2, found in raw=1" in checks[RAW_TX_CHECK].detail


def test_eth_half_ingested_batch_is_flagged(eth_lake):
    """An ingest that died after the side tables but before the block rows of
    block 103: raw traces and lake traces exist above the top block."""
    raw, transformed = _consistent_tables()
    raw["trace"].append({"block_id": 103})
    write_deltalake(
        eth_lake.get_table_path("trace"),
        pa.Table.from_pydict(
            {"block_id": [103], "partition": [0]},
            schema=pa.schema([("block_id", pa.int64()), ("partition", pa.int64())]),
        ),
        mode="append",
    )
    findings = ConsistencyChecker(
        _fake_db(raw, transformed, hb_du=102), "eth", dtc=eth_lake, n_blocks=3
    ).run()
    checks = _by_check(findings)
    assert checks["raw rows above highest block"].status == Status.WARN
    assert "trace: blocks 103-103" in checks["raw rows above highest block"].detail
    assert checks["lake rows above highest block"].status == Status.WARN
    assert "trace: up to block 103" in checks["lake rows above highest block"].detail
    # the window itself is still consistent
    assert [f for f in findings if f.status == Status.FAIL] == []


def test_heights_transformed_ahead_of_lake_fails(eth_lake):
    raw, transformed = _consistent_tables()
    raw["block"].append({"block_id": 103, "transaction_count": 0})
    findings = ConsistencyChecker(
        _fake_db(raw, transformed, hb_du=103), "eth", dtc=eth_lake, n_blocks=3
    ).run()
    heights = _by_check(findings)["heights"]
    assert heights.status == Status.FAIL
    assert "ahead of the lake" in heights.detail


def test_window_respects_first_block():
    c = ConsistencyChecker(SimpleNamespace(), "trx", n_blocks=100)
    assert c.window(5) == [1, 2, 3, 4, 5]


# -- UTXO: raw vs lake counts, raw tx rows and the address recount ----------------


def _io(address, value):
    return SimpleNamespace(address=[address], value=value)


# block 200: coinbase (skipped by the sampler) + A pays B 7, change 2 back to A
def _tx(tx_id, coinbase, inputs, outputs):
    return SimpleNamespace(
        tx_id=tx_id,
        coinbase=coinbase,
        inputs=inputs,
        outputs=outputs,
        block_id=200,
        timestamp=0,
        tx_hash=bytes([tx_id]),
    )


UTXO_TXS = {
    200: [
        _tx(1, True, [], [_io("M", 50)]),
        _tx(2, False, [_io("A", 10)], [_io("B", 7), _io("A", 2)]),
    ]
}


class FakeTxSpanDb:
    """raw._db for COUNT(*) over the `transaction` table's tx_id span."""

    def __init__(self, tx_rows):
        self.tx_rows = tx_rows

    def get_prepared_statement(self, cql):
        return SimpleNamespace(bind=lambda params: (cql, params))

    def execute_statement(self, bound):
        cql, p = bound
        n = sum(
            1
            for t in self.tx_rows
            if t // 10 in p["groups"] and t >= p["lo"] and t <= p.get("hi", t)
        )
        return [SimpleNamespace(n=n)]


class FakeUtxoRaw:
    def __init__(self, tx_rows=(1, 2), block_tx_lists=None):
        self._db = FakeTxSpanDb(set(tx_rows))
        self.block_tx_lists = block_tx_lists or {200: [1, 2]}

    def get_keyspace(self):
        return "btc_raw"

    def get_highest_block(self):
        return 200

    def get_block_bucket_size(self):
        return 100

    def get_tx_bucket_size(self):
        return 10

    def get_id_group(self, id_, bucket_size):
        return id_ // bucket_size

    def select(self, table, columns, where):
        if table == "block":
            return [SimpleNamespace(block_id=200, no_transactions=2)]
        assert table == "block_transactions"
        ids = self.block_tx_lists.get(where["block_id"])
        if ids is None:
            return []
        return [SimpleNamespace(txs=[SimpleNamespace(tx_id=i) for i in ids])]

    def get_transactions_in_block(self, b):
        return UTXO_TXS[b]


class FakeUtxoTransformed:
    """address A (id 1) and B (id 2); ``rows`` are address_transactions."""

    def __init__(self, counters, rows):
        self.counters = counters
        self.rows = rows
        self._db = SimpleNamespace(has_table=lambda ks, t: False)

    def get_keyspace(self):
        return "btc_transformed"

    def get_highest_block_delta_updater(self):
        return 200

    def is_first_delta_update_run(self):
        return False

    def delta_updater_history_has_block(self, b):
        return True

    def get_exchange_rates_by_block(self, b):
        return SimpleNamespace(fiat_values=[1.0, 2.0])

    def get_address_id_bucket_size(self):
        return 10

    def get_id_group(self, id_, bucket_size):
        return id_ // bucket_size

    def to_db_address(self, adr):
        return SimpleNamespace(prefix=adr[:1], db_encoding=adr)

    def select_one_safe(self, table, columns, where):
        aid = {"A": 1, "B": 2}.get(where["address"])
        return SimpleNamespace(address_id=aid) if aid else None

    def select_one(self, table, columns, where):
        n_in, n_out = self.counters[where["address_id"]]
        return SimpleNamespace(no_incoming_txs=n_in, no_outgoing_txs=n_out)

    def select(self, table, columns, where):
        return [
            SimpleNamespace(is_outgoing=o, tx_id=t, value=v)
            for o, t, v in self.rows[where["address_id"]]
        ]


@pytest.fixture
def btc_lake(tmp_path):
    base = tmp_path / "btc"
    _write(
        base,
        "block",
        {"block_id": [200], "no_transactions": [2], "partition": [0]},
        pa.schema(
            [
                ("block_id", pa.int64()),
                ("no_transactions", pa.int64()),
                ("partition", pa.int64()),
            ]
        ),
    )
    ids = pa.schema([("block_id", pa.int64()), ("partition", pa.int64())])
    _write(base, "transaction", {"block_id": [200, 200], "partition": [0, 0]}, ids)
    return DeltaTableConnector(str(base), None)


CONSISTENT_COUNTERS = {1: (1, 1), 2: (1, 0)}
# A nets -8 (outgoing) in tx 2, B nets +7; plus an older history row for A.
CONSISTENT_ROWS = {1: [(True, 2, -8), (False, 0, 3)], 2: [(False, 2, 7)]}


def _run_utxo(btc_lake, counters=CONSISTENT_COUNTERS, rows=CONSISTENT_ROWS, **raw):
    db = SimpleNamespace(
        raw=FakeUtxoRaw(**raw), transformed=FakeUtxoTransformed(counters, rows)
    )
    return _by_check(
        ConsistencyChecker(db, "btc", dtc=btc_lake, n_blocks=1, seed=0).run()
    )


def test_utxo_end_to_end_consistent(btc_lake):
    checks = _run_utxo(btc_lake)
    assert checks["block tx counts"].status == Status.OK
    assert "raw transaction rows" in checks["block tx counts"].detail
    assert checks["raw rows above highest block"].status == Status.OK
    sample = checks["transformed address counters (sample)"]
    assert sample.status == Status.OK
    assert "2 addresses" in sample.detail


def test_utxo_end_to_end_detects_double_applied_counter(btc_lake):
    # B's counter was incremented twice for one tx (a batch applied twice).
    checks = _run_utxo(btc_lake, counters={1: (1, 1), 2: (2, 0)})
    sample = checks["transformed address counters (sample)"]
    assert sample.status == Status.FAIL
    assert "B (id 2): counters in/out 2/0" in sample.detail


def test_utxo_missing_raw_tx_row(btc_lake):
    # block_transactions lists tx 2 but its `transaction` row was never written
    checks = _run_utxo(btc_lake, tx_rows=(1,))
    assert checks["block tx counts"].status == Status.FAIL
    assert "raw transaction rows=1" in checks["block tx counts"].detail


def test_utxo_leftovers_above_top(btc_lake):
    # an interrupted batch wrote tx 3 and the tx list of block 201, not block 201
    checks = _run_utxo(
        btc_lake, tx_rows=(1, 2, 3), block_tx_lists={200: [1, 2], 201: [3]}
    )
    above = checks["raw rows above highest block"]
    assert above.status == Status.WARN
    assert "block_transactions: blocks 201-201" in above.detail
    assert "transaction: 1 rows past tx_id 2" in above.detail
    assert checks["block tx counts"].status == Status.OK


# -- CLI wiring --------------------------------------------------------------------


def test_cli_check_consistency(monkeypatch):
    from contextlib import nullcontext

    from click.testing import CliRunner

    import graphsenselib.config
    import graphsenselib.db
    import graphsenselib.monitoring.cli as mcli

    monkeypatch.setattr(
        graphsenselib.config,
        "get_config",
        lambda: SimpleNamespace(get_deltaupdater_config=lambda env, cur: None),
    )
    monkeypatch.setattr(mcli, "get_config", graphsenselib.config.get_config)
    monkeypatch.setattr(
        graphsenselib.db,
        "DbFactory",
        lambda: SimpleNamespace(
            from_config=lambda env, cur, readonly: nullcontext(
                SimpleNamespace(transformed=SimpleNamespace(get_keyspace=lambda: "ks"))
            )
        ),
    )
    seen, sent = {}, []

    class FakeChecker:
        def __init__(self, db, currency, **kw):
            seen.update(kw, currency=currency)

        def run(self):
            return [consistency.Finding("x", Status.FAIL, "broken")]

    monkeypatch.setattr(consistency, "ConsistencyChecker", FakeChecker)
    monkeypatch.setattr(mcli, "send_msg_to_topic", lambda t, m: sent.append((t, m)))

    result = CliRunner().invoke(
        mcli.monitoring_cli,
        [
            "monitoring",
            "check-consistency",
            "-e",
            "test",
            "-c",
            "btc",
            "--blocks",
            "7",
            "--no-lock",
            "--topic",
            "alerts",
        ],
    )
    assert result.exit_code == consistency.EXIT_INCONSISTENT, result.output
    assert seen["n_blocks"] == 7 and seen["dtc"] is None and seen["currency"] == "btc"
    assert sent == [("alerts", "Consistency check test/btc failed:\n- x: broken")]
