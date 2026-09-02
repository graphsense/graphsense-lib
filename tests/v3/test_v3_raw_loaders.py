"""The lake -> v3 raw backfill.

The lake is stubbed rather than written as real Delta tables: these assert the
transformations, and a Delta round trip would only test Delta. The strongest
assertion in the file is that every frame the loaders build conforms to the
schema table it targets -- which is the whole reason the schema is a model.
"""

import pytest

from graphsense_v3.codec import block_of_tx_id, encode_address, tx_id
from graphsense_v3.config import config_for
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import raw_account, raw_utxo
from graphsense_v3.spark.writer import conformance_errors

GENESIS_COINBASE = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
SEGWIT = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"

# Explicit schemas throughout: pyspark infers a nested dict as a MAP with a
# single unified value type, not as a struct, so an inferred fixture would not
# have the lake's shape at all. These mirror
# graphsenselib/schema/resources/parquet/*.py.
UTXO_BLOCK = "block_id INT, block_hash BINARY, timestamp INT, no_transactions INT"

UTXO_INPUT = (
    "spent_transaction_hash BINARY, spent_output_index INT, index INT, "
    "sequence BIGINT, script_hex STRING, txinwitness ARRAY<BINARY>, type STRING, "
    "addresses ARRAY<STRING>, value BIGINT"
)
UTXO_OUTPUT = (
    "index INT, script_hex STRING, addresses ARRAY<STRING>, "
    "required_signatures TINYINT, type STRING, value BIGINT"
)
UTXO_TRANSACTION = (
    "tx_hash BINARY, block_id INT, timestamp INT, coinbase BOOLEAN, "
    "total_input BIGINT, total_output BIGINT, "
    f"outputs ARRAY<STRUCT<{UTXO_OUTPUT}>>, inputs ARRAY<STRUCT<{UTXO_INPUT}>>, "
    "coinjoin BOOLEAN, version BIGINT, lock_time BIGINT, index INT, "
    "input_count INT, output_count INT"
)

ACCOUNT_BLOCK = (
    "block_id INT, block_hash BINARY, parent_hash BINARY, nonce BINARY, "
    "sha3_uncles BINARY, logs_bloom BINARY, transactions_root BINARY, "
    "state_root BINARY, receipts_root BINARY, miner BINARY, difficulty BINARY, "
    "total_difficulty BINARY, size BIGINT, extra_data BINARY, gas_limit INT, "
    "gas_used INT, base_fee_per_gas BIGINT, timestamp INT, transaction_count INT"
)
ACCOUNT_TRANSACTION = (
    "tx_hash_prefix STRING, tx_hash BINARY, nonce INT, block_hash BINARY, "
    "block_id INT, transaction_index INT, from_address BINARY, to_address BINARY, "
    "value BINARY, gas INT, gas_price BIGINT, input BINARY, block_timestamp INT, "
    "max_fee_per_gas BIGINT, max_priority_fee_per_gas BIGINT, "
    "transaction_type BIGINT, receipt_cumulative_gas_used BIGINT, "
    "receipt_gas_used BIGINT, receipt_contract_address BINARY, "
    "receipt_root BINARY, receipt_status BIGINT, "
    "receipt_effective_gas_price BIGINT, max_fee_per_blob_gas BIGINT, "
    "blob_versioned_hashes ARRAY<BINARY>, v INT, r BINARY, s BINARY"
)
ACCOUNT_LOG = (
    "block_id INT, block_hash BINARY, address BINARY, data BINARY, "
    "topics ARRAY<BINARY>, topic0 BINARY, tx_hash BINARY, log_index SMALLINT, "
    "transaction_index INT"
)
ETH_TRACE = (
    "block_id INT, tx_hash BINARY, transaction_index INT, from_address BINARY, "
    "to_address BINARY, value BINARY, input BINARY, output BINARY, "
    "trace_type STRING, call_type STRING, reward_type STRING, gas INT, "
    "gas_used BIGINT, subtraces INT, trace_address STRING, error STRING, "
    "status SMALLINT, trace_id STRING, trace_index INT"
)
TRX_TRACE = (
    "block_id INT, tx_hash BINARY, internal_index SMALLINT, "
    "transferto_address BINARY, call_info_index SMALLINT, caller_address BINARY, "
    "call_value BINARY, rejected BOOLEAN, call_token_id INT, note STRING, "
    "trace_index INT"
)
TRX_TRC10 = (
    "owner_address BINARY, name STRING, abbr STRING, total_supply BIGINT, "
    "trx_num BIGINT, num BIGINT, start_time BIGINT, end_time BIGINT, "
    "description STRING, url STRING, id INT, "
    "frozen_supply ARRAY<STRUCT<frozen_amount BIGINT, frozen_days BIGINT>>, "
    "public_latest_free_net_time BIGINT, vote_score SMALLINT, "
    "free_asset_net_limit BIGINT, public_free_asset_net_limit BIGINT, "
    "precision SMALLINT"
)
TRX_FEE = (
    "block_id INT, tx_hash BINARY, fee BIGINT, energy_usage BIGINT, "
    "energy_fee BIGINT, origin_energy_usage BIGINT, energy_usage_total BIGINT, "
    "net_usage BIGINT, net_fee BIGINT, result INT, energy_penalty_total BIGINT"
)


class FakeLake:
    """A ``DeltaLake`` with in-memory tables.

    Only ``spark`` and ``read`` are used by the loaders, and the block-range
    filter is applied here the way the real reader applies it, so a ranged test
    exercises the same code path.
    """

    def __init__(self, spark, tables):
        self.spark = spark
        self.tables = tables
        self.network = "test"
        self.partition_size = 100

    def read(
        self, table, *, start_block=None, end_block=None, block_column="block_id", **_
    ):
        from pyspark.sql import functions as F

        df = self.tables[table]
        if block_column is not None and block_column in df.columns:
            if start_block is not None:
                df = df.filter(F.col(block_column) >= start_block)
            if end_block is not None:
                df = df.filter(F.col(block_column) <= end_block)
        return df


# --------------------------------------------------------------------------- #
# utxo                                                                         #
# --------------------------------------------------------------------------- #


def _utxo_input(spent_hash, spent_index, index, addresses, value, script="ab"):
    return {
        "spent_transaction_hash": spent_hash,
        "spent_output_index": spent_index,
        "index": index,
        "sequence": 4294967295,
        "script_hex": script,
        "txinwitness": [b"\x01"],
        "type": "pubkeyhash",
        "addresses": addresses,
        "value": value,
    }


def _utxo_output(index, addresses, value, script="cd", type_="pubkeyhash"):
    return {
        "index": index,
        "script_hex": script,
        "addresses": addresses,
        "required_signatures": 1,
        "type": type_,
        "value": value,
    }


@pytest.fixture(scope="module")
def utxo_lake(spark):
    """Three blocks: 5 transactions in total, so the running tx_id is testable."""
    blocks = spark.createDataFrame(
        [
            {
                "block_id": 0,
                "block_hash": b"\x00",
                "timestamp": 0,
                "no_transactions": 1,
            },
            {
                "block_id": 1,
                "block_hash": b"\x01",
                "timestamp": 86_400,
                "no_transactions": 2,
            },
            {
                "block_id": 2,
                "block_hash": b"\x02",
                "timestamp": 172_800,
                "no_transactions": 2,
            },
        ],
        schema=UTXO_BLOCK,
    )

    def tx(block_id, index, tx_hash, inputs, outputs):
        return {
            "block_id": block_id,
            "index": index,
            "tx_hash": tx_hash,
            "timestamp": block_id * 86_400,
            "coinbase": index == 0,
            "coinjoin": False,
            "total_input": sum(i["value"] for i in inputs),
            "total_output": sum(o["value"] for o in outputs),
            "version": 1,
            "lock_time": 0,
            "input_count": len(inputs),
            "output_count": len(outputs),
            "inputs": inputs,
            "outputs": outputs,
        }

    txs = spark.createDataFrame(
        [
            tx(0, 0, b"\xa0" * 32, [], [_utxo_output(0, [GENESIS_COINBASE], 5000)]),
            tx(1, 0, b"\xb0" * 32, [], [_utxo_output(0, [SEGWIT], 5000)]),
            tx(
                1,
                1,
                b"\xb1" * 32,
                [_utxo_input(b"\xa0" * 32, 0, 0, [GENESIS_COINBASE], 5000)],
                [_utxo_output(0, [SEGWIT], 4000)],
            ),
            tx(2, 0, b"\xc0" * 32, [], [_utxo_output(0, [GENESIS_COINBASE], 5000)]),
            tx(
                2,
                1,
                b"\xc1" * 32,
                [_utxo_input(b"\xb1" * 32, 0, 0, [SEGWIT], 4000)],
                [_utxo_output(0, [], 0, type_="nulldata")],
            ),
        ],
        schema=UTXO_TRANSACTION,
    )
    return FakeLake(spark, {"block": blocks, "transaction": txs})


def test_utxo_frames_conform_to_the_schema(utxo_lake) -> None:
    """The payoff for schema-as-a-model: a column mismatch is caught here, not
    six hours into a backfill."""
    schema = schema_for("btc", Kind.RAW)
    frames = raw_utxo.build(utxo_lake, "btc", "btc_raw_v3")
    assert set(frames) == set(raw_utxo.TABLES)
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []


def test_utxo_tx_id_is_the_compound_block_and_index(utxo_lake) -> None:
    """(block_id << 32) + index, the same rule the account families use."""
    rows = raw_utxo.build(utxo_lake, "btc", "ks")["transaction"].collect()
    by_hash = {bytes(r["tx_hash"])[:1]: r["tx_id"] for r in rows}
    assert by_hash == {
        b"\xa0": tx_id(0, 0),
        b"\xb0": tx_id(1, 0),
        b"\xb1": tx_id(1, 1),
        b"\xc0": tx_id(2, 0),
        b"\xc1": tx_id(2, 1),
    }


def test_utxo_tx_id_still_orders_chronologically(utxo_lake) -> None:
    """The property the running counter was really providing: block order first,
    position within the block second. Every ORDER BY tx_id keeps its meaning."""
    rows = raw_utxo.build(utxo_lake, "btc", "ks")["transaction"].collect()
    ordered = sorted(rows, key=lambda r: r["tx_id"])
    assert [(r["block_id"], bytes(r["tx_hash"])[0]) for r in ordered] == [
        (0, 0xA0),
        (1, 0xB0),
        (1, 0xB1),
        (2, 0xC0),
        (2, 0xC1),
    ]


def test_utxo_tx_id_needs_nothing_outside_the_range(utxo_lake) -> None:
    """A ranged load reads no earlier block, so ranged and parallel backfills
    need no coordination -- the v2 transform counted every transaction before
    start_block on every run (transformation/utxo.py:145-148)."""
    rows = raw_utxo.build(utxo_lake, "btc", "ks", start_block=2)[
        "transaction"
    ].collect()
    assert sorted(r["tx_id"] for r in rows) == [tx_id(2, 0), tx_id(2, 1)]


def test_utxo_tx_id_decodes_back_to_its_block(utxo_lake) -> None:
    """first_tx_id -> height is arithmetic now. v2 spent a point read on
    `transaction` for it (db/asynchronous/cassandra.py:1986-1997)."""
    rows = raw_utxo.build(utxo_lake, "btc", "ks")["transaction"].collect()
    for row in rows:
        assert block_of_tx_id(row["tx_id"]) == row["block_id"]


def test_utxo_transaction_io_explodes_both_sides(utxo_lake) -> None:
    io = raw_utxo.build(utxo_lake, "btc", "ks")["transaction_io"]
    rows = io.where(io.tx_id == tx_id(1, 1)).collect()
    assert sorted((r["is_output"], r["io_index"]) for r in rows) == [
        (False, 0),
        (True, 0),
    ]
    spending = next(r for r in rows if not r["is_output"])
    assert spending["address"] == [encode_address("btc", GENESIS_COINBASE)]
    assert spending["address_type"] == 3  # pubkeyhash
    assert bytes(spending["script_hex"]) == b"\xab"
    assert spending["sequence"] == 4294967295


def test_utxo_addressless_outputs_store_no_address(utxo_lake) -> None:
    """`nulldata` and friends carry no address on the ingest path either."""
    io = raw_utxo.build(utxo_lake, "btc", "ks")["transaction_io"]
    row = io.where((io.tx_id == tx_id(2, 1)) & io.is_output).collect()[0]
    assert row["address"] is None
    assert row["address_type"] == 7


def test_utxo_spending_tables_mirror_each_other(utxo_lake) -> None:
    frames = raw_utxo.build(utxo_lake, "btc", "ks")
    spent_in = frames["transaction_spent_in"].collect()
    spending = frames["transaction_spending"].collect()
    assert len(spent_in) == len(spending) == 2
    row = next(r for r in spent_in if bytes(r["spent_tx_hash"])[0] == 0xA0)
    assert row["spent_tx_prefix"] == "a0a0a"  # tx_prefix_length = 5
    assert row["spent_output_index"] == 0
    assert bytes(row["spending_tx_hash"])[0] == 0xB1


def test_no_family_has_a_block_transactions_table(utxo_lake) -> None:
    """Gone from both under D12/D13. On UTXO `transaction` already held every
    column it had; on account its job was id -> hash, and the id now addresses
    the transaction directly."""
    assert "block_transactions" not in raw_utxo.build(utxo_lake, "btc", "ks")
    for network in ("btc", "eth", "trx"):
        assert "block_transactions" not in schema_for(network, Kind.RAW).table_names()


def test_utxo_block_by_date_buckets_by_utc_day(utxo_lake) -> None:
    rows = raw_utxo.build(utxo_lake, "btc", "ks")["block_by_date"].collect()
    assert sorted(str(r["day"]) for r in rows) == [
        "1970-01-01",
        "1970-01-02",
        "1970-01-03",
    ]


def test_utxo_configuration_carries_the_keyspace(utxo_lake) -> None:
    row = raw_utxo.build(utxo_lake, "btc", "btc_raw_v3")["configuration"].collect()[0]
    assert row["keyspace_name"] == "btc_raw_v3"
    assert row["block_bucket_size"] == config_for("btc").block_bucket_size


def test_utxo_preflight_is_clean_on_a_well_formed_lake(utxo_lake) -> None:
    assert raw_utxo.preflight(utxo_lake, "btc") == []


def test_utxo_preflight_tolerates_an_index_gap(spark, utxo_lake) -> None:
    """A gap in the indices is harmless now: (block << 32) + index does not care
    whether the run is dense. Under the running counter a gap shifted every
    subsequent id in the chain."""
    from pyspark.sql import functions as F

    gapped = utxo_lake.tables["transaction"].withColumn(
        "index",
        F.when(F.col("block_id") == 2, F.col("index") + 5).otherwise(F.col("index")),
    )
    lake = FakeLake(spark, {**utxo_lake.tables, "transaction": gapped})
    assert raw_utxo.preflight(lake, "btc") == []


def test_utxo_preflight_catches_colliding_indices(spark, utxo_lake) -> None:
    """The one thing the compound id does require: index is unique per block."""
    from pyspark.sql import functions as F

    broken = utxo_lake.tables["transaction"].withColumn("index", F.lit(0))
    lake = FakeLake(spark, {**utxo_lake.tables, "transaction": broken})
    problems = raw_utxo.preflight(lake, "btc")
    assert problems and "collide on a single tx_id" in problems[0]


# --------------------------------------------------------------------------- #
# account                                                                      #
# --------------------------------------------------------------------------- #


def _account_block(block_id):
    return {
        "block_id": block_id,
        "block_hash": b"\xb0",
        "parent_hash": b"\xa0",
        "nonce": b"\x00",
        "sha3_uncles": b"\x00",
        "logs_bloom": b"\x00",
        "transactions_root": b"\x00",
        "state_root": b"\x00",
        "receipts_root": b"\x00",
        "miner": b"\x11" * 20,
        # wide integers arrive from the lake as big-endian bytes
        "difficulty": (2**60).to_bytes(8, "big"),
        "total_difficulty": (2**70).to_bytes(9, "big"),
        "size": 1234,
        "extra_data": b"",
        "gas_limit": 30_000_000,
        "gas_used": 21_000,
        "base_fee_per_gas": 7,
        "timestamp": block_id * 86_400,
        "transaction_count": 1,
    }


def _account_tx(block_id, index, tx_hash):
    return {
        "tx_hash": tx_hash,
        "tx_hash_prefix": "ignored",
        "nonce": 1,
        "block_hash": b"\xb0",
        "block_id": block_id,
        "transaction_index": index,
        "from_address": b"\x01" * 20,
        "to_address": b"\x02" * 20,
        "value": (10**18).to_bytes(8, "big"),
        "gas": 21_000,
        "gas_price": 1_000_000_000,
        "input": b"",
        "block_timestamp": block_id * 86_400,
        "max_fee_per_gas": 2,
        "max_priority_fee_per_gas": 1,
        "transaction_type": 2,
        "receipt_cumulative_gas_used": 21_000,
        "receipt_gas_used": 21_000,
        "receipt_contract_address": None,
        "receipt_root": None,
        "receipt_status": 1,
        "receipt_effective_gas_price": 2,
        "max_fee_per_blob_gas": None,
        "blob_versioned_hashes": [],
        "v": 27,
        "r": b"\xff" * 32,
        "s": b"\xee" * 32,
    }


@pytest.fixture(scope="module")
def eth_lake(spark):
    blocks = spark.createDataFrame(
        [_account_block(0), _account_block(1)], schema=ACCOUNT_BLOCK
    )
    txs = spark.createDataFrame(
        [_account_tx(0, 0, b"\xa0" * 32), _account_tx(1, 0, b"\xb0" * 32)],
        schema=ACCOUNT_TRANSACTION,
    )
    logs = spark.createDataFrame(
        [
            {
                "block_id": 0,
                "block_hash": b"\xb0",
                "address": b"\x03" * 20,
                "data": b"",
                "topics": [b"\x01" * 32],
                "topic0": b"\x01" * 32,
                "tx_hash": b"\xa0" * 32,
                "log_index": index,
                "transaction_index": 0,
            }
            for index in (0, 1, 2)
        ],
        schema=ACCOUNT_LOG,
    )
    traces = spark.createDataFrame(
        [
            {
                "block_id": 0,
                "tx_hash": b"\xa0" * 32,
                "transaction_index": 0,
                "from_address": b"\x01" * 20,
                "to_address": b"\x02" * 20,
                "value": (10**18).to_bytes(8, "big"),
                "input": b"",
                "output": b"",
                "trace_type": "call",
                "call_type": "call",
                "reward_type": None,
                "gas": 21_000,
                "gas_used": 21_000,
                "subtraces": 0,
                "trace_address": "",
                "error": None,
                "status": 1,
                "trace_id": "x",
                "trace_index": 0,
            }
        ],
        schema=ETH_TRACE,
    )
    return FakeLake(
        spark, {"block": blocks, "transaction": txs, "log": logs, "trace": traces}
    )


def test_eth_frames_conform_to_the_schema(eth_lake) -> None:
    schema = schema_for("eth", Kind.RAW)
    frames = raw_account.build(eth_lake, "eth", "eth_raw_v3")
    assert set(frames) == set(raw_account.tables_for("eth"))
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []


def test_eth_range_pointers_address_the_logs(eth_lake) -> None:
    """Three logs on one transaction become (first, count), which is the whole
    reason there is no per-transaction log table."""
    rows = raw_account.build(eth_lake, "eth", "ks")["transaction"].collect()
    first = next(r for r in rows if bytes(r["tx_hash"])[0] == 0xA0)
    assert (first["first_log_index"], first["no_logs"]) == (0, 3)
    assert (first["first_trace_index"], first["no_traces"]) == (0, 1)


def test_eth_transaction_without_logs_gets_a_zero_count(eth_lake) -> None:
    """A left join would otherwise leave NULL, which reads as 'unknown' rather
    than 'none'."""
    rows = raw_account.build(eth_lake, "eth", "ks")["transaction"].collect()
    empty = next(r for r in rows if bytes(r["tx_hash"])[0] == 0xB0)
    assert empty["no_logs"] == 0 and empty["first_log_index"] is None


def test_eth_wide_integers_survive_as_decimals(eth_lake) -> None:
    row = raw_account.build(eth_lake, "eth", "ks")["block"].collect()[0]
    assert int(row["difficulty"]) == 2**60
    assert int(row["total_difficulty"]) == 2**70


def test_eth_signature_halves_stay_opaque_bytes(eth_lake) -> None:
    """v2 stored r and s as varint, where a random 32-byte half routinely needs
    77 decimal digits -- more than any decimal type carries."""
    row = raw_account.build(eth_lake, "eth", "ks")["transaction"].collect()[0]
    assert bytes(row["r"]) == b"\xff" * 32


def test_eth_hash_lookup_goes_through_the_shared_prefix_table(eth_lake) -> None:
    """D13: account resolves a hash the way UTXO does, and the prefix comes from
    the hash rather than the lake's own column (which the fixture sets wrong)."""
    frames = raw_account.build(eth_lake, "eth", "ks")
    rows = frames["transaction_by_tx_prefix"].collect()
    assert {r["tx_prefix"] for r in rows} == {"a0a0a", "b0b0b"}
    # and the id it yields addresses the transaction directly
    ids = {r["tx_id"] for r in rows}
    assert ids == {r["tx_id"] for r in frames["transaction"].collect()}


def test_eth_transaction_is_addressed_by_id(eth_lake) -> None:
    rows = raw_account.build(eth_lake, "eth", "ks")["transaction"].collect()
    for row in rows:
        assert row["tx_id"] == tx_id(row["block_id"], row["transaction_index"])
        assert block_of_tx_id(row["tx_id"]) == row["block_id"]


def test_eth_preflight_is_clean_on_contiguous_indices(eth_lake) -> None:
    assert raw_account.preflight(eth_lake, "eth") == []


def test_eth_preflight_catches_a_log_index_gap(spark, eth_lake) -> None:
    """The doc's PRE-RUN CHECK: if an index run has a hole, (first, count)
    addresses rows that are not the transaction's."""
    from pyspark.sql import functions as F

    holed = eth_lake.tables["log"].withColumn(
        "log_index",
        F.when(F.col("log_index") == 2, F.lit(9)).otherwise(F.col("log_index")),
    )
    lake = FakeLake(spark, {**eth_lake.tables, "log": holed})
    problems = raw_account.preflight(lake, "eth")
    assert problems and "log_index" in problems[0]


@pytest.fixture(scope="module")
def trx_lake(spark, eth_lake):
    traces = spark.createDataFrame(
        [
            {
                "block_id": 0,
                "tx_hash": b"\xa0" * 32,
                "internal_index": 0,
                "transferto_address": b"\x02" * 20,
                "call_info_index": 0,
                "caller_address": b"\x01" * 20,
                "call_value": (10**6).to_bytes(4, "big"),
                "rejected": False,
                "call_token_id": 1002000,
                "note": "call",
                "trace_index": 0,
            }
        ],
        schema=TRX_TRACE,
    )
    trc10 = spark.createDataFrame(
        [
            {
                "id": 1002000,
                "owner_address": b"\x01" * 20,
                "name": "Token",
                "abbr": "TKN",
                "total_supply": 1_000_000,
                "trx_num": 1,
                "num": 1,
                "start_time": 0,
                "end_time": 0,
                "description": "",
                "url": "",
                "frozen_supply": [{"frozen_amount": 1, "frozen_days": 2}],
                "public_latest_free_net_time": 0,
                "vote_score": 0,
                "free_asset_net_limit": 0,
                "public_free_asset_net_limit": 0,
                "precision": 6,
            }
        ],
        schema=TRX_TRC10,
    )
    fees = spark.createDataFrame(
        [
            {
                "block_id": 0,
                "tx_hash": b"\xa0" * 32,
                "fee": 100,
                "energy_usage": 1,
                "energy_fee": 2,
                "origin_energy_usage": 3,
                "energy_usage_total": 4,
                "net_usage": 5,
                "net_fee": 6,
                "result": 0,
                "energy_penalty_total": 7,
            }
        ],
        schema=TRX_FEE,
    )
    return FakeLake(
        spark,
        {
            "block": eth_lake.tables["block"],
            "transaction": eth_lake.tables["transaction"],
            "log": eth_lake.tables["log"],
            "trace": traces,
            "trc10": trc10,
            "fee": fees,
        },
    )


def test_trx_frames_conform_to_the_schema(trx_lake) -> None:
    """TRON's trace model and its two extra tables are what forced the account
    raw schema to render per chain."""
    schema = schema_for("trx", Kind.RAW)
    frames = raw_account.build(trx_lake, "trx", "trx_raw_v3")
    assert {"trc10", "fee"} <= set(frames)
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []


def test_trx_fee_is_keyed_like_the_transaction(trx_lake) -> None:
    """D13 all the way down: the tx_id in hand reads the fee directly. Keyed by
    hash it would have cost a third hop, id -> tx -> hash -> prefix -> fee."""
    frames = raw_account.build(trx_lake, "trx", "ks")
    fee = frames["fee"].collect()[0]
    tx = next(
        r
        for r in frames["transaction"].collect()
        if bytes(r["tx_hash"]) == bytes(fee["tx_hash"])
    )
    assert (fee["block_id_group"], fee["tx_id"]) == (tx["block_id_group"], tx["tx_id"])


def test_trx_trace_is_renamed_onto_the_shared_columns(trx_lake) -> None:
    """The lake keeps TRON's names; the keyspace does not."""
    row = raw_account.build(trx_lake, "trx", "ks")["trace"].collect()[0]
    assert int(row["value"]) == 10**6
    assert bytes(row["from_address"]) == b"\x01" * 20
    assert bytes(row["to_address"]) == b"\x02" * 20
    assert row["call_token_id"] == 1002000
    fields = row.asDict()
    assert "transaction_index" not in fields
    assert not {"caller_address", "transferto_address", "call_value"} & set(fields)


def test_utxo_preflight_reports_the_fattest_block(spark, utxo_lake) -> None:
    """A block is a partition now, and the bound on it is consensus rather than
    arithmetic -- v2's dense tx_id gave exactly tx_bucket_size rows. Consensus is
    loose on BCH, so measure the tail instead of assuming it."""
    assert raw_utxo.biggest_block(utxo_lake.tables["transaction"]) == (1, 2)
    assert raw_utxo.preflight(utxo_lake, "btc") == []


def test_eth_log_and_trace_carry_the_transaction_id(eth_lake) -> None:
    """Both link back by id now, so nothing has to resolve a hash first."""
    frames = raw_account.build(eth_lake, "eth", "ks")
    expected = tx_id(0, 0)
    assert {r["tx_id"] for r in frames["log"].collect()} == {expected}
    assert {r["tx_id"] for r in frames["trace"].collect()} == {expected}


def test_trx_trace_gets_its_id_from_the_hash_join(trx_lake) -> None:
    """A TRON trace row has no transaction_index, so the id costs a join. That
    is the whole reason this was flagged as a cost rather than a rename."""
    frames = raw_account.build(trx_lake, "trx", "ks")
    trace = frames["trace"].collect()[0]
    tx = next(
        r
        for r in frames["transaction"].collect()
        if bytes(r["tx_hash"]) == bytes(trace["tx_hash"])
    )
    assert trace["tx_id"] == tx["tx_id"]


@pytest.mark.parametrize(
    ("rejected", "expected"), [(False, 1), (True, 0), (None, None)]
)
def test_trx_rejected_maps_onto_the_eth_status_convention(
    spark, trx_lake, rejected, expected
) -> None:
    """TRON has no status code. 1 = success, 0 = failed, and an unknown
    `rejected` stays unknown rather than being read as success."""
    from pyspark.sql import functions as F

    traces = trx_lake.tables["trace"].withColumn(
        "rejected", F.lit(rejected).cast("boolean")
    )
    lake = FakeLake(spark, {**trx_lake.tables, "trace": traces})
    row = raw_account.build(lake, "trx", "ks")["trace"].collect()[0]
    assert row["status"] == expected
    assert "rejected" not in row.asDict()
