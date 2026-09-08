"""Raw account -> derived address tables.

The parts worth testing hardest are the ones with no UTXO counterpart: decoding
a Transfer log, and pricing a token, where a wrong answer looks plausible.
"""

from decimal import Decimal

import pytest

from graphsense_v3.codec import tx_id
from graphsense_v3.config import config_for
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.spark import derived_account as tf
from graphsense_v3.spark.writer import conformance_errors

ALICE = b"\xa1" * 20
BOB = b"\xb0" * 20
USDT = b"\xda\xc1" + b"\x7f" * 18

TRACE_SCHEMA = (
    "block_id_group INT, block_id INT, trace_index INT, tx_hash BINARY, "
    "tx_id BIGINT, from_address BINARY, to_address BINARY, value DECIMAL(38,0), "
    "status SMALLINT, trace_type STRING, call_type STRING"
)
LOG_SCHEMA = (
    "block_id_group INT, block_id INT, log_index INT, address BINARY, "
    "data BINARY, topics ARRAY<BINARY>, topic0 BINARY, tx_id BIGINT"
)
TOKEN_SCHEMA = (
    "currency_ticker STRING, token_address BINARY, standard STRING, decimals INT, "
    "decimal_divisor BIGINT, peg_currency STRING"
)
#: The keyspace's fiat ordering. `fiat_values` is a positional list, so a test
#: that reads it has to say which position it means -- and the source of that
#: ordering is the configuration row, never a literal here.
FIAT = config_for("eth").fiat_currencies

RATES_SCHEMA = "asset STRING, block_id INT, fiat_values MAP<STRING,DOUBLE>"
BLOCK_SCHEMA = (
    "block_id INT, timestamp BIGINT, miner BINARY, gas_used BIGINT, "
    "base_fee_per_gas BIGINT"
)
TX_SCHEMA = (
    "tx_id BIGINT, block_id INT, from_address BINARY, to_address BINARY, "
    "value DECIMAL(38,0), receipt_status TINYINT, receipt_gas_used BIGINT, "
    "gas_price DECIMAL(38,0), receipt_contract_address BINARY"
)


def _topic(address: bytes) -> bytes:
    """An indexed address topic: 32 bytes, left-padded."""
    return b"\x00" * 12 + address


def _word(value: int) -> bytes:
    return value.to_bytes(32, "big")


@pytest.fixture(scope="module")
def traces(spark):
    """One successful transfer, one failed, and a contract creation."""
    return spark.createDataFrame(
        [
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": 0,
                "tx_hash": b"\xa0",
                "tx_id": tx_id(1, 0),
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(10**18),
                "status": 1,
                "trace_type": "call",
            },
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": 1,
                "tx_hash": b"\xa0",
                "tx_id": tx_id(1, 0),
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(5 * 10**18),
                "status": 0,
                "trace_type": "call",
            },
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": 2,
                "tx_hash": b"\xa1",
                "tx_id": tx_id(1, 1),
                "from_address": ALICE,
                "to_address": USDT,
                "value": Decimal(0),
                "status": 1,
                "trace_type": "create",
            },
        ],
        schema=TRACE_SCHEMA,
    )


@pytest.fixture(scope="module")
def logs(spark):
    """A USDT Transfer, and a same-signature event from an unconfigured token."""
    return spark.createDataFrame(
        [
            {
                "block_id_group": 0,
                "block_id": 1,
                "log_index": 0,
                "address": USDT,
                "data": _word(2_000_000),
                "topics": [tf.TRANSFER_TOPIC0, _topic(ALICE), _topic(BOB)],
                "topic0": tf.TRANSFER_TOPIC0,
                "tx_id": tx_id(1, 0),
            },
            {
                "block_id_group": 0,
                "block_id": 1,
                "log_index": 1,
                "address": b"\xff" * 20,
                "data": _word(999),
                "topics": [tf.TRANSFER_TOPIC0, _topic(ALICE), _topic(BOB)],
                "topic0": tf.TRANSFER_TOPIC0,
                "tx_id": tx_id(1, 0),
            },
        ],
        schema=LOG_SCHEMA,
    )


@pytest.fixture(scope="module")
def token_config(spark):
    return spark.createDataFrame(
        [
            {
                "currency_ticker": "USDT",
                "token_address": USDT,
                "standard": "ERC20",
                "decimals": 6,
                "decimal_divisor": 10**6,
                "peg_currency": "USD",
            }
        ],
        schema=TOKEN_SCHEMA,
    )


MINER = b"\xcc" * 20


@pytest.fixture(scope="module")
def blocks(spark):
    return spark.createDataFrame(
        [
            {
                "block_id": 1,
                "timestamp": 0,
                "miner": MINER,
                "gas_used": 100,
                "base_fee_per_gas": 2,
            }
        ],
        schema=BLOCK_SCHEMA,
    )


@pytest.fixture(scope="module")
def transactions(spark):
    """One transaction, paying 21000 * 10 gwei in gas."""
    return spark.createDataFrame(
        [
            {
                "tx_id": tx_id(1, 0),
                "block_id": 1,
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(10**18),
                "receipt_status": 1,
                "receipt_gas_used": 21_000,
                "gas_price": Decimal(10**10),
                "receipt_contract_address": None,
            }
        ],
        schema=TX_SCHEMA,
    )


@pytest.fixture(scope="module")
def rates(spark):
    return spark.createDataFrame(
        [
            {
                "asset": "ETH",
                "block_id": 1,
                "fiat_values": {"EUR": 2000.0, "USD": 2500.0},
            }
        ],
        schema=RATES_SCHEMA,
    )


def _build(traces, logs, token_config, blocks, rates, transactions):
    return tf.build(
        traces,
        logs,
        token_config,
        blocks,
        rates,
        "eth",
        transactions=transactions,
    )


def test_a_failed_trace_moved_nothing(traces) -> None:
    """Counting it would invent a transfer."""
    rows = tf.native_transfers(traces, "eth").collect()
    assert [int(r["value"]) for r in rows] == [10**18, 0]


def test_transfers_come_from_traces_not_the_transaction(traces) -> None:
    """A trace covers the top-level transfer and every internal one, so reading
    traces gets both. There is no separate native-transfer source."""
    rows = tf.native_transfers(traces, "eth").collect()
    assert {r["currency"] for r in rows} == {"ETH"}
    assert all(r["log_index"] is None for r in rows)
    assert sorted(r["trace_index"] for r in rows) == [0, 2]


def test_token_transfer_is_decoded_from_the_log(logs, token_config) -> None:
    """topic0 is the Transfer selector, topics 1 and 2 are the indexed from and
    to (address in the low 20 bytes of a 32-byte word), and the value is the
    single non-indexed parameter in the first word of data."""
    rows = tf.token_transfers(logs, token_config).collect()
    assert len(rows) == 1
    row = rows[0]
    assert bytes(row["src_address"]) == ALICE
    assert bytes(row["dst_address"]) == BOB
    assert int(row["value"]) == 2_000_000
    assert row["currency"] == "USDT"
    assert row["trace_index"] is None and row["log_index"] == 0


def test_a_uint256_too_wide_for_a_decimal_is_stored_null(
    spark, logs, token_config
) -> None:
    """A scam token minting 2^255 units is ordinary, so the run must not stop
    on it -- but NULL, never a wrapped or clamped number: Cassandra's varint
    would hold it and Arrow's decimal128 is what cannot."""
    from pyspark.sql import functions as F

    huge = logs.where(F.col("address") == USDT).withColumn("data", F.lit(_word(2**255)))
    rows = tf.token_transfers(huge, token_config).collect()
    assert len(rows) == 1
    assert rows[0]["value"] is None
    assert rows[0]["currency"] == "USDT"


def test_an_unconfigured_token_cannot_decide_the_run(spark, logs, token_config) -> None:
    """The value decode happens AFTER the token_configuration join, so a
    contract this keyspace does not store cannot reach the decoder at all. It
    could before, and an unconfigured token minting 2^255 units failed the
    first eth backfill on a row that was about to be discarded anyway."""
    from pyspark.sql import functions as F

    unconfigured = logs.where(F.col("address") != USDT).withColumn(
        "data", F.lit(_word(2**255))
    )
    assert unconfigured.count() == 1
    assert tf.token_transfers(unconfigured, token_config).collect() == []


def test_the_decode_never_runs_on_a_row_the_join_discards(
    monkeypatch, logs, token_config
) -> None:
    """The ORDERING, not just its outcome. A decoder that raises on everything
    must still yield an empty result for unconfigured logs -- it is never
    reached. With the decode before the join it would raise instead, which is
    precisely how an eth backfill failed on a row it was about to discard."""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DecimalType

    @pandas_udf(DecimalType(38, 0))  # ty: ignore[no-matching-overload]
    def _never(values: pd.Series) -> pd.Series:
        raise AssertionError("the decoder ran on a row the join discards")

    monkeypatch.setattr(tf, "bytes_to_varint_udf", lambda *a, **k: _never)
    unconfigured = logs.where(F.col("address") != USDT)
    assert unconfigured.count() == 1
    assert tf.token_transfers(unconfigured, token_config).collect() == []


def test_only_configured_assets_are_counted_as_unrepresentable(
    spark, logs, token_config
) -> None:
    """The counter has to mean 'we could not store this', not 'we decoded
    something we were going to throw away'."""
    from pyspark.sql import functions as F

    wide = logs.withColumn("data", F.lit(_word(2**255)))
    transfers = tf.token_transfers(wide, token_config)
    assert tf.unrepresentable_values(transfers) == [("USDT", 1)]


def test_a_native_value_that_wide_still_stops_the_run(traces) -> None:
    """The same width in a native amount is a misread, not a chain fact: ETH's
    entire supply is ~1.2e26 wei. That one must fail loudly."""
    from pyspark.sql import functions as F

    from graphsense_v3.spark.columns import bytes_to_varint_udf

    varint = bytes_to_varint_udf("trace.value")
    with pytest.raises(Exception) as caught:
        traces.select(varint(F.lit(_word(2**255))).alias("v")).collect()
    assert "38" in str(caught.value)


def test_a_total_containing_an_unrepresentable_value_is_null_not_short(
    spark,
) -> None:
    """`F.sum` skips nulls, so the affected asset's total would silently
    UNDERSTATE. NULL says unknown; an understated balance says a wrong number
    with confidence."""
    from graphsense_v3.spark import derived_common as common

    rows = spark.createDataFrame(
        [
            ("a", "USDT", Decimal(5)),
            ("a", "USDT", None),
            ("a", "ETH", Decimal(7)),
        ],
        "address STRING, currency STRING, value DECIMAL(38,0)",
    )
    totals = {
        (r["address"], r["currency"]): r["_value"]
        for r in rows.groupBy("address", "currency").agg(common.sum_or_null()).collect()
    }
    assert totals[("a", "USDT")] is None
    # The native total is untouched: only the asset with the null is unknown.
    assert int(totals[("a", "ETH")]) == 7


def test_account_fiat_totals_are_really_summed(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """The regression only the bulk writer caught. `sum_fiat` indexes
    `fiat_values` POSITIONALLY, and `priced` used to emit a map; getItem(0) on
    a map<text,double> does not fail -- Spark casts the key to "0" and returns
    NULL -- so every priced account total came out NULL, silently, until
    "Collection elements cannot be null" surfaced inside the UDT codec.

    Asserted on a total that actually MOVED something: an address with nothing
    incoming is zero-filled to [0.0, 0.0] by `zero_currency`, which is non-NULL
    under the bug too and makes a laxer assertion pass on nothing.
    """
    frames = _build(traces, logs, token_config, blocks, rates, transactions)
    moved = [
        row["total_received"]
        for row in frames["address_stats"].collect()
        if row["total_received"]["value"] > 0
    ]
    assert moved, "no address received anything, so nothing was priced"
    for total in moved:
        assert total["fiat_values"] is not None
        assert len(total["fiat_values"]) == len(FIAT)
        assert all(v is not None for v in total["fiat_values"])
        assert any(v > 0 for v in total["fiat_values"])


def test_a_group_with_nothing_priced_is_null_not_a_list_of_nulls(spark) -> None:
    """`F.array` over all-NULL sums yields a list CONTAINING nulls, which
    Cassandra forbids in a collection. NULL is also the honest answer: no leg
    had a rate, so the total is unknown rather than zero."""
    from graphsense_v3.spark import derived_common as common

    rows = spark.createDataFrame(
        [("a", None), ("a", None), ("b", [1.5, 2.0])],
        "k STRING, fiat_values ARRAY<DOUBLE>",
    )
    out = {r["k"]: r["_fiat"] for r in common.sum_fiat(rows, ["k"], FIAT).collect()}
    assert out["a"] is None
    assert out["b"] == [1.5, 2.0]


def test_a_partly_rated_group_still_totals_what_it_knows(spark) -> None:
    """One unrated leg must not void the whole total -- `sum` skipping it is
    the documented behaviour, and only an entirely unrated group is unknown."""
    from graphsense_v3.spark import derived_common as common

    rows = spark.createDataFrame(
        [("a", [1.5, 2.0]), ("a", None)], "k STRING, fiat_values ARRAY<DOUBLE>"
    )
    assert common.sum_fiat(rows, ["k"], FIAT).collect()[0]["_fiat"] == [1.5, 2.0]


def test_unrepresentable_values_are_counted_per_asset(spark) -> None:
    """A run has to say what it could not store, or the missing rows are only
    discoverable by noticing a balance is wrong."""
    moves = spark.createDataFrame(
        [("USDT", None), ("USDT", None), ("ETH", Decimal(3)), ("MOON", None)],
        "currency STRING, value DECIMAL(38,0)",
    )
    assert tf.unrepresentable_values(moves) == [("USDT", 2), ("MOON", 1)]


def test_an_unconfigured_contract_is_not_a_token(logs, token_config) -> None:
    """Any contract can emit a Transfer with the same signature. Only a
    configured asset has a ticker, decimals and a peg to price it with."""
    rows = tf.token_transfers(logs, token_config).collect()
    assert all(bytes(r["src_address"]) == ALICE for r in rows)
    assert len(rows) == 1  # the 0xff.. contract's event is not a transfer


def test_a_pegged_token_is_worth_its_face_value(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """A USD-pegged stablecoin is 2.0 USD for 2_000_000 base units at 6
    decimals; the other fiat currency follows from the base cross rate."""
    moves = tf.priced(
        tf.transfers(traces, logs, token_config, "eth"),
        rates,
        token_config,
        "eth",
        FIAT,
    )
    token = next(r for r in moves.collect() if r["currency"] == "USDT")
    assert token["fiat_values"][FIAT.index("USD")] == pytest.approx(2.0)
    # EUR per USD = 2000/2500, so 2 USD is 1.60 EUR
    assert token["fiat_values"][FIAT.index("EUR")] == pytest.approx(1.6)


def test_the_native_coin_is_priced_from_the_block_rate(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    moves = tf.priced(
        tf.transfers(traces, logs, token_config, "eth"),
        rates,
        token_config,
        "eth",
        FIAT,
    )
    native = next(
        r for r in moves.collect() if r["currency"] == "ETH" and int(r["value"]) > 0
    )
    assert native["fiat_values"][FIAT.index("USD")] == pytest.approx(2500.0)


def test_an_unpegged_token_without_a_rate_gets_no_fiat(
    spark, traces, logs, rates
) -> None:
    """Not a zero, which would read as 'worthless' rather than 'unknown'."""
    unpegged = spark.createDataFrame(
        [
            {
                "currency_ticker": "USDT",
                "token_address": USDT,
                "standard": "ERC20",
                "decimals": 6,
                "decimal_divisor": 10**6,
                "peg_currency": None,
            }
        ],
        schema=TOKEN_SCHEMA,
    )
    moves = tf.priced(
        tf.transfers(traces, logs, unpegged, "eth"), rates, unpegged, "eth", FIAT
    )
    token = next(r for r in moves.collect() if r["currency"] == "USDT")
    assert token["fiat_values"] is None
    assert int(token["value"]) == 2_000_000  # the amount itself is unaffected


def test_a_transfer_names_both_ends_so_a_leg_is_not_netted(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """No apportioning and no netting question: direction is a property of the
    leg, not of a sum, which is why D7 has no account counterpart."""
    moves = tf.priced(
        tf.transfers(traces, logs, token_config, "eth"),
        rates,
        token_config,
        "eth",
        FIAT,
    )
    rows = tf.legs(moves).collect()
    alice = [r for r in rows if bytes(r["address"]) == ALICE]
    assert all(r["is_outgoing"] for r in alice)
    assert {r["currency"] for r in alice} == {"ETH", "USDT"}


def test_a_contract_deployed_internally_is_still_a_contract(traces) -> None:
    """A factory-deployed contract appears only as an internal create trace --
    what the delta updater missed on TRON until 2026-06-30."""
    rows = tf.contracts(traces, "eth").collect()
    assert [bytes(r["address"]) for r in rows] == [USDT]
    assert all(r["is_contract"] for r in rows)


def test_stats_separate_native_from_token_totals(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    stats = _build(traces, logs, token_config, blocks, rates, transactions)[
        "address_stats"
    ]
    alice = next(r for r in stats.collect() if bytes(r["address"]) == ALICE)
    assert int(alice["total_spent"]["value"]) == 10**18
    assert int(alice["total_tokens_spent"]["USDT"]["value"]) == 2_000_000
    assert alice["is_contract"] is False
    assert alice["out_degree"] == 2  # BOB and the created contract


def test_balance_is_per_asset(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """An account address holds a balance in every token it has touched, which
    is why currency is in this table's key and not in UTXO's."""
    rows = _build(traces, logs, token_config, blocks, rates, transactions)[
        "balance"
    ].collect()
    by_key = {(bytes(r["address"]), r["currency"]): int(r["balance"]) for r in rows}
    assert by_key[(ALICE, "USDT")] == -2_000_000
    assert by_key[(BOB, "USDT")] == 2_000_000
    # The native balance is NOT the sum of the transfer legs: the sender also
    # paid for the gas. graphsense-spark has the same five terms.
    gas = 21_000 * 10**10
    assert by_key[(ALICE, "ETH")] == -(10**18) - gas
    # The miner is paid the gas and burns the base fee, keeping the priority
    # fee -- so it is not simply `gas` either.
    assert by_key[(MINER, "ETH")] == gas - 2 * 100


def test_the_gas_a_transfer_paid_is_not_a_transfer(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """A fee moves value without being a transfer, so it must not appear in the
    transfer graph -- only in the balance."""
    frames = _build(traces, logs, token_config, blocks, rates, transactions)
    addresses = {bytes(r["address"]) for r in frames["address_transactions"].collect()}
    assert MINER not in addresses


def test_relations_carry_token_values(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    frames = _build(traces, logs, token_config, blocks, rates, transactions)
    edge = next(
        r
        for r in frames["address_outgoing_relations"].collect()
        if bytes(r["dst_address"]) == BOB
    )
    assert edge["no_transactions"] == 2  # one ETH, one USDT
    assert int(edge["value"]["value"]) == 10**18
    assert int(edge["token_values"]["USDT"]["value"]) == 2_000_000


def test_link_transactions_are_partitioned_per_edge(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """The account half of D10: fewer addresses with more transactions per edge,
    so the repeated destination costs less than the partitions it saves."""
    table = schema_for("eth", Kind.DERIVED).table("address_link_transactions")
    assert table.key.partition == ("src_address", "dst_address", "tx_page")
    links = _build(traces, logs, token_config, blocks, rates, transactions)[
        "address_link_transactions"
    ].collect()
    assert {r["currency"] for r in links} == {"ETH", "USDT"}


def test_every_frame_conforms_to_its_table(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    schema = schema_for("eth", Kind.DERIVED)
    frames = _build(traces, logs, token_config, blocks, rates, transactions)
    assert set(frames) == set(tf.TABLES)
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []


def test_tron_uses_its_own_native_symbol_and_divisor() -> None:
    assert tf.NATIVE["trx"] == ("TRX", 10**6)
    assert config_for("trx").entity_buckets > 0


def test_zero_value_transfers_are_a_separate_partition(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """The reason this exists: on ETH and TRON a zero-value transfer is a
    contract call that moved nothing, and they dominate an address's listing."""
    frames = _build(traces, logs, token_config, blocks, rates, transactions)
    rows = frames["address_transactions"].collect()
    zero = [r for r in rows if r["is_zero_value"]]
    assert zero and all(int(r["value"]) == 0 for r in zero)
    assert all(int(r["value"]) != 0 for r in rows if not r["is_zero_value"])


def test_balance_history_is_per_asset_and_cumulative(
    traces, logs, token_config, blocks, rates, transactions
) -> None:
    """Per asset, like `balance` -- an account address holds a history in every
    token it has touched."""
    rows = _build(traces, logs, token_config, blocks, rates, transactions)[
        "balance_history"
    ].collect()
    by_key = {(bytes(r["address"]), r["currency"]): int(r["balance"]) for r in rows}
    assert by_key[(ALICE, "USDT")] == -2_000_000
    assert by_key[(BOB, "ETH")] == 10**18
    assert {r["day"] for r in rows} == {19700101}


def test_a_borrowed_context_call_is_not_a_transfer(spark) -> None:
    """delegatecall, callcode and staticcall execute in the CALLER's context.
    Their `value` is apparatus, not a movement, and counting it invents
    transfers -- graphsense-spark excludes exactly these three
    (`eth/Transformation.scala:164`). A NULL call_type is kept, as there."""
    traces = spark.createDataFrame(
        [
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": i,
                "tx_hash": b"\xa0",
                "tx_id": tx_id(1, 0),
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(10**18),
                "status": 1,
                "trace_type": "call",
                "call_type": call_type,
            }
            for i, call_type in enumerate(
                ["call", "delegatecall", "callcode", "staticcall", None]
            )
        ],
        schema=TRACE_SCHEMA,
    )
    kept = tf.native_transfers(traces, "eth").collect()
    assert sorted(r["trace_index"] for r in kept) == [0, 4]


TRX_TRACE_SCHEMA = (
    "block_id_group INT, block_id INT, trace_index INT, tx_hash BINARY, "
    "tx_id BIGINT, from_address BINARY, to_address BINARY, value DECIMAL(38,0), "
    "status SMALLINT, call_token_id INT, note STRING"
)
TRX_FEE_SCHEMA = "tx_id BIGINT, fee BIGINT"


@pytest.fixture(scope="module")
def trx_traces(spark):
    """An internal call, a TRC-10 movement, and a contract deployment."""
    return spark.createDataFrame(
        [
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": 0,
                "tx_hash": b"\xa0",
                "tx_id": tx_id(1, 0),
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(7),
                "status": 1,
                "call_token_id": None,
                "note": "call",
            },
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": 1,
                "tx_hash": b"\xa0",
                "tx_id": tx_id(1, 0),
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(999),
                "status": 1,
                "call_token_id": 31_303,
                "note": "call",
            },
            {
                "block_id_group": 0,
                "block_id": 1,
                "trace_index": 2,
                "tx_hash": b"\xa1",
                "tx_id": tx_id(1, 1),
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(5),
                "status": 1,
                "call_token_id": None,
                "note": "create",
            },
        ],
        schema=TRX_TRACE_SCHEMA,
    )


def test_a_trc10_trace_is_not_a_native_trx_transfer(trx_traces) -> None:
    """`isTrxTrace = callTokenId.isNull` (`trx/Transformation.scala:50`): a
    trace carrying a token id moved a TRC-10, not TRX, and counting its value as
    native would credit the address in the wrong asset. `isCallTrace` drops the
    deployment the same way."""
    rows = tf.native_transfers(trx_traces, "trx").collect()
    assert [int(r["value"]) for r in rows] == [7]
    assert {r["currency"] for r in rows} == {"TRX"}


def test_tron_top_level_transfers_come_from_the_transaction(spark, trx_traces) -> None:
    """A TRON trace is an INTERNAL call, so the transfer the transaction itself
    performs is in none of them -- which is why graphsense-spark sums traces AND
    transactions (`trx/Transformation.scala:174-183`)."""
    txs = spark.createDataFrame(
        [
            {
                "tx_id": tx_id(1, 0),
                "block_id": 1,
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(100),
                "receipt_status": 1,
                "receipt_gas_used": 0,
                "gas_price": Decimal(0),
                "receipt_contract_address": None,
            },
            {  # a deployment: no recipient of its own, so not a transfer
                "tx_id": tx_id(1, 1),
                "block_id": 1,
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(3),
                "receipt_status": 1,
                "receipt_gas_used": 0,
                "gas_price": Decimal(0),
                "receipt_contract_address": b"\xde" * 20,
            },
            {  # failed: moved nothing
                "tx_id": tx_id(1, 2),
                "block_id": 1,
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(50),
                "receipt_status": 0,
                "receipt_gas_used": 0,
                "gas_price": Decimal(0),
                "receipt_contract_address": None,
            },
        ],
        schema=TX_SCHEMA,
    )
    assert [int(r["value"]) for r in tf.top_level_transfers(txs, "trx").collect()] == [
        100
    ]
    # and the trace's 7 is a separate, additional movement
    values = sorted(
        int(r["value"])
        for r in tf.transfers(
            trx_traces, _no_logs(spark), _no_tokens(spark), "trx", txs
        ).collect()
    )
    assert values == [7, 100]


def _no_logs(spark):
    return spark.createDataFrame([], schema=LOG_SCHEMA)


def _no_tokens(spark):
    return spark.createDataFrame([], schema=TOKEN_SCHEMA)


def test_tron_burns_the_fee_so_no_one_receives_it(spark, trx_traces) -> None:
    """`trx/Transformation.scala:196` debits the sender and has no miner term at
    all -- the commented-out txFeeDebits there records that as a finding."""
    txs = spark.createDataFrame(
        [
            {
                "tx_id": tx_id(1, 0),
                "block_id": 1,
                "from_address": ALICE,
                "to_address": BOB,
                "value": Decimal(100),
                "receipt_status": 1,
                "receipt_gas_used": 0,
                "gas_price": Decimal(0),
                "receipt_contract_address": None,
            }
        ],
        schema=TX_SCHEMA,
    )
    fees = spark.createDataFrame(
        [{"tx_id": tx_id(1, 0), "fee": 265}], schema=TRX_FEE_SCHEMA
    )
    blocks = spark.createDataFrame([], schema=BLOCK_SCHEMA)
    rows = tf.fee_events(txs, blocks, fees, "trx").collect()
    assert [(bytes(r["address"]), int(r["delta"])) for r in rows] == [(ALICE, -265)]


# --------------------------------------------------------------------------- #
# The link page cursor, which nothing wrote until now                          #
# --------------------------------------------------------------------------- #

EDGE = "src_address BINARY, dst_address BINARY, tx_id BIGINT"


def test_the_cursor_names_the_page_holding_the_newest_transactions(spark) -> None:
    """Ordinals ascend with tx_id, so an edge's newest transactions are in its
    HIGHEST page. Without this a reader can only see page 0 -- the oldest -- and
    `/links` has to refuse a hub-to-hub edge rather than answer from the wrong
    end of its history."""
    from dataclasses import replace

    cfg = replace(config_for("eth"), tx_page_size=2)
    moves = spark.createDataFrame(
        [
            {"src_address": b"\xa1", "dst_address": b"\xb2", "tx_id": n}
            for n in range(5)
        ],
        schema=EDGE,
    )
    row = tf.link_cursors(moves, cfg).collect()[0]
    # 5 transactions, 2 per page -> ordinals 0..4 -> pages 0,0,1,1,2
    assert row["link_page_max"] == 2
    assert row["link_ordinal_next"] == 5


def test_the_cursor_agrees_with_the_pages_actually_assigned(spark) -> None:
    """Both come from the same per-edge ordinal, and this is what stops them
    disagreeing about where an edge ends -- a cursor pointing past the last
    page reads an empty partition and reports the edge as having no
    transactions."""
    from dataclasses import replace

    cfg = replace(config_for("eth"), tx_page_size=2)
    moves = spark.createDataFrame(
        [
            {
                "src_address": b"\xa1",
                "dst_address": b"\xb2",
                "tx_id": n,
                "trace_index": None,
                "log_index": None,
                "currency": "ETH",
                "value": 1,
            }
            for n in range(5)
        ],
        schema=EDGE + ", trace_index INT, log_index INT, currency STRING, value BIGINT",
    )
    pages = {r["tx_page"] for r in tf.address_link_transactions(moves, cfg).collect()}
    cursor = tf.link_cursors(moves, cfg).collect()[0]
    assert max(pages) == cursor["link_page_max"]


def test_each_edge_gets_its_own_cursor(spark) -> None:
    """It is keyed by the EDGE. A cursor computed per source would give a quiet
    address the page count of the busiest edge its counterparty has."""

    moves = spark.createDataFrame(
        [
            {"src_address": b"\xa1", "dst_address": b"\xb2", "tx_id": 1},
            {"src_address": b"\xa1", "dst_address": b"\xc3", "tx_id": 2},
            {"src_address": b"\xa1", "dst_address": b"\xc3", "tx_id": 3},
        ],
        schema=EDGE,
    )
    rows = {
        bytes(r["dst_address"]): r["link_ordinal_next"]
        for r in tf.link_cursors(moves, config_for("eth")).collect()
    }
    assert rows == {b"\xb2": 1, b"\xc3": 2}
