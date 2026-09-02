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
    "status SMALLINT, trace_type STRING"
)
LOG_SCHEMA = (
    "block_id_group INT, block_id INT, log_index INT, address BINARY, "
    "data BINARY, topics ARRAY<BINARY>, topic0 BINARY, tx_id BIGINT"
)
TOKEN_SCHEMA = (
    "currency_ticker STRING, token_address BINARY, standard STRING, decimals INT, "
    "decimal_divisor BIGINT, peg_currency STRING"
)
RATES_SCHEMA = "asset STRING, block_id INT, fiat_values MAP<STRING,DOUBLE>"
BLOCK_SCHEMA = "block_id INT, timestamp BIGINT"


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


@pytest.fixture(scope="module")
def blocks(spark):
    return spark.createDataFrame([{"block_id": 1, "timestamp": 0}], schema=BLOCK_SCHEMA)


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


def _build(traces, logs, token_config, blocks, rates):
    return tf.build(traces, logs, token_config, blocks, rates, "eth")


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


def test_an_unconfigured_contract_is_not_a_token(logs, token_config) -> None:
    """Any contract can emit a Transfer with the same signature. Only a
    configured asset has a ticker, decimals and a peg to price it with."""
    rows = tf.token_transfers(logs, token_config).collect()
    assert all(bytes(r["src_address"]) == ALICE for r in rows)
    assert len(rows) == 1  # the 0xff.. contract's event is not a transfer


def test_a_pegged_token_is_worth_its_face_value(
    traces, logs, token_config, blocks, rates
) -> None:
    """A USD-pegged stablecoin is 2.0 USD for 2_000_000 base units at 6
    decimals; the other fiat currency follows from the base cross rate."""
    moves = tf.priced(
        tf.transfers(traces, logs, token_config, "eth"),
        rates,
        token_config,
        "eth",
    )
    token = next(r for r in moves.collect() if r["currency"] == "USDT")
    assert token["fiat_values"]["USD"] == pytest.approx(2.0)
    # EUR per USD = 2000/2500, so 2 USD is 1.60 EUR
    assert token["fiat_values"]["EUR"] == pytest.approx(1.6)


def test_the_native_coin_is_priced_from_the_block_rate(
    traces, logs, token_config, blocks, rates
) -> None:
    moves = tf.priced(
        tf.transfers(traces, logs, token_config, "eth"),
        rates,
        token_config,
        "eth",
    )
    native = next(
        r for r in moves.collect() if r["currency"] == "ETH" and int(r["value"]) > 0
    )
    assert native["fiat_values"]["USD"] == pytest.approx(2500.0)


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
        tf.transfers(traces, logs, unpegged, "eth"), rates, unpegged, "eth"
    )
    token = next(r for r in moves.collect() if r["currency"] == "USDT")
    assert token["fiat_values"] is None
    assert int(token["value"]) == 2_000_000  # the amount itself is unaffected


def test_a_transfer_names_both_ends_so_a_leg_is_not_netted(
    traces, logs, token_config, blocks, rates
) -> None:
    """No apportioning and no netting question: direction is a property of the
    leg, not of a sum, which is why D7 has no account counterpart."""
    moves = tf.priced(
        tf.transfers(traces, logs, token_config, "eth"),
        rates,
        token_config,
        "eth",
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
    traces, logs, token_config, blocks, rates
) -> None:
    stats = _build(traces, logs, token_config, blocks, rates)["address_stats"]
    alice = next(r for r in stats.collect() if bytes(r["address"]) == ALICE)
    assert int(alice["total_spent"]["value"]) == 10**18
    assert int(alice["total_tokens_spent"]["USDT"]["value"]) == 2_000_000
    assert alice["is_contract"] is False
    assert alice["out_degree"] == 2  # BOB and the created contract


def test_balance_is_per_asset(traces, logs, token_config, blocks, rates) -> None:
    """An account address holds a balance in every token it has touched, which
    is why currency is in this table's key and not in UTXO's."""
    rows = _build(traces, logs, token_config, blocks, rates)["balance"].collect()
    by_key = {(bytes(r["address"]), r["currency"]): int(r["balance"]) for r in rows}
    assert by_key[(ALICE, "USDT")] == -2_000_000
    assert by_key[(BOB, "USDT")] == 2_000_000
    assert by_key[(ALICE, "ETH")] == -(10**18)


def test_relations_carry_token_values(
    traces, logs, token_config, blocks, rates
) -> None:
    frames = _build(traces, logs, token_config, blocks, rates)
    edge = next(
        r
        for r in frames["address_outgoing_relations"].collect()
        if bytes(r["dst_address"]) == BOB
    )
    assert edge["no_transactions"] == 2  # one ETH, one USDT
    assert int(edge["value"]["value"]) == 10**18
    assert int(edge["token_values"]["USDT"]["value"]) == 2_000_000


def test_link_transactions_are_partitioned_per_edge(
    traces, logs, token_config, blocks, rates
) -> None:
    """The account half of D10: fewer addresses with more transactions per edge,
    so the repeated destination costs less than the partitions it saves."""
    table = schema_for("eth", Kind.DERIVED).table("address_link_transactions")
    assert table.key.partition == ("src_address", "dst_address", "tx_page")
    links = _build(traces, logs, token_config, blocks, rates)[
        "address_link_transactions"
    ].collect()
    assert {r["currency"] for r in links} == {"ETH", "USDT"}


def test_every_frame_conforms_to_its_table(
    traces, logs, token_config, blocks, rates
) -> None:
    schema = schema_for("eth", Kind.DERIVED)
    frames = _build(traces, logs, token_config, blocks, rates)
    assert set(frames) == set(tf.TABLES)
    for name, frame in frames.items():
        assert conformance_errors(list(frame.columns), schema.table(name)) == []


def test_tron_uses_its_own_native_symbol_and_divisor() -> None:
    assert tf.NATIVE["trx"] == ("TRX", 10**6)
    assert config_for("trx").entity_buckets > 0


def test_zero_value_transfers_are_a_separate_partition(
    traces, logs, token_config, blocks, rates
) -> None:
    """The reason this exists: on ETH and TRON a zero-value transfer is a
    contract call that moved nothing, and they dominate an address's listing."""
    frames = _build(traces, logs, token_config, blocks, rates)
    rows = frames["address_transactions"].collect()
    zero = [r for r in rows if r["is_zero_value"]]
    assert zero and all(int(r["value"]) == 0 for r in zero)
    assert all(int(r["value"]) != 0 for r in rows if not r["is_zero_value"])


def test_balance_history_is_per_asset_and_cumulative(
    traces, logs, token_config, blocks, rates
) -> None:
    """Per asset, like `balance` -- an account address holds a history in every
    token it has touched."""
    rows = _build(traces, logs, token_config, blocks, rates)[
        "balance_history"
    ].collect()
    by_key = {(bytes(r["address"]), r["currency"]): int(r["balance"]) for r in rows}
    assert by_key[(ALICE, "USDT")] == -2_000_000
    assert by_key[(BOB, "ETH")] == 10**18
    assert {r["day"] for r in rows} == {19700101}
