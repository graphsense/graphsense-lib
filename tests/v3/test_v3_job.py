"""The end-to-end driver.

It exists to be run once against a cluster, so the things worth testing are the
ones that would waste that run: refusing early, and the dry run actually
checking every frame.
"""

import logging

import pytest

from graphsense_v3.settings import RunSettings
from graphsense_v3.spark import job

pytest.importorskip("pyspark")


def _settings(network: str) -> RunSettings:
    return RunSettings(
        network=network,
        env="prod",
        lake_root="/nonexistent",
        raw_keyspace=f"{network}_raw_v3",
        derived_keyspace=f"{network}_derived_v3",
        rates_keyspace=f"{network}_raw_20260101",
        cassandra_nodes=["node"],
    )


@pytest.mark.parametrize("network", ["btc", "eth", "trx"])
def test_every_network_can_run_both_stages(spark, network: str) -> None:
    """Both families transform now, so nothing is refused on family grounds --
    a run fails on the missing lake, not on the stage selection."""
    with pytest.raises(Exception) as caught:
        job.run(spark, _settings(network), dry_run=True)
    assert "UTXO-only" not in str(caught.value)


V2_RAW = "date STRING, fiat_values MAP<STRING,DOUBLE>"
V2_TOKEN = "asset STRING, date STRING, fiat_values MAP<STRING,DOUBLE>"
V3_RAW = "asset STRING, date STRING, fiat_values MAP<STRING,DOUBLE>"
V3_DERIVED = (
    "asset STRING, block_id_group INT, block_id INT, fiat_values MAP<STRING,DOUBLE>"
)


@pytest.fixture
def blocks(spark):
    return spark.createDataFrame(
        [{"block_id": 1, "timestamp": 0}, {"block_id": 2, "timestamp": 86_400}],
        schema="block_id INT, timestamp BIGINT",
    )


def test_a_v2_rate_table_gains_an_asset_column(spark) -> None:
    """v2's exchange_rates is keyed by date alone and holds the native coin
    only -- no asset column at all. v3 merged the two rate tables, so the asset
    has to be synthesised before anything can join on it."""
    native = spark.createDataFrame(
        [{"date": "1970-01-01", "fiat_values": {"EUR": 1.5}}], schema=V2_RAW
    )
    rows = job.normalise_rates(native, symbol="LTC").collect()
    assert [(r["asset"], r["date"]) for r in rows] == [("LTC", "1970-01-01")]


def test_a_v3_rate_table_keeps_its_own_asset(spark) -> None:
    """A v3 source already carries every asset, native included, so nothing is
    synthesised and no token table is unioned."""
    rates = spark.createDataFrame(
        [
            {"asset": "LTC", "date": "1970-01-01", "fiat_values": {"EUR": 1.5}},
            {"asset": "USDT", "date": "1970-01-01", "fiat_values": {"EUR": 0.9}},
        ],
        schema=V3_RAW,
    )
    rows = job.normalise_rates(rates, symbol="IGNORED").collect()
    assert {r["asset"] for r in rows} == {"LTC", "USDT"}


def test_a_per_block_source_needs_no_date_join(spark, blocks) -> None:
    """A v3 derived keyspace has already resolved dates to blocks."""
    rates = spark.createDataFrame(
        [
            {
                "asset": "LTC",
                "block_id_group": 0,
                "block_id": 2,
                "fiat_values": {"EUR": 9.0},
            }
        ],
        schema=V3_DERIVED,
    )
    normalised = job.normalise_rates(rates, symbol="LTC")
    assert "block_id" in normalised.columns and "date" not in normalised.columns
    rows = job.rates_by_block(blocks, normalised).collect()
    assert [(r["asset"], r["block_id"]) for r in rows] == [("LTC", 2)]


def test_a_positional_fiat_list_is_refused(spark) -> None:
    """v2's *transformed* tables store fiat_values as a list whose meaning
    depends on an ordering held elsewhere. Reading it against the wrong
    ordering is the defect v3's map replaced, so refuse rather than guess."""
    rates = spark.createDataFrame(
        [{"block_id": 1, "fiat_values": [1.5, 2.0]}],
        schema="block_id INT, fiat_values ARRAY<DOUBLE>",
    )
    with pytest.raises(SystemExit, match="positional list"):
        job.normalise_rates(rates, symbol="LTC")


def test_rates_join_by_date(spark, blocks) -> None:
    """A block whose date has no rate gets no row at all, so the transform
    contributes no fiat for it -- "unknown", not "worthless".

    The derived stage then refuses to COVER such a block at all; see
    `bound_to_rated_blocks`. An unrated block is not servable, so leaving it in
    would trade a missing fiat value for a failed request."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "date": "1970-01-01", "fiat_values": {"EUR": 1.5}}],
        schema=V3_RAW,
    )
    rows = {
        (r["asset"], r["block_id"]): r["fiat_values"]
        for r in job.rates_by_block(
            blocks, job.normalise_rates(rates, symbol="LTC")
        ).collect()
    }
    assert rows == {("LTC", 1): {"EUR": 1.5}}


def test_a_v2_account_source_unions_its_token_table(spark) -> None:
    """token_exchange_rates exists on v2 account keyspaces and nowhere else."""
    native = job.normalise_rates(
        spark.createDataFrame(
            [{"date": "1970-01-01", "fiat_values": {"EUR": 1.5}}], schema=V2_RAW
        ),
        symbol="ETH",
    )
    tokens = job.normalise_rates(
        spark.createDataFrame(
            [{"asset": "USDT", "date": "1970-01-01", "fiat_values": {"EUR": 0.9}}],
            schema=V2_TOKEN,
        ),
        symbol="ETH",
    )
    rows = native.unionByName(tokens).collect()
    assert {r["asset"] for r in rows} == {"ETH", "USDT"}


def test_stage_reports_its_cost(caplog) -> None:
    """The whole output of a benchmark run, so it had better say something."""
    with caplog.at_level(logging.INFO):
        with job.Stage("a thing"):
            pass
    assert "START a thing" in caplog.text
    assert "done  a thing" in caplog.text


def test_stage_reports_a_failure(caplog) -> None:
    with caplog.at_level(logging.INFO), pytest.raises(ValueError):
        with job.Stage("a thing"):
            raise ValueError("nope")
    assert "FAILED" in caplog.text


def test_a_dry_run_executes_rather_than_only_typechecks(spark, caplog) -> None:
    """Conformance checks resolve schemas and run nothing, so a pandas UDF that
    fails on the executors would pass a dry run and then fail hours into the
    real one. `sample` materialises a few rows through the whole DAG."""
    import logging

    frame = spark.createDataFrame(
        [{"a": i} for i in range(50)], schema="a INT"
    ).withColumn("b", frame_doubler())
    with caplog.at_level(logging.INFO):
        job.sample({"thing": frame}, "raw", rows=3)
    assert "thing produced 3 of the first 3 rows" in caplog.text


def frame_doubler():
    """A column that only evaluates when the plan actually runs."""
    from pyspark.sql import functions as F

    return F.col("a") * 2


# --------------------------------------------------------------------------
# Bounding the derived stage to blocks a rate exists for.
#
# The first live back-to-back run failed on this: v3 wrote blocks and
# transactions past the last rated block, and `RatesService` raises
# BlockNotFoundException for a block with no rate -- so those rows were not
# degraded, they were unservable, and the failure surfaced three layers up as
# "block not found" for a block that was plainly there.
# --------------------------------------------------------------------------

RATES_SCHEMA = "asset STRING, block_id INT, fiat_values MAP<STRING, DOUBLE>"


def test_rated_tip_is_the_highest_block_with_a_rate(spark) -> None:
    rates = spark.createDataFrame(
        [
            {"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.0}},
            {"asset": "LTC", "block_id": 9, "fiat_values": {"EUR": 1.0}},
        ],
        schema=RATES_SCHEMA,
    )
    assert job.rated_tip(rates) == 9


def test_rated_tip_is_none_when_nothing_is_rated(spark) -> None:
    """Distinguishable from 0, which is a real block."""
    empty = spark.createDataFrame([], schema=RATES_SCHEMA)
    assert job.rated_tip(empty) is None


def test_blocks_past_the_rated_tip_are_dropped(spark) -> None:
    blocks = spark.createDataFrame(
        [{"block_id": 8}, {"block_id": 9}, {"block_id": 10}],
        schema="block_id INT",
    )
    bounded = job.bound_to_rated_blocks({"block": blocks}, 9)
    assert [r["block_id"] for r in bounded["block"].collect()] == [8, 9]


def test_a_tx_id_keyed_frame_is_bounded_at_the_block_boundary(spark) -> None:
    """`transaction_io` has no block_id, only tx_id. Since tx_id is
    (block_id << 32) + index, the block bound IS a tx_id bound -- and it must
    keep the LAST transaction of the rated block while dropping the first of
    the next one, which a bound computed off the wrong side would invert."""
    last_of_9 = (9 << 32) + 4294967295
    first_of_10 = 10 << 32
    io = spark.createDataFrame(
        [{"tx_id": last_of_9}, {"tx_id": first_of_10}],
        schema="tx_id LONG",
    )
    bounded = job.bound_to_rated_blocks({"transaction_io": io}, 9)
    assert [r["tx_id"] for r in bounded["transaction_io"].collect()] == [last_of_9]


def test_a_frame_with_neither_key_passes_through(spark) -> None:
    """`configuration` has no block or tx column and must not be dropped."""
    config = spark.createDataFrame(
        [{"keyspace_name": "ltc_derived_v3_x"}], schema="keyspace_name STRING"
    )
    bounded = job.bound_to_rated_blocks({"configuration": config}, 9)
    assert bounded["configuration"].count() == 1


def test_block_id_is_preferred_over_tx_id_when_both_are_present(spark) -> None:
    """`transaction` carries both. Bounding on block_id is the exact test;
    falling through to the tx_id bound would be equivalent here but is one more
    place for the shift to be wrong."""
    frame = spark.createDataFrame(
        [
            {"block_id": 9, "tx_id": (9 << 32) + 1},
            {"block_id": 10, "tx_id": (10 << 32) + 1},
        ],
        schema="block_id INT, tx_id LONG",
    )
    bounded = job.bound_to_rated_blocks({"transaction": frame}, 9)
    assert [r["block_id"] for r in bounded["transaction"].collect()] == [9]


BLOCKS_SCHEMA = "block_id INT, timestamp LONG"
DAY = 86400


def test_recent_unrated_blocks_get_the_last_known_rate(spark) -> None:
    """Rates land a day at a time, so the chain tip is ALWAYS unrated for up to
    ~24h. Refusing those blocks pins the backend a day behind the chain."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 5, "timestamp": 1_000_000},
            {"block_id": 6, "timestamp": 1_000_000 + DAY},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = {
        r["block_id"]: r["fiat_values"]
        for r in job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY).collect()
    }
    assert filled == {5: {"EUR": 1.5}, 6: {"EUR": 1.5}}


def test_a_block_beyond_the_window_is_not_filled(spark) -> None:
    """The cap is the point: past it the rate feed is broken rather than
    lagging, and a stale rate carried forever would hide that behind plausible
    numbers."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 5, "timestamp": 1_000_000},
            {"block_id": 9, "timestamp": 1_000_000 + 10 * DAY},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY)
    assert [r["block_id"] for r in filled.collect()] == [5]


def test_the_fill_never_moves_a_rate_backwards(spark) -> None:
    """Only blocks AFTER the last rate are filled. An earlier block with no
    rate is a hole in the feed, and filling it forward would be filling it
    backward -- a different number than the day actually had."""
    rates = spark.createDataFrame(
        [{"asset": "LTC", "block_id": 5, "fiat_values": {"EUR": 1.5}}],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 3, "timestamp": 1_000_000 - DAY},
            {"block_id": 5, "timestamp": 1_000_000},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY)
    assert [r["block_id"] for r in filled.collect()] == [5]


def test_each_asset_carries_its_own_last_rate(spark) -> None:
    """A token whose feed stopped earlier must not borrow the native coin's
    freshness -- that would invent a rate for a token nobody priced."""
    rates = spark.createDataFrame(
        [
            {"asset": "ETH", "block_id": 5, "fiat_values": {"EUR": 1.5}},
            {"asset": "USDT", "block_id": 3, "fiat_values": {"EUR": 0.9}},
        ],
        schema=RATES_SCHEMA,
    )
    blocks = spark.createDataFrame(
        [
            {"block_id": 3, "timestamp": 1_000_000},
            {"block_id": 5, "timestamp": 1_000_000 + 60},
            {"block_id": 6, "timestamp": 1_000_000 + 120},
        ],
        schema=BLOCKS_SCHEMA,
    )
    filled = {
        (r["asset"], r["block_id"]): r["fiat_values"]
        for r in job.forward_fill_rates(rates, blocks, within_seconds=2 * DAY).collect()
    }
    # ETH extends from 5; USDT extends from its OWN last rate at 3.
    assert filled[("ETH", 6)] == {"EUR": 1.5}
    assert filled[("USDT", 5)] == {"EUR": 0.9}
    assert filled[("USDT", 6)] == {"EUR": 0.9}
