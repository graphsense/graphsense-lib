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
    contributes no fiat for it -- "unknown", not "worthless"."""
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
