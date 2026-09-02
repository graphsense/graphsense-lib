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


def test_exchange_rates_join_by_date(spark) -> None:
    """One table covers the native coin and every token, so this is one join.
    An asset with no rate for a block gets no row at all, so the transform
    contributes no fiat for it rather than a zero -- "unknown", not "worthless".
    """
    blocks = spark.createDataFrame(
        [
            {"block_id": 1, "timestamp": 0},
            {"block_id": 2, "timestamp": 86_400},
        ],
        schema="block_id INT, timestamp BIGINT",
    )
    rates = spark.createDataFrame(
        [
            {"asset": "ETH", "date": "1970-01-01", "fiat_values": {"EUR": 1.5}},
            {"asset": "USDT", "date": "1970-01-01", "fiat_values": {"EUR": 0.9}},
        ],
        schema="asset STRING, date STRING, fiat_values MAP<STRING,DOUBLE>",
    )
    rows = {
        (r["asset"], r["block_id"]): r["fiat_values"]
        for r in job.exchange_rates_by_block(spark, blocks, rates).collect()
    }
    assert rows == {("ETH", 1): {"EUR": 1.5}, ("USDT", 1): {"EUR": 0.9}}


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
