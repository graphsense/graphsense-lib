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
        transformed_keyspace=f"{network}_transformed_v3",
        rates_keyspace=f"{network}_raw_20260101",
        cassandra_nodes=["node"],
    )


def test_refuses_an_account_network_for_the_transformed_stage(spark) -> None:
    """The transformed side is UTXO-only so far. Better a SystemExit at submit
    than a half-written keyspace -- and before the lake is read, not after."""
    with pytest.raises(SystemExit, match="UTXO-only"):
        job.run(spark, _settings("eth"), dry_run=True)


def test_the_raw_stage_alone_is_allowed_for_an_account_network(spark) -> None:
    """It fails on the missing lake, not on the stage selection: the refusal
    above is about the transformed stage only."""
    with pytest.raises(Exception) as caught:
        job.run(spark, _settings("eth"), dry_run=True, stages=("raw",))
    assert "UTXO-only" not in str(caught.value)


def test_exchange_rates_join_by_date(spark) -> None:
    """A block whose date has no rate gets none, so the transform contributes no
    fiat for it rather than a zero."""
    blocks = spark.createDataFrame(
        [
            {"block_id": 1, "timestamp": 0},
            {"block_id": 2, "timestamp": 86_400},
        ],
        schema="block_id INT, timestamp BIGINT",
    )
    rates = spark.createDataFrame(
        [{"date": "1970-01-01", "fiat_values": {"EUR": 1.5}}],
        schema="date STRING, fiat_values MAP<STRING,DOUBLE>",
    )
    rows = {
        r["block_id"]: r["fiat_values"]
        for r in job.exchange_rates_by_block(spark, blocks, rates).collect()
    }
    assert rows == {1: {"EUR": 1.5}, 2: None}


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
