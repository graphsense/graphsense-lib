import json
import logging
import os
import tempfile

import pytest
from pydantic import ValidationError

from graphsenselib.config import get_config, get_reorg_backoff_blocks
from graphsenselib.config.config import (
    AppConfig,
    Environment,
    get_default_data_configuration,
)


def test_config_is_loaded_by_default():
    # But real config should not be automatically loaded.
    assert get_config().is_loaded() is True

    assert list(get_config().environments.keys()) == ["pytest"]

    assert list(get_config().get_environment("pytest").cassandra_nodes)[0].startswith(
        "localhost"
    )

    config = get_config()

    assert config.coingecko_api_key == ""
    assert config.coinmarketcap_api_key == ""
    assert config.s3_credentials is None

    assert config.underlying_file is None

    assert config.path() is None

    assert list(config.get_configured_environments()) == ["pytest"]
    assert list(config.get_configured_slack_topics()) == []

    assert (
        config.get_keyspace_config("pytest", "btc").raw_keyspace_name
        == "pytest_btc_raw"
    )


def test_get_approx_reorg_backoff_blocks():
    assert get_reorg_backoff_blocks("eth") == 70


def test_default_data_configuration_omits_pk_column():
    # The configuration-row PK ("id" for raw, "keyspace_name" for transformed)
    # must NOT be in the defaults — it has to be injected at seed time with the
    # actual target keyspace name, otherwise dated keyspaces get a stale row
    # keyed by the un-suffixed prefix and the real ingest adds a duplicate.
    for currency in ["btc", "ltc", "bch", "zec", "eth", "trx"]:
        raw = get_default_data_configuration(currency, "raw")
        assert "id" not in raw, (
            f"defaults for {currency}/raw must not preset 'id': {raw}"
        )
        transformed = get_default_data_configuration(currency, "transformed")
        assert "keyspace_name" not in transformed, (
            f"defaults for {currency}/transformed must not preset "
            f"'keyspace_name': {transformed}"
        )


def test_unknown_keys_emit_warnings(caplog):
    cfg = """
environments:
  dev:
    cassandra_nodes: [localhost]
    typo_at_env_level: true
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw_dev
        transformed_keyspace_name: btc_transformed_dev
        schema_type: utxo
        typo_in_keyspace: 123
        ingest_config:
          node_reference: http://localhost:8332
          soruce_max_workers: 99
          raw_keyspace_file_sinks:
            delta:
              directory: /tmp/test
              unknown_sink_key: oops
top_level_typo: hello
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg)
        fname = f.name

    try:
        with caplog.at_level(logging.WARNING, logger="graphsenselib.config.config"):
            AppConfig(load=True, config_file=fname)

        warnings = [r.message for r in caplog.records]
        assert any("top_level_typo" in w for w in warnings)
        assert any("typo_at_env_level" in w and "Environment" in w for w in warnings)
        assert any("typo_in_keyspace" in w and "KeyspaceConfig" in w for w in warnings)
        assert any("soruce_max_workers" in w and "IngestConfig" in w for w in warnings)
        assert any("unknown_sink_key" in w and "FileSink" in w for w in warnings)
    finally:
        os.unlink(fname)


def test_load_partial_parses_slack_topics_from_env(monkeypatch):
    cfg = AppConfig(load=False)
    monkeypatch.setenv(
        "GRAPHSENSE_SLACK_TOPICS",
        json.dumps(
            {
                "exceptions": {
                    "hooks": ["https://hooks.slack.com/services/T000/B000/TESTHOOK"]
                }
            }
        ),
    )

    ok, errors = cfg.load_partial(filename="/does/not/exist.yaml")

    assert ok is True
    assert errors == []
    topic = cfg.get_slack_hooks_by_topic("exceptions")
    assert topic is not None
    assert topic.hooks == ["https://hooks.slack.com/services/T000/B000/TESTHOOK"]


def test_load_partial_rejects_invalid_slack_topics_env(monkeypatch):
    cfg = AppConfig(load=False)
    monkeypatch.setenv("GRAPHSENSE_SLACK_TOPICS", "not-json")

    ok, errors = cfg.load_partial(filename="/does/not/exist.yaml")

    assert ok is False
    assert any(e.startswith("GRAPHSENSE_SLACK_TOPICS:") for e in errors)


def test_environment_consistency_level_defaults():
    # Defaults must preserve the previously hardcoded sync-connection behavior.
    env = Environment(cassandra_nodes=["localhost"], keyspaces={})
    assert env.consistency_level == "LOCAL_QUORUM"
    assert env.serial_consistency_level == "LOCAL_SERIAL"


def test_environment_consistency_level_custom_values():
    env = Environment(
        cassandra_nodes=["localhost"],
        keyspaces={},
        consistency_level="LOCAL_ONE",
        serial_consistency_level="SERIAL",
    )
    assert env.consistency_level == "LOCAL_ONE"
    assert env.serial_consistency_level == "SERIAL"


def test_environment_rejects_invalid_consistency_level():
    with pytest.raises(ValidationError) as exc_info:
        Environment(
            cassandra_nodes=["localhost"],
            keyspaces={},
            consistency_level="BOGUS",
        )
    assert "consistency_level must be one of" in str(exc_info.value)


def test_environment_rejects_non_serial_serial_consistency_level():
    # serial_consistency_level only accepts SERIAL / LOCAL_SERIAL; a regular
    # level like QUORUM is a valid consistency_level but not a serial one.
    with pytest.raises(ValidationError) as exc_info:
        Environment(
            cassandra_nodes=["localhost"],
            keyspaces={},
            serial_consistency_level="QUORUM",
        )
    assert "serial_consistency_level must be one of" in str(exc_info.value)


def test_s3_configs_inherit_from_baseline():
    cfg_yaml = """
s3_configs:
  baseline:
    AWS_ENDPOINT_URL: https://s3.example.com
    AWS_REGION: eu-central-1
    AWS_ACCESS_KEY_ID: baseline-key
  prod_user:
    AWS_ACCESS_KEY_ID: prod-key
    AWS_SECRET_ACCESS_KEY: prod-secret
  other_region:
    AWS_REGION: eu-west-1
    AWS_ACCESS_KEY_ID: other-key
    AWS_SECRET_ACCESS_KEY: other-secret
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg_yaml)
        fname = f.name

    try:
        cfg = AppConfig(load=True, config_file=fname)

        # Named entries inherit baseline values...
        assert cfg.get_s3_credentials("prod_user") == {
            "AWS_ENDPOINT_URL": "https://s3.example.com",
            "AWS_REGION": "eu-central-1",
            "AWS_ACCESS_KEY_ID": "prod-key",
            "AWS_SECRET_ACCESS_KEY": "prod-secret",
        }

        # ...but their own values take precedence.
        other = cfg.get_s3_credentials("other_region")
        assert other is not None
        assert other["AWS_REGION"] == "eu-west-1"
        assert other["AWS_ENDPOINT_URL"] == "https://s3.example.com"
        assert other["AWS_ACCESS_KEY_ID"] == "other-key"

        # Requesting baseline returns it as-is, not merged with itself.
        assert cfg.get_s3_credentials("baseline") == cfg.s3_configs["baseline"]
    finally:
        os.unlink(fname)


def test_s3_configs_without_baseline_unchanged():
    cfg_yaml = """
s3_configs:
  prod_user:
    AWS_ACCESS_KEY_ID: prod-key
    AWS_SECRET_ACCESS_KEY: prod-secret
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg_yaml)
        fname = f.name

    try:
        cfg = AppConfig(load=True, config_file=fname)
        assert cfg.get_s3_credentials("prod_user") == {
            "AWS_ACCESS_KEY_ID": "prod-key",
            "AWS_SECRET_ACCESS_KEY": "prod-secret",
        }
    finally:
        os.unlink(fname)


def test_spark_config_flat_legacy_form():
    # Legacy shape: spark properties live directly under spark_config.
    cfg_yaml = """
spark_config:
  spark.master: spark://host:7077
  spark.executor.memory: 8g
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg_yaml)
        fname = f.name

    try:
        cfg = AppConfig(load=True, config_file=fname)
        assert cfg.get_spark_config() == {
            "spark.master": "spark://host:7077",
            "spark.executor.memory": "8g",
        }
        # Asking for a profile in flat form is a misconfiguration.
        with pytest.raises(ValueError, match="flat \\(legacy\\) form"):
            cfg.get_spark_config("anything")
    finally:
        os.unlink(fname)


def test_spark_config_nested_inherits_from_baseline():
    cfg_yaml = """
spark_config:
  baseline:
    spark.master: spark://host:7077
    spark.executor.memory: 8g
    spark.sql.shuffle.partitions: "200"
  big:
    spark.executor.memory: 32g
    spark.executor.cores: "8"
  local:
    spark.master: local[*]
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg_yaml)
        fname = f.name

    try:
        cfg = AppConfig(load=True, config_file=fname)

        # No profile name -> baseline only.
        assert cfg.get_spark_config() == {
            "spark.master": "spark://host:7077",
            "spark.executor.memory": "8g",
            "spark.sql.shuffle.partitions": "200",
        }

        # 'big' overrides executor memory and adds cores.
        assert cfg.get_spark_config("big") == {
            "spark.master": "spark://host:7077",
            "spark.executor.memory": "32g",
            "spark.sql.shuffle.partitions": "200",
            "spark.executor.cores": "8",
        }

        # 'local' overrides only the master URL.
        assert cfg.get_spark_config("local") == {
            "spark.master": "local[*]",
            "spark.executor.memory": "8g",
            "spark.sql.shuffle.partitions": "200",
        }

        # baseline requested explicitly returns it as-is.
        assert cfg.get_spark_config("baseline") == cfg.spark_config["baseline"]

        # Unknown profile errors and excludes 'baseline' from suggestions.
        with pytest.raises(ValueError, match="not found"):
            cfg.get_spark_config("nope")
    finally:
        os.unlink(fname)


def test_spark_config_empty_defaults_to_empty_dict():
    cfg = AppConfig(load=False)
    cfg._init_with_field_defaults()
    assert cfg.get_spark_config() == {}


def test_spark_packages_empty_defaults_to_empty_dict():
    cfg = AppConfig(load=False)
    cfg._init_with_field_defaults()
    assert cfg.get_spark_packages() == {}


def test_spark_packages_loaded_from_yaml():
    cfg_yaml = """
spark_packages:
  hadoop_aws: org.apache.hadoop:hadoop-aws:3.3.4
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg_yaml)
        fname = f.name

    try:
        cfg = AppConfig(load=True, config_file=fname)
        assert cfg.get_spark_packages() == {
            "hadoop_aws": "org.apache.hadoop:hadoop-aws:3.3.4",
        }
    finally:
        os.unlink(fname)


def test_environment_consistency_level_loaded_from_yaml():
    cfg = """
environments:
  dev:
    cassandra_nodes: [localhost]
    consistency_level: LOCAL_ONE
    serial_consistency_level: SERIAL
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw_dev
        transformed_keyspace_name: btc_transformed_dev
        schema_type: utxo
        ingest_config:
          node_reference: http://localhost:8332
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(cfg)
        fname = f.name

    try:
        app = AppConfig(load=True, config_file=fname)
        env = app.get_environment("dev")
        assert env.consistency_level == "LOCAL_ONE"
        assert env.serial_consistency_level == "SERIAL"
    finally:
        os.unlink(fname)
