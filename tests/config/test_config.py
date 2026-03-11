import logging
import tempfile
import os

from graphsenselib.config import get_config, get_reorg_backoff_blocks
from graphsenselib.config.config import AppConfig


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
