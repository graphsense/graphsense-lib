from graphsenselib.config import get_config, get_reorg_backoff_blocks


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
