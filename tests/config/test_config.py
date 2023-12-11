from graphsenselib.config import config, get_approx_reorg_backoff_blocks


def test_config_is_not_loaded_by_default():
    # But real config should not be automatically loaded.
    assert config.is_loaded() is False


def test_get_approx_reorg_backoff_blocks():
    assert get_approx_reorg_backoff_blocks("btc") == 12
    assert get_approx_reorg_backoff_blocks("bch", 1.5) == 9
    assert get_approx_reorg_backoff_blocks("eth") == 480
    assert get_approx_reorg_backoff_blocks("ltc") == 48
