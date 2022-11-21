from graphsenselib.config import config


def test_config_is_not_loaded_by_default():
    # But real config should not be automatically loaded.
    assert config.is_loaded() is False
