from graphsenselib.config import config


def test_config_is_not_loaded_by_default():
    assert config.is_loaded() is False


# def test_config_get_environment():
#     assert config.get_environment("prod") is False
