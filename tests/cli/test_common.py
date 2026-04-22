import pytest

from graphsenselib.cli.common import try_load_config


class FakeConfig:
    def __init__(self, underlying_file=None):
        self.underlying_file = underlying_file
        self.model_config = {
            "default_files": ["~/.graphsense.yaml"],
            "file_env_var": "GRAPHSENSE_CONFIG",
        }
        self.load_partial_called = False
        self.load_called = False

    def load_partial(self, filename=None):
        self.load_partial_called = True
        return True, []

    def load(self, filename=None):
        self.load_called = True

    def generate_yaml(self, DEBUG=False):
        return ""


def test_try_load_config_web_is_optional(monkeypatch):
    cfg = FakeConfig(underlying_file=None)

    monkeypatch.setattr("graphsenselib.cli.common.get_config", lambda: cfg)
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "web", "openapi"],
    )

    loaded_cfg, md5hash = try_load_config(filename=None)

    assert loaded_cfg is cfg
    assert md5hash == "no-config-file"
    assert cfg.load_partial_called
    assert not cfg.load_called


def test_try_load_config_web_with_global_config_option(monkeypatch):
    cfg = FakeConfig(underlying_file=None)

    monkeypatch.setattr("graphsenselib.cli.common.get_config", lambda: cfg)
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        [
            "graphsense-cli",
            "--config-file",
            "/does/not/exist.yaml",
            "web",
            "openapi",
        ],
    )

    loaded_cfg, md5hash = try_load_config(filename="/does/not/exist.yaml")

    assert loaded_cfg is cfg
    assert md5hash == "no-config-file"
    assert cfg.load_partial_called
    assert not cfg.load_called


def test_try_load_config_non_optional_command_is_strict(monkeypatch):
    cfg = FakeConfig(underlying_file=None)

    monkeypatch.setattr("graphsenselib.cli.common.get_config", lambda: cfg)
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "rates", "coin-prices"],
    )

    with pytest.raises(SystemExit) as excinfo:
        try_load_config(filename=None)

    assert excinfo.value.code == 10
    assert not cfg.load_partial_called
    assert cfg.load_called
