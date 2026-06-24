from typing import Any, cast

import pytest
from goodconf import GoodConfConfigDict

from graphsenselib.config.config import AppConfig
from graphsenselib.utils import resolve_env_vars


@pytest.fixture(autouse=True)
def _isolate_config_discovery(monkeypatch):
    """Prevent these tests from reading a real config file on disk.

    Empties AppConfig.default_files (so neither ./.graphsense.yaml nor
    ~/.graphsense.yaml is auto-discovered) and clears the file-pointer env
    vars. The repo-wide ``patch_config`` fixture already does the former; this
    keeps the module self-contained and also guards the REST CONFIG_FILE path.
    """
    monkeypatch.setattr(
        AppConfig,
        "model_config",
        GoodConfConfigDict(
            default_files=[],
            env_prefix="GRAPHSENSE_PYTEST_",
            file_env_var="GRAPHSENSE_CONFIG_YAML",
        ),
    )
    monkeypatch.delenv("GRAPHSENSE_CONFIG_YAML", raising=False)
    monkeypatch.delenv("CONFIG_FILE", raising=False)


def test_resolve_simple_placeholder(monkeypatch):
    monkeypatch.setenv("MY_HOST", "db.example.com")
    assert resolve_env_vars("${MY_HOST}") == "db.example.com"


def test_resolve_inline_placeholder(monkeypatch):
    monkeypatch.setenv("PW", "secret")
    monkeypatch.setenv("USER", "alice")
    assert (
        resolve_env_vars("postgresql://${USER}:${PW}@localhost/db")
        == "postgresql://alice:secret@localhost/db"
    )


def test_resolve_nested_structure(monkeypatch):
    monkeypatch.setenv("NODE", "cassandra-1")
    monkeypatch.setenv("API_KEY", "abc123")
    cfg = {
        "environments": {
            "prod": {"cassandra_nodes": ["${NODE}", "static-node"]},
        },
        "coingecko_api_key": "${API_KEY}",
        "port": 9042,
        "enabled": True,
        "missing": None,
    }
    resolved = resolve_env_vars(cfg)
    assert resolved["environments"]["prod"]["cassandra_nodes"] == [
        "cassandra-1",
        "static-node",
    ]
    assert resolved["coingecko_api_key"] == "abc123"
    # Non-string scalars pass through untouched.
    assert resolved["port"] == 9042
    assert resolved["enabled"] is True
    assert resolved["missing"] is None


def test_default_value_used_when_unset(monkeypatch):
    monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
    assert resolve_env_vars("${DOES_NOT_EXIST:-fallback}") == "fallback"


def test_set_value_overrides_default(monkeypatch):
    monkeypatch.setenv("HAS_VALUE", "real")
    assert resolve_env_vars("${HAS_VALUE:-fallback}") == "real"


def test_undefined_without_default_raises(monkeypatch):
    monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
    with pytest.raises(ValueError, match="DOES_NOT_EXIST"):
        resolve_env_vars("${DOES_NOT_EXIST}")


def test_no_placeholder_is_unchanged():
    assert resolve_env_vars("plain string") == "plain string"
    assert resolve_env_vars("price is $5") == "price is $5"


def test_escaped_placeholder_is_literal(monkeypatch):
    # $${VAR} must not be substituted even if VAR is set.
    monkeypatch.setenv("VAR", "should-not-appear")
    assert resolve_env_vars("$${VAR}") == "${VAR}"
    assert resolve_env_vars("path/$${HOME}/end") == "path/${HOME}/end"


def test_escaped_and_real_placeholder_mixed(monkeypatch):
    monkeypatch.setenv("REAL", "value")
    assert resolve_env_vars("$${LITERAL} and ${REAL}") == "${LITERAL} and value"


def test_app_config_load_partial_resolves_env(monkeypatch, tmp_path):
    monkeypatch.setenv("TEST_CASSANDRA_NODE", "resolved-node")
    monkeypatch.setenv("TEST_COINGECKO_KEY", "resolved-key")

    config_yaml = tmp_path / "graphsense.yaml"
    config_yaml.write_text(
        "default_environment: prod\n"
        "coingecko_api_key: ${TEST_COINGECKO_KEY}\n"
        "environments:\n"
        "  prod:\n"
        "    cassandra_nodes:\n"
        "      - ${TEST_CASSANDRA_NODE}\n"
        "    keyspaces: {}\n"
    )

    cfg = AppConfig(load=False)
    ok, errors = cfg.load_partial(filename=str(config_yaml))

    assert ok, errors
    assert cfg.coingecko_api_key == "resolved-key"
    # load_partial stores nested fields raw (dict), so access the dict form.
    environments = cast(dict[str, Any], cfg.environments)
    assert environments["prod"]["cassandra_nodes"] == ["resolved-node"]


def test_gsrest_load_config_resolves_env(monkeypatch, tmp_path):
    from graphsenselib.web.app import load_config

    monkeypatch.setenv("TAGSTORE_URL", "postgresql://u:p@host:5432/db")

    config_yaml = tmp_path / "gsrest.yaml"
    config_yaml.write_text("gs-tagstore:\n  url: ${TAGSTORE_URL}\n")

    raw = load_config(str(config_yaml))
    assert raw["gs-tagstore"]["url"] == "postgresql://u:p@host:5432/db"
