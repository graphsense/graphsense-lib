import pytest

from graphsenselib.mcp.config import GSMCPConfig, SearchNeighborsConfig


def test_defaults(monkeypatch):
    for var in [
        "GS_MCP_ENABLED",
        "GS_MCP_PATH",
        "GS_MCP_STATELESS_HTTP",
        "GS_MCP_CURATION_FILE",
        "GS_MCP_SEARCH_NEIGHBORS__BASE_URL",
    ]:
        monkeypatch.delenv(var, raising=False)

    cfg = GSMCPConfig()
    assert cfg.enabled is True
    assert cfg.path == "/mcp"
    assert cfg.stateless_http is True  # favor clean shutdown
    assert cfg.search_neighbors is None
    assert cfg.resolved_curation_path().name == "tools.yaml"


def test_stateless_http_toggle_via_env(monkeypatch):
    monkeypatch.setenv("GS_MCP_STATELESS_HTTP", "false")
    cfg = GSMCPConfig()
    assert cfg.stateless_http is False


def test_enabled_toggle_via_env(monkeypatch):
    monkeypatch.setenv("GS_MCP_ENABLED", "false")
    cfg = GSMCPConfig()
    assert cfg.enabled is False


def test_path_override_via_env(monkeypatch):
    monkeypatch.setenv("GS_MCP_PATH", "/custom-mcp")
    cfg = GSMCPConfig()
    assert cfg.path == "/custom-mcp"


def test_nested_search_neighbors_config(monkeypatch):
    monkeypatch.setenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", "https://search.example/")
    monkeypatch.setenv("GS_MCP_SEARCH_NEIGHBORS__API_KEY_ENV", "MY_KEY")
    monkeypatch.setenv("GS_MCP_SEARCH_NEIGHBORS__POLL_INTERVAL_S", "2.5")
    cfg = GSMCPConfig()
    assert cfg.search_neighbors is not None
    assert cfg.search_neighbors.base_url == "https://search.example/"
    assert cfg.search_neighbors.api_key_env == "MY_KEY"
    assert cfg.search_neighbors.poll_interval_s == 2.5
    assert cfg.search_neighbors.auth_header == "Authorization"


def test_search_neighbors_config_requires_fields():
    with pytest.raises(ValueError):
        SearchNeighborsConfig()  # base_url + api_key_env missing


def test_bundled_instructions_loaded_by_default(monkeypatch):
    monkeypatch.delenv("GS_MCP_INSTRUCTIONS", raising=False)
    monkeypatch.delenv("GS_MCP_INSTRUCTIONS_FILE", raising=False)

    cfg = GSMCPConfig()
    text = cfg.resolved_instructions()
    assert text is not None
    assert "GraphSense" in text


def test_instructions_explicit_override_wins(monkeypatch):
    monkeypatch.setenv("GS_MCP_INSTRUCTIONS", "Custom system prompt.")
    cfg = GSMCPConfig()
    assert cfg.resolved_instructions() == "Custom system prompt."


def test_instructions_empty_string_suppresses(monkeypatch):
    """Empty explicit value must opt out of sending any instructions."""
    monkeypatch.setenv("GS_MCP_INSTRUCTIONS", "")
    cfg = GSMCPConfig()
    assert cfg.resolved_instructions() is None


def test_instructions_file_override(monkeypatch, tmp_path):
    monkeypatch.delenv("GS_MCP_INSTRUCTIONS", raising=False)
    custom = tmp_path / "custom.md"
    custom.write_text("From a custom file.")
    monkeypatch.setenv("GS_MCP_INSTRUCTIONS_FILE", str(custom))
    cfg = GSMCPConfig()
    assert cfg.resolved_instructions() == "From a custom file."
