"""Tests for the new consolidated ``Settings`` model.

Covers:

- env-only construction (steps 1)
- YAML-only (step 2)
- env overrides YAML (step 2)
- legacy-prefix translation + DeprecationWarning (step 3)
- new-prefix wins over legacy (step 3)
- per-field provenance (step 6)
"""

from __future__ import annotations

import warnings

import pytest

from graphsenselib.config.settings import Settings, get_settings, reset_settings
from graphsenselib.config._sources import get_sink


# ---------------------------------------------------------------------------
# Env-only
# ---------------------------------------------------------------------------


def test_defaults_construct_without_env_or_yaml(tmp_path, monkeypatch):
    """A bare Settings() in an empty env yields documented defaults."""
    monkeypatch.chdir(tmp_path)  # avoid picking up a stray .graphsense.yaml
    s = Settings()
    assert s.mcp.path == "/mcp"
    assert s.mcp.enabled is True
    assert s.cassandra is None
    assert s.tagstore is None
    assert s.keyspaces == {}
    assert s.environment is None


def test_env_populates_nested_fields(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_MCP__PATH", "/custom-mcp")
    monkeypatch.setenv("GRAPHSENSE_CASSANDRA__NODES", '["host1:9042"]')
    s = Settings()
    assert s.mcp.path == "/custom-mcp"
    assert s.cassandra is not None
    assert s.cassandra.nodes == ["host1:9042"]


# ---------------------------------------------------------------------------
# YAML
# ---------------------------------------------------------------------------


def _write_yaml(path, body: str):
    path.write_text(body, encoding="utf-8")


def test_yaml_only(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    _write_yaml(
        yaml_file,
        """
        mcp:
          path: /yaml-mcp
        cassandra:
          nodes: ["yaml-host:9042"]
        web:
          ALLOWED_ORIGINS: ["https://yaml.example"]
          hide_private_tags: true
        """,
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))
    s = Settings()
    assert s.mcp.path == "/yaml-mcp"
    assert s.cassandra is not None
    assert s.cassandra.nodes == ["yaml-host:9042"]
    assert s.web is not None
    assert s.web.hide_private_tags is True
    # legacy_web_dict is mirrored from raw YAML
    assert s.legacy_web_dict == {
        "ALLOWED_ORIGINS": ["https://yaml.example"],
        "hide_private_tags": True,
    }
    assert s.yaml_loaded_path == yaml_file


def test_env_overrides_yaml(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    _write_yaml(yaml_file, "mcp:\n  path: /from-yaml\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))
    monkeypatch.setenv("GRAPHSENSE_MCP__PATH", "/from-env")
    s = Settings()
    assert s.mcp.path == "/from-env"


# ---------------------------------------------------------------------------
# Legacy env-prefix translation
# ---------------------------------------------------------------------------


def test_legacy_env_translated_with_warning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GS_MCP_PATH", "/from-legacy")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        s = Settings()

    assert s.mcp.path == "/from-legacy"
    matching = [
        w
        for w in caught
        if issubclass(w.category, DeprecationWarning)
        and "GS_MCP_PATH" in str(w.message)
    ]
    assert len(matching) == 1
    assert "GRAPHSENSE_MCP__PATH" in str(matching[0].message)


def test_new_env_wins_over_legacy(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GS_MCP_PATH", "/from-legacy")
    monkeypatch.setenv("GRAPHSENSE_MCP__PATH", "/from-new")
    s = Settings()
    assert s.mcp.path == "/from-new"


@pytest.mark.parametrize(
    "legacy,new,value",
    [
        ("GS_CASSANDRA_ASYNC_PORT", "GRAPHSENSE_CASSANDRA__PORT", "9999"),
        ("GRAPHSENSE_TAGSTORE_READ_URL", "GRAPHSENSE_TAGSTORE__URL", "postgresql://x"),
        # Exact-var rename: collapsed tagstore_db.db_url -> tagstore.url
        ("GS_TAGSTORE_DB_URL", "GRAPHSENSE_TAGSTORE__URL", "postgresql://y"),
        ("GSREST_HIDE_PRIVATE_TAGS", "GRAPHSENSE_WEB__HIDE_PRIVATE_TAGS", "true"),
        ("GS_MCP_PATH", "GRAPHSENSE_MCP__PATH", "/custom"),
    ],
)
def test_all_legacy_prefixes_translate(tmp_path, monkeypatch, legacy, new, value):
    """Every prefix in LEGACY_PREFIX_MAP / LEGACY_VAR_MAP is honored."""
    import os as _os

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(legacy, value)
    Settings()  # mutates os.environ
    assert _os.environ.get(new) == value


def test_tagstore_url_collapses_legacy_db_url(tmp_path, monkeypatch):
    """GS_TAGSTORE_DB_URL (legacy server-side) and GRAPHSENSE_TAGSTORE_READ_URL
    (legacy reader) both land on the consolidated Settings.tagstore.url."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GS_TAGSTORE_DB_URL", "postgresql://from-db-url")
    s = Settings()
    assert s.tagstore is not None
    assert s.tagstore.url == "postgresql://from-db-url"


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------


def test_provenance_records_yaml_vs_env(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    _write_yaml(yaml_file, "mcp:\n  path: /from-yaml\n  enabled: false\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))
    monkeypatch.setenv("GRAPHSENSE_MCP__PATH", "/from-env")

    s = Settings()
    sink = get_sink(s)
    assert sink is not None

    # path was overridden by env; enabled came from yaml.
    path_value, path_label = sink.data["mcp.path"]
    enabled_value, enabled_label = sink.data["mcp.enabled"]
    assert path_value == "/from-env"
    assert path_label == "env"
    assert enabled_value is False
    assert enabled_label.startswith("yaml:")


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


def test_get_settings_is_lazy_and_cached(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_MCP__PATH", "/first")
    s1 = get_settings()
    # Mutate env after first call — singleton must not pick it up.
    monkeypatch.setenv("GRAPHSENSE_MCP__PATH", "/second")
    s2 = get_settings()
    assert s1 is s2
    assert s2.mcp.path == "/first"
    # reset_settings() forces re-read.
    reset_settings()
    s3 = get_settings()
    assert s3 is not s1
    assert s3.mcp.path == "/second"


# ---------------------------------------------------------------------------
# Layered loader: base + per-env overlay
# ---------------------------------------------------------------------------


def test_layered_loader_overlay_wins_over_base(tmp_path, monkeypatch):
    base = tmp_path / "graphsense.yaml"
    overlay = tmp_path / "graphsense.prod.yaml"
    _write_yaml(
        base,
        """
        cassandra:
          nodes: ["shared-host:9042"]
          username: shared_user
        mcp:
          path: /base-mcp
        """,
    )
    _write_yaml(
        overlay,
        """
        cassandra:
          nodes: ["prod-host:9042"]
        mcp:
          path: /prod-mcp
        """,
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(base))

    s, _ = Settings.try_load(env="prod")
    assert s is not None
    assert s.cassandra is not None
    # Overlay wins on conflict
    assert s.cassandra.nodes == ["prod-host:9042"]
    assert s.mcp.path == "/prod-mcp"
    # Base survives where overlay is silent
    assert s.cassandra.username == "shared_user"
    assert s.environment == "prod"


def test_overlay_missing_file_falls_back_to_base(tmp_path, monkeypatch):
    base = tmp_path / "graphsense.yaml"
    _write_yaml(base, "mcp:\n  path: /base-only\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(base))

    s, _ = Settings.try_load(env="prod")  # no graphsense.prod.yaml present
    assert s is not None
    assert s.mcp.path == "/base-only"


def test_env_without_base_file(tmp_path, monkeypatch):
    # User has only graphsense.prod.yaml, no base file. We still want
    # the overlay loader to find it.
    overlay = tmp_path / "graphsense.prod.yaml"
    _write_yaml(overlay, "mcp:\n  path: /prod-only\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(tmp_path / "graphsense.yaml"))

    s, _ = Settings.try_load(env="prod")
    # Base file doesn't exist. Overlay is resolved relative to the base
    # filename, so with the base missing we currently fall through to no
    # YAML — overlay loading is tied to finding a base. Documenting
    # current behavior: if you want env-only, create an empty base file.
    assert s is not None
    assert s.mcp.path == "/mcp"  # default, not /prod-only


# ---------------------------------------------------------------------------
# Legacy monolithic YAML lift
# ---------------------------------------------------------------------------


def test_legacy_environments_lifts_to_root(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    _write_yaml(
        yaml_file,
        """
        default_environment: prod
        environments:
          prod:
            cassandra_nodes: ["legacy-host:9042"]
            username: legacy_user
            password: legacy_pass
            keyspaces:
              btc:
                raw_keyspace_name: btc_raw
                transformed_keyspace_name: btc_transformed
                schema_type: utxo
        """,
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        s, _ = Settings.try_load(env="prod")

    assert s is not None
    assert s.cassandra is not None
    assert s.cassandra.nodes == ["legacy-host:9042"]
    assert s.cassandra.username == "legacy_user"
    assert s.cassandra.password == "legacy_pass"
    assert s.keyspaces["btc"].raw_keyspace_name == "btc_raw"
    assert s.environment == "prod"

    lift_warnings = [
        w
        for w in caught
        if issubclass(w.category, DeprecationWarning)
        and "environments.prod" in str(w.message)
    ]
    assert len(lift_warnings) == 1


def test_legacy_lift_uses_default_environment_when_no_env_flag(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    _write_yaml(
        yaml_file,
        """
        default_environment: dev
        environments:
          dev:
            cassandra_nodes: ["dev-host:9042"]
            keyspaces:
              btc:
                raw_keyspace_name: btc_raw
                transformed_keyspace_name: btc_transformed
                schema_type: utxo
        """,
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    s, _ = Settings.try_load()  # no env arg
    assert s is not None
    assert s.cassandra is not None
    # default_environment = dev → dev keyspaces lift to root
    assert s.cassandra.nodes == ["dev-host:9042"]
    assert "btc" in s.keyspaces


def test_explicit_cassandra_wins_over_lifted_legacy(tmp_path, monkeypatch):
    # A user mid-migration has both the legacy `environments` block and
    # an explicit root-level `cassandra` section. Root wins.
    yaml_file = tmp_path / "graphsense.yaml"
    _write_yaml(
        yaml_file,
        """
        cassandra:
          nodes: ["new-host:9042"]
        environments:
          prod:
            cassandra_nodes: ["old-host:9042"]
            keyspaces: {}
        """,
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        s, _ = Settings.try_load(env="prod")

    assert s is not None
    assert s.cassandra is not None
    assert s.cassandra.nodes == ["new-host:9042"]


# ---------------------------------------------------------------------------
# ./instance/config.yaml default lookup (REST deployment convention)
# ---------------------------------------------------------------------------


def test_instance_config_yaml_default_lookup(tmp_path, monkeypatch):
    (tmp_path / "instance").mkdir()
    _write_yaml(tmp_path / "instance" / "config.yaml", "mcp:\n  path: /rest-style\n")
    monkeypatch.chdir(tmp_path)

    s = Settings()
    assert s.mcp.path == "/rest-style"


def test_legacy_CONFIG_FILE_env_var_honored(tmp_path, monkeypatch):
    """REST's CONFIG_FILE env var (gsrest convention) picks up the YAML
    when GRAPHSENSE_CONFIG_YAML is not set, so existing Docker
    deployments keep working."""
    yaml_file = tmp_path / "somewhere" / "config.yaml"
    yaml_file.parent.mkdir()
    _write_yaml(yaml_file, "mcp:\n  path: /from-legacy-CONFIG_FILE\n")
    monkeypatch.chdir(tmp_path)  # no default files here
    # Deliberately NOT setting GRAPHSENSE_CONFIG_YAML — CONFIG_FILE alone
    # should be enough.
    monkeypatch.setenv("CONFIG_FILE", str(yaml_file))

    s = Settings()
    assert s.mcp.path == "/from-legacy-CONFIG_FILE"


def test_graphsense_config_yaml_wins_over_legacy_CONFIG_FILE(tmp_path, monkeypatch):
    new = tmp_path / "new.yaml"
    old = tmp_path / "old.yaml"
    _write_yaml(new, "mcp:\n  path: /new\n")
    _write_yaml(old, "mcp:\n  path: /old\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(new))
    monkeypatch.setenv("CONFIG_FILE", str(old))

    s = Settings()
    assert s.mcp.path == "/new"


def test_try_load_returns_errors_instead_of_raising(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # No required-field violations are in the new Settings (everything has
    # a default), so try_load() succeeds even with no input. Verify the
    # error-capture branch by passing an explicit invalid kwarg.
    s, errs = Settings.try_load()
    assert s is not None
    assert errs == []

    s_bad, errs_bad = Settings.try_load(mcp={"enabled": "not-a-bool-or-int"})
    assert s_bad is None
    assert errs_bad
