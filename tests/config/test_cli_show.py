"""Tests for ``gs config show --resolved [--source]``."""

from __future__ import annotations

from click.testing import CliRunner

from graphsenselib.config.cli import config_cli


def test_show_resolved_renders_table(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    yaml_file.write_text("mcp:\n  path: /yaml-mcp\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    result = CliRunner().invoke(config_cli, ["config", "show", "--resolved"])

    assert result.exit_code == 0, result.output
    assert "/yaml-mcp" in result.output
    assert "mcp.path" in result.output
    # No 'source' column without --source flag
    assert "source" not in result.output.lower().split("path")[0]


def test_show_resolved_with_source_includes_origin(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    yaml_file.write_text("mcp:\n  path: /yaml-mcp\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))
    monkeypatch.setenv("GRAPHSENSE_TAGSTORE__URL", "postgresql://from-env/x")

    result = CliRunner().invoke(
        config_cli, ["config", "show", "--resolved", "--source"]
    )

    assert result.exit_code == 0, result.output
    # YAML-sourced field is labelled with the absolute path.
    assert "yaml:" in result.output
    # Env-sourced field is labelled 'env'.
    assert "from-env" in result.output
    # Untouched fields fall back to 'default'.
    assert "default" in result.output


def test_show_without_resolved_uses_legacy_path(monkeypatch):
    """The bare `show` (no --resolved) still goes through the legacy
    AppConfig.text() / model_dump_json() — backward compatible."""
    result = CliRunner().invoke(config_cli, ["config", "show"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# gs config migrate
# ---------------------------------------------------------------------------


_LEGACY_YAML = """\
default_environment: prod
environments:
  prod:
    cassandra_nodes: ["cas-prod:9042"]
    username: prod_user
    password: prod_pass
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw
        transformed_keyspace_name: btc_transformed
        schema_type: utxo
  dev:
    cassandra_nodes: ["localhost:9042"]
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw_dev
        transformed_keyspace_name: btc_transformed_dev
        schema_type: utxo
web:
  gs-tagstore:
    url: postgresql://tagstore/tagstore
  ALLOWED_ORIGINS: "*"
  hide_private_tags: false
slack_topics:
  exceptions:
    hooks: ["https://hooks.slack.com/XXX"]
coingecko_api_key: api_key_abc
"""


def test_migrate_to_stdout(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy.yaml"
    legacy.write_text(_LEGACY_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(config_cli, ["config", "migrate", "--in", str(legacy)])
    assert result.exit_code == 0, result.output

    # Three file sections, one per env + shared
    assert "===== graphsense.yaml =====" in result.output
    assert "===== graphsense.prod.yaml =====" in result.output
    assert "===== graphsense.dev.yaml =====" in result.output

    # Shared section hoists gs-tagstore to root and keeps the web block minus it
    assert "tagstore:" in result.output
    assert "postgresql://tagstore/tagstore" in result.output
    # Per-env has cassandra + keyspaces; no cassandra_nodes at root
    assert "cas-prod:9042" in result.output
    assert "btc_raw_dev" in result.output


def test_migrate_outputs_files_and_roundtrips(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy.yaml"
    out_dir = tmp_path / "out"
    legacy.write_text(_LEGACY_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        config_cli,
        ["config", "migrate", "--in", str(legacy), "--out-dir", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    assert (out_dir / "graphsense.yaml").exists()
    assert (out_dir / "graphsense.prod.yaml").exists()
    assert (out_dir / "graphsense.dev.yaml").exists()

    # Round-trip: migrated files load cleanly under the new Settings.
    from graphsenselib.config.settings import Settings

    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(out_dir / "graphsense.yaml"))
    s, errors = Settings.try_load(env="prod")
    assert errors == []
    assert s is not None
    assert s.cassandra is not None
    assert s.tagstore is not None
    assert s.web is not None
    assert s.environment == "prod"
    assert s.cassandra.nodes == ["cas-prod:9042"]
    assert s.cassandra.username == "prod_user"
    assert s.keyspaces["btc"].raw_keyspace_name == "btc_raw"
    assert s.tagstore.url == "postgresql://tagstore/tagstore"
    assert s.web.hide_private_tags is False
    assert s.slack_topics["exceptions"].hooks == ["https://hooks.slack.com/XXX"]


_SINGLE_ENV_YAML = """\
environments:
  prod:
    cassandra_nodes: ["cas-prod:9042"]
    username: prod_user
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw
        transformed_keyspace_name: btc_transformed
        schema_type: utxo
web:
  ALLOWED_ORIGINS: "*"
"""


_IDENTICAL_ENVS_YAML = """\
environments:
  prod:
    cassandra_nodes: ["shared:9042"]
    username: shared_user
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw
        transformed_keyspace_name: btc_transformed
        schema_type: utxo
  staging:
    cassandra_nodes: ["shared:9042"]
    username: shared_user
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw
        transformed_keyspace_name: btc_transformed
        schema_type: utxo
"""


_PARTIAL_OVERLAP_YAML = """\
environments:
  prod:
    cassandra_nodes: ["cas-prod:9042"]
    username: shared_user
    password: prod_pass
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw
        transformed_keyspace_name: btc_transformed
        schema_type: utxo
  dev:
    cassandra_nodes: ["localhost:9042"]
    username: shared_user
    password: dev_pass
    keyspaces:
      btc:
        raw_keyspace_name: btc_raw_dev
        transformed_keyspace_name: btc_transformed_dev
        schema_type: utxo
"""


def test_migrate_single_env_lifts_everything_to_shared(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy.yaml"
    out_dir = tmp_path / "out"
    legacy.write_text(_SINGLE_ENV_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        config_cli,
        ["config", "migrate", "--in", str(legacy), "--out-dir", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    # Only the shared file exists — per-env file would be redundant.
    assert (out_dir / "graphsense.yaml").exists()
    assert not (out_dir / "graphsense.prod.yaml").exists()

    shared = (out_dir / "graphsense.yaml").read_text()
    assert "cas-prod:9042" in shared
    assert "prod_user" in shared
    assert "btc_raw" in shared


def test_migrate_identical_envs_lifts_everything(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy.yaml"
    out_dir = tmp_path / "out"
    legacy.write_text(_IDENTICAL_ENVS_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        config_cli,
        ["config", "migrate", "--in", str(legacy), "--out-dir", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    assert (out_dir / "graphsense.yaml").exists()
    # No per-env files for envs that had identical content.
    assert not (out_dir / "graphsense.prod.yaml").exists()
    assert not (out_dir / "graphsense.staging.yaml").exists()


def test_migrate_partial_overlap_lifts_shared_subfields(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy.yaml"
    out_dir = tmp_path / "out"
    legacy.write_text(_PARTIAL_OVERLAP_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        config_cli,
        ["config", "migrate", "--in", str(legacy), "--out-dir", str(out_dir)],
    )
    assert result.exit_code == 0, result.output

    # Shared file carries the identical username.
    shared = (out_dir / "graphsense.yaml").read_text()
    assert "shared_user" in shared
    # Per-env files exist because nodes/password/keyspaces differ.
    prod = (out_dir / "graphsense.prod.yaml").read_text()
    dev = (out_dir / "graphsense.dev.yaml").read_text()
    assert "cas-prod:9042" in prod and "localhost:9042" in dev
    assert "prod_pass" in prod and "dev_pass" in dev
    # Duplicated username should NOT appear in the per-env files.
    assert "shared_user" not in prod and "shared_user" not in dev

    # Round-trip: loading the migrated files with --env prod gives the
    # right merged view (username from shared, nodes+password from prod).
    from graphsenselib.config.settings import Settings

    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(out_dir / "graphsense.yaml"))
    s, errs = Settings.try_load(env="prod")
    assert errs == []
    assert s is not None
    assert s.cassandra is not None
    assert s.cassandra.nodes == ["cas-prod:9042"]
    assert s.cassandra.username == "shared_user"
    assert s.cassandra.password == "prod_pass"


# ---------------------------------------------------------------------------
# gs config dump — deterministic before/after migration comparison
# ---------------------------------------------------------------------------


def test_dump_is_deterministic(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    yaml_file.write_text(
        "cassandra:\n  nodes: [a:9042]\nmcp:\n  path: /x\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    r1 = CliRunner().invoke(config_cli, ["config", "dump"])
    # Reset singleton to force a re-read
    from graphsenselib.config.settings import reset_settings

    reset_settings()
    r2 = CliRunner().invoke(config_cli, ["config", "dump"])

    assert r1.exit_code == 0 and r2.exit_code == 0
    assert r1.output == r2.output  # byte-identical across runs


def test_dump_hash_matches_before_and_after_migration(tmp_path, monkeypatch):
    """The whole point of the command: a legacy monolithic YAML and its
    per-env-split migrated equivalent produce the same effective Settings,
    so the dump hashes match. This is the guarantee users need when
    migrating a live system."""
    legacy = tmp_path / "legacy.yaml"
    out_dir = tmp_path / "out"
    legacy.write_text(_LEGACY_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    # Migrate.
    mig = CliRunner().invoke(
        config_cli,
        ["config", "migrate", "--in", str(legacy), "--out-dir", str(out_dir)],
    )
    assert mig.exit_code == 0, mig.output

    # Dump before (legacy YAML).
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(legacy))
    from graphsenselib.config.settings import reset_settings

    reset_settings()
    before = CliRunner().invoke(config_cli, ["config", "--env", "prod", "dump"])
    assert before.exit_code == 0, before.output

    # Dump after (migrated per-env files).
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(out_dir / "graphsense.yaml"))
    reset_settings()
    after = CliRunner().invoke(config_cli, ["config", "--env", "prod", "dump"])
    assert after.exit_code == 0, after.output

    # Effective Settings are byte-identical → dumps match.
    assert before.output == after.output


def test_dump_hash_flag(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    yaml_file.write_text("mcp:\n  path: /foo\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    result = CliRunner().invoke(config_cli, ["config", "dump", "--hash"])
    assert result.exit_code == 0, result.output
    assert result.output.startswith("sha256:")
    # 64 hex chars after the "sha256:" prefix
    digest = result.output.strip().removeprefix("sha256:")
    assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)


def test_dump_yaml_format(tmp_path, monkeypatch):
    yaml_file = tmp_path / "graphsense.yaml"
    yaml_file.write_text("mcp:\n  path: /y\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GRAPHSENSE_CONFIG_YAML", str(yaml_file))

    result = CliRunner().invoke(config_cli, ["config", "dump", "--format", "yaml"])
    assert result.exit_code == 0, result.output
    assert "mcp:" in result.output
    assert "path: /y" in result.output


def test_migrate_refuses_overwrite_without_flag(tmp_path, monkeypatch):
    legacy = tmp_path / "legacy.yaml"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "graphsense.yaml").write_text("pre-existing\n", encoding="utf-8")
    legacy.write_text(_LEGACY_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        config_cli,
        ["config", "migrate", "--in", str(legacy), "--out-dir", str(out_dir)],
    )
    assert result.exit_code != 0
    assert "Refusing to overwrite" in result.output
    # Pre-existing file was not touched.
    assert (out_dir / "graphsense.yaml").read_text() == "pre-existing\n"

    # With --overwrite it proceeds.
    result2 = CliRunner().invoke(
        config_cli,
        [
            "config",
            "migrate",
            "--in",
            str(legacy),
            "--out-dir",
            str(out_dir),
            "--overwrite",
        ],
    )
    assert result2.exit_code == 0, result2.output
    assert (out_dir / "graphsense.yaml").read_text() != "pre-existing\n"
