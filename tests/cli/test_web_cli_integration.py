import json

from click.testing import CliRunner

from graphsenselib.cli.main import cli


def test_web_openapi_works_without_config_file(tmp_path, monkeypatch):
    missing_cfg = tmp_path / "does-not-exist.yaml"
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "web", "openapi"],
    )

    result = CliRunner().invoke(
        cli,
        ["web", "openapi"],
        env={"GRAPHSENSE_CONFIG_YAML": str(missing_cfg)},
    )

    assert result.exit_code == 0

    spec = json.loads(result.output)
    assert spec["openapi"].startswith("3.")
    assert "paths" in spec


def test_tagpack_tool_version_works_without_config_file(tmp_path, monkeypatch):
    missing_cfg = tmp_path / "does-not-exist.yaml"
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "tagpack-tool", "--version"],
    )

    result = CliRunner().invoke(
        cli,
        ["tagpack-tool", "--version"],
        env={"GRAPHSENSE_CONFIG_YAML": str(missing_cfg)},
    )

    assert result.exit_code == 0
    assert "tagpack-tool" in result.output.lower()


def test_tagstore_version_works_without_config_file(tmp_path, monkeypatch):
    missing_cfg = tmp_path / "does-not-exist.yaml"
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "tagstore", "version"],
    )

    result = CliRunner().invoke(
        cli,
        ["tagstore", "version"],
        env={"GRAPHSENSE_CONFIG_YAML": str(missing_cfg)},
    )

    assert result.exit_code == 0
    assert result.output.strip()
