from click.testing import CliRunner

from graphsenselib.cli.main import cli


def test_mcp_help_lists_subcommands(tmp_path, monkeypatch):
    missing_cfg = tmp_path / "does-not-exist.yaml"
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "mcp", "--help"],
    )
    result = CliRunner().invoke(
        cli,
        ["mcp", "--help"],
        env={"GRAPHSENSE_CONFIG_YAML": str(missing_cfg)},
    )
    assert result.exit_code == 0, result.output
    assert "validate-curation" in result.output
    # `mcp serve` was removed; MCP mounts inside the main FastAPI app.
    # The group help mentions this, so check for absence of the subcommand
    # listing under the "Commands:" section rather than any occurrence.
    commands_section = result.output.split("Commands:", 1)[-1]
    assert "serve" not in commands_section


def test_mcp_validate_curation_exits_zero_on_bundled_yaml(tmp_path, monkeypatch):
    missing_cfg = tmp_path / "does-not-exist.yaml"
    monkeypatch.setattr(
        "graphsenselib.cli.common.sys.argv",
        ["graphsense-cli", "mcp", "validate-curation"],
    )
    result = CliRunner().invoke(
        cli,
        ["mcp", "validate-curation"],
        env={"GRAPHSENSE_CONFIG_YAML": str(missing_cfg)},
    )
    assert result.exit_code == 0, result.output
    assert "Curation OK" in result.output
