from pathlib import Path
from typing import Optional

import click


@click.group()
def mcp_cli():
    pass


@mcp_cli.group("mcp")
def mcp():
    """MCP (Model Context Protocol) management for graphsense.

    The MCP endpoint is mounted inside the main FastAPI app (see
    graphsenselib.web.app.create_app); there is no separate `serve` command.
    Use `graphsense-cli web openapi` or your uvicorn/gunicorn entry point to
    run the web stack — MCP comes up automatically at GS_MCP_PATH (default
    /mcp).
    """


@mcp.command("validate-curation")
@click.option(
    "--curation-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to a curation YAML (defaults to the bundled file)",
)
def validate_curation_cmd(curation_file: Optional[Path]):
    """Check the curation YAML against the live FastAPI app. Exits non-zero on drift.

    Uses the minimal spec app (no DB connection required) so this is suitable
    for CI.
    """
    from graphsenselib.mcp import GSMCPConfig, validate_curation
    from graphsenselib.web.app import create_spec_app

    cfg = GSMCPConfig()
    if curation_file is not None:
        cfg.curation_file = curation_file

    app = create_spec_app()
    errors = validate_curation(app, cfg)
    if errors:
        for err in errors:
            click.echo(f"ERROR: {err}", err=True)
        raise click.exceptions.Exit(1)
    click.echo(f"Curation OK: {cfg.resolved_curation_path()}")
