"""Root `gs` Click group."""

from __future__ import annotations

import sys
from typing import Optional

try:
    import click
except ImportError as exc:  # pragma: no cover - only hit without [cli]
    raise SystemExit(
        "graphsense CLI requires the [cli] extra: `pip install graphsense-python[cli]`"
    ) from exc

from graphsense.cli.bulk_cmd import bulk_command
from graphsense.cli.context import CliContext
from graphsense.cli.convenience import register_convenience_commands
from graphsense.cli.errors import FriendlyErrorGroup
from graphsense.cli.raw import build_raw_group
from graphsense.ext.client import API_KEY_ENV_VARS, HOST_ENV_VARS

pass_ctx = click.make_pass_decorator(CliContext)


FORMATS = click.Choice(["json", "jsonl", "csv"], case_sensitive=False)
INPUT_FORMATS = click.Choice(["auto", "json", "csv", "lines"], case_sensitive=False)


@click.group(
    cls=FriendlyErrorGroup,
    help="GraphSense command-line interface. "
    "Query blockchain analytics, attribute addresses, and work with "
    "JSON/CSV pipelines (`jq`/`sed`-friendly).",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option("--api-key", envvar=list(API_KEY_ENV_VARS), default=None)
@click.option(
    "--host",
    envvar=list(HOST_ENV_VARS),
    default=None,
    help="Base URL of the GraphSense REST API.",
)
@click.option("--format", "-f", "fmt", type=FORMATS, default=None)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
)
@click.option(
    "--directory",
    "-d",
    type=click.Path(file_okay=False, writable=True),
    default=None,
    help="Write one file per record into this directory.",
)
@click.option(
    "--input",
    "-i",
    "input_file",
    type=click.Path(dir_okay=False, exists=True),
    default=None,
)
@click.option("--input-format", type=INPUT_FORMATS, default="auto")
@click.option(
    "--address-jq",
    "address_jq",
    default=None,
    help="jmespath selector (e.g. '[].address') that extracts ids from "
    "JSON input. Named --address-jq for parity with --network-jq; on "
    "lookup-cluster / lookup-tx the selected ids are cluster ids / tx "
    "hashes, respectively.",
)
@click.option(
    "--address-col",
    "address_col",
    default=None,
    help="CSV column name or 0-based index from which to extract ids.",
)
@click.option(
    "--network-jq",
    default=None,
    help="jmespath selector that extracts the per-row network/currency "
    "(aligned with --address-jq). Overrides the positional CURRENCY on a "
    "per-row basis; rows where this returns empty fall back to the "
    "positional.",
)
@click.option(
    "--network-col",
    default=None,
    help="CSV column for per-row network/currency extraction (aligned "
    "with --address-col). Same fallback rule as --network-jq.",
)
@click.option(
    "--bulk/--no-bulk",
    "bulk_flag",
    default=None,
    help="Force or disable the /bulk endpoint; default is threshold-based.",
)
@click.option("--bulk-threshold", type=int, default=10)
@click.option(
    "--color",
    type=click.Choice(["auto", "always", "never"], case_sensitive=False),
    default="auto",
    help="Colorize JSON output. 'auto' enables only on a TTY stdout and "
    "disables when NO_COLOR is set. File output (-o/-d) and CSV are never "
    "colored.",
)
@click.option(
    "--no-color",
    "no_color_flag",
    is_flag=True,
    default=False,
    help="Shorthand for --color=never.",
)
@click.option("--quiet", "-q", is_flag=True, default=False)
@click.option("--verbose", "-v", count=True, default=0)
@click.pass_context
def cli(
    click_ctx: click.Context,
    api_key: Optional[str],
    host: Optional[str],
    fmt: Optional[str],
    output: Optional[str],
    directory: Optional[str],
    input_file: Optional[str],
    input_format: str,
    address_jq: Optional[str],
    address_col: Optional[str],
    network_jq: Optional[str],
    network_col: Optional[str],
    bulk_flag: Optional[bool],
    bulk_threshold: int,
    color: str,
    no_color_flag: bool,
    quiet: bool,
    verbose: int,
) -> None:
    effective_color = "never" if no_color_flag else color
    click_ctx.obj = CliContext(
        api_key=api_key,
        host=host,
        format=fmt,
        output=output,
        directory=directory,
        input=input_file,
        input_format=input_format,
        address_jq=address_jq,
        address_col=address_col,
        network_jq=network_jq,
        network_col=network_col,
        bulk=bulk_flag,
        bulk_threshold=bulk_threshold,
        color=effective_color,
        quiet=quiet,
        verbose=verbose,
    )


# Populate the command tree.
register_convenience_commands(cli)
cli.add_command(bulk_command, name="bulk")
cli.add_command(build_raw_group(), name="raw")


def main() -> None:  # pragma: no cover - trivial
    cli()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(cli() or 0)
