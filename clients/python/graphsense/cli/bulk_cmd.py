"""`graphsense bulk <operation>` — direct access to /bulk.json / /bulk.csv."""

from __future__ import annotations

from typing import Optional

import click

from graphsense.cli.context import CliContext
from graphsense.ext import io as io_mod
from graphsense.ext import output as out_mod

pass_ctx = click.make_pass_decorator(CliContext)


@click.command(name="bulk")
@click.argument("operation")
@click.argument("currency")
@click.argument("keys", nargs=-1)
@click.option(
    "--key-field",
    default="address",
    help="Field name the bulk operation expects (address, tx_hash, cluster, ...).",
)
@click.option("--num-pages", type=int, default=1)
@pass_ctx
def bulk_command(
    ctx: CliContext,
    operation: str,
    currency: str,
    keys: tuple[str, ...],
    key_field: str,
    num_pages: int,
) -> None:
    """Call /bulk.json/<operation> or /bulk.csv/<operation> for a list of keys.

    Keys come from positionals, --input, or stdin (JSON/CSV/lines, with
    --address-jq / --address-col).
    Output format follows --format (default json).
    """
    ids = _collect_keys(ctx, keys)
    if not ids:
        raise click.UsageError("no keys provided for bulk")

    fmt = (ctx.format or "json").lower()
    gs = ctx.gs()
    result = gs.bulk(
        operation,
        ids,
        currency=currency,
        format="csv" if fmt == "csv" else "json",
        num_pages=num_pages,
        key_field=key_field,
    )

    if fmt == "csv":
        _write_raw(ctx, result)
    else:
        out_mod.write(
            result,
            output=ctx.output,
            directory=ctx.directory,
            format=ctx.format,
            color=ctx.color,
        )


def _collect_keys(ctx: CliContext, positional: tuple[str, ...]) -> list[str]:
    if positional:
        return list(positional)
    text = ctx.read_input_text()
    if text is None:
        return []
    return io_mod.parse_input(
        text,
        input_format=ctx.input_format,
        jq=ctx.address_jq,
        col=ctx.address_col,
    )


def _write_raw(ctx: CliContext, payload) -> None:
    """For CSV bulk output, the server already returns flat rows; pass through."""
    text: Optional[str]
    if isinstance(payload, (bytes, bytearray)):
        text = payload.decode("utf-8")
    elif isinstance(payload, str):
        text = payload
    else:
        # Some generated clients deserialize the CSV as {"value": "...raw..."}
        text = str(payload)
    with out_mod.open_out(ctx.output) as fh:
        fh.write(text)
        if not text.endswith("\n"):
            fh.write("\n")
