"""`graphsense gs ...` — read GraphSense `.gs` save files (graph + pathfinder).

The extraction commands `txs` and `addresses` emit a uniform
`[{"network", "id"}, ...]` shape that feeds directly into `lookup-tx`
and `lookup-address` via the standard
`--address-jq '[].id' --network-jq '[].network'` selectors.

Example:
    graphsense gs txs graph.gs | \\
        graphsense --address-jq '[].id' --network-jq '[].network' \\
        lookup-tx btc
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import rich_click as click

from graphsense.cli.context import CliContext
from graphsense.ext import output as out_mod
from graphsense.gs_files import (
    GraphData,
    PathfinderData,
    decode_gs,
    structure,
    summarize,
    to_jsonable,
)

pass_ctx = click.make_pass_decorator(CliContext)


def _write(ctx: CliContext, payload: Any) -> None:
    out_mod.write(
        payload,
        output=ctx.output,
        directory=ctx.directory,
        format=ctx.format,
        color=ctx.color,
    )


def _load(file: Path) -> PathfinderData | GraphData:
    return structure(decode_gs(file))


def _dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate (network, id) pairs, preserving first-seen order."""
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for r in records:
        key = (str(r["network"]), str(r["id"]))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _txs(data: PathfinderData | GraphData) -> list[dict[str, Any]]:
    if isinstance(data, PathfinderData):
        return [{"network": t.id.currency, "id": t.id.id} for t in data.txs]
    return []


def _addresses(data: PathfinderData | GraphData) -> list[dict[str, Any]]:
    if isinstance(data, PathfinderData):
        return [{"network": t.id.currency, "id": t.id.id} for t in data.addresses]
    return [{"network": a.currency, "id": a.address} for a in data.addresses]


@click.group(name="gs")
def gs_group() -> None:
    """Read GraphSense `.gs` save files (Pathfinder + Graph dashboards)."""


@gs_group.command("txs")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--dedupe/--no-dedupe",
    default=True,
    help="Drop duplicate (network, id) pairs. Default: on.",
)
@pass_ctx
def txs_cmd(ctx: CliContext, file: Path, dedupe: bool) -> None:
    """Emit `{network, id}` records for every transaction in FILE.

    Only Pathfinder files contain transactions; for Graph files this emits
    an empty list.
    """
    data = _load(file)
    records = _txs(data)
    if dedupe:
        records = _dedupe(records)
    _write(ctx, records)


@gs_group.command("addresses")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--dedupe/--no-dedupe",
    default=True,
    help="Drop duplicate (network, id) pairs. Default: on.",
)
@pass_ctx
def addresses_cmd(ctx: CliContext, file: Path, dedupe: bool) -> None:
    """Emit `{network, id}` records for every address in FILE."""
    data = _load(file)
    records = _addresses(data)
    if dedupe:
        records = _dedupe(records)
    _write(ctx, records)


@gs_group.command("decode")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--raw",
    is_flag=True,
    default=False,
    help="Emit the raw decoded JSON instead of the structured view.",
)
@pass_ctx
def decode_cmd(ctx: CliContext, file: Path, raw: bool) -> None:
    """Decode FILE to JSON (structured by default)."""
    if raw:
        _write(ctx, decode_gs(file))
        return
    _write(ctx, to_jsonable(structure(decode_gs(file))))


@gs_group.command("summary")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@pass_ctx
def summary_cmd(ctx: CliContext, file: Path) -> None:
    """Emit a short summary (kind, version, counts) for FILE."""
    _write(ctx, summarize(_load(file)))
