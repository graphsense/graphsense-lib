"""CLI for GraphSense `.gs` save file encoding/decoding."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import click

from .encoder import builder_from_spec, spec_from_pathfinder
from .layout import layout_metrics
from .parser import PathfinderData, decode_gs, decode_gs_bytes, structure
from .summary import summarize
from .writer import write_decoded, write_json


@click.group(name="gs-files")
def gs_files_cli() -> None:
    """Encode and decode GraphSense .gs save files (graph/pathfinder dashboards)."""


@gs_files_cli.command("decode")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["raw", "structured", "both"]),
    default="structured",
    show_default=True,
    help="What to emit: the raw decoded JSON, the structured view, or both.",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Write to file(s) instead of stdout. For --format=both writes "
        "<output>.raw.json and <output>.structured.json."
    ),
)
@click.option(
    "--indent",
    type=int,
    default=2,
    show_default=True,
    help="JSON indent, 0 for compact.",
)
def decode_cmd(file: Path, fmt: str, output: Path | None, indent: int) -> None:
    """Decode a .gs file to raw and/or structured JSON."""
    raw = decode_gs(file)
    indent_opt = indent or None
    structured = structure(raw) if fmt in ("structured", "both") else None
    write_decoded(raw, structured, fmt, output, indent_opt)  # type: ignore[arg-type]


@gs_files_cli.command("summary")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write summary to file instead of stdout.",
)
@click.option(
    "--indent",
    type=int,
    default=2,
    show_default=True,
    help="JSON indent, 0 for compact.",
)
def summary_cmd(file: Path, output: Path | None, indent: int) -> None:
    """Print a short JSON summary (version, counts) for a .gs file."""
    raw = decode_gs(file)
    write_json(summarize(structure(raw)), output, indent or None)


@gs_files_cli.command("encode")
@click.option(
    "-i",
    "--input",
    "input_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="JSON spec path. Reads stdin when omitted or '-'.",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output .gs path.",
)
@click.option(
    "--name",
    default="",
    help="Graph name embedded in the file.",
)
@click.option(
    "--network",
    default="btc",
    show_default=True,
    help="Default network when an item doesn't specify one.",
)
@click.option(
    "--verify/--no-verify",
    default=False,
    help="Round-trip the output through the decoder as a sanity check.",
)
def encode_cmd(
    input_path: Path | None,
    output: Path,
    name: str,
    network: str,
    verify: bool,
) -> None:
    """Build a Pathfinder .gs from a JSON spec.

    See `graphsenselib.convert.gs_files.encoder.builder_from_spec` for the
    spec schema. The file format produced is identical to what the
    Pathfinder dashboard's "Save graph" button writes.
    """
    if input_path is None or str(input_path) == "-":
        spec = json.load(sys.stdin)
    else:
        spec = json.loads(input_path.read_text(encoding="utf-8"))
    builder = builder_from_spec(spec, name=name, default_network=network)
    out = builder.write(output)
    click.echo(f"wrote {out} ({out.stat().st_size} bytes)", err=True)
    if verify:
        decode_gs_bytes(out.read_bytes())
        click.echo("verify ok", err=True)


# Same variables the graphsense python client reads.
_HOST_ENV_VARS = ("GRAPHSENSE_HOST", "IKNAIO_HOST", "GS_HOST")
_API_KEY_ENV_VARS = ("GRAPHSENSE_API_KEY", "IKNAIO_API_KEY", "GS_API_KEY")


def _with_backend(api_url: str, api_key: str | None, run):
    """Run ``run(backend)`` against a REST backend at ``api_url``."""
    # Imported here: httpx and the pathfinder package are only needed
    # when a lookup actually runs.
    import httpx

    from graphsenselib import pathfinder

    async def main():
        headers = {"Authorization": api_key} if api_key else {}
        async with httpx.AsyncClient(
            base_url=api_url, headers=headers, timeout=60.0
        ) as client:
            # One backend, so later steps reuse fetched txs.
            return await run(pathfinder.RestBackend(client))

    return asyncio.run(main())


@gs_files_cli.command("layout")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output .gs path (may be the input file).",
)
@click.option(
    "--lookup/--no-lookup",
    default=True,
    show_default=True,
    help=(
        "Ask the REST API which way money flows through each tx, so "
        "inflows are drawn left of the address they pay into. Without it "
        "(or without an API URL) the undirected layout is used."
    ),
)
@click.option(
    "--api-url",
    envvar=list(_HOST_ENV_VARS),
    default=None,
    help=f"GraphSense REST base URL. Env: {', '.join(_HOST_ENV_VARS)}.",
)
@click.option(
    "--api-key",
    envvar=list(_API_KEY_ENV_VARS),
    default=None,
    help=f"API key for --api-url. Env: {', '.join(_API_KEY_ENV_VARS)}.",
)
@click.option(
    "--report",
    is_flag=True,
    default=False,
    help="Print layout metrics before and after to stderr, as JSON.",
)
def layout_cmd(
    file: Path,
    output: Path,
    lookup: bool,
    api_url: str | None,
    api_key: str | None,
    report: bool,
) -> None:
    """Re-lay out an existing Pathfinder .gs file.

    Every node is placed afresh; labels, colours, starting points and
    edges are kept. Nodes are arranged in columns by hop distance from
    the starting points; with the direction lookup, senders sit left of
    their tx and receivers right of it, and a swap or bridge is laid out
    the way Pathfinder arranges it on load (a U-turn: the other chain
    runs right to left below).
    """
    from graphsenselib.pathfinder import relayout
    from graphsenselib.pathfinder.relayout import default_network

    data = structure(decode_gs(file))
    if not isinstance(data, PathfinderData):
        raise click.UsageError("only Pathfinder files can be laid out")

    original = spec_from_pathfinder(data)
    network = default_network(data)

    if lookup and api_url:
        laid, warnings = _with_backend(
            api_url, api_key, lambda backend: relayout(data, backend)
        )
    else:
        laid, _ = asyncio.run(relayout(data, None))
        warnings = (
            [
                "no API URL (--api-url / GRAPHSENSE_HOST): laying out without "
                "flow direction, so inflows may be drawn on the right"
            ]
            if lookup
            else []
        )
    for w in warnings:
        click.echo(f"warning: {w}", err=True)

    out = builder_from_spec(laid, name=data.name, default_network=network).write(output)
    click.echo(f"wrote {out} ({out.stat().st_size} bytes)", err=True)

    if report:
        # Score the original positions with the same flow information, so
        # the before/after numbers are comparable.
        flows = {t["id"]: t for t in laid["txs"]}
        original["txs"] = [
            {
                **t,
                "senders": flows[t["id"]].get("senders", []),
                "receivers": flows[t["id"]].get("receivers", []),
            }
            for t in original["txs"]
        ]
        click.echo(
            json.dumps(
                {"before": layout_metrics(original), "after": layout_metrics(laid)},
                indent=2,
            ),
            err=True,
        )


@gs_files_cli.command("convert")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output path. Default: named after the graph, in the current directory.",
)
@click.option(
    "--to",
    "to",
    type=click.Choice(["gs", "graphml"]),
    default="graphml",
    show_default=True,
    help="Target format.",
)
@click.option(
    "--tx-mode",
    type=click.Choice(["node", "aggregate"]),
    default="node",
    show_default=True,
    help=(
        "Graph formats: one node per tx (lossless), or one edge per sender, "
        "receiver and asset."
    ),
)
@click.option(
    "--layout",
    type=click.Choice(["keep", "auto"]),
    default="keep",
    show_default=True,
    help="Keep the positions, or lay the graph out afresh (as `layout`).",
)
@click.option(
    "--include",
    multiple=True,
    type=click.Choice(["tags"]),
    help="Graph formats: add address-level attribution. Repeatable.",
)
@click.option(
    "--api-url",
    envvar=list(_HOST_ENV_VARS),
    default=None,
    help=f"GraphSense REST base URL. Env: {', '.join(_HOST_ENV_VARS)}.",
)
@click.option(
    "--api-key",
    envvar=list(_API_KEY_ENV_VARS),
    default=None,
    help=f"API key for --api-url. Env: {', '.join(_API_KEY_ENV_VARS)}.",
)
def convert_cmd(
    file: Path,
    output: Path | None,
    to: str,
    tx_mode: str,
    layout: str,
    include: tuple[str, ...],
    api_url: str | None,
    api_key: str | None,
) -> None:
    """Convert a Pathfinder graph (.gs, or its JSON payload) to another format.

    Graph formats need the REST API (--api-url) for the flows: a .gs file
    stores which txs are on the graph, not which addresses they connect.
    Without it every tx is exported without flow edges.
    """
    from graphsenselib.convert.graph_convert import convert

    raw = file.read_bytes()
    payload = json.loads(raw) if raw.lstrip()[:1] == b"[" else raw

    def run(backend):
        return convert(
            payload,
            backend,
            to=to,
            tx_mode=tx_mode,  # type: ignore[arg-type]
            layout=layout,  # type: ignore[arg-type]
            include=include,
        )

    try:
        if api_url:
            result = _with_backend(api_url, api_key, run)
        else:
            if to != "gs" or layout == "auto":
                click.echo(
                    "no API URL (--api-url / GRAPHSENSE_HOST): converting "
                    "without lookups",
                    err=True,
                )
            result = asyncio.run(run(None))
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    for w in result.warnings:
        click.echo(f"warning: {w}", err=True)
    out = output or Path(result.filename)
    out.write_bytes(result.content)
    click.echo(f"wrote {out} ({len(result.content)} bytes)", err=True)
