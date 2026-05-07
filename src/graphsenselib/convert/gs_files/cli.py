"""CLI for GraphSense `.gs` save file encoding/decoding."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from .encoder import builder_from_spec
from .parser import decode_gs, decode_gs_bytes, structure
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
