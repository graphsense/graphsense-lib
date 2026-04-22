"""CLI for GraphSense `.gs` save file decoding."""

from __future__ import annotations

from pathlib import Path

import click

from .parser import decode_gs, structure
from .summary import summarize
from .writer import write_decoded, write_json


@click.group(name="gs-files")
def gs_files_cli() -> None:
    """Decode GraphSense .gs save files (graph/pathfinder dashboards)."""


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
