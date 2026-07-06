"""CLI for scanning files/blobs for cryptocurrency addresses."""

from __future__ import annotations

import json

import click

from .scanner import build_report, render_text


@click.command("scan-for-addresses")
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option(
    "--context",
    is_flag=True,
    help="Print surrounding text for each hit.",
)
@click.option(
    "--hide-rejected",
    is_flag=True,
    help="Do not list candidates that failed validation.",
)
@click.option(
    "--rejected-limit",
    type=int,
    default=20,
    show_default=True,
    help="Max rejected candidates to print per type (0 = all).",
)
@click.option(
    "--no-decompress",
    is_flag=True,
    help="Do not unwrap gzip/zlib/bz2/xz/zip/tar/gs containers.",
)
@click.option(
    "--carve",
    is_flag=True,
    help="Also hunt for zlib/gzip streams embedded in binaries.",
)
@click.option(
    "--max-decompressed-mb",
    type=int,
    default=1024,
    show_default=True,
    help="Cap on total decompressed bytes per file (bomb guard).",
)
@click.option(
    "--tx-hashes",
    is_flag=True,
    help=(
        "Also report 64-hex tx-hash CANDIDATES. WARNING: these are matched by "
        "format only and are NOT checksum-verifiable, so every 64-hex string "
        "(SHA-256 file hashes, API tokens, session ids, ...) is picked up too."
    ),
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit results as JSON instead of human-readable text.",
)
def scan_for_addresses_cmd(
    files: tuple[str, ...],
    context: bool,
    hide_rejected: bool,
    rejected_limit: int,
    no_decompress: bool,
    carve: bool,
    max_decompressed_mb: int,
    tx_hashes: bool,
    as_json: bool,
) -> None:
    """Scan text/SQL file(s) for cryptocurrency addresses.

    Extraction is deliberately permissive; the real filter is checksum
    validation, which removes the many address-shaped false positives found in
    database dumps (hashes, session ids, base64 blobs, filenames). Compressed
    containers (gzip/zlib/bz2/xz/zip/tar, GraphSense .gs) are unwrapped
    transparently; use --carve to also inflate streams embedded in binaries.
    """
    if tx_hashes:
        click.secho(
            "WARNING: --tx-hashes matches ANY 64-hex string by format only "
            "(no checksum). SHA-256 file hashes, tokens and ids will be "
            "reported as candidates too.",
            fg="yellow",
            err=True,
        )

    report = build_report(
        list(files),
        decompress=not no_decompress,
        carve=carve,
        max_decompressed_mb=max_decompressed_mb,
        tx_hashes=tx_hashes,
        context=context,
        hide_rejected=hide_rejected,
        rejected_limit=rejected_limit,
    )

    for f in report["files"]:
        if "error" in f:
            click.echo(f"!! cannot read {f['path']}: {f['error']}", err=True)

    if as_json:
        click.echo(json.dumps(report, indent=2))
    else:
        render_text(report, lambda line: click.echo(line))
