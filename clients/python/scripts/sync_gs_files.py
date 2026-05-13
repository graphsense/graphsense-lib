#!/usr/bin/env python3
"""Sync `gs_files` module from graphsenselib into the standalone python client.

Source: ../../src/graphsenselib/convert/gs_files/
Target: ../graphsense/gs_files/

The client is published as a standalone package (`graphsense-python`) and
must not import `graphsenselib`. The `gs_files` module is pure stdlib, so
we vendor a copy and keep it in lockstep via this script (run from a
Makefile target and the repo's pre-commit hooks).

`cli.py` is excluded — the client wires its own `rich_click` integration
in `graphsense/cli/gs.py`.

Usage:
    python sync_gs_files.py           # write target files
    python sync_gs_files.py --check   # exit nonzero if target is stale
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE_DIR = REPO_ROOT / "src" / "graphsenselib" / "convert" / "gs_files"
TARGET_DIR = Path(__file__).resolve().parents[1] / "graphsense" / "gs_files"

EXCLUDE = {"cli.py", "__pycache__"}

HEADER = (
    "# AUTO-GENERATED — DO NOT EDIT.\n"
    "# Synced from src/graphsenselib/convert/gs_files/{name} via\n"
    "# clients/python/scripts/sync_gs_files.py. Edit the source and re-run\n"
    "# `make -C clients/python sync-gs-files`.\n"
)


def _wanted_files(src: Path) -> list[Path]:
    return sorted(p for p in src.iterdir() if p.is_file() and p.name not in EXCLUDE)


def _render(src_file: Path) -> str:
    body = src_file.read_text(encoding="utf-8")
    return HEADER.format(name=src_file.name) + body


def sync(check: bool) -> int:
    if not SOURCE_DIR.is_dir():
        print(f"source dir not found: {SOURCE_DIR}", file=sys.stderr)
        return 2

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    sources = _wanted_files(SOURCE_DIR)
    expected_names = {p.name for p in sources}

    drift: list[str] = []

    for src_file in sources:
        new_content = _render(src_file)
        dst_file = TARGET_DIR / src_file.name
        existing = dst_file.read_text(encoding="utf-8") if dst_file.exists() else None
        if existing != new_content:
            if check:
                drift.append(f"  stale: {dst_file.relative_to(REPO_ROOT)}")
            else:
                dst_file.write_text(new_content, encoding="utf-8")

    for stale in TARGET_DIR.iterdir():
        if stale.is_dir() or stale.name in expected_names:
            continue
        if check:
            drift.append(f"  unexpected: {stale.relative_to(REPO_ROOT)}")
        else:
            stale.unlink()

    if check and drift:
        print(
            "gs_files vendored copy is out of date:\n"
            + "\n".join(drift)
            + "\nRun `make -C clients/python sync-gs-files`.",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit nonzero if the vendored copy is out of date.",
    )
    args = parser.parse_args()
    return sync(check=args.check)


if __name__ == "__main__":
    sys.exit(main())
