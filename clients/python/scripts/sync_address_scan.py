#!/usr/bin/env python3
"""Sync `address_scan` module from graphsenselib into the standalone client.

Source: ../../src/graphsenselib/convert/address_scan/
Target: ../graphsense/address_scan/

The client is published as a standalone package (`graphsense-python`) and must
not import `graphsenselib`. The scanner itself is pure stdlib, but in
graphsenselib two of its imports reach into the library:

  * `detectors.py` uses `graphsenselib.utils.address` (which is NOT standalone —
    it pulls in bitarray/bch/tron/base58/bech32/eth-hash). In the client we
    rewrite that import to the vendored, stdlib-only `address_scan.validators`.
  * `decompress.py` uses `graphsenselib.convert.gs_files.parser.lzw_unpack`. In
    the client that lives at `graphsense.gs_files.parser` (already vendored via
    `sync_gs_files.py`).

`cli.py` is excluded — the client wires its own `rich_click` integration in
`graphsense/cli/scan.py`.

Usage:
    python sync_address_scan.py           # write target files
    python sync_address_scan.py --check   # exit nonzero if target is stale
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE_DIR = REPO_ROOT / "src" / "graphsenselib" / "convert" / "address_scan"
TARGET_DIR = Path(__file__).resolve().parents[1] / "graphsense" / "address_scan"

EXCLUDE = {"cli.py", "__pycache__"}

# Exact import-line rewrites applied to the vendored copy. These keep the client
# standalone; if the source imports change, update these (the --check drift guard
# and the cross-check tests will flag a mismatch).
REWRITES = {
    "from graphsenselib.utils.address import validate_address, validate_xrp_address": (
        "from graphsense.address_scan.validators import "
        "validate_address, validate_xrp_address"
    ),
    "from graphsenselib.convert.gs_files.parser import lzw_unpack": (
        "from graphsense.gs_files.parser import lzw_unpack"
    ),
}

HEADER = (
    "# AUTO-GENERATED — DO NOT EDIT.\n"
    "# Synced from src/graphsenselib/convert/address_scan/{name} via\n"
    "# clients/python/scripts/sync_address_scan.py. Edit the source and re-run\n"
    "# `make -C clients/python sync-address-scan`.\n"
)


def _wanted_files(src: Path) -> list[Path]:
    return sorted(p for p in src.iterdir() if p.is_file() and p.name not in EXCLUDE)


def _render(src_file: Path) -> str:
    body = src_file.read_text(encoding="utf-8")
    for old, new in REWRITES.items():
        body = body.replace(old, new)
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
            "address_scan vendored copy is out of date:\n"
            + "\n".join(drift)
            + "\nRun `make -C clients/python sync-address-scan`.",
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
