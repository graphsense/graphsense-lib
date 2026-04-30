"""Guard rail: our hand-written paths must be protected from regeneration."""

from __future__ import annotations

import fnmatch
from pathlib import Path

IGNORE_FILE = Path(__file__).parent.parent / ".openapi-generator-ignore"

PROTECTED_PATHS = [
    "graphsense/ext/client.py",
    "graphsense/ext/io.py",
    "graphsense/ext/output.py",
    "graphsense/ext/bulk.py",
    "graphsense/ext/selectors.py",
    "graphsense/ext/deprecation.py",
    "graphsense/cli/main.py",
    "graphsense/cli/convenience.py",
    "graphsense/cli/bulk_cmd.py",
    "graphsense/cli/raw.py",
    "graphsense/cli/context.py",
    "tests/conftest.py",
    "tests/test_ext_client.py",
    "docs/ext/index.md",
    "docs/cli/index.md",
    "README_CLI.md",
    "README_EXT.md",
    "CLAUDE.md",
]


def _patterns() -> list[str]:
    lines = IGNORE_FILE.read_text().splitlines()
    return [
        line.strip()
        for line in lines
        if line.strip() and not line.strip().startswith("#")
    ]


def _matches_any(path: str, patterns: list[str]) -> bool:
    for pat in patterns:
        if fnmatch.fnmatch(path, pat):
            return True
        # openapi-generator also honors `prefix/*` as a recursive glob for our paths.
        if pat.endswith("/*") and path.startswith(pat[: -len("/*")] + "/"):
            return True
    return False


def test_every_protected_path_is_covered():
    patterns = _patterns()
    missing = [p for p in PROTECTED_PATHS if not _matches_any(p, patterns)]
    assert not missing, f"not covered by .openapi-generator-ignore: {missing}"


def test_public_symbols_still_import():
    from graphsense.cli.main import cli  # noqa: F401
    from graphsense.ext import GraphSense  # noqa: F401
    from graphsense.ext.selectors import select_csv, select_json  # noqa: F401
