"""Public selector API; thin re-exports for code outside the CLI that
just wants to run a jmespath query or a CSV column pick.

The heavy-lifting lives in `graphsense.ext.io`; this module exists so
library users can `from graphsense.ext.selectors import select_json` without
touching CLI internals.
"""

from __future__ import annotations

from typing import Optional

from graphsense.ext.io import parse_input


def select_json(text: str, expression: Optional[str] = None) -> list[str]:
    """Return a list of ids from a JSON blob, optionally filtered by jmespath."""
    return parse_input(text, input_format="json", jq=expression)


def select_csv(text: str, column: Optional[str] = None) -> list[str]:
    """Return a list of ids from a CSV blob; `column` is a name or 0-based index."""
    return parse_input(text, input_format="csv", col=column)


def select_lines(text: str) -> list[str]:
    """Return a list of ids from a plain-text blob (one per line, `#`-comments ok)."""
    return parse_input(text, input_format="lines")
