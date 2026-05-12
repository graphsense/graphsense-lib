"""Input parsing for the CLI: detect JSON / CSV / plain lines from a stream
or string, and extract a list of ids given a selector (jmespath / column).
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Iterable, Optional


def detect_format(sample: str) -> str:
    """Best-effort sniff of the input format.

    Rules:
      - starts with `{` or `[` -> json
      - first non-empty line contains a comma -> csv
      - otherwise -> lines
    """
    stripped = sample.lstrip()
    if not stripped:
        return "lines"
    if stripped[0] in "{[":
        return "json"
    first_line = stripped.splitlines()[0] if stripped else ""
    if "," in first_line:
        return "csv"
    return "lines"


def read_text(source: str | io.IOBase | None) -> str:
    """Read the full body of a string, file path, or file-like object."""
    if source is None:
        return ""
    if isinstance(source, str):
        # Treat short strings as literal content; paths should be opened by caller.
        return source
    return source.read()


def parse_input(
    text: str,
    *,
    input_format: str = "auto",
    jq: Optional[str] = None,
    col: Optional[str] = None,
    skip_empty: bool = True,
) -> list[str]:
    """Turn an input blob into a list of id strings.

    `input_format`: "auto" | "json" | "csv" | "lines".
    `jq`: jmespath expression applied to JSON input.
    `col`: column name or 0-based index for CSV input.
    `skip_empty`: drop rows/lines where the selected value is empty. Default
    True (original id-extraction behavior); set False when running a second
    parallel pass (e.g. for per-row network) that must stay aligned with a
    primary pass that DID skip empties upstream.
    """
    fmt = detect_format(text) if input_format == "auto" else input_format
    if fmt == "json":
        return _extract_from_json(text, jq)
    if fmt == "csv":
        return _extract_from_csv(text, col, skip_empty=skip_empty)
    if fmt == "lines":
        return _extract_from_lines(text, skip_empty=skip_empty)
    raise ValueError(f"unknown input format: {fmt!r}")


def parse_input_with_network(
    text: str,
    *,
    input_format: str = "auto",
    jq: Optional[str] = None,
    col: Optional[str] = None,
    network_jq: Optional[str] = None,
    network_col: Optional[str] = None,
    default_network: Optional[str] = None,
) -> list[tuple[Optional[str], str]]:
    """Parse input as `[(network, id), ...]`.

    If neither `network_jq` nor `network_col` is set, the `default_network`
    is used for every row. Otherwise the network is extracted from the input
    in parallel to the id (two passes over the same input with different
    selectors); the two resulting lists must be the same length. Empty
    extracted values fall back to `default_network`.

    `network` is the preferred term; the generated REST API keeps calling
    this parameter `currency` for backward compatibility.
    """
    ids = parse_input(text, input_format=input_format, jq=jq, col=col)
    if network_jq is None and network_col is None:
        return [(default_network, i) for i in ids]

    # skip_empty=False keeps the network list aligned row-for-row with the
    # id list even when some network cells are blank (fallback handled below).
    networks = parse_input(
        text,
        input_format=input_format,
        jq=network_jq,
        col=network_col,
        skip_empty=False,
    )
    if len(networks) != len(ids):
        raise ValueError(
            f"network selector returned {len(networks)} values but "
            f"id selector returned {len(ids)}; selectors must be aligned"
        )
    return [(n or default_network, i) for n, i in zip(networks, ids)]


def _extract_from_json(text: str, jq: Optional[str]) -> list[str]:
    doc = json.loads(text)
    if jq:
        try:
            import jmespath  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - only hit without [cli]
            raise RuntimeError(
                "jmespath is required for --jq; install graphsense-python[cli]"
            ) from exc
        value = jmespath.search(jq, doc)
    else:
        value = doc
    return _coerce_to_str_list(value)


def _extract_from_csv(
    text: str, col: Optional[str], *, skip_empty: bool = True
) -> list[str]:
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return []
    header = rows[0]

    # Named --col: row 1 is the header, look the name up directly. This
    # avoids relying on `_looks_like_header` heuristics for the path that
    # users hit most often.
    if col is not None:
        try:
            idx = int(col)
        except ValueError:
            try:
                idx = header.index(col)
            except ValueError as exc:
                raise ValueError(
                    f"column {col!r} not found in header {header!r}"
                ) from exc
            return [_cell(r, idx) for r in rows[1:] if _row_keep(r, idx, skip_empty)]

        data_rows = rows[1:] if _looks_like_header(header) else rows
        return [_cell(r, idx) for r in data_rows if _row_keep(r, idx, skip_empty)]

    # No --col: only single-column input is unambiguous.
    has_header = _looks_like_header(header)
    if len(header) != 1:
        raise ValueError("CSV has multiple columns; specify --col <name_or_index>")
    data_rows = rows[1:] if has_header else rows
    return [_cell(r, 0) for r in data_rows if _row_keep(r, 0, skip_empty)]


def _cell(row: list[str], idx: int) -> str:
    return row[idx] if 0 <= idx < len(row) else ""


def _row_keep(row: list[str], idx: int, skip_empty: bool) -> bool:
    if skip_empty:
        return 0 <= idx < len(row) and row[idx].strip() != ""
    return True


def _looks_like_header(row: list[str]) -> bool:
    """Treat the first row as a header unless every cell looks like an id."""
    if not row:
        return False
    for cell in row:
        c = cell.strip()
        if not c:
            return False
        # Header cells: letters + underscores only, capped length. Digits
        # disqualify (would suggest an id like `1A1z…`, `0xabc…`, or a
        # numeric cluster id).
        stripped = c.replace("_", "")
        if not stripped.isalpha() or len(c) > 32:
            return False
    return True


def _extract_from_lines(text: str, *, skip_empty: bool = True) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not skip_empty:
            out.append(line)
            continue
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def _coerce_to_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float)):
        return [str(value)]
    if isinstance(value, dict):
        # Ambiguous; callers should use --jq to narrow down first.
        raise ValueError(
            "selector result is a dict, not a list of ids; use --jq to project a list"
        )
    if isinstance(value, Iterable):
        out: list[str] = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, (str, int, float)):
                out.append(str(item))
            elif isinstance(item, dict):
                raise ValueError(
                    "selector returned a list of dicts; "
                    "use --jq to project a scalar field (e.g. '[].address')"
                )
            else:
                out.append(str(item))
        return out
    return [str(value)]
