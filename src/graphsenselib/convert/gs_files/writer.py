"""Serialize decoded `.gs` data to JSON (stdout or file)."""

from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path
from typing import Any, Literal

from .parser import GraphData, PathfinderData

Format = Literal["raw", "structured", "both"]


def to_jsonable(obj: Any) -> Any:
    """Recursively convert dataclasses into JSON-compatible structures."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: to_jsonable(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj


def write_json(obj: Any, path: Path | None, indent: int | None) -> None:
    text = json.dumps(obj, indent=indent, ensure_ascii=False)
    if path is None:
        sys.stdout.write(text + "\n")
    else:
        path.write_text(text, encoding="utf-8")


def _resolve_paths(output: Path | None, fmt: Format) -> tuple[Path | None, Path | None]:
    """Return (raw_path, structured_path) based on --output and --format.

    - output=None              -> both stdout
    - fmt=raw                  -> (output, None)
    - fmt=structured           -> (None, output)
    - fmt=both with output     -> (output.raw.json, output.structured.json)
    """
    if output is None:
        return None, None
    if fmt == "raw":
        return output, None
    if fmt == "structured":
        return None, output
    return output.with_suffix(".raw.json"), output.with_suffix(".structured.json")


def write_decoded(
    raw: Any,
    structured: PathfinderData | GraphData | None,
    fmt: Format,
    output: Path | None,
    indent: int | None,
) -> None:
    """Write the decoded payload according to `fmt`.

    For fmt="structured" or "both", `structured` must be provided.
    """
    raw_path, structured_path = _resolve_paths(output, fmt)
    if fmt in ("raw", "both"):
        write_json(raw, raw_path, indent)
    if fmt in ("structured", "both"):
        if structured is None:
            raise ValueError("structured payload required for fmt=structured|both")
        write_json(to_jsonable(structured), structured_path, indent)
