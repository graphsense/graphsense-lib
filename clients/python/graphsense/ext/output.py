"""Output writers: JSON / JSONL / CSV -> stdout, file (-o) or directory (-d)."""

from __future__ import annotations

import csv
import inspect
import json
import os
import sys
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Optional

from graphsense.ext.client import _model_to_dict


def should_colorize(stream: Any, *, override: Optional[str] = None) -> bool:
    """Decide whether to emit ANSI color codes.

    `override`: None/'auto' → TTY + NO_COLOR check. 'always' → force on.
    'never' → force off. Also honors the standard `NO_COLOR` env var and
    `CLICK_COLOR=0`/'1'.
    """
    mode = (override or "auto").lower()
    if mode == "never":
        return False
    if mode == "always":
        return True
    # auto
    if os.environ.get("NO_COLOR"):
        return False
    click_color = os.environ.get("CLICK_COLOR")
    if click_color == "0":
        return False
    if click_color == "1":
        return True
    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


def colorize_json(text: str) -> str:
    """Syntax-highlight a JSON blob. No-op if pygments is unavailable."""
    try:
        from pygments import highlight

        # pygments loads lexers/formatters dynamically; static analyzers
        # like ty can't see these members without runtime introspection.
        from pygments.formatters import TerminalFormatter  # ty: ignore[unresolved-import]
        from pygments.lexers import JsonLexer  # ty: ignore[unresolved-import]
    except ImportError:  # pragma: no cover - only without [cli] extra
        return text
    # highlight() appends a trailing newline; strip so we control spacing.
    return highlight(text, JsonLexer(), TerminalFormatter()).rstrip("\n")


def infer_format_from_path(path: str) -> Optional[str]:
    lower = path.lower()
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return "jsonl"
    if lower.endswith(".json"):
        return "json"
    if lower.endswith(".csv"):
        return "csv"
    return None


def resolve_format(
    *, explicit: Optional[str], output_path: Optional[str], is_list: bool
) -> str:
    """Pick an output format.

    Precedence: explicit `--format` > extension of `-o PATH` > default.
    Default is `json` for single records and `jsonl` for list/stream output.
    """
    if explicit:
        return explicit
    if output_path:
        inferred = infer_format_from_path(output_path)
        if inferred:
            return inferred
    return "jsonl" if is_list else "json"


@contextmanager
def open_out(path: Optional[str], *, mode: str = "w") -> Iterator[Any]:
    """Yield a writable text stream — stdout, or an opened file."""
    if path is None or path == "-":
        yield sys.stdout
    else:
        with open(path, mode, encoding="utf-8", newline="") as fh:
            yield fh


def write(
    records: Any,
    *,
    output: Optional[str] = None,
    directory: Optional[str] = None,
    format: Optional[str] = None,
    id_key: str = "address",
    color: Optional[str] = None,
) -> None:
    """Top-level writer entry point.

    * If `directory` is set, write one file per record named `<id_key>.json`.
    * Else, stream records to `output` (path) or stdout in the chosen format.

    `color`: 'auto' (default), 'always', or 'never'. Colors are only applied
    when writing JSON/JSONL to a TTY stdout — never to files, never to CSV.
    """
    is_list = _is_gen_like(records)
    if directory is not None:
        os.makedirs(directory, exist_ok=True)
        records_iter = records if is_list else [records]
        for rec in records_iter:
            rec_d = _model_to_dict(rec) or {}
            id_val = _pick_id(rec_d, id_key)
            path = os.path.join(directory, f"{id_val}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(rec_d, fh, indent=2, default=_json_default)
                fh.write("\n")
        return

    fmt = resolve_format(explicit=format, output_path=output, is_list=is_list)

    # Colors only when writing JSON/JSONL to stdout. File output and CSV
    # never get ANSI codes (would break spreadsheets / re-parsers).
    apply_color = (
        output is None
        and fmt in ("json", "jsonl")
        and should_colorize(sys.stdout, override=color)
    )

    if fmt == "json":
        data = records
        if is_list:
            data = [_model_to_dict(r) for r in records]
        else:
            data = _model_to_dict(records)
        serialized = json.dumps(data, indent=2, default=_json_default)
        if apply_color:
            serialized = colorize_json(serialized)
        with open_out(output) as fh:
            fh.write(serialized)
            fh.write("\n")
    elif fmt == "jsonl":
        with open_out(output) as fh:
            if is_list:
                for rec in records:
                    line = json.dumps(_model_to_dict(rec), default=_json_default)
                    if apply_color:
                        line = colorize_json(line)
                    fh.write(line)
                    fh.write("\n")
                    fh.flush()
            else:
                line = json.dumps(_model_to_dict(records), default=_json_default)
                if apply_color:
                    line = colorize_json(line)
                fh.write(line)
                fh.write("\n")
    elif fmt == "csv":
        _write_csv(records if is_list else [records], output=output)
    else:
        raise ValueError(f"unknown output format: {fmt!r}")


def _pick_id(rec: dict[str, Any], key: str) -> str:
    if key in rec and rec[key] not in (None, ""):
        return str(rec[key])
    # fallback: first scalar value
    for v in rec.values():
        if isinstance(v, (str, int)):
            return str(v)
    return "record"


def _is_gen_like(obj: Any) -> bool:
    """True iff `obj` is a concrete sequence of records we should stream.

    Only actual lists/tuples and generators qualify. Pydantic models and dicts
    expose __iter__ but represent a single record, not a list of them —
    classifying them as list-like would spread their fields into (key, value)
    tuples per line in jsonl mode (which is exactly the Stats bug).
    """
    return isinstance(obj, (list, tuple)) or inspect.isgenerator(obj)


def _json_default(o: Any) -> Any:  # pragma: no cover - trivial
    if hasattr(o, "to_dict"):
        return o.to_dict()
    raise TypeError(f"not JSON serializable: {type(o)!r}")


def _write_csv(records: Iterable[Any], *, output: Optional[str]) -> None:
    """Write CSV with nested dicts flattened using dotted keys.

    Columns are determined from the first record's flattened keys, in insertion
    order. Additional keys seen in later records are appended after the
    initial set.
    """
    peek = iter(records)
    try:
        first = next(peek)
    except StopIteration:
        with open_out(output) as fh:
            fh.write("")
        return

    flat_first = _flatten(_model_to_dict(first) or {})
    fieldnames: list[str] = list(flat_first.keys())

    with open_out(output) as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerow({k: _csv_cell(v) for k, v in flat_first.items()})
        fh.flush()
        for rec in peek:
            flat = _flatten(_model_to_dict(rec) or {})
            for k in flat.keys():
                if k not in fieldnames:
                    fieldnames.append(k)
            writer.writerow({k: _csv_cell(v) for k, v in flat.items()})
            fh.flush()


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                out.update(_flatten(v, key))
            else:
                out[key] = v
    else:
        out[prefix or "value"] = obj
    return out


def _csv_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return json.dumps(v, default=_json_default)
    return str(v)
