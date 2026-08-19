"""Decide between per-item calls and the /bulk endpoint."""

from __future__ import annotations

import csv
import io
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterable, Iterator, Optional

DEFAULT_BULK_THRESHOLD = 10

# The server bounds how many items one bulk request may carry (10,000 by
# default) and answers a longer list with a 400.
# Splitting below that bound keeps arbitrarily long key lists working from the
# client's side. Deployments that lower the server cap need a matching
# `chunk_size` on the call.
DEFAULT_BULK_CHUNK_SIZE = 5000


def should_bulk(
    n: int,
    *,
    threshold: int = DEFAULT_BULK_THRESHOLD,
    override: Optional[bool] = None,
) -> bool:
    """True if we should go through the bulk endpoint.

    `override=None`: use threshold.
    `override=True`: always bulk (even for small N).
    `override=False`: never bulk.
    """
    if override is not None:
        return override
    return n >= threshold


def run_parallel(
    call: Callable[[str], Any],
    keys: Iterable[str],
    *,
    max_workers: int = 8,
) -> list[Any]:
    """Call `call(k)` for each k in keys across a thread pool, preserving order."""
    items = list(keys)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(call, items))


def announce_switch(stream=None) -> None:
    """Emit a one-line stderr notice the first time we switch to bulk.

    Caller must implement the "first time" logic — this just prints.
    """
    out = stream if stream is not None else sys.stderr
    print(
        "notice: switching to bulk endpoint "
        "(rows are flat; per-item typed models are available with --no-bulk)",
        file=out,
    )


def chunked(items: Iterable[Any], size: int) -> Iterator[list]:
    """Yield consecutive lists of at most `size` items."""
    if size <= 0:
        raise ValueError("chunk size must be positive")
    batch: list = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def merge_json_chunks(chunks: Iterable[Any]) -> list:
    """Concatenate the row lists returned by several bulk.json calls."""
    rows: list = []
    for chunk in chunks:
        if chunk is None:
            continue
        rows.extend(chunk if isinstance(chunk, list) else [chunk])
    return rows


def merge_csv_chunks(chunks: Iterable[str]) -> str:
    """Concatenate several bulk.csv responses into one CSV document.

    Each response carries its own header, and the server infers those columns
    from the first rows of that request — so two chunks of the same query can
    legitimately disagree on the column set. Rows are therefore re-keyed onto
    the union of all headers (in first-seen order) rather than pasted together.
    """
    rows: list[dict] = []
    columns: list[str] = []
    seen = set()
    for chunk in chunks:
        if not chunk or not chunk.strip():
            continue
        reader = csv.DictReader(io.StringIO(chunk))
        for name in reader.fieldnames or []:
            if name not in seen:
                seen.add(name)
                columns.append(name)
        rows.extend(reader)

    if not columns:
        return ""

    out = io.StringIO()
    writer = csv.DictWriter(out, columns, restval="", extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return out.getvalue()
