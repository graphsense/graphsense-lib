"""Decide between per-item calls and the /bulk endpoint."""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterable, Optional

DEFAULT_BULK_THRESHOLD = 10


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
