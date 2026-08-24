"""Return unused heap memory to the OS at phase boundaries.

A process under a hard memory cap (``docker run -m …``) is killed on **peak**
RSS across its whole lifetime, not on the live set at any instant, so
consecutive phases stack their high-water marks. Neither allocator in play
gives the pages back on its own:

* **pyarrow** allocates from Arrow's bundled mimalloc pool, which caches freed
  buffers and has no wall-clock purge. ``gc.collect()`` does not touch it;
  ``MemoryPool.release_unused()`` does.
* **Rust extensions** (deltalake) allocate through glibc malloc, which keeps a
  freed chunk on a free list unless it was ``mmap``-served or sits at the top of
  an arena. ``malloc_trim(0)`` releases the page-aligned free runs in every
  arena.

Prefer running independent phases as separate processes — process exit is the
most reliable reclaim there is. Use this where that is not practical.
"""

import ctypes
import gc
import logging
import os
import sys
from typing import Optional

logger = logging.getLogger(__name__)

_STATM = "/proc/self/statm"
_HAS_STATM = os.path.exists(_STATM)
_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


def _resolve_malloc_trim():
    """glibc's malloc_trim, or None on a libc without it (e.g. musl, macOS)."""
    libc = ctypes.CDLL(None)  # symbols already loaded into this process
    if not hasattr(libc, "malloc_trim"):
        return None
    fn = libc.malloc_trim
    fn.argtypes = [ctypes.c_size_t]
    fn.restype = ctypes.c_int
    return fn


_MALLOC_TRIM = _resolve_malloc_trim()


def current_rss_bytes() -> int:
    """Resident set size of this process; 0 where /proc is unavailable."""
    if not _HAS_STATM:
        return 0
    with open(_STATM) as f:
        return int(f.read().split()[1]) * _PAGE_SIZE


def release_unused_memory(context: Optional[str] = None) -> int:
    """Return unreachable heap memory to the OS. Returns the bytes freed.

    Costs milliseconds — call it at phase boundaries, not in a hot loop. When
    ``context`` names the boundary ("before compaction") the effect is logged at
    INFO, which is the only way it is visible in production.
    """
    before = current_rss_bytes()

    gc.collect()

    # Only when pyarrow is already imported: importing it here to drain a pool
    # that was never populated would cost far more than it frees.
    pyarrow = sys.modules.get("pyarrow")
    if pyarrow is not None:
        pyarrow.default_memory_pool().release_unused()

    if _MALLOC_TRIM is not None:
        _MALLOC_TRIM(0)

    after = current_rss_bytes()
    if context is not None:
        logger.info(
            f"Released unused memory {context}: "
            f"RSS {before / 1e6:.0f} MB -> {after / 1e6:.0f} MB "
            f"(freed {(before - after) / 1e6:.0f} MB)"
        )
    return before - after
