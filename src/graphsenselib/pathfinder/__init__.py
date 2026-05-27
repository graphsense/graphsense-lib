"""Pathfinder-domain logic that is independent of any specific transport
or framework — usable by the MCP build tool, by CLI scripts, and by the
python client (which can write a thin backend adapter against the
:class:`GraphsenseBackend` Protocol).

Two verification surfaces:

* :func:`verify_structural` — sync, no backend, checks in-spec
  consistency (orphan txs, dangling endpoints, stray addresses, …).
* :func:`verify_against_backend` — async, takes a
  :class:`GraphsenseBackend`, additionally cross-checks the spec
  against on-chain reality (does the tx exist; does it mediate the
  claimed edge; does the address exist).

Use :func:`verify_structural` alone when you only want internal
sanity-checking (cheap, no I/O); use both when a backend is available
and you want to surface semantic mistakes too.
"""

from graphsenselib.pathfinder.verify_backend import (
    GraphsenseBackend,
    RestBackend,
    verify_against_backend,
)
from graphsenselib.pathfinder.verify_structural import verify_structural

__all__ = [
    "GraphsenseBackend",
    "RestBackend",
    "verify_against_backend",
    "verify_structural",
]
