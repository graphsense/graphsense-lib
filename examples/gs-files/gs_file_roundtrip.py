# /// script
# requires-python = ">=3.10,<3.14"
# dependencies = [
#     "graphsense-lib==2.15.1",
# ]
# ///
# ruff: noqa: T201
"""Encode and decode a GraphSense Pathfinder ``.gs`` save file.

Run it standalone — the PEP 723 header above pins the library, so uv
installs it into a throwaway environment; no checkout needed::

    uv run --script gs_file_roundtrip.py [out.gs]

Or, from a graphsense-lib checkout, against the working tree::

    uv run python examples/gs-files/gs_file_roundtrip.py [out.gs]

The client-side counterpart, using graphsense-python instead of
graphsense-lib, is ``gs_file_roundtrip_client.py`` next to this file.

Versions
--------
Verified 2026-08-04 against graphsense-lib 2.15.1 (current stable, the
pin above) and 2.16.0.dev31+g6ea1e6694 (develop). Byte-identical output
on both. Per-API minimums, if you pin something older:

* ``GsBuilder`` / the ``.gs`` codec — since 2.13.0.
* ``apply_hierarchical_layout`` — since 2.13.4, but 2.13.5 reworked it
  (BFS tidy-tree, row pitch 3.0 → 2.5, multi-tx de-overlap), so only
  **>= 2.13.5** places this graph at the coordinates printed below.
* ``verify_structural`` — since 2.13.5. On 2.13.5–2.15.1 it emits one
  *spurious* "tx_ids references tx hash(es) not in `txs`" warning for
  the string shorthand used here (``"txs": ["e67a0550…"]``); the check
  is warning-only, so the file is written correctly regardless. Fixed
  in 2.15.2 — bump the pin to ``>=2.15.2`` for a clean run.

What it shows, in order:

1. build the *graph data* — the plain dict (addresses, txs,
   aggregated edges) that both the CLI and the MCP ``build_pathfinder_file``
   tool accept;
2. sanity-check it with :func:`verify_structural`;
3. run the standard layout, :func:`apply_hierarchical_layout`, so the file
   opens with sensible coordinates instead of everything at (0, 0);
4. encode it to ``.gs`` bytes via :func:`builder_from_spec`;
5. decode the file back to typed dataclasses and summarize it;
6. assert the round-trip is lossless.

The same thing from the shell (the CLI has no layout step — it always uses
the columnar ``GsBuilder`` defaults)::

    graphsense-cli convert gs-files encode -i graph.json -o out.gs --verify
    graphsense-cli convert gs-files decode out.gs --format structured
    graphsense-cli convert gs-files summary out.gs
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from graphsenselib.convert.gs_files import (
    PathfinderData,
    apply_hierarchical_layout,
    builder_from_spec,
    decode_gs,
    structure,
    summarize,
)
from graphsenselib.pathfinder import verify_structural

# ---------------------------------------------------------------------------
# 1. The graph data
# ---------------------------------------------------------------------------
#
# Four rules worth knowing before you write one:
#
# * **The data has to be real.** A ``.gs`` file is a set of references,
#   not a self-contained graph: the pathfinder UI resolves every address
#   and tx against the backend when the file is opened. An address that
#   doesn't exist, or an edge whose tx doesn't actually have both
#   endpoints in its inputs/outputs, produces a file that opens but shows
#   unconnected nodes. Invented ids are the number-one way to get a
#   "broken" file — see the verification step in `main` below.
# * A transaction only renders as a node if it is listed in ``txs`` AND
#   referenced from some ``agg_edges[].tx_ids``. Listing it in just one of
#   the two gives you a dangling node or an abstract edge.
# * ``starting_point: true`` marks the anchors of the investigation. At
#   least one anchor is what makes the hierarchical layout meaningful —
#   columns are hop distances from the anchors.
# * ``color`` is RGBA, each component in [0, 1] — not 0..255.
#
# The graph below is a real BTC flow, pulled from the GraphSense API:
# two addresses co-spend into tx 756a95ba…, whose single output funds
# 1P6ZvBft…, which two blocks-of-months later is one of 27 inputs to
# tx e67a0550… that consolidates ~79 956 BTC into 1FeexV6b…

NETWORK = "btc"

_ANCHOR = "14QK3yVfakMHD2W5oect54AtCez77wJgGf"
_CO_SPENDER = "15Lv7zkEtTfTeBvUB9Py7BZQoKKBpFQLsn"
_MERGE = "1P6ZvBftEqhrvqWSWicWRHrbTeSKLFzJMy"
_CONSOLIDATION = "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF"

# Height 89064 — inputs {_ANCHOR, _CO_SPENDER}, single output _MERGE.
_TX_MERGE = "756a95ba337d5dab4ee32fb46071e6cdcd78a6dd3970f025b25daae7c67298e5"
# Height 111194 — 27 inputs (one of them _MERGE), output[0] _CONSOLIDATION.
_TX_CONSOLIDATE = "e67a0550848b7932d7796aeea16ab0e48a5cfe81c4e8cca2c5b03e0416850114"

DATA: dict[str, Any] = {
    "addresses": [
        {
            "id": _ANCHOR,
            # Labels are for case-specific context. Don't restate
            # attribution tags — the UI already renders those on the node.
            "label": "entry point",
            "starting_point": True,
            "color": [0.9, 0.3, 0.2, 1.0],
        },
        {"id": _CO_SPENDER, "label": "co-spender"},
        {"id": _MERGE, "label": "merge output"},
        {"id": _CONSOLIDATION, "label": "consolidation"},
    ],
    "txs": [_TX_MERGE, _TX_CONSOLIDATE],
    "agg_edges": [
        {"a": _ANCHOR, "b": _MERGE, "tx_ids": [_TX_MERGE]},
        # Same tx, second input: the co-spender. One tx mediating two
        # edges is the common case, and the layout handles it by snapping
        # the tx to the mean y of all its endpoints.
        {"a": _CO_SPENDER, "b": _MERGE, "tx_ids": [_TX_MERGE]},
        {"a": _MERGE, "b": _CONSOLIDATION, "tx_ids": [_TX_CONSOLIDATE]},
    ],
}


def encode(data: dict[str, Any], out_path: Path) -> Path:
    """Lay out the graph data and write it as a ``.gs`` file."""
    # The standard layout: multi-source BFS from every ``starting_point``,
    # one column per hop, txs snapped onto the line between their
    # endpoints. It returns a *copy* of the data with x/y stamped on every
    # node; caller-supplied x/y are preserved verbatim.
    #
    # Skip this call and you get GsBuilder's columnar fallback instead
    # (addresses in one column, txs in another) — fine for a flat list of
    # nodes, wrong for anything with a path structure.
    laid_out = apply_hierarchical_layout(data)

    builder = builder_from_spec(
        laid_out,
        name="example investigation",
        default_network=NETWORK,
    )
    return builder.write(out_path)


def decode(path: Path) -> PathfinderData:
    """Read a ``.gs`` file back into typed dataclasses."""
    raw = decode_gs(path)  # bytes -> raw JSON payload (a list)
    data = structure(raw)  # raw JSON -> PathfinderData | GraphData
    if not isinstance(data, PathfinderData):
        raise TypeError(f"expected a pathfinder file, got {type(data).__name__}")
    return data


def main(argv: list[str]) -> int:
    out_path = Path(argv[1]) if len(argv) > 1 else Path("example.gs")

    # Cheap, no I/O: catches internal inconsistencies that yield a valid
    # file with an empty or broken-looking graph — orphan txs, edges with
    # no tx_ids, endpoints missing from `addresses`, duplicates.
    #
    # What it does NOT check is whether the ids are real: data that is
    # internally consistent but wrong on chain (nonexistent address, or
    # an edge whose tx doesn't actually connect a to b) is the failure
    # mode that opens as unconnected nodes in the UI. That needs the
    # second, backend-aware verifier — not run here, since it needs a
    # REST endpoint, but this is the whole invocation::
    #
    #     import asyncio, httpx
    #     from graphsenselib.pathfinder import (
    #         RestBackend, verify_against_backend,
    #     )
    #
    #     async def check(data):
    #         async with httpx.AsyncClient(
    #             base_url="https://api.iknaio.com",
    #             headers={"Authorization": "<api-key>"},
    #         ) as client:
    #             return await verify_against_backend(
    #                 data,
    #                 default_network=NETWORK,
    #                 backend=RestBackend(client),
    #             )
    #
    #     warnings += asyncio.run(check(DATA))
    #
    # It asks the backend whether each address exists and whether each
    # tx really mediates the edge that claims it, at most
    # `max_concurrency` (default 8) requests in flight.
    warnings = verify_structural(DATA)
    print("structural warnings:", warnings or "none")

    written = encode(DATA, out_path)
    print(f"wrote {written} ({written.stat().st_size} bytes)\n")

    decoded = decode(written)

    print("summary:")
    for key, value in summarize(decoded).items():
        print(f"  {key}: {value}")

    # Annotations carry the labels and colors, keyed by the node's
    # (currency, id) — the decoder calls the network field `currency`.
    labels = {(a.id.currency, a.id.id): a.label for a in decoded.annotations}

    print("\naddresses (x, y from the hierarchical layout):")
    for addr in decoded.addresses:
        label = labels.get((addr.id.currency, addr.id.id), "")
        anchor = " [anchor]" if addr.is_starting_point else ""
        print(f"  {addr.x:6.1f} {addr.y:6.1f}  {addr.id.id[:16]}…  {label}{anchor}")

    print("\ntxs:")
    for tx in decoded.txs:
        print(f"  {tx.x:6.1f} {tx.y:6.1f}  {tx.id.id[:16]}…")

    print("\nagg edges:")
    for edge in decoded.agg_edges:
        print(f"  {edge.a.id[:12]}… -> {edge.b.id[:12]}…  ({len(edge.txs)} tx)")

    # Round-trip check: re-encoding the decoded graph must reproduce the
    # exact same bytes, so nothing was lost on either leg.
    assert encode(DATA, out_path).read_bytes() == written.read_bytes()
    print("\nround-trip ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
