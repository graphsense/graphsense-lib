"""MCP tool: build a Pathfinder ``.gs`` save file from an agent's
investigation findings.

An investigation agent that has used ``lookup_address``, ``list_neighbors``,
``list_txs_for`` etc. accumulates a graph of addresses, transactions, and
their relationships. This tool takes that accumulated graph as a spec and
returns the bytes of a ``.gs`` file the user can open in the pathfinder UI
to verify the agent's findings.

The spec shape mirrors what :func:`builder_from_spec` already accepts, so
the agent's mental model maps 1:1 onto the tool surface.

Layout policy: pathfinder ``.gs`` files commit coordinates at save time
(the UI normally generates them on the fly), so this tool has to pick a
layout. By default it uses :func:`apply_hierarchical_layout` whenever the
spec marks at least one node ``starting_point=true`` (the typical agent
case — anchors are known), and falls back to the existing columnar
defaults in :class:`GsBuilder` otherwise.
"""

from __future__ import annotations

import logging
import re
from contextlib import AsyncExitStack
from typing import Any, Literal, Optional

from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_request
from fastmcp.tools.base import ToolResult
from fastmcp.utilities.types import File
from pydantic import BaseModel, ConfigDict, Field

from graphsenselib.convert.gs_files import (
    apply_hierarchical_layout,
    builder_from_spec,
)
from graphsenselib.web.file_store import FileTooLargeError

logger = logging.getLogger(__name__)

# Mirrored from tools/consolidated.py — same conservative guards on
# user-controlled identifiers (currency tickers, address ids, tx hashes)
# so a malformed spec is rejected at the boundary instead of producing
# an unopenable .gs.
_CURRENCY_PATTERN = re.compile(r"^[a-z0-9]{2,10}$")
_ID_PATTERN = re.compile(r"^[a-zA-Z0-9]{1,150}$")
# Filenames are derived from the user-supplied graph name; keep the
# derived basename safe for any OS the client might save to.
_FILENAME_SAFE = re.compile(r"[^A-Za-z0-9._-]+")

Color = tuple[float, float, float, float]


class _AddressSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(description="Address identifier on the given network.")
    network: Optional[str] = Field(
        default=None,
        description="Network ticker (e.g. 'btc', 'eth'). Defaults to default_network.",
    )
    label: Optional[str] = Field(
        default=None,
        description=(
            "Free-text label shown next to the node. Do NOT restate "
            "attribution tags here — the UI already renders an address's "
            "tags on the node. Reserve the label for case-specific context "
            "that is not derivable from tags, e.g. the role of the address "
            "in the investigation ('victim wallet', 'attacker cash-out', "
            "'first hop after the hack')."
        ),
    )
    color: Optional[Color] = Field(
        default=None, description="RGBA tuple, each component in [0, 1]."
    )
    starting_point: bool = Field(
        default=False,
        description=(
            "Anchor this address. Setting at least one anchor opts the spec into "
            "the BFS hierarchical layout (column per hop)."
        ),
    )
    side: Optional[Literal["input", "output", "left", "right"]] = Field(
        default=None,
        description=(
            "Hint for the columnar layout only (ignored by hierarchical). "
            "'input'/'left' pin x=-8, 'output'/'right' pin x=+8."
        ),
    )
    x: Optional[float] = Field(
        default=None, description="Override x coordinate (in graph units)."
    )
    y: Optional[float] = Field(
        default=None, description="Override y coordinate (in graph units)."
    )


class _TxSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(description="Transaction hash on the given network.")
    network: Optional[str] = Field(default=None)
    index: int = Field(
        default=0, description="Within-block index; almost always 0 for one-off txs."
    )
    label: Optional[str] = Field(
        default=None,
        description=(
            "Free-text label shown next to the transaction node. Do NOT "
            "restate the transaction's date or value here — the UI already "
            "renders both. Reserve the label for case-specific context not "
            "visible from the transaction data itself."
        ),
    )
    color: Optional[Color] = Field(default=None)
    starting_point: bool = Field(default=False)
    x: Optional[float] = Field(default=None)
    y: Optional[float] = Field(default=None)


class _EdgeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    a: str = Field(description="Address id of one endpoint.")
    b: str = Field(description="Address id of the other endpoint.")
    tx_ids: Optional[list[str]] = Field(
        default=None,
        description=(
            "Transaction hashes mediating the a↔b relationship. The hashes "
            "listed here must also appear in the top-level `txs` list — that "
            "is how pathfinder links an edge to the actual transactions. If "
            "omitted, the edge renders as an abstract a↔b line and NO "
            "transactions appear for it, even if you populated `txs`."
        ),
    )
    network: Optional[str] = Field(default=None)
    a_network: Optional[str] = Field(default=None)
    b_network: Optional[str] = Field(default=None)


class PathfinderSpec(BaseModel):
    """Graph the agent wants encoded into a ``.gs`` file."""

    model_config = ConfigDict(extra="forbid")

    addresses: list[_AddressSpec] = Field(
        default_factory=list,
        description="All address nodes that should appear in pathfinder.",
    )
    txs: list[_TxSpec] = Field(
        default_factory=list,
        description=(
            "Transaction nodes. To make a transaction appear in pathfinder "
            "you must BOTH list it here AND reference its hash from "
            "`agg_edges.tx_ids`. Leaving this empty while providing "
            "`agg_edges` yields a graph of abstract a↔b lines with no "
            "transactions."
        ),
    )
    agg_edges: list[_EdgeSpec] = Field(
        default_factory=list,
        description="Aggregated address↔address relationships, optionally tx-mediated.",
    )


def _validate_currency(currency: str, *, field_name: str = "network") -> None:
    if not _CURRENCY_PATTERN.match(currency):
        raise ToolError(f"Invalid {field_name} identifier: {currency!r}")


def _validate_id(name: str, value: str) -> None:
    if not _ID_PATTERN.match(value):
        raise ToolError(f"Invalid {name}: {value!r}")


def _validate_spec(spec: PathfinderSpec, default_network: str) -> None:
    _validate_currency(default_network, field_name="default_network")
    for a in spec.addresses:
        _validate_id("address id", a.id)
        if a.network is not None:
            _validate_currency(a.network)
    for t in spec.txs:
        _validate_id("tx hash", t.id)
        if t.network is not None:
            _validate_currency(t.network)
    for e in spec.agg_edges:
        _validate_id("agg_edge.a", e.a)
        _validate_id("agg_edge.b", e.b)
        for tid in e.tx_ids or []:
            _validate_id("agg_edge.tx_ids[]", tid)
        for net_field, net in (
            ("network", e.network),
            ("a_network", e.a_network),
            ("b_network", e.b_network),
        ):
            if net is not None:
                _validate_currency(net, field_name=f"agg_edge.{net_field}")


def _spec_to_dict(spec: PathfinderSpec) -> dict[str, Any]:
    return spec.model_dump(exclude_none=True)


def _safe_filename(name: str) -> str:
    base = _FILENAME_SAFE.sub("_", name).strip("._-") or "pathfinder"
    return f"{base[:80]}.gs"


def _should_use_hierarchical(spec: PathfinderSpec) -> bool:
    return any(a.starting_point for a in spec.addresses) or any(
        t.starting_point for t in spec.txs
    )


# Cap how many unknown-ref ids we list in a single warning so a
# malformed spec can't bloat the response. The truncation suffix tells
# the agent how many more there were.
_WARNING_REF_LIMIT = 10


def _collect_warnings(spec: PathfinderSpec) -> list[str]:
    """Surface common spec shapes that produce a syntactically valid .gs
    file but visually wrong / empty pathfinder output.

    These are warnings, not errors: sometimes the agent really does want
    an abstract relationship graph with no txs. But silently shipping
    a "no transactions appear" file (the historical failure mode) is
    worse than telling the agent up front so it can fix and retry.
    """
    warnings: list[str] = []
    address_ids = {a.id for a in spec.addresses}
    tx_ids = {t.id for t in spec.txs}

    if spec.agg_edges and not spec.txs:
        warnings.append(
            f"spec has {len(spec.agg_edges)} agg_edge(s) but no txs were "
            "provided; pathfinder will show abstract address-to-address "
            "links only (no transactions render). Populate `txs` and "
            "reference the hashes from `agg_edges.tx_ids` to make "
            "transactions appear."
        )

    edges_without_tx_ids = sum(1 for e in spec.agg_edges if not e.tx_ids)
    if edges_without_tx_ids and spec.txs:
        warnings.append(
            f"{edges_without_tx_ids} of {len(spec.agg_edges)} agg_edge(s) "
            "have no tx_ids; those edges will render as abstract a↔b lines "
            "and the txs you provided will not be tied to them."
        )

    unknown_tx: list[str] = []
    seen_tx: set[str] = set()
    for e in spec.agg_edges:
        for tid in e.tx_ids or []:
            if tid not in tx_ids and tid not in seen_tx:
                unknown_tx.append(tid)
                seen_tx.add(tid)
    if unknown_tx:
        shown = unknown_tx[:_WARNING_REF_LIMIT]
        more = len(unknown_tx) - len(shown)
        suffix = f" (+{more} more)" if more > 0 else ""
        warnings.append(
            "agg_edge.tx_ids references tx hash(es) not in `txs`: "
            f"{', '.join(shown)}{suffix}. Add them to `txs` or remove the "
            "references."
        )

    unknown_addr: list[str] = []
    seen_addr: set[str] = set()
    for e in spec.agg_edges:
        for endpoint in (e.a, e.b):
            if endpoint not in address_ids and endpoint not in seen_addr:
                unknown_addr.append(endpoint)
                seen_addr.add(endpoint)
    if unknown_addr:
        shown = unknown_addr[:_WARNING_REF_LIMIT]
        more = len(unknown_addr) - len(shown)
        suffix = f" (+{more} more)" if more > 0 else ""
        warnings.append(
            "agg_edge endpoints reference address(es) not in `addresses`: "
            f"{', '.join(shown)}{suffix}. Add them to `addresses` or fix the "
            "typo."
        )

    return warnings


class _PathfinderBuildSummary(BaseModel):
    """Metadata about a built .gs file. Travels in `structured_content`
    (i.e. the LLM-visible part of the response); the actual file bytes
    travel separately as an embedded resource so they bypass the model
    context."""

    n_addresses: int
    n_txs: int
    n_agg_edges: int
    layout: Literal["hierarchical", "columnar"]
    byte_size: int
    warnings: list[str]


class _PathfinderBuildResult(BaseModel):
    filename: str
    download_url: Optional[str] = None
    summary: _PathfinderBuildSummary


def register(mcp: FastMCP, app: FastAPI, stack: AsyncExitStack) -> None:  # noqa: ARG001
    """Register the build_pathfinder_file tool on the given MCP instance.

    Matches the (mcp, app, stack) signature of other consolidated tools
    even though this one needs neither the FastAPI app (no REST fan-out)
    nor the exit stack (no httpx client lifecycle).
    """

    @mcp.tool(tags={"gs_pathfinder", "gs_export"})
    async def build_pathfinder_file(
        name: str,
        default_network: str,
        spec: PathfinderSpec,
        layout: Literal["auto", "hierarchical", "columnar"] = "auto",
    ) -> ToolResult:
        """Build a Pathfinder .gs save file from an investigation graph.

        Pass the addresses, transactions, and address↔address relationships
        you discovered. The tool encodes them into a Pathfinder ``.gs``
        file; the user opens that file in the Pathfinder UI to verify your
        findings visually.

        YOU DO NOT RECEIVE THE FILE CONTENT. The ``.gs`` payload is binary
        and is deliberately kept out of your context. Do not try to read,
        decode, reconstruct, or base64-encode it, and never invent a
        download link or a ``data:`` URL of your own.

        Once the tool succeeds, tell the user where their file is:

        * If the result has a non-null ``download_url``, give that link to
          the user verbatim — it is a real, time-limited download link.
        * Otherwise the file is delivered as an attachment embedded in this
          tool's result; tell the user to open or save it from their
          client (the file is named ``filename``).

        Either way, surface any ``summary.warnings`` to the user.

        IMPORTANT — how transactions render in pathfinder: to make a
        transaction appear you must do TWO things. (1) list it as an
        entry in ``txs`` and (2) reference its hash from
        ``agg_edges.tx_ids`` on the edge(s) it mediates. An ``agg_edge``
        without ``tx_ids`` becomes an abstract a↔b line and no
        transactions are shown for it. If you provide ``agg_edges`` but
        leave ``txs`` empty, the response includes a warning and the
        resulting .gs renders only abstract relationship lines.

        Labels — keep ``label`` (on addresses and txs) for case context
        the UI cannot already show. Pathfinder renders attribution tags
        on address nodes, and the date and value on transaction nodes,
        by itself — so do NOT copy tag names, exchange names, dates or
        amounts into ``label``; that is redundant. Reserve ``label`` for
        context that comes from the investigation itself and is not
        derivable from tags or transaction data, e.g. "victim wallet",
        "attacker cash-out", "first hop after the hack".

        Mark the address(es) or tx(s) you started from with
        ``starting_point=true`` so the layout can place anchors at column
        0 and arrange the rest by hop distance.

        Example (a single tx between two addresses, anchored at addrA)::

            {
              "addresses": [
                {"id": "addrA", "starting_point": true, "label": "anchor"},
                {"id": "addrB"}
              ],
              "txs": [{"id": "txhash1"}],
              "agg_edges": [
                {"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]}
              ]
            }

        Args:
            name: Graph name embedded in the .gs file (shown in the UI).
            default_network: Network ticker for items that don't carry an
                explicit network (e.g. "btc", "eth").
            spec: Addresses, txs, and aggregated edges of the graph.
            layout: "auto" (default) picks hierarchical when at least one
                starting_point is set, else columnar. "hierarchical" forces
                BFS-by-hop layout; "columnar" forces the GsBuilder default
                (addresses, txs, side-aware columns).

        Returns:
            A tool result whose structured content carries
            ``{filename, download_url, summary}`` — this, and only this,
            is what you can read. ``download_url`` is either a real,
            time-limited download link or null (null when the server has
            no file store configured, or could not address the link); the
            file is then delivered as an embedded attachment instead.
            Inspect ``summary.warnings`` (it flags common authoring
            mistakes) and mention any to the user. The .gs bytes travel
            as an embedded MCP resource and/or via the download link;
            they never enter your context, so do not expect to access or
            relay the file's contents.
        """
        _validate_spec(spec, default_network)

        chosen = layout
        if chosen == "auto":
            chosen = "hierarchical" if _should_use_hierarchical(spec) else "columnar"

        spec_dict = _spec_to_dict(spec)
        if chosen == "hierarchical":
            spec_dict = apply_hierarchical_layout(spec_dict)

        try:
            builder = builder_from_spec(
                spec_dict, name=name, default_network=default_network
            )
            payload = builder.to_bytes()
        except ValueError as exc:
            # builder_from_spec raises ValueError for malformed colors etc;
            # surface that as a ToolError so the agent gets a clean message.
            raise ToolError(f"invalid spec: {exc}") from exc

        filename = _safe_filename(name)

        # Optional download link. When the web app has a file store
        # configured, stash the payload and hand back an unguessable,
        # time-limited URL — the channel weak MCP hosts can still use even
        # when they drop embedded resources. The size cap lives in the
        # store; exceeding it is a hard error.
        download_url: Optional[str] = None
        store = getattr(app.state, "file_store", None)
        if store is not None:
            try:
                token = await store.put(
                    payload,
                    filename=filename,
                    content_type="application/octet-stream",
                )
            except FileTooLargeError as exc:
                raise ToolError(
                    "the built Pathfinder file is too large to share "
                    f"({exc}); reduce the number of addresses, transactions "
                    "and edges in the spec, then retry"
                ) from exc
            try:
                download_url = store.url_for(get_http_request(), token)
            except Exception:
                # The file is stored; we just could not derive an absolute
                # URL (e.g. no HTTP request context). Never let link
                # building sink the tool — fall through and embed instead.
                logger.warning(
                    "build_pathfinder_file: could not build a download URL; "
                    "delivering the embedded resource only",
                    exc_info=True,
                )
                download_url = None

        # Embedded-resource delivery: included when the server is
        # configured to (file_store_embed_resource), or whenever there is
        # no download link to fall back on — so the result never lacks the
        # file entirely.
        embed_resource = getattr(app.state, "file_store_embed_resource", True)
        content: list[Any] = []
        if embed_resource or download_url is None:
            # File() builds a BlobResourceContents wrapped in an
            # EmbeddedResource. We pass the basename without extension and
            # format="gs" separately because File appends the format as a
            # dotted suffix when synthesising the URI; passing the name with
            # the extension would yield "name.gs.gs" in the URI.
            file_resource = File(
                data=payload,
                name=filename.removesuffix(".gs"),
                format="gs",
            ).to_resource_content(mime_type="application/octet-stream")
            content = [file_resource]

        result = _PathfinderBuildResult(
            filename=filename,
            download_url=download_url,
            summary=_PathfinderBuildSummary(
                n_addresses=len(spec.addresses),
                n_txs=len(spec.txs),
                n_agg_edges=len(spec.agg_edges),
                layout=chosen,
                byte_size=len(payload),
                warnings=_collect_warnings(spec),
            ),
        )
        return ToolResult(
            content=content,
            structured_content=result.model_dump(),
        )
