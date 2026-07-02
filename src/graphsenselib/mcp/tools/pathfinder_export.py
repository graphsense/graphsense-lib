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

import inspect
import logging
import re
from contextlib import AsyncExitStack
from typing import Any, Literal, Optional

import httpx
from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_request
from fastmcp.tools.base import ToolResult
from fastmcp.utilities.types import File
from mcp.types import TextContent
from pydantic import BaseModel, ConfigDict, Field

from graphsenselib.convert.gs_files import (
    apply_hierarchical_layout,
    builder_from_spec,
)
from graphsenselib.mcp.tools.consolidated import _make_client
from graphsenselib.pathfinder import (
    RestBackend,
    verify_against_backend,
    verify_structural,
)
from graphsenselib.web.file_store import FileTooLargeError

logger = logging.getLogger(__name__)

# Mirrored from tools/consolidated.py — same conservative guards on
# user-controlled identifiers (currency tickers, address ids, tx hashes)
# so a malformed spec is rejected at the boundary instead of producing
# an unopenable .gs.
_CURRENCY_PATTERN = re.compile(r"^[a-z0-9]{2,10}$")
# Underscore is included so account-model sub-payment identifiers
# (e.g. `<hash>_I948` for internal traces, `<hash>_T3` for token
# transfers) pass validation. Both `_` and bare alphanumerics are
# URL-safe (RFC 3986 unreserved), and the REST endpoint resolves
# identifier strings natively — see the
# `project_tx_endpoint_identifier_resolution` memory.
_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_]{1,150}$")
# Filenames are derived from the user-supplied graph name; keep the
# derived basename safe for any OS the client might save to.
_FILENAME_SAFE = re.compile(r"[^A-Za-z0-9._-]+")

# The open-in-Pathfinder deep link is feature-gated (see
# GSMCPConfig.pathfinder_open_url_enabled). Passages of the tool
# docstring that advertise it are wrapped in these sentinels; at
# registration time the sentinels are stripped (feature on) or the whole
# wrapped passage is removed (feature off), so a disabled deployment
# never advertises `open_url` to the model.
_OPEN_URL_DOC_RE = re.compile(r"\[\[open-url\]\](.*?)\[\[/open-url\]\]", re.DOTALL)


def _resolve_open_url_docs(doc: str, *, enabled: bool) -> str:
    return _OPEN_URL_DOC_RE.sub(lambda m: m.group(1) if enabled else "", doc)


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

    id: str = Field(
        description=(
            "Transaction identifier. On UTXO chains (BTC, BCH, LTC, ZEC) "
            "use the bare `tx_hash` — there's nothing else to pass. On "
            "account-model chains (ETH, TRX) a single on-chain transaction "
            "can carry both the native transfer and one or more token / "
            "internal sub-payments; the response carries an `identifier` "
            "field for each. Two cases:\n"
            "\n"
            "  * For the native/base transfer, pass the bare `tx_hash` "
            "    (simplest and what the validator expects most often).\n"
            "  * To point at a SPECIFIC sub-payment (an internal trace or "
            "    a token transfer), pass the corresponding `identifier` "
            "    verbatim — it looks like `<hash>_I<n>` (internal) or "
            "    `<hash>_T<n>` (token).\n"
            "\n"
            "Allowed characters: alphanumeric and underscore, 1-150 chars. "
            "Anything else is a format error at the input boundary, NOT a "
            "verify finding — do not toggle `verify` to work around a "
            "format complaint."
        )
    )
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
    # Forgiving alias for the top-level `layout` argument. The model
    # often nests layout inside `spec` because it reads as a graph-shape
    # option; before this field existed that produced a Pydantic
    # "extra_forbidden" error that the model couldn't recover from. When
    # set here AND the top-level argument is the default ("auto"), the
    # value here wins; otherwise the top-level argument wins.
    layout: Optional[Literal["auto", "hierarchical", "columnar"]] = Field(
        default=None,
        description=(
            "Layout name. This is also accepted as a top-level argument to "
            "the tool, and the top-level argument is the preferred place — "
            "but for backwards compatibility / agent forgiveness, putting "
            "it here works too."
        ),
    )


def _validate_currency(currency: str, *, field_name: str = "network") -> None:
    if not _CURRENCY_PATTERN.match(currency):
        raise ToolError(f"Invalid {field_name} identifier: {currency!r}")


def _validate_id(name: str, value: str) -> None:
    if not _ID_PATTERN.match(value):
        # Make it clear this is a FORMAT complaint at input validation,
        # not a verify finding about whether the value exists on chain
        # — agents have re-tried with verify=false because the wording
        # implied "verify rejected it".
        raise ToolError(
            f"{name} {value!r} has an invalid format (allowed: "
            "alphanumeric and underscore, 1-150 chars). This is a "
            "format check at the input boundary, NOT a verify finding."
        )


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


# The structural and backend-aware verifiers live in
# `graphsenselib.pathfinder`. This module only adapts the MCP pydantic
# spec to the dict shape they expect.


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
    open_url: Optional[str] = None
    summary: _PathfinderBuildSummary


async def _run_verifier(
    spec_dict: dict[str, Any],
    default_network: str,
    app: FastAPI,
) -> list[str]:
    """Backend-aware checks for the build tool. Wraps
    :func:`verify_against_backend` with the existing MCP httpx client
    lifecycle and downgrades transport errors to a single "verifier
    unavailable" warning — the file is structurally valid, so a backend
    hiccup shouldn't sink the call."""
    client = _make_client(app)
    try:
        async with client:
            backend = RestBackend(client)
            return await verify_against_backend(
                spec_dict, default_network=default_network, backend=backend
            )
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        logger.warning(
            "build_pathfinder_file: backend verifier failed (%s); "
            "shipping file without backend-aware warnings",
            exc,
        )
        return [
            "backend verifier unavailable; backend-aware checks were "
            "skipped — structural checks still ran."
        ]


def register(mcp: FastMCP, app: FastAPI, stack: AsyncExitStack) -> None:  # noqa: ARG001
    """Register the build_pathfinder_file tool on the given MCP instance.

    Matches the (mcp, app, stack) signature of other consolidated tools.
    The exit stack is unused — the build path holds no long-lived
    resources — but ``app`` is used when ``verify=True`` to mint the
    httpx client the verifier needs (see ``_run_verifier``).
    """

    async def build_pathfinder_file(
        name: str,
        default_network: str,
        spec: PathfinderSpec,
        layout: Literal["auto", "hierarchical", "columnar"] = "auto",
        verify: bool = True,
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

        [[open-url]]* If the result has a non-null ``open_url``, give that link to the
          user verbatim — clicking it opens the graph directly in the
          Pathfinder web app (no manual download/import needed). It is
          time-limited like the download link.
        [[/open-url]]* If the result has a non-null ``download_url``, give that link to
          the user verbatim — it is a real, time-limited download link
          for the ``.gs`` file itself.
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

        For every tx you include, provide at least one source and one
        destination address — i.e. add an ``agg_edge`` with the tx's
        ``from``-address as ``a`` and the tx's ``to``-address as ``b``,
        with the tx hash in ``tx_ids``. Optional but strongly
        recommended for proper visualisation: a tx not referenced from
        any ``agg_edge`` renders as a floating node, and on ETH the
        renderer can silently drop edges whose tx node is off-line.

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
            verify: When True (the default), additionally cross-check
                the spec against the backend: every address and tx hash
                is looked up, and each agg_edge.tx_ids reference is
                verified to actually mediate the claimed a↔b
                relationship on chain. Findings are appended to
                ``summary.warnings`` and to the text content block.
                Adds N+M backend calls (capped by an internal
                concurrency limit). Pass False to skip for fast
                iteration while drafting; backend hiccups during verify
                are downgraded to a soft warning so a flaky backend
                cannot sink a structurally valid file.

        Returns:
            A tool result whose structured content carries
            ``{filename, download_url,[[open-url]] open_url,[[/open-url]] summary}`` — this, and
            only this, is what you can read. ``download_url`` is either a
            real, time-limited download link or null (null when the server
            has no file store configured, or could not address the link);
            the file is then delivered as an embedded attachment instead.
            [[open-url]]``open_url`` is a deep link that opens the graph directly in
            the Pathfinder web app (null when no file store is
            configured); prefer surfacing it first when present.
            [[/open-url]]Inspect ``summary.warnings`` (it flags common authoring
            mistakes) and mention any to the user. The .gs bytes travel
            as an embedded MCP resource and/or via the download link;
            they never enter your context, so do not expect to access or
            relay the file's contents.
        """
        _validate_spec(spec, default_network)

        # If the caller put `layout` inside `spec` instead of at top
        # level (a common LLM misplacement), respect it as long as the
        # top-level arg is the default ("auto"). Explicit top-level
        # always wins.
        if layout == "auto" and spec.layout is not None:
            layout = spec.layout

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
        open_url: Optional[str] = None
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
            # Open-in-Pathfinder deep link (feature-gated): the dashboard's
            # `?import=<id>` loader takes the opaque store token (NOT a
            # URL) and fetches `<REST>/download/<id>` itself, so this
            # only needs the token plus the configured Pathfinder base
            # URL (set on app.state by mcp/server.py:build_mcp — None
            # when pathfinder_open_url_enabled is off).
            if open_url_enabled:
                open_url = f"{pathfinder_base_url}/pathfinder?import={token}"
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
            content.append(file_resource)
        # Always include a TextContent block so MCP hosts that only
        # render `content` (and ignore `structured_content`) — Mistral Le
        # Chat is the known offender — show the user something usable.
        warnings = verify_structural(spec_dict)
        if verify:
            warnings.extend(await _run_verifier(spec_dict, default_network, app))
        if download_url is not None:
            text = f"Pathfinder file `{filename}` is ready. Download: {download_url}"
        else:
            text = (
                f"Pathfinder file `{filename}` is ready (embedded in this "
                f"response; no download link configured)."
            )
        if open_url is not None:
            text += f"\nOpen directly in Pathfinder: {open_url}"
        # Fold warnings into the text block too: hosts that drop
        # `structured_content` (Mistral Le Chat) otherwise never see them,
        # and silently shipping a broken .gs is the exact failure mode
        # these warnings exist to prevent.
        if warnings:
            text += "\n\nWarnings — fix the spec and rebuild:\n" + "\n".join(
                f"- {w}" for w in warnings
            )
        content.append(TextContent(type="text", text=text))

        result = _PathfinderBuildResult(
            filename=filename,
            download_url=download_url,
            open_url=open_url,
            summary=_PathfinderBuildSummary(
                n_addresses=len(spec.addresses),
                n_txs=len(spec.txs),
                n_agg_edges=len(spec.agg_edges),
                layout=chosen,
                byte_size=len(payload),
                warnings=warnings,
            ),
        )
        # When the open-url feature is off, drop the key entirely (rather
        # than sending null) so the structured content matches the
        # advertised `{filename, download_url, summary}` shape.
        structured = result.model_dump(
            exclude=None if open_url_enabled else {"open_url"}
        )
        return ToolResult(
            content=content,
            structured_content=structured,
        )

    # Feature gate, resolved once at registration: build_mcp puts the
    # Pathfinder base URL on app.state only when
    # GSMCPConfig.pathfinder_open_url_enabled is true. The same flag
    # controls whether the docstring advertises `open_url` at all.
    pathfinder_base_url = getattr(
        app.state, "_graphsense_mcp_pathfinder_base_url", None
    )
    open_url_enabled = pathfinder_base_url is not None

    # cleandoc mirrors what FastMCP does to a plain docstring; without it
    # the explicit description would keep the source indentation.
    mcp.tool(
        tags={"gs_pathfinder", "gs_export"},
        description=_resolve_open_url_docs(
            inspect.cleandoc(build_pathfinder_file.__doc__ or ""),
            enabled=open_url_enabled,
        ),
    )(build_pathfinder_file)
