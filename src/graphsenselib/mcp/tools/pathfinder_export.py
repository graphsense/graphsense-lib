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

import base64
import logging
import re
from contextlib import AsyncExitStack
from typing import Any, Literal, Optional, Union

from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, ConfigDict, Field

from graphsenselib.convert.gs_files import (
    apply_hierarchical_layout,
    builder_from_spec,
)

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
        default=None, description="Free-text label shown next to the node."
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
    label: Optional[str] = Field(default=None)
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
        description="Transaction hashes mediating the a↔b relationship.",
    )
    network: Optional[str] = Field(default=None)
    a_network: Optional[str] = Field(default=None)
    b_network: Optional[str] = Field(default=None)


class PathfinderSpec(BaseModel):
    """Graph the agent wants encoded into a ``.gs`` file."""

    model_config = ConfigDict(extra="forbid")

    addresses: list[_AddressSpec] = Field(default_factory=list)
    txs: list[_TxSpec] = Field(default_factory=list)
    agg_edges: list[_EdgeSpec] = Field(default_factory=list)


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


def register(mcp: FastMCP, app: FastAPI, stack: AsyncExitStack) -> None:  # noqa: ARG001
    """Register the build_pathfinder_file tool on the given MCP instance.

    Matches the (mcp, app, stack) signature of other consolidated tools
    even though this one needs neither the FastAPI app (no REST fan-out)
    nor the exit stack (no httpx client lifecycle).
    """

    @mcp.tool(tags={"gs_pathfinder", "gs_export"})
    def build_pathfinder_file(
        name: str,
        default_network: str,
        spec: PathfinderSpec,
        layout: Literal["auto", "hierarchical", "columnar"] = "auto",
    ) -> dict[str, Union[str, dict[str, Any]]]:
        """Build a Pathfinder .gs save file from an investigation graph.

        Pass the addresses, transactions, and address↔address relationships
        you discovered. The tool returns the bytes of a .gs file (base64-
        encoded) that the user can open in the pathfinder UI to verify
        your findings visually.

        Mark the address(es) or tx(s) you started from with
        ``starting_point=true`` so the layout can place anchors at column
        0 and arrange the rest by hop distance.

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
            ``{filename, content_base64, summary}`` where ``summary``
            reports the chosen layout and node/edge counts.
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

        return {
            "filename": _safe_filename(name),
            "content_base64": base64.b64encode(payload).decode("ascii"),
            "summary": {
                "n_addresses": len(spec.addresses),
                "n_txs": len(spec.txs),
                "n_agg_edges": len(spec.agg_edges),
                "layout": chosen,
                "byte_size": len(payload),
            },
        }
