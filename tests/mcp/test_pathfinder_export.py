"""Tests for the build_pathfinder_file MCP tool.

Mirrors the pattern used by test_consolidated.py: register the tool on a
local FastMCP, call it via fastmcp.Client, assert on the structured
content. The tool does not call back into the FastAPI app, so we pass an
empty FastAPI to satisfy the (mcp, app, stack) registrar signature.
"""

from __future__ import annotations

import base64
from contextlib import AsyncExitStack

import pytest
from fastapi import FastAPI
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from graphsenselib.convert.gs_files import (
    PathfinderData,
    decode_gs_bytes,
    structure,
)
from graphsenselib.convert.gs_files.encoder import _HIER_X_STEP
from graphsenselib.mcp.tools.pathfinder_export import register


def _mcp() -> FastMCP:
    mcp = FastMCP(name="test")
    register(mcp, FastAPI(), AsyncExitStack())
    return mcp


async def _call(mcp: FastMCP, payload: dict):
    async with Client(mcp) as c:
        return (await c.call_tool("build_pathfinder_file", payload)).structured_content


def _decode(content: dict) -> PathfinderData:
    raw_bytes = base64.b64decode(content["content_base64"])
    data = structure(decode_gs_bytes(raw_bytes))
    # structure() returns PathfinderData | GraphData; the encoder always
    # emits a pathfinder-v1 payload, so the GraphData branch shouldn't fire.
    assert isinstance(data, PathfinderData)
    return data


async def test_happy_path_round_trips_through_decoder() -> None:
    """Build -> base64-decode -> parse -> typed PathfinderData; the
    addresses, txs, and edges must survive the full pipeline."""
    result = await _call(
        _mcp(),
        {
            "name": "agent-finding",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True, "label": "anchor"},
                    {"id": "addrB"},
                ],
                "txs": [{"id": "txhash1"}],
                "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]}],
            },
        },
    )
    assert result["filename"] == "agent-finding.gs"
    assert result["summary"] == {
        "n_addresses": 2,
        "n_txs": 1,
        "n_agg_edges": 1,
        "layout": "hierarchical",  # auto picked hierarchical (starting_point set)
        "byte_size": result["summary"]["byte_size"],
    }
    assert result["summary"]["byte_size"] > 0

    data = _decode(result)
    assert data.name == "agent-finding"
    assert {a.id.id for a in data.addresses} == {"addrA", "addrB"}
    assert {t.id.id for t in data.txs} == {"txhash1"}
    assert len(data.agg_edges) == 1
    # Anchor sits at x=0, the tx (one hop away) at x = X_STEP.
    addr_by_id = {a.id.id: a for a in data.addresses}
    tx_by_id = {t.id.id: t for t in data.txs}
    assert addr_by_id["addrA"].x == 0.0
    assert tx_by_id["txhash1"].x == _HIER_X_STEP


async def test_auto_layout_picks_columnar_without_starting_point() -> None:
    result = await _call(
        _mcp(),
        {
            "name": "no-anchor",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "x"}, {"id": "y"}],
                "txs": [],
                "agg_edges": [{"a": "x", "b": "y"}],
            },
        },
    )
    assert result["summary"]["layout"] == "columnar"

    data = _decode(result)
    # Columnar default puts un-side-hinted addresses at GsBuilder._ADDR_COL_X.
    assert all(a.x == -5.0 for a in data.addresses)


async def test_explicit_columnar_overrides_auto() -> None:
    result = await _call(
        _mcp(),
        {
            "name": "force-cols",
            "default_network": "btc",
            "layout": "columnar",
            "spec": {
                "addresses": [{"id": "a", "starting_point": True}],
                "txs": [],
                "agg_edges": [],
            },
        },
    )
    # Despite starting_point=True the caller forced columnar.
    assert result["summary"]["layout"] == "columnar"


async def test_invalid_address_id_rejected() -> None:
    """Slashes / control chars must be rejected at the boundary so a
    malformed spec never lands inside the encoder."""
    with pytest.raises(ToolError, match="address id"):
        await _call(
            _mcp(),
            {
                "name": "bad",
                "default_network": "btc",
                "spec": {
                    "addresses": [{"id": "addr/with/slashes"}],
                    "txs": [],
                    "agg_edges": [],
                },
            },
        )


async def test_invalid_currency_rejected() -> None:
    with pytest.raises(ToolError, match="default_network"):
        await _call(
            _mcp(),
            {
                "name": "bad",
                "default_network": "BTC!",  # uppercase + punctuation
                "spec": {"addresses": [], "txs": [], "agg_edges": []},
            },
        )


async def test_filename_is_sanitised() -> None:
    """A graph name with path separators or unicode must not leak into
    the filename (clients may use it directly to save to disk)."""
    result = await _call(
        _mcp(),
        {
            "name": "../danger\\name with spaces",
            "default_network": "btc",
            "spec": {"addresses": [{"id": "a"}], "txs": [], "agg_edges": []},
        },
    )
    fn = result["filename"]
    assert fn.endswith(".gs")
    assert "/" not in fn and "\\" not in fn and ".." not in fn
    assert " " not in fn


async def test_hierarchical_explicit_with_no_anchors_still_runs() -> None:
    """If the caller forces hierarchical layout with no starting points,
    everything collapses to a single column (documented behaviour)."""
    result = await _call(
        _mcp(),
        {
            "name": "forced-hier",
            "default_network": "btc",
            "layout": "hierarchical",
            "spec": {
                "addresses": [{"id": "a"}, {"id": "b"}],
                "txs": [],
                "agg_edges": [{"a": "a", "b": "b"}],
            },
        },
    )
    assert result["summary"]["layout"] == "hierarchical"
    data = _decode(result)
    assert all(a.x == 0.0 for a in data.addresses)


async def test_extra_field_in_spec_rejected_by_pydantic() -> None:
    """The spec models use extra='forbid' so a typo in a field name fails
    fast with a clear validation error rather than being silently dropped."""
    with pytest.raises(Exception):  # fastmcp wraps as ToolError or ValidationError
        await _call(
            _mcp(),
            {
                "name": "typo",
                "default_network": "btc",
                "spec": {
                    "addresses": [
                        {"id": "a", "starting": True}
                    ],  # typo: starting_point
                    "txs": [],
                    "agg_edges": [],
                },
            },
        )
