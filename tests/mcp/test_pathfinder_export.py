"""Tests for the build_pathfinder_file MCP tool.

Mirrors the pattern used by test_consolidated.py: register the tool on a
local FastMCP, call it via fastmcp.Client, assert on the structured
content. The tool does not call back into the FastAPI app, so we pass an
empty FastAPI to satisfy the (mcp, app, stack) registrar signature.

The tool returns a ToolResult: ``structured_content`` carries the JSON
metadata (filename + summary + warnings) the model reads; ``content``
carries a single EmbeddedResource with the .gs bytes as a base64 blob.
These tests assert both halves: the metadata is well-formed, and the
embedded resource round-trips back into a typed PathfinderData.
"""

from __future__ import annotations

import base64
from contextlib import AsyncExitStack
from typing import Any

import mcp.types as mcp_types
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


def _mcp(app: FastAPI | None = None) -> FastMCP:
    mcp = FastMCP(name="test")
    register(mcp, app if app is not None else FastAPI(), AsyncExitStack())
    return mcp


def _app_with_store(store, *, embed_resource: bool = True) -> FastAPI:
    """A FastAPI whose state carries a file store, as the web app sets up."""
    app = FastAPI()
    app.state.file_store = store
    app.state.file_store_embed_resource = embed_resource
    return app


# Minimal valid spec — the shape of the graph is irrelevant to the
# download-link tests, only that a non-empty .gs file gets built.
_MINIMAL_SPEC = {
    "name": "linkable",
    "default_network": "btc",
    "spec": {
        "addresses": [{"id": "addrA", "starting_point": True}],
        "txs": [],
        "agg_edges": [],
    },
}


async def _call(mcp: FastMCP, payload: dict) -> Any:
    """Return the full CallToolResult so tests can inspect both
    structured_content (LLM-visible) and content (binary resource)."""
    async with Client(mcp) as c:
        return await c.call_tool("build_pathfinder_file", payload)


def _decode(call_result: Any) -> PathfinderData:
    """Extract the .gs blob from the embedded resource and round-trip
    it back to a typed PathfinderData."""
    assert len(call_result.content) == 1, call_result.content
    block = call_result.content[0]
    assert isinstance(block, mcp_types.EmbeddedResource), type(block)
    assert isinstance(block.resource, mcp_types.BlobResourceContents)
    raw_bytes = base64.b64decode(block.resource.blob)
    data = structure(decode_gs_bytes(raw_bytes))
    # structure() returns PathfinderData | GraphData; the encoder always
    # emits a pathfinder-v1 payload, so the GraphData branch shouldn't fire.
    assert isinstance(data, PathfinderData)
    return data


def _structured(call_result: Any) -> dict:
    """Convenience accessor — tests mostly want the JSON metadata."""
    assert call_result.structured_content is not None
    return call_result.structured_content


async def test_happy_path_round_trips_through_decoder() -> None:
    """Structured metadata is well-formed, and the embedded resource
    round-trips back through the decoder. Asserts both halves of the
    split: model-visible JSON vs binary resource."""
    call_result = await _call(
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
    structured = _structured(call_result)
    assert structured["filename"] == "agent-finding.gs"
    assert structured["summary"] == {
        "n_addresses": 2,
        "n_txs": 1,
        "n_agg_edges": 1,
        "layout": "hierarchical",  # auto picked hierarchical (starting_point set)
        "byte_size": structured["summary"]["byte_size"],
        # Well-formed spec: tx is listed AND referenced from the edge, so
        # nothing surprising for the agent to know about.
        "warnings": [],
    }
    assert structured["summary"]["byte_size"] > 0
    # The whole point of moving to an embedded resource: the structured
    # content does NOT carry the file bytes. The model never sees them.
    assert "content_base64" not in structured
    assert "blob" not in structured

    data = _decode(call_result)
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
    call_result = await _call(
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
    assert _structured(call_result)["summary"]["layout"] == "columnar"

    data = _decode(call_result)
    # Columnar default puts un-side-hinted addresses at GsBuilder._ADDR_COL_X.
    assert all(a.x == -5.0 for a in data.addresses)


async def test_explicit_columnar_overrides_auto() -> None:
    call_result = await _call(
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
    assert _structured(call_result)["summary"]["layout"] == "columnar"


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
    call_result = await _call(
        _mcp(),
        {
            "name": "../danger\\name with spaces",
            "default_network": "btc",
            "spec": {"addresses": [{"id": "a"}], "txs": [], "agg_edges": []},
        },
    )
    fn = _structured(call_result)["filename"]
    assert fn.endswith(".gs")
    assert "/" not in fn and "\\" not in fn and ".." not in fn
    assert " " not in fn


async def test_hierarchical_explicit_with_no_anchors_still_runs() -> None:
    """If the caller forces hierarchical layout with no starting points,
    everything collapses to a single column (documented behaviour)."""
    call_result = await _call(
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
    assert _structured(call_result)["summary"]["layout"] == "hierarchical"
    data = _decode(call_result)
    assert all(a.x == 0.0 for a in data.addresses)


async def test_warning_when_agg_edges_present_but_no_txs() -> None:
    """The historical agent failure: build agg_edges with no tx_ids and
    no txs list, ship a .gs that shows only abstract a↔b lines. The
    tool must still produce the file, but warn loudly so the agent can
    fix and retry."""
    call_result = await _call(
        _mcp(),
        {
            "name": "no-txs",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                ],
                "txs": [],
                "agg_edges": [{"a": "addrA", "b": "addrB"}],
            },
        },
    )
    summary = _structured(call_result)["summary"]
    assert any("no txs were provided" in w for w in summary["warnings"]), summary
    # File is still produced — warnings are advisory, not fatal.
    assert summary["byte_size"] > 0


async def test_warning_when_some_edges_have_no_tx_ids() -> None:
    """When `txs` is populated but some edges still omit `tx_ids`, those
    edges silently lose their tx linkage. Flag the count so the agent
    can spot it."""
    call_result = await _call(
        _mcp(),
        {
            "name": "partial",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                    {"id": "addrC"},
                ],
                "txs": [{"id": "txhash1"}],
                "agg_edges": [
                    {"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]},
                    {"a": "addrA", "b": "addrC"},  # no tx_ids
                ],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any("1 of 2 agg_edge(s) have no tx_ids" in w for w in warnings), warnings


async def test_warning_when_edge_references_unknown_tx() -> None:
    call_result = await _call(
        _mcp(),
        {
            "name": "dangling-tx",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                ],
                "txs": [{"id": "txhash1"}],
                "agg_edges": [
                    {"a": "addrA", "b": "addrB", "tx_ids": ["txhash1", "missing"]},
                ],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any(
        "references tx hash(es) not in `txs`" in w and "missing" in w for w in warnings
    ), warnings


async def test_warning_when_edge_references_unknown_address() -> None:
    """Typos in `a`/`b` endpoints are the most likely silent failure —
    pathfinder draws an edge to a node that doesn't exist."""
    call_result = await _call(
        _mcp(),
        {
            "name": "dangling-addr",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "addrA", "starting_point": True}],
                "txs": [{"id": "txhash1"}],
                "agg_edges": [
                    {"a": "addrA", "b": "typoB", "tx_ids": ["txhash1"]},
                ],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any(
        "reference address(es) not in `addresses`" in w and "typoB" in w
        for w in warnings
    ), warnings


async def test_warning_when_tx_has_no_source_or_destination_edge() -> None:
    """A tx listed in `txs` but never referenced from any `agg_edge.tx_ids`
    has no source/destination address pairing, so pathfinder renders it
    as a floating node. On ETH this can also trigger the renderer's
    off-line drop. Warn the agent so they can attach the tx to an edge."""
    call_result = await _call(
        _mcp(),
        {
            "name": "floating-tx",
            "default_network": "eth",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                ],
                # txhash1 is wired up; txhash2 is not referenced anywhere.
                "txs": [{"id": "txhash1"}, {"id": "txhash2"}],
                "agg_edges": [
                    {"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]},
                ],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any(
        "not referenced from any agg_edge.tx_ids" in w
        and "txhash2" in w
        # The wired-up tx must not be flagged.
        and "txhash1" not in w
        for w in warnings
    ), warnings


async def test_layout_inside_spec_is_accepted() -> None:
    """LLMs frequently nest `layout` inside `spec` because it reads as a
    graph-shape option. Pydantic previously rejected it with an
    extra_forbidden error that the model couldn't recover from. The spec
    now accepts `layout` as a forgiving alias for the top-level argument."""
    call_result = await _call(
        _mcp(),
        {
            "name": "layout-in-spec",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "addrA", "starting_point": True}],
                "txs": [],
                "agg_edges": [],
                # nested where the model would naturally put it
                "layout": "hierarchical",
            },
        },
    )
    summary = _structured(call_result)["summary"]
    # Hierarchical was the explicit choice — confirm it landed.
    assert summary["layout"] == "hierarchical"


async def test_top_level_layout_wins_over_spec_layout() -> None:
    """If the caller is explicit at the top level, that always wins
    over a nested fallback — the nested form is forgiveness, not
    precedence."""
    call_result = await _call(
        _mcp(),
        {
            "name": "explicit-tops",
            "default_network": "btc",
            "layout": "columnar",
            "spec": {
                "addresses": [{"id": "addrA"}],
                "txs": [],
                "agg_edges": [],
                "layout": "hierarchical",
            },
        },
    )
    summary = _structured(call_result)["summary"]
    assert summary["layout"] == "columnar"


async def test_no_warnings_for_well_formed_abstract_graph() -> None:
    """An addresses-only spec (no edges, no txs) is legitimately
    abstract — no warning should fire."""
    call_result = await _call(
        _mcp(),
        {
            "name": "addrs-only",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "a"}, {"id": "b"}],
                "txs": [],
                "agg_edges": [],
            },
        },
    )
    assert _structured(call_result)["summary"]["warnings"] == []


async def test_embedded_resource_carries_blob_with_filename_uri() -> None:
    """Lock in the resource shape: the .gs bytes travel as a
    BlobResourceContents wrapped in an EmbeddedResource, with a URI
    that includes the filename so the MCP client can render it as a
    downloadable attachment named after the graph."""
    call_result = await _call(
        _mcp(),
        {
            "name": "anchor-investigation",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "addrA", "starting_point": True}],
                "txs": [],
                "agg_edges": [],
            },
        },
    )
    assert len(call_result.content) == 1
    block = call_result.content[0]
    assert isinstance(block, mcp_types.EmbeddedResource)
    assert block.type == "resource"
    assert isinstance(block.resource, mcp_types.BlobResourceContents)
    # Filename should be embedded in the URI so the client can use it.
    assert "anchor-investigation" in str(block.resource.uri)
    assert str(block.resource.uri).endswith(".gs")
    # Bytes must be real, decode cleanly, and structurally valid.
    decoded = base64.b64decode(block.resource.blob)
    assert len(decoded) > 0
    pf = structure(decode_gs_bytes(decoded))
    assert isinstance(pf, PathfinderData)


async def test_download_url_absent_without_file_store() -> None:
    """With no file store on app.state the tool degrades to embedded-only:
    download_url is null and the embedded resource is still present."""
    call_result = await _call(_mcp(), _MINIMAL_SPEC)
    assert _structured(call_result)["download_url"] is None
    assert len(call_result.content) == 1


async def test_download_url_present_with_file_store(
    make_file_store, monkeypatch
) -> None:
    """With a file store configured the tool stashes the .gs file and
    returns a download_url; the stored bytes match the embedded resource."""
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export.get_http_request",
        lambda: None,
    )
    store = make_file_store()
    call_result = await _call(_mcp(_app_with_store(store)), _MINIMAL_SPEC)

    structured = _structured(call_result)
    url = structured["download_url"]
    assert url is not None
    assert url.startswith("https://files.example.test/download/")

    token = url.rsplit("/", 1)[-1]
    assert token in store.files
    stored = store.files[token]
    assert stored.content_type == "application/octet-stream"
    assert stored.filename == structured["filename"]
    # embed_resource defaults True -> the resource is still attached, and
    # its bytes are exactly what was stored.
    assert len(call_result.content) == 1
    block = call_result.content[0]
    assert stored.data == base64.b64decode(block.resource.blob)


async def test_embed_resource_false_yields_link_only(
    make_file_store, monkeypatch
) -> None:
    """With embed_resource disabled the tool returns the link only — no
    embedded resource in the content channel."""
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export.get_http_request",
        lambda: None,
    )
    store = make_file_store()
    mcp = _mcp(_app_with_store(store, embed_resource=False))
    call_result = await _call(mcp, _MINIMAL_SPEC)

    assert _structured(call_result)["download_url"] is not None
    assert call_result.content == []


async def test_embed_resource_false_still_embeds_when_link_unavailable(
    make_file_store,
) -> None:
    """embed_resource is disabled, but with no HTTP request context the
    link cannot be built — the tool must fall back to embedding the
    resource so the result never lacks the file entirely."""
    store = make_file_store()
    mcp = _mcp(_app_with_store(store, embed_resource=False))
    # get_http_request() is NOT monkeypatched: in the in-memory client it
    # raises, so url_for fails and download_url stays null.
    call_result = await _call(mcp, _MINIMAL_SPEC)

    assert _structured(call_result)["download_url"] is None
    assert len(call_result.content) == 1


async def test_oversize_file_rejected_with_tool_error(make_file_store) -> None:
    """When a file store is configured, a built .gs larger than the cap
    fails hard with a ToolError (the hard size limit)."""
    tiny_store = make_file_store(max_bytes=10)  # any real .gs exceeds 10 bytes
    with pytest.raises(ToolError, match="too large"):
        await _call(_mcp(_app_with_store(tiny_store)), _MINIMAL_SPEC)


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
