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
from graphsenselib.mcp.tools.pathfinder_export import (
    _run_verifier as _real_run_verifier,
    register,
)


def _mcp(app: FastAPI | None = None) -> FastMCP:
    mcp = FastMCP(name="test")
    register(mcp, app if app is not None else FastAPI(), AsyncExitStack())
    return mcp


@pytest.fixture(autouse=True)
def _stub_verifier(monkeypatch):
    """Short-circuit the backend-aware verifier in every test.

    The MCP tool now runs the verifier by default (verify=True). Without
    this fixture, every test would call ``_run_verifier`` against the
    bare ``FastAPI()`` used here, which 404s every URL and therefore
    decorates every result with bogus "address does not exist" warnings.

    Returns a list of (args, kwargs) tuples recording each invocation,
    so tests that care about WHEN the verifier ran can assert on it.
    Tests that need a different verifier behaviour can re-monkeypatch
    ``_run_verifier`` themselves; the per-test override wins.
    """
    calls: list[tuple[tuple, dict]] = []

    async def stub(*args, **kwargs):
        calls.append((args, kwargs))
        return []

    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export._run_verifier",
        stub,
    )
    return calls


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


def _embedded(call_result: Any) -> mcp_types.EmbeddedResource:
    """Find the EmbeddedResource block in the content list. The tool
    also emits a TextContent block (for clients that only render
    `content`), so we can't index by position."""
    blocks = [
        b for b in call_result.content if isinstance(b, mcp_types.EmbeddedResource)
    ]
    assert len(blocks) == 1, call_result.content
    return blocks[0]


def _text(call_result: Any) -> mcp_types.TextContent:
    blocks = [b for b in call_result.content if isinstance(b, mcp_types.TextContent)]
    assert len(blocks) == 1, call_result.content
    return blocks[0]


def _decode(call_result: Any) -> PathfinderData:
    """Extract the .gs blob from the embedded resource and round-trip
    it back to a typed PathfinderData."""
    block = _embedded(call_result)
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


async def test_underscored_tx_identifier_accepted() -> None:
    """Account-model sub-payment identifiers (`<hash>_I948`, `<hash>_T3`)
    contain an underscore and must validate. Previously rejected as
    "Invalid tx hash" — agents misread that as a verify failure and
    retried with verify=false, which didn't help."""
    await _call(
        _mcp(),
        {
            "name": "subpayment",
            "default_network": "eth",
            "spec": {
                "addresses": [{"id": "addrA", "starting_point": True}, {"id": "addrB"}],
                "txs": [{"id": "0xabc_I948"}, {"id": "0xdef_T3"}],
                "agg_edges": [
                    {"a": "addrA", "b": "addrB", "tx_ids": ["0xabc_I948", "0xdef_T3"]},
                ],
            },
        },
    )


async def test_format_error_message_calls_out_format_vs_verify() -> None:
    """When the id pattern fails, the error message must make clear
    it's a FORMAT check at the boundary, not a verify finding — that
    confusion caused a wasted retry in real usage."""
    with pytest.raises(ToolError, match="format"):
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


async def test_warnings_are_surfaced_in_text_content() -> None:
    """Hosts that ignore `structured_content` (Mistral Le Chat) would
    otherwise silently ship a broken .gs. The TextContent block must
    repeat the warnings so the agent has a chance of seeing them."""
    call_result = await _call(
        _mcp(),
        {
            "name": "floating-tx-text",
            "default_network": "eth",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                ],
                "txs": [{"id": "txhash1"}, {"id": "txhash2"}],
                "agg_edges": [
                    {"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]},
                ],
            },
        },
    )
    text = _text(call_result).text
    assert "Warnings" in text
    assert "txhash2" in text


async def test_no_warning_preamble_when_spec_is_clean() -> None:
    """The warnings preamble must not appear when there are none, so
    well-formed builds stay terse."""
    text = _text(
        await _call(
            _mcp(),
            {
                "name": "clean",
                "default_network": "btc",
                "spec": {
                    "addresses": [
                        {"id": "addrA", "starting_point": True},
                        {"id": "addrB"},
                    ],
                    "txs": [{"id": "txhash1"}],
                    "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]}],
                },
            },
        )
    ).text
    assert "Warnings" not in text


async def test_verify_default_is_on(_stub_verifier) -> None:
    """The tool now verifies by default — a call that omits the flag
    invokes the verifier exactly once."""
    await _call(_mcp(), _MINIMAL_SPEC)
    assert len(_stub_verifier) == 1


async def test_verify_flag_off_opts_out_of_backend_calls(_stub_verifier) -> None:
    """Passing verify=False skips the verifier entirely so drafting
    iterations stay fast (and don't require a reachable backend)."""
    payload = {**_MINIMAL_SPEC, "verify": False}
    await _call(_mcp(), payload)
    assert _stub_verifier == []


async def test_verify_flag_appends_backend_warnings(monkeypatch) -> None:
    """The verifier's findings merge into summary.warnings AND the text
    content block (so Mistral-style hosts see them)."""

    async def fake_run_verifier(*args, **kwargs):
        return [
            "backend says these tx hash(es) do not exist on their declared "
            "network: tx1."
        ]

    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export._run_verifier",
        fake_run_verifier,
    )
    call_result = await _call(
        _mcp(),
        {
            "name": "verified",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                ],
                "txs": [{"id": "tx1"}],
                "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any("do not exist" in w and "tx1" in w for w in warnings), warnings
    # Text content block must also carry the warning — that's the whole
    # point of the channel-folding fix.
    text = _text(call_result).text
    assert "tx1" in text
    assert "Warnings" in text


async def test_verify_flag_downgrades_backend_error_to_warning(
    monkeypatch,
) -> None:
    """A transport-level failure during verification must not sink the
    build: the file is structurally valid, so we ship it with a single
    "verifier unavailable" warning instead of raising."""
    import httpx as _httpx

    async def boom(*args, **kwargs):
        raise _httpx.ConnectError("backend down")

    # Override the autouse stub: restore the real _run_verifier so the
    # error-downgrade path inside it actually runs. We captured the
    # original at module load time (before the autouse stub replaces
    # it). Then patch the verify_against_backend call site underneath
    # it so it raises.
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export._run_verifier",
        _real_run_verifier,
    )
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export.verify_against_backend",
        boom,
    )
    call_result = await _call(
        _mcp(),
        {
            "name": "flaky",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "addrA", "starting_point": True}],
                "txs": [],
                "agg_edges": [],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any("verifier unavailable" in w for w in warnings), warnings
    # Despite the verifier failure the file IS built (structurally valid).
    assert _structured(call_result)["summary"]["byte_size"] > 0


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
    """An addresses-only spec where every address is explicitly an
    anchor (starting_point) is legitimately abstract — no warning
    should fire."""
    call_result = await _call(
        _mcp(),
        {
            "name": "addrs-only",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "a", "starting_point": True},
                    {"id": "b", "starting_point": True},
                ],
                "txs": [],
                "agg_edges": [],
            },
        },
    )
    assert _structured(call_result)["summary"]["warnings"] == []


async def test_warning_when_address_is_unreferenced_and_not_anchor() -> None:
    """An address listed in `addresses` but never appearing as an edge
    endpoint — and not declared a starting_point — renders as a
    floating node. Flag it the same way we flag floating txs."""
    call_result = await _call(
        _mcp(),
        {
            "name": "stray-addr",
            "default_network": "btc",
            "spec": {
                "addresses": [
                    {"id": "addrA", "starting_point": True},
                    {"id": "addrB"},
                    {"id": "addrStray"},  # not in any edge, not an anchor
                ],
                "txs": [{"id": "txhash1"}],
                "agg_edges": [
                    {"a": "addrA", "b": "addrB", "tx_ids": ["txhash1"]},
                ],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert any(
        "address(es) are not referenced" in w
        and "addrStray" in w
        # The wired-up and anchor addresses must not be flagged.
        and "addrA" not in w
        and "addrB" not in w
        for w in warnings
    ), warnings


async def test_starting_point_address_with_no_edges_is_not_flagged() -> None:
    """A starting_point address without any edges is the documented
    "anchor only" case (matches _MINIMAL_SPEC). It must NOT be flagged
    as stray — that's the explicit opt-out."""
    call_result = await _call(
        _mcp(),
        {
            "name": "anchor-only",
            "default_network": "btc",
            "spec": {
                "addresses": [{"id": "addrA", "starting_point": True}],
                "txs": [],
                "agg_edges": [],
            },
        },
    )
    warnings = _structured(call_result)["summary"]["warnings"]
    assert not any("are not referenced" in w for w in warnings), warnings


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
    block = _embedded(call_result)
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
    # Embedded resource + text fallback for content-only MCP hosts.
    _embedded(call_result)
    _text(call_result)


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
    block = _embedded(call_result)
    assert isinstance(block.resource, mcp_types.BlobResourceContents)
    assert stored.data == base64.b64decode(block.resource.blob)
    # The text fallback should carry the download URL verbatim.
    assert url in _text(call_result).text


async def test_embed_resource_false_yields_link_only(
    make_file_store, monkeypatch
) -> None:
    """With embed_resource disabled the tool returns the link only — no
    embedded resource in the content channel, but the TextContent
    fallback IS still emitted so MCP hosts that only render `content`
    (Mistral Le Chat) still show the user the download URL."""
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export.get_http_request",
        lambda: None,
    )
    store = make_file_store()
    mcp = _mcp(_app_with_store(store, embed_resource=False))
    call_result = await _call(mcp, _MINIMAL_SPEC)

    url = _structured(call_result)["download_url"]
    assert url is not None
    # No embedded blob in the content channel.
    assert [
        b for b in call_result.content if isinstance(b, mcp_types.EmbeddedResource)
    ] == []
    # But a single TextContent carrying the download URL.
    text_block = _text(call_result)
    assert url in text_block.text


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
    _embedded(call_result)
    _text(call_result)


async def test_open_url_present_with_file_store_and_base_url(
    make_file_store, monkeypatch
) -> None:
    """With a file store AND a pathfinder base URL on app.state (set by
    mcp/server.py:build_mcp), the tool returns an open_url deep link
    carrying the store token as the `?import=` id — the dashboard fetches
    `<REST>/download/<id>` itself."""
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export.get_http_request",
        lambda: None,
    )
    store = make_file_store()
    app = _app_with_store(store)
    app.state._graphsense_mcp_pathfinder_base_url = "https://app.iknaio.com"
    call_result = await _call(_mcp(app), _MINIMAL_SPEC)

    structured = _structured(call_result)
    open_url = structured["open_url"]
    assert open_url is not None
    token = open_url.rsplit("=", 1)[-1]
    assert open_url == f"https://app.iknaio.com/pathfinder?import={token}"
    assert token in store.files
    # The text fallback should carry the open link verbatim.
    assert open_url in _text(call_result).text


async def test_open_url_absent_without_base_url(make_file_store, monkeypatch) -> None:
    """A file store alone is not enough — without the pathfinder base URL
    on app.state (feature flag off, or standalone tool registration) the
    structured content carries no open_url key at all."""
    monkeypatch.setattr(
        "graphsenselib.mcp.tools.pathfinder_export.get_http_request",
        lambda: None,
    )
    store = make_file_store()
    call_result = await _call(_mcp(_app_with_store(store)), _MINIMAL_SPEC)
    structured = _structured(call_result)
    assert "open_url" not in structured
    assert structured["download_url"] is not None


async def test_open_url_absent_without_file_store() -> None:
    """No file store means no token, so no open link either."""
    app = FastAPI()
    app.state._graphsense_mcp_pathfinder_base_url = "https://app.iknaio.com"
    call_result = await _call(_mcp(app), _MINIMAL_SPEC)
    assert _structured(call_result)["open_url"] is None


async def test_open_url_survives_download_link_failure(make_file_store) -> None:
    """open_url only needs the store token + base URL, not url_for — when
    link building fails (no HTTP request context) the open link must
    still be returned even though download_url is null."""
    store = make_file_store()
    app = _app_with_store(store)
    app.state._graphsense_mcp_pathfinder_base_url = "https://app.iknaio.com"
    # get_http_request() is NOT monkeypatched: in the in-memory client it
    # raises, so url_for fails and download_url stays null.
    call_result = await _call(_mcp(app), _MINIMAL_SPEC)

    structured = _structured(call_result)
    assert structured["download_url"] is None
    assert structured["open_url"] is not None
    assert structured["open_url"].startswith(
        "https://app.iknaio.com/pathfinder?import="
    )


async def _tool_description(mcp: FastMCP) -> str:
    async with Client(mcp) as c:
        tools = {t.name: t for t in await c.list_tools()}
    return tools["build_pathfinder_file"].description or ""


async def test_open_url_advertised_only_when_enabled(make_file_store) -> None:
    """The tool description mentions open_url only when the feature flag
    put a base URL on app.state; the [[open-url]] sentinels never leak."""
    enabled_app = _app_with_store(make_file_store())
    enabled_app.state._graphsense_mcp_pathfinder_base_url = "https://app.iknaio.com"
    enabled_desc = await _tool_description(_mcp(enabled_app))
    assert "open_url" in enabled_desc
    assert "[[open-url]]" not in enabled_desc and "[[/open-url]]" not in enabled_desc

    disabled_desc = await _tool_description(_mcp())
    assert "open_url" not in disabled_desc
    assert "[[open-url]]" not in disabled_desc and "[[/open-url]]" not in disabled_desc
    # The download_url delivery instructions must survive the stripping.
    assert "download_url" in disabled_desc


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
