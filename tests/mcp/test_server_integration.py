"""In-process end-to-end test: attach the MCP to the real spec FastAPI app and
verify that `fastmcp.Client` can list the curated tool surface.
"""

from __future__ import annotations

import pytest
from fastmcp import Client

from graphsenselib.mcp import GSMCPConfig, attach_to_fastapi, build_mcp


@pytest.fixture
async def bundled_mcp(monkeypatch):
    """Build the MCP using the BUNDLED curation against the real spec app."""
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__API_KEY_ENV", raising=False)

    from graphsenselib.web.app import create_spec_app

    app = create_spec_app()
    cfg = GSMCPConfig()
    mcp, stack = build_mcp(app, cfg)
    async with stack:
        yield mcp


async def test_tool_surface_shape(bundled_mcp):
    async with Client(bundled_mcp) as c:
        tools = await c.list_tools()
        names = {t.name for t in tools}

    # Must-have auto-generated passthroughs
    assert "get_statistics" in names
    assert "get_block" in names
    assert "search" in names
    assert "list_taxonomies" in names

    # Consolidated wrappers
    assert "lookup_address" in names
    assert "lookup_cluster" in names
    assert "lookup_tx_details" in names
    assert "list_neighbors" in names
    assert "list_txs_for" in names

    # Old wrapper names must be gone (renamed)
    assert "lookup_entity" not in names
    assert "lookup_tx_io" not in names

    # Replaced endpoints MUST NOT be exposed (consolidation wins)
    assert "get_address" not in names
    assert "get_address_entity" not in names
    assert "get_tag_summary_by_address" not in names
    assert "list_tags_by_address" not in names
    assert "list_related_addresses" not in names
    assert "get_entity" not in names
    assert "get_cluster" not in names
    assert "list_address_tags_by_entity" not in names
    assert "list_address_tags_by_cluster" not in names
    assert "list_address_neighbors" not in names
    assert "list_entity_neighbors" not in names
    assert "list_cluster_neighbors" not in names
    assert "list_address_txs" not in names
    assert "list_address_links" not in names
    assert "list_entity_links" not in names
    assert "list_cluster_links" not in names
    assert "get_tx" not in names
    assert "get_tx_io" not in names
    assert "get_spending_txs" not in names
    assert "get_spent_in_txs" not in names

    # Excluded-by-omission endpoints MUST NOT be exposed
    assert "bulk_csv" not in names
    assert "bulk_json" not in names
    assert "report_tag" not in names


async def test_external_tool_skipped_without_config(bundled_mcp):
    """search_neighbors is enabled in curation but not configured -> not registered."""
    async with Client(bundled_mcp) as c:
        names = {t.name for t in await c.list_tools()}
    assert "search_neighbors" not in names


async def test_external_tool_registered_when_configured(monkeypatch):
    from graphsenselib.web.app import create_spec_app

    monkeypatch.setenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", "https://upstream.example")
    monkeypatch.setenv("GS_MCP_SEARCH_NEIGHBORS__API_KEY_ENV", "TEST_KEY_FOR_SN")
    monkeypatch.setenv("TEST_KEY_FOR_SN", "whatever")

    app = create_spec_app()
    cfg = GSMCPConfig()
    mcp, stack = build_mcp(app, cfg)
    async with stack:
        async with Client(mcp) as c:
            names = {t.name for t in await c.list_tools()}
    assert "search_neighbors" in names


async def test_curated_descriptions_applied(bundled_mcp):
    async with Client(bundled_mcp) as c:
        tools = {t.name: t for t in await c.list_tools()}
    stats = tools["get_statistics"]
    assert "snapshot" in (stats.description or "").lower()
    assert "API: GET /stats" in (stats.description or "")


async def test_spec_app_auto_mounts_mcp(monkeypatch):
    """create_spec_app auto-attaches the MCP at /mcp. Verify the mount exists
    and is reachable via in-process ASGI.
    """
    import httpx

    from graphsenselib.web.app import create_spec_app

    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    monkeypatch.setenv("GS_MCP_PATH", "/mcp")

    app = create_spec_app()
    mounted_paths = [r.path for r in app.routes if hasattr(r, "path")]
    assert "/mcp" in mounted_paths

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://graphsense"
    ) as client:
        # A bare GET on the streamable-http endpoint without the required MCP
        # headers should be answered by the mounted sub-app — directly (4xx)
        # or via the trailing-slash redirect (307).
        response = await client.get("/mcp")
        assert response.status_code in {307, 400, 405, 406, 415}


async def test_instructions_sent_on_handshake(monkeypatch):
    """FastMCP should forward config.resolved_instructions() to the client
    via InitializeResult.instructions (MCP's server-provided system prompt).
    """
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    monkeypatch.setenv("GS_MCP_INSTRUCTIONS", "marker-from-test")

    from graphsenselib.mcp import GSMCPConfig, build_mcp
    from graphsenselib.web.app import create_spec_app

    app = create_spec_app()
    mcp, stack = build_mcp(app, GSMCPConfig())
    async with stack:
        async with Client(mcp) as c:
            assert c.initialize_result.instructions == "marker-from-test"


async def test_mcp_trailing_slash_redirect_is_relative(monkeypatch):
    """The /mcp -> /mcp/ redirect must use a relative Location. Starlette's
    Mount default emits an absolute URL derived from the Host header, which
    leaks the upstream hostname behind a reverse proxy that rewrites Host
    to the internal upstream (e.g., APISIX with pass_host: node). A relative
    Location keeps the client on the origin it already connected to.
    """
    import httpx

    from graphsenselib.web.app import create_spec_app

    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    monkeypatch.setenv("GS_MCP_PATH", "/mcp")

    app = create_spec_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://graphsense"
    ) as client:
        for method in ("GET", "POST", "DELETE"):
            r = await client.request(
                method,
                "/mcp",
                headers={"Host": "gs-rest:9000"},
                follow_redirects=False,
            )
            assert r.status_code == 307, (method, r.status_code)
            location = r.headers.get("location")
            assert location == "/mcp/", (method, location)


async def test_attach_to_fastapi_twice_raises(monkeypatch):
    """Calling attach_to_fastapi on an already-attached app is a programming
    error. With auto-attach in create_spec_app, a single extra manual attach
    reproduces this.
    """
    from graphsenselib.mcp import MCPBootstrapError
    from graphsenselib.web.app import create_spec_app

    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", raising=False)
    app = create_spec_app()  # already attached
    with pytest.raises(MCPBootstrapError):
        attach_to_fastapi(app, GSMCPConfig())
