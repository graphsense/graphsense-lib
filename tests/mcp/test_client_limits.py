"""Claude Code truncates the server instructions and every tool description
at 2048 characters before they reach the model. It does so silently and the
limit is not part of the MCP spec, so nothing else would catch it.
"""

import pytest
from fastmcp import Client

from graphsenselib.mcp import GSMCPConfig, build_mcp

CLIENT_TRUNCATION_LIMIT = 2048


@pytest.mark.parametrize("open_url_enabled", [False, True])
async def test_instructions_and_tool_descriptions_fit_client_limit(
    monkeypatch, open_url_enabled
):
    monkeypatch.delenv("GS_MCP_INSTRUCTIONS", raising=False)
    monkeypatch.delenv("GS_MCP_INSTRUCTIONS_FILE", raising=False)
    monkeypatch.delenv("GS_MCP_PATHFINDER_BASE_URL", raising=False)
    monkeypatch.setenv(
        "GS_MCP_PATHFINDER_OPEN_URL_ENABLED", str(open_url_enabled).lower()
    )
    # Registers search_neighbors too; a refused port, never contacted here.
    monkeypatch.setenv("GS_MCP_SEARCH_NEIGHBORS__BASE_URL", "http://127.0.0.1:1")
    monkeypatch.delenv("GS_MCP_SEARCH_NEIGHBORS__API_KEY_ENV", raising=False)

    from graphsenselib.web.app import create_spec_app

    mcp, stack = build_mcp(create_spec_app(), GSMCPConfig())
    async with stack, Client(mcp) as c:
        tools = await c.list_tools()

    assert "search_neighbors" in {t.name for t in tools}
    assert len(mcp.instructions or "") <= CLIENT_TRUNCATION_LIMIT
    too_long = {
        t.name: len(t.description or "")
        for t in tools
        if len(t.description or "") > CLIENT_TRUNCATION_LIMIT
    }
    assert too_long == {}
