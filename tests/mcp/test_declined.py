"""A tool result that leaves out the externally served networks says so.

A merged REST answer whose external backend did not answer is a 200 with the
backend named in X-Ikn-Backends-Declined. The model only reads the tool
result: without a note, a search for a bnb address during an outage of the
lite service reads as "not found on any network"."""

from __future__ import annotations

from contextlib import AsyncExitStack
from types import SimpleNamespace

from fastapi import FastAPI, Query, Response
from fastmcp import Client, FastMCP

from graphsenselib.mcp.declined import (
    DeclinedBackendsNoteMiddleware,
    declined_note,
    networks_by_backend,
    record_declined,
)
from graphsenselib.web.config import ExternalBackendsConfig
from graphsenselib.web.middleware.external_backends import DECLINED_HEADER

LITE_URL = "http://external-backend-adapter:9876"
NETWORKS = {LITE_URL: ["arb", "bnb"]}
STATS = {"currencies": [{"name": "btc", "no_blocks": 1}]}


def test_the_note_names_the_missing_networks_and_no_backend_url():
    note = declined_note({LITE_URL}, NETWORKS)
    assert "arb, bnb" in note
    assert "not checked, not not found" in note
    assert LITE_URL not in note


def test_an_unknown_backend_still_gets_a_note():
    note = declined_note({"http://elsewhere"}, NETWORKS)
    assert "the externally served networks" in note
    assert "http://elsewhere" not in note


def test_networks_by_backend_reads_the_rest_config():
    config = ExternalBackendsConfig(
        enabled=True,
        networks={"bnb": {"url": LITE_URL}, "arb": {"url": LITE_URL}},
    )
    app = SimpleNamespace(
        state=SimpleNamespace(config=SimpleNamespace(external_backends=config))
    )
    assert networks_by_backend(app) == NETWORKS


def test_no_external_backends_means_no_mapping():
    assert networks_by_backend(SimpleNamespace(state=SimpleNamespace())) == {}
    disabled = ExternalBackendsConfig(
        enabled=False, networks={"bnb": {"url": LITE_URL}}
    )
    app = SimpleNamespace(
        state=SimpleNamespace(config=SimpleNamespace(external_backends=disabled))
    )
    assert networks_by_backend(app) == {}


def _stats_app(declined: bool) -> FastAPI:
    app = FastAPI()

    @app.get("/stats", operation_id="get_statistics")
    async def _stats(response: Response):
        if declined:
            response.headers[DECLINED_HEADER] = LITE_URL
        return STATS

    return app


def _generated_tools(app: FastAPI) -> FastMCP:
    """As server.build_mcp wires them: the hook on the generated tools'
    client, the note middleware on the server."""
    mcp = FastMCP.from_fastapi(
        app=app,
        httpx_client_kwargs={"event_hooks": {"response": [record_declined]}},
    )
    mcp.add_middleware(DeclinedBackendsNoteMiddleware(NETWORKS))
    return mcp


async def test_a_generated_tool_result_carries_the_note():
    async with Client(_generated_tools(_stats_app(declined=True))) as client:
        result = await client.call_tool("get_statistics", {})
    assert result.content[0].text == declined_note({LITE_URL}, NETWORKS)
    # the answer itself is unchanged
    assert result.structured_content == STATS


async def test_a_complete_answer_gets_no_note():
    async with Client(_generated_tools(_stats_app(declined=False))) as client:
        result = await client.call_tool("get_statistics", {})
    assert all("Partial result" not in part.text for part in result.content)
    assert result.structured_content == STATS


async def test_a_consolidated_tool_result_carries_the_note():
    """lookup_address with cross-chain addresses reads the twin list, which
    the REST app merges from the external backend."""
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    app = FastAPI()

    @app.get("/{currency}/addresses/{address}")
    async def _addr(currency: str, address: str):
        return {"currency": currency, "address": address}

    @app.get("/{currency}/addresses/{address}/related_addresses")
    async def _related(
        response: Response,
        currency: str,
        address: str,
        address_relation_type: str = Query("pubkey"),
    ):
        response.headers[DECLINED_HEADER] = LITE_URL
        return {"related_addresses": []}

    mcp = FastMCP(name="test")
    register_lookup_address(mcp, app, AsyncExitStack())
    mcp.add_middleware(DeclinedBackendsNoteMiddleware(NETWORKS))
    async with Client(mcp) as client:
        result = await client.call_tool(
            "lookup_address",
            {
                "currency": "eth",
                "address": "0xabc",
                "include_cluster": False,
                "include_tag_summary": False,
                "include_cross_chain_addresses": True,
            },
        )
    assert result.content[0].text == declined_note({LITE_URL}, NETWORKS)
    assert result.structured_content["cross_chain_addresses"] == {
        "related_addresses": []
    }
