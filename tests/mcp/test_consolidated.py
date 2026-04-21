"""Focused behavior tests for consolidated tools using minimal stub FastAPI
apps. Complements test_server_integration.py (which builds against the
real graphsense create_spec_app) by exercising specific code paths —
best_cluster_tag exposure, 404 tolerance, cross-chain addresses, and
path-segment validation — without needing a real backend.
"""

from __future__ import annotations

from contextlib import AsyncExitStack

import pytest
from fastapi import FastAPI, HTTPException, Query
from fastmcp import Client, FastMCP


def _tool(stub_app: FastAPI, register_fn) -> FastMCP:
    mcp = FastMCP(name="test")
    register_fn(mcp, stub_app, AsyncExitStack())
    return mcp


@pytest.fixture
def stub_app_with_cluster() -> FastAPI:
    """Stub returning a cluster body with best_address_tag."""
    app = FastAPI()

    @app.get("/{currency}/addresses/{address}")
    async def _addr(currency: str, address: str):
        return {
            "currency": currency,
            "address": address,
            "balance": {"value": 123, "fiat_values": []},
        }

    @app.get("/{currency}/addresses/{address}/entity")
    async def _cluster(currency: str, address: str):
        return {
            "cluster": 42,
            "best_address_tag": {"label": "Known Exchange", "source": "test"},
        }

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def _ts(currency: str, address: str):
        return {"tag_count": 0}

    @app.get("/{currency}/addresses/{address}/tags")
    async def _tags(currency: str, address: str):
        return {"address_tags": []}

    @app.get("/{currency}/addresses/{address}/related_addresses")
    async def _related(
        currency: str, address: str, address_relation_type: str = Query("pubkey")
    ):
        return {
            "related_addresses": [
                {
                    "address": "other-chain-addr",
                    "currency": "bch",
                    "relation_type": "pubkey",
                }
            ]
        }

    return app


@pytest.fixture
def stub_app_no_cluster() -> FastAPI:
    """Stub where /addresses/{addr}/entity returns 404."""
    app = FastAPI()

    @app.get("/{currency}/addresses/{address}")
    async def _addr(currency: str, address: str):
        return {"address": address}

    @app.get("/{currency}/addresses/{address}/entity")
    async def _cluster_missing(currency: str, address: str):
        raise HTTPException(status_code=404, detail="no cluster")

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def _ts(currency: str, address: str):
        return {"tag_count": 0}

    @app.get("/{currency}/addresses/{address}/tags")
    async def _tags(currency: str, address: str):
        return {"address_tags": []}

    return app


async def test_best_cluster_tag_surfaced_even_with_all_flags_false(
    stub_app_with_cluster,
):
    """The best_cluster_tag must appear at the top level regardless of
    include_tags / include_cluster / include_tag_summary settings — that's
    the whole point of the unconditional-exposure change.
    """
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_with_cluster, register_lookup_address)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_address",
            {
                "currency": "btc",
                "address": "abc",
                "include_tags": False,
                "include_cluster": False,
                "include_tag_summary": False,
            },
        )
        data = r.structured_content
        assert data is not None
        assert data["best_cluster_tag"] == {
            "label": "Known Exchange",
            "source": "test",
        }
        assert "cluster" not in data
        assert "tag_summary" not in data
        assert "tags" not in data
        assert "cross_chain_addresses" not in data


async def test_lookup_address_tolerates_missing_cluster(stub_app_no_cluster):
    """A 404 on /addresses/{addr}/entity must not fail the whole call;
    best_cluster_tag should be None and cluster should be absent, but the
    base address body and the requested optional fields must still come
    back.
    """
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_no_cluster, register_lookup_address)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_address",
            {
                "currency": "btc",
                "address": "abc",
                "include_tags": True,
                "include_cluster": True,
                "include_tag_summary": True,
            },
        )
        data = r.structured_content
        assert data is not None
        assert data["best_cluster_tag"] is None
        assert "cluster" not in data  # 404 on fetch -> not merged
        assert data["address"] == {"address": "abc"}
        assert data["tag_summary"] == {"tag_count": 0}
        assert data["tags"] == {"address_tags": []}


async def test_cross_chain_addresses_populates_field(stub_app_with_cluster):
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_with_cluster, register_lookup_address)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_address",
            {
                "currency": "btc",
                "address": "abc",
                "include_tags": False,
                "include_cluster": False,
                "include_tag_summary": False,
                "include_cross_chain_addresses": True,
            },
        )
        data = r.structured_content
        assert data is not None
        assert "cross_chain_addresses" in data
        entries = data["cross_chain_addresses"]["related_addresses"]
        assert len(entries) == 1
        assert entries[0]["currency"] == "bch"
        assert entries[0]["address"] == "other-chain-addr"


@pytest.fixture
def stub_app_with_tx() -> FastAPI:
    """Stub for lookup_tx_details — implements the endpoints it hits."""
    app = FastAPI()

    @app.get("/{currency}/txs/{tx_hash}")
    async def _tx(currency: str, tx_hash: str):
        return {"currency": currency, "tx_hash": tx_hash, "inputs": [], "outputs": []}

    @app.get("/{currency}/txs/{tx_hash}/conversions")
    async def _conv(currency: str, tx_hash: str):
        return [
            {
                "conversion_type": "dex_swap",
                "from_address": "a1",
                "to_address": "a2",
                "from_asset": "USDC",
                "to_asset": "ETH",
                "from_amount": "1000",
            },
            {
                "conversion_type": "bridge_tx",
                "from_address": "a3",
                "to_address": "a4",
                "from_asset": "ETH",
                "to_asset": "WETH",
                "from_amount": "1",
            },
        ]

    return app


async def test_lookup_tx_details_conversions_populated(stub_app_with_tx):
    """include_conversions=True appends the unified swap+bridge list."""
    from graphsenselib.mcp.tools.consolidated import register_lookup_tx_details

    mcp = _tool(stub_app_with_tx, register_lookup_tx_details)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_tx_details",
            {"currency": "eth", "tx_hash": "abc", "include_conversions": True},
        )
        data = r.structured_content
        assert data is not None
        assert "conversions" in data
        conversions = data["conversions"]
        assert len(conversions) == 2
        types = {c["conversion_type"] for c in conversions}
        assert types == {"dex_swap", "bridge_tx"}


async def test_lookup_tx_details_conversions_absent_by_default(stub_app_with_tx):
    """Default behavior: no conversions key when include_conversions=False."""
    from graphsenselib.mcp.tools.consolidated import register_lookup_tx_details

    mcp = _tool(stub_app_with_tx, register_lookup_tx_details)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_tx_details",
            {"currency": "eth", "tx_hash": "abc"},
        )
        data = r.structured_content
        assert data is not None
        assert "conversions" not in data


async def test_invalid_currency_rejected(stub_app_with_cluster):
    """Path-segment validation must refuse currency values that don't
    match [a-z0-9]{2,10}. Guards against URL-path escape attempts.
    """
    from fastmcp.exceptions import ToolError

    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_with_cluster, register_lookup_address)
    async with Client(mcp) as c:
        with pytest.raises(ToolError, match="Invalid currency"):
            await c.call_tool(
                "lookup_address",
                {"currency": "../admin", "address": "abc"},
            )


async def test_invalid_address_rejected(stub_app_with_cluster):
    from fastmcp.exceptions import ToolError

    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_with_cluster, register_lookup_address)
    async with Client(mcp) as c:
        with pytest.raises(ToolError, match="Invalid address"):
            await c.call_tool(
                "lookup_address",
                {"currency": "btc", "address": "abc/../etc"},
            )
