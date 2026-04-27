"""Focused behavior tests for consolidated tools using minimal stub FastAPI
apps. Complements test_server_integration.py (which builds against the
real graphsense create_spec_app) by exercising specific code paths —
legacy-field stripping, tag_summary defaults, 404 tolerance, cross-chain
addresses, and path-segment validation — without needing a real backend.
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
    """Stub returning a cluster body with legacy fields. The MCP wrapper
    must strip `actors` and `best_address_tag` from the cluster body and
    `actors` from the address body.
    """
    app = FastAPI()

    @app.get("/{currency}/addresses/{address}")
    async def _addr(currency: str, address: str):
        return {
            "currency": currency,
            "address": address,
            "balance": {"value": 123, "fiat_values": []},
            "actors": [{"id": "actor-x", "label": "X"}],
        }

    @app.get("/{currency}/addresses/{address}/entity")
    async def _cluster(currency: str, address: str):
        return {
            "cluster": 42,
            "best_address_tag": {"label": "Known Exchange", "source": "test"},
            "actors": [{"id": "actor-x", "label": "X"}],
            "no_addresses": 7,
        }

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def _ts(currency: str, address: str):
        return {
            "tag_count": 1,
            "broad_category": "exchange",
            "best_label": "Known Exchange",
            "best_actor": "actor-x",
            "label_summary": {
                "Known Exchange": {
                    "label": "Known Exchange",
                    "count": 1,
                    "confidence": 0.9,
                    "relevance": 1.0,
                    "creators": ["GraphSense Core Team"],
                    "sources": ["https://example.test/tagpacks/known.yaml"],
                    "concepts": ["exchange"],
                    "lastmod": 1700000000,
                }
            },
            "concept_tag_cloud": {"exchange": {"cnt": 1, "weighted": 1.0}},
        }

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
        return {"tag_count": 0, "broad_category": "unknown"}

    return app


async def test_lookup_address_strips_legacy_fields(stub_app_with_cluster):
    """The legacy `actors` field on the address body and `actors` /
    `best_address_tag` on the cluster body must be removed — the MCP
    contract exposes tag context only via `tag_summary`. No top-level
    `best_cluster_tag` is surfaced.
    """
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_with_cluster, register_lookup_address)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_address",
            {
                "currency": "btc",
                "address": "abc",
                "include_cluster": True,
                "include_tag_summary": True,
            },
        )
        data = r.structured_content
        assert data is not None
        assert "best_cluster_tag" not in data
        assert "actors" not in data["address"]
        assert "actors" not in data["cluster"]
        assert "best_address_tag" not in data["cluster"]
        # tag_summary is reformatted to the slim LLM-friendly shape:
        # provenance fields (creators, lastmod, per-label concepts) gone,
        # `label_summary` -> `labels`, `concept_tag_cloud` -> `concepts`
        # flattened to label->weight.
        ts = data["tag_summary"]
        assert ts["tag_count"] == 1
        assert ts["broad_category"] == "exchange"
        assert ts["best_label"] == "Known Exchange"
        assert ts["best_actor"] == "actor-x"
        label_entry = ts["labels"]["Known Exchange"]
        assert label_entry == {
            "count": 1,
            "confidence": 0.9,
            "relevance": 1.0,
            "sources": ["https://example.test/tagpacks/known.yaml"],
        }
        assert ts["concepts"] == {"exchange": 1.0}


async def test_lookup_address_tolerates_missing_cluster(stub_app_no_cluster):
    """A 404 on /addresses/{addr}/entity must not fail the whole call;
    `cluster` should be absent, and the base address body and
    `tag_summary` must still come back.
    """
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_no_cluster, register_lookup_address)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_address",
            {
                "currency": "btc",
                "address": "abc",
                "include_cluster": True,
                "include_tag_summary": True,
            },
        )
        data = r.structured_content
        assert data is not None
        assert "best_cluster_tag" not in data
        assert "cluster" not in data
        assert data["address"] == {"address": "abc"}
        # Slim-shape tag_summary always carries the four canonical fields.
        assert data["tag_summary"]["tag_count"] == 0
        assert data["tag_summary"]["broad_category"] == "unknown"
        assert "labels" not in data["tag_summary"]  # empty label_summary omitted


async def test_cross_chain_addresses_populates_field(stub_app_with_cluster):
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    mcp = _tool(stub_app_with_cluster, register_lookup_address)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "lookup_address",
            {
                "currency": "btc",
                "address": "abc",
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


async def test_lookup_cluster_strips_legacy_fields(stub_app_with_cluster):
    """lookup_cluster must drop legacy `actors` / `best_address_tag` from
    the cluster body and must not surface a top-level `best_cluster_tag`.
    """
    from graphsenselib.mcp.tools.consolidated import register_lookup_cluster

    app = stub_app_with_cluster

    @app.get("/{currency}/clusters/{cluster}")
    async def _cluster(currency: str, cluster: int):
        return {
            "cluster": cluster,
            "best_address_tag": {"label": "Known Exchange", "source": "test"},
            "actors": [{"id": "actor-x", "label": "X"}],
            "no_addresses": 7,
        }

    mcp = _tool(app, register_lookup_cluster)
    async with Client(mcp) as c:
        r = await c.call_tool("lookup_cluster", {"currency": "btc", "cluster": 42})
        data = r.structured_content
        assert data is not None
        assert "best_cluster_tag" not in data
        assert "actors" not in data["cluster"]
        assert "best_address_tag" not in data["cluster"]
        assert data["cluster"]["no_addresses"] == 7


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


def test_slim_tag_summary_full_input():
    """Direct unit test for the slim helper: drops creators/lastmod/
    per-label concepts/inherited_from, flattens concept_tag_cloud, and
    renames label_summary→labels / concept_tag_cloud→concepts.
    """
    from graphsenselib.mcp.tools.consolidated import _slim_tag_summary

    src = {
        "broad_category": "organization",
        "tag_count": 2,
        "tag_count_indirect": 5,
        "best_label": "internet archive",
        "best_actor": "actor-archive",
        "label_summary": {
            "internet archive": {
                "label": "internet archive",
                "count": 2,
                "confidence": 0.7,
                "relevance": 0.9,
                "creators": ["GraphSense Core Team"],
                "sources": ["https://archive.org/donate/cryptocurrency"],
                "concepts": ["organization"],
                "lastmod": 1636675200,
                "inherited_from": None,
            }
        },
        "concept_tag_cloud": {
            "organization": {"cnt": 2, "weighted": 1.5},
            "donation": {"cnt": 1, "weighted": 0.4},
        },
    }
    out = _slim_tag_summary(src)
    assert out == {
        "tag_count": 2,
        "broad_category": "organization",
        "best_label": "internet archive",
        "best_actor": "actor-archive",
        "tag_count_indirect": 5,
        "labels": {
            "internet archive": {
                "count": 2,
                "confidence": 0.7,
                "relevance": 0.9,
                "sources": ["https://archive.org/donate/cryptocurrency"],
            }
        },
        "concepts": {"organization": 1.5, "donation": 0.4},
    }


def test_slim_tag_summary_handles_none_and_empty():
    from graphsenselib.mcp.tools.consolidated import _slim_tag_summary

    assert _slim_tag_summary(None) is None
    # Empty dict still produces the canonical four fields (all None).
    assert _slim_tag_summary({}) == {
        "tag_count": None,
        "broad_category": None,
        "best_label": None,
        "best_actor": None,
    }


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


@pytest.fixture
def stub_app_with_neighbors() -> FastAPI:
    """Stub exposing neighbors + per-address tag_summary for enrichment."""
    app = FastAPI()

    @app.get("/{currency}/addresses/{address}/neighbors")
    async def _neighbors(
        currency: str,
        address: str,
        direction: str = Query("out"),
        include_actors: bool | None = Query(None),
        include_labels: bool | None = Query(None),
    ):
        # Echo the suppression query params so the test can assert the
        # wrapper sent the legacy-disabling values to the upstream.
        return {
            "neighbors": [
                {
                    "value": {"value": 1, "fiat_values": []},
                    "no_txs": 2,
                    "address": {
                        "address": "n1",
                        "balance": {"value": 1, "fiat_values": []},
                        # Defensive: simulate upstream still emitting actors
                        # despite include_actors=False — wrapper must strip.
                        "actors": [{"id": "a", "label": "L"}],
                    },
                    "labels": ["should-be-stripped"],
                },
                {
                    "value": {"value": 2, "fiat_values": []},
                    "no_txs": 3,
                    "address": {
                        "address": "n2",
                        "balance": {"value": 2, "fiat_values": []},
                    },
                },
            ],
            "next_page": None,
            "_received": {
                "include_actors": include_actors,
                "include_labels": include_labels,
                "direction": direction,
            },
        }

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def _tag_summary(currency: str, address: str):
        # n1 looks like an exchange; n2 has only a generic concept hit.
        if address == "n1":
            return {
                "tag_count": 2,
                "broad_category": "exchange",
                "best_label": "Coinbase 3",
                "best_actor": "coinbase",
                "label_summary": {
                    "Coinbase 3": {"count": 2, "confidence": 0.9, "relevance": 1.0}
                },
                "concept_tag_cloud": {"exchange": {"cnt": 2, "weighted": 1.5}},
            }
        return {
            "tag_count": 1,
            "broad_category": "service",
            "best_label": f"label-for-{address}",
            "label_summary": {
                f"label-for-{address}": {
                    "count": 1,
                    "confidence": 0.4,
                    "relevance": 0.5,
                }
            },
            "concept_tag_cloud": {"service": {"cnt": 1, "weighted": 0.5}},
        }

    return app


async def test_list_neighbors_strips_legacy_fields(stub_app_with_neighbors):
    """In non-compact mode the wrapper still strips `labels` from each
    row and `actors` from the nested address, and forwards
    `include_actors=False` / `include_labels=False` to the upstream.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_neighbors",
            {
                "currency": "btc",
                "address": "abc",
                "include_tag_summary": False,
                "compact": False,
            },
        )
        data = r.structured_content
        assert data is not None
        for n in data["neighbors"]:
            assert "labels" not in n
            assert isinstance(n["address"], dict)
            assert "actors" not in n["address"]
        # Suppression flags reached the upstream
        assert data["_received"]["include_actors"] is False
        assert data["_received"]["include_labels"] is False


async def test_list_neighbors_compact_default(stub_app_with_neighbors):
    """compact=True (default) flattens each row: `address` becomes a
    bare string and the heavy nested balance/totals block is dropped.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_neighbors",
            {
                "currency": "btc",
                "address": "abc",
                "include_tag_summary": False,
            },
        )
        data = r.structured_content
        assert data is not None
        addrs = {n["address"] for n in data["neighbors"]}
        assert addrs == {"n1", "n2"}
        for n in data["neighbors"]:
            assert isinstance(n["address"], str)
            # No leftover legacy/heavy blocks at the row level
            assert "balance" not in n
            assert "actors" not in n
            assert "labels" not in n


async def test_list_neighbors_tag_filter_matches_label(stub_app_with_neighbors):
    """tag_filter is a case-insensitive substring match against best_actor,
    best_label, broad_category, label keys, and concept keys.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        # Match by label — only n1 has "Coinbase" in its best_label.
        r = await c.call_tool(
            "list_neighbors",
            {"currency": "btc", "address": "abc", "tag_filter": "coinbase"},
        )
        data = r.structured_content
        assert data is not None
        addrs = [n["address"] for n in data["neighbors"]]
        assert addrs == ["n1"]
        assert data["neighbors"][0]["tag_summary"]["best_label"] == "Coinbase 3"


async def test_list_neighbors_tag_filter_matches_category(stub_app_with_neighbors):
    """Filter by broad_category — both "exchange" (n1) and category-only
    matches must be supported.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_neighbors",
            {"currency": "btc", "address": "abc", "tag_filter": "exchange"},
        )
        data = r.structured_content
        assert data is not None
        addrs = [n["address"] for n in data["neighbors"]]
        assert addrs == ["n1"]


async def test_list_neighbors_tag_filter_matches_concept(stub_app_with_neighbors):
    """tag_filter must also match concept-dict keys, not just labels."""
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_neighbors",
            {"currency": "btc", "address": "abc", "tag_filter": "service"},
        )
        data = r.structured_content
        assert data is not None
        addrs = [n["address"] for n in data["neighbors"]]
        assert addrs == ["n2"]


async def test_list_neighbors_tag_filter_forces_tag_summary(stub_app_with_neighbors):
    """Setting tag_filter must override include_tag_summary=False — the
    wrapper needs the data to filter against, otherwise no row could match.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_neighbors",
            {
                "currency": "btc",
                "address": "abc",
                "tag_filter": "coinbase",
                "include_tag_summary": False,  # should be ignored
            },
        )
        data = r.structured_content
        assert data is not None
        assert len(data["neighbors"]) == 1
        assert "tag_summary" in data["neighbors"][0]


async def test_list_neighbors_enriches_tag_summary_by_default(stub_app_with_neighbors):
    """include_tag_summary defaults to True — each neighbor row should
    gain a `tag_summary` field via a per-neighbor lookup.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool("list_neighbors", {"currency": "btc", "address": "abc"})
        data = r.structured_content
        assert data is not None
        labels = {n["tag_summary"]["best_label"] for n in data["neighbors"]}
        assert labels == {"Coinbase 3", "label-for-n2"}


async def test_list_neighbors_no_tag_summary_when_disabled(stub_app_with_neighbors):
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    mcp = _tool(stub_app_with_neighbors, register_list_neighbors)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_neighbors",
            {"currency": "btc", "address": "abc", "include_tag_summary": False},
        )
        data = r.structured_content
        assert data is not None
        assert all("tag_summary" not in n for n in data["neighbors"])


@pytest.fixture
def stub_app_with_txs_and_links() -> FastAPI:
    """Stub exposing both /txs and /links for an address."""
    app = FastAPI()

    @app.get("/{currency}/addresses/{address}/txs")
    async def _txs(
        currency: str,
        address: str,
        direction: str | None = Query(None),
        neighbor: str | None = Query(None),
    ):
        return {
            "address_txs": [
                {"_endpoint": "txs", "direction": direction, "address": address}
            ],
            "next_page": None,
        }

    @app.get("/{currency}/addresses/{address}/links")
    async def _links(
        currency: str,
        address: str,
        neighbor: str = Query(...),
    ):
        return {
            "links": [{"_endpoint": "links", "neighbor": neighbor, "address": address}],
            "next_page": None,
        }

    return app


async def test_list_txs_for_without_neighbor_hits_txs_endpoint(
    stub_app_with_txs_and_links,
):
    from graphsenselib.mcp.tools.consolidated import register_list_txs_for

    mcp = _tool(stub_app_with_txs_and_links, register_list_txs_for)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_txs_for",
            {"currency": "btc", "address": "abc", "direction": "out"},
        )
        data = r.structured_content
        assert data is not None
        assert "address_txs" in data
        assert data["address_txs"][0]["_endpoint"] == "txs"
        assert data["address_txs"][0]["direction"] == "out"


async def test_list_txs_for_with_neighbor_hits_links_endpoint(
    stub_app_with_txs_and_links,
):
    from graphsenselib.mcp.tools.consolidated import register_list_txs_for

    mcp = _tool(stub_app_with_txs_and_links, register_list_txs_for)
    async with Client(mcp) as c:
        r = await c.call_tool(
            "list_txs_for",
            {"currency": "btc", "address": "abc", "neighbor": "def"},
        )
        data = r.structured_content
        assert data is not None
        assert "links" in data
        assert data["links"][0]["_endpoint"] == "links"
        assert data["links"][0]["neighbor"] == "def"


async def test_list_txs_for_rejects_direction_with_neighbor(
    stub_app_with_txs_and_links,
):
    from fastmcp.exceptions import ToolError

    from graphsenselib.mcp.tools.consolidated import register_list_txs_for

    mcp = _tool(stub_app_with_txs_and_links, register_list_txs_for)
    async with Client(mcp) as c:
        with pytest.raises(ToolError, match="direction cannot be combined"):
            await c.call_tool(
                "list_txs_for",
                {
                    "currency": "btc",
                    "address": "abc",
                    "neighbor": "def",
                    "direction": "out",
                },
            )


async def test_list_txs_for_validates_neighbor(stub_app_with_txs_and_links):
    from fastmcp.exceptions import ToolError

    from graphsenselib.mcp.tools.consolidated import register_list_txs_for

    mcp = _tool(stub_app_with_txs_and_links, register_list_txs_for)
    async with Client(mcp) as c:
        with pytest.raises(ToolError, match="Invalid neighbor"):
            await c.call_tool(
                "list_txs_for",
                {"currency": "btc", "address": "abc", "neighbor": "bad/../path"},
            )


@pytest.fixture
def recording_tag_app() -> tuple[FastAPI, list[dict]]:
    """Stub that records query params received on /tag_summary and /tags
    so tests can assert the wrappers always send `include_best_cluster_tag`.
    """
    app = FastAPI()
    received: list[dict] = []

    @app.get("/{currency}/addresses/{address}")
    async def _addr(currency: str, address: str):
        return {"address": address}

    @app.get("/{currency}/addresses/{address}/entity")
    async def _cluster(currency: str, address: str):
        raise HTTPException(status_code=404, detail="no cluster")

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def _ts(
        currency: str,
        address: str,
        include_best_cluster_tag: str | None = Query(None),
    ):
        received.append(
            {
                "endpoint": "tag_summary",
                "address": address,
                "include_best_cluster_tag": include_best_cluster_tag,
            }
        )
        return {"tag_count": 0, "broad_category": "unknown"}

    @app.get("/{currency}/addresses/{address}/tags")
    async def _tags(
        currency: str,
        address: str,
        page: str | None = Query(None),
        pagesize: int | None = Query(None),
        include_best_cluster_tag: str | None = Query(None),
    ):
        received.append(
            {
                "endpoint": "tags",
                "address": address,
                "include_best_cluster_tag": include_best_cluster_tag,
                "page": page,
                "pagesize": pagesize,
            }
        )
        return {"address_tags": [], "next_page": None}

    return app, received


async def test_lookup_address_passes_include_best_cluster_tag(recording_tag_app):
    """The tag_summary call inside lookup_address must always send
    `include_best_cluster_tag=true` for UI parity.
    """
    from graphsenselib.mcp.tools.consolidated import register_lookup_address

    app, received = recording_tag_app
    mcp = _tool(app, register_lookup_address)
    async with Client(mcp) as c:
        await c.call_tool(
            "lookup_address",
            {"currency": "btc", "address": "abc", "include_tag_summary": True},
        )
    ts_calls = [r for r in received if r["endpoint"] == "tag_summary"]
    assert len(ts_calls) == 1
    assert ts_calls[0]["include_best_cluster_tag"] == "true"


async def test_list_tags_by_address_always_sends_include_best_cluster_tag(
    recording_tag_app,
):
    """The list_tags_by_address wrapper always forwards
    `include_best_cluster_tag=true` to the upstream (UI parity); it is
    not exposed as a caller-overridable parameter.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_tags_by_address

    app, received = recording_tag_app
    mcp = _tool(app, register_list_tags_by_address)
    async with Client(mcp) as c:
        await c.call_tool(
            "list_tags_by_address",
            {"currency": "btc", "address": "abc"},
        )
    tag_calls = [r for r in received if r["endpoint"] == "tags"]
    assert len(tag_calls) == 1
    assert tag_calls[0]["include_best_cluster_tag"] == "true"


async def test_list_neighbors_passes_include_best_cluster_tag_in_enrichment():
    """list_neighbors enriches each neighbor with tag_summary and the
    enrichment call must pass include_best_cluster_tag=true.
    """
    from graphsenselib.mcp.tools.consolidated import register_list_neighbors

    app = FastAPI()
    received: list[dict] = []

    @app.get("/{currency}/addresses/{address}/neighbors")
    async def _neighbors(currency: str, address: str, direction: str = Query("out")):
        return {
            "neighbors": [
                {
                    "value": {"value": 1, "fiat_values": []},
                    "no_txs": 1,
                    "address": {"address": "n1"},
                }
            ],
            "next_page": None,
        }

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def _ts(
        currency: str,
        address: str,
        include_best_cluster_tag: str | None = Query(None),
    ):
        received.append(
            {"address": address, "include_best_cluster_tag": include_best_cluster_tag}
        )
        return {"tag_count": 0, "broad_category": "unknown"}

    mcp = _tool(app, register_list_neighbors)
    async with Client(mcp) as c:
        await c.call_tool("list_neighbors", {"currency": "btc", "address": "abc"})
    assert len(received) == 1
    assert received[0]["address"] == "n1"
    assert received[0]["include_best_cluster_tag"] == "true"
