"""External chain-data backends (middleware/external_backends.py).

Networks without a Cassandra keyspace can be served by a GraphSense-API-
compatible external backend (e.g. the iknaio external backend adapter).
These tests run without containers: a small local app stands in for the
routers and an httpx MockTransport stands in for the backend.
"""

import json

import httpx
from fastapi import FastAPI, Request
from starlette.testclient import TestClient

from graphsenselib.web.config import ExternalBackendsConfig, GSRestConfig
from graphsenselib.web.middleware.external_backends import ExternalBackendMiddleware

BACKEND_URL = "https://backend.test"

BACKEND_STATS = {
    "version": "backend-version",
    "currencies": [
        {"name": "bnb", "no_blocks": 42, "some_unmodeled_field": "kept"},
        {"name": "eth", "no_blocks": 9},  # NOT configured -> must be filtered
    ],
}

BACKEND_SEARCH = {
    "currencies": [
        {"currency": "bnb", "addresses": ["0xbnbhit"], "txs": []},
        {"currency": "eth", "addresses": ["0xethhit"], "txs": []},  # filtered
    ],
    "labels": [],
    "actors": [],
}


def make_client(enabled=True, api_key="backend-key"):
    """Local stand-in app + recording mock backend behind the middleware."""
    app = FastAPI()

    @app.get("/stats")
    async def stats():
        return {
            "version": "local-version",
            "currencies": [{"name": "btc", "no_blocks": 1}],
        }

    @app.get("/search")
    async def search(request: Request):
        return {
            "currencies": [{"currency": "btc", "addresses": ["1local"], "txs": []}],
            "labels": ["Binance"],
            "actors": [],
        }

    @app.get("/{currency}/blocks/{height}")
    async def block(currency: str, height: int):
        return {"served": "local", "currency": currency}

    @app.get("/{currency}/addresses/{address}/tags")
    async def address_tags(currency: str, address: str):
        return {"address_tags": ["local-tag"]}

    @app.get("/{currency}/addresses/{address}/tag_summary")
    async def tag_summary(currency: str, address: str):
        return {"summary": "local"}

    @app.get("/{currency}/entities/{entity}/tags")
    async def entity_tags(currency: str, entity: int):
        return {"address_tags": ["local-entity-tag"]}

    @app.post("/{currency}/bulk.json/{operation}")
    async def bulk(currency: str, operation: str):
        return {"served": "local"}

    seen: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/stats":
            return httpx.Response(200, json=BACKEND_STATS)
        if request.url.path == "/search":
            return httpx.Response(200, json=BACKEND_SEARCH)
        return httpx.Response(200, json={"backend_path": request.url.path})

    config = ExternalBackendsConfig(
        enabled=enabled,
        networks={"bnb": {"url": BACKEND_URL, "api_key": api_key}},
    )
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    return TestClient(app), seen


def test_disabled_is_pass_through():
    client, seen = make_client(enabled=False)
    response = client.get("/bnb/blocks/1")
    assert response.json() == {"served": "local", "currency": "bnb"}
    assert client.get("/stats").json()["currencies"] == [
        {"name": "btc", "no_blocks": 1}
    ]
    assert seen == []


def test_configured_network_paths_proxy():
    client, seen = make_client()
    response = client.get("/bnb/blocks/1?include_io=true")
    assert response.status_code == 200
    assert response.json() == {"backend_path": "/bnb/blocks/1"}
    assert response.headers["x-served-by"] == "external-backend"
    assert seen[-1].url.query == b"include_io=true"
    assert seen[-1].headers["authorization"] == "backend-key"


def test_other_networks_stay_local():
    client, seen = make_client()
    assert client.get("/btc/blocks/1").json() == {
        "served": "local",
        "currency": "btc",
    }
    assert client.get("/btc/addresses/1abc/tags").json() == {
        "address_tags": ["local-tag"]
    }
    assert seen == []


def test_address_tag_routes_of_external_network_stay_local():
    """TagStore data is keyed by real chain addresses and owned by this
    deployment, no matter who serves the chain data."""
    client, seen = make_client()
    assert client.get("/bnb/addresses/0xabc/tags").json() == {
        "address_tags": ["local-tag"]
    }
    assert client.get("/bnb/addresses/0xabc/tag_summary").json() == {"summary": "local"}
    assert seen == []


def test_entity_routes_of_external_network_proxy():
    """Entity/cluster ids are minted per-backend; the local TagStore must
    never be queried with a backend-minted id (and vice versa)."""
    client, seen = make_client()
    response = client.get("/bnb/entities/42/tags")
    assert response.json() == {"backend_path": "/bnb/entities/42/tags"}
    assert response.headers["x-served-by"] == "external-backend"


def test_post_body_is_forwarded():
    client, seen = make_client()
    response = client.post("/bnb/bulk.json/get_block", json={"height": [1, 2]})
    assert response.json() == {"backend_path": "/bnb/bulk.json/get_block"}
    assert json.loads(seen[-1].content) == {"height": [1, 2]}
    assert seen[-1].headers["content-type"] == "application/json"


def test_stats_merges_configured_networks_only():
    client, seen = make_client()
    body = client.get("/stats").json()
    # local scalars win: they describe THIS deployment
    assert body["version"] == "local-version"
    # local entries first, backend entries for CONFIGURED networks appended;
    # unmodeled backend fields survive (mirrored, not re-validated)
    assert body["currencies"] == [
        {"name": "btc", "no_blocks": 1},
        {"name": "bnb", "no_blocks": 42, "some_unmodeled_field": "kept"},
    ]
    assert seen[-1].url.path == "/stats"


def test_search_without_currency_merges():
    client, seen = make_client()
    body = client.get("/search?q=xyz").json()
    assert body["labels"] == ["Binance"]  # TagStore data stays local
    assert body["currencies"] == [
        {"currency": "btc", "addresses": ["1local"], "txs": []},
        {"currency": "bnb", "addresses": ["0xbnbhit"], "txs": []},
    ]
    assert seen[-1].url.path == "/search"
    assert seen[-1].url.query == b"q=xyz"


def test_search_filtered_to_external_network_proxies_outright():
    client, seen = make_client()
    response = client.get("/search?q=xyz&currency=bnb")
    # the backend's answer is mirrored verbatim, no local merge
    assert response.json() == BACKEND_SEARCH
    assert response.headers["x-served-by"] == "external-backend"
    assert seen[-1].url.query == b"q=xyz&currency=bnb"


def test_search_filtered_to_local_network_skips_backends():
    client, seen = make_client()
    body = client.get("/search?q=xyz&currency=btc").json()
    assert body["labels"] == ["Binance"]
    assert body["currencies"] == [
        {"currency": "btc", "addresses": ["1local"], "txs": []}
    ]
    assert seen == []


def test_backend_error_is_loud():
    """A broken backend must surface, not be shaped into an empty answer."""
    app = FastAPI()
    config = ExternalBackendsConfig(
        enabled=True, networks={"bnb": {"url": BACKEND_URL}}
    )
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: (_ for _ in ()).throw(httpx.ConnectError("down"))
            )
        ),
    )
    client = TestClient(app, raise_server_exceptions=False)
    assert client.get("/bnb/blocks/1").status_code == 500


def test_config_parses_from_dict():
    config = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "external_backends": {
                "enabled": True,
                "networks": {"bnb": {"url": BACKEND_URL}},
            },
        }
    )
    assert config.external_backends.enabled is True
    assert config.external_backends.networks["bnb"].url == BACKEND_URL
    assert config.external_backends.networks["bnb"].api_key is None
    # absent section stays None -> feature entirely off
    assert (
        GSRestConfig.from_dict({"database": {"nodes": ["localhost"]}}).external_backends
        is None
    )
