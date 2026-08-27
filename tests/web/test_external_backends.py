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
        {
            "name": "bnb",
            "no_blocks": 42,
            "some_unmodeled_field": "kept",
            "capabilities": [],  # legacy declaration: must be stripped (rule 2)
        },
        {"name": "eth", "no_blocks": 9},  # NOT configured -> must be filtered
    ],
}

BACKEND_CAPABILITIES = {
    "networks": [
        {
            "network": "bnb",
            "disabled": ["relations", "clusters", "tags", "exact_stats"],
        },
        {"network": "eth", "disabled": []},  # NOT configured -> must be filtered
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


def make_client(
    enabled=True,
    api_key="backend-key",
    backend_stats=BACKEND_STATS,
    backend_capabilities=BACKEND_CAPABILITIES,
):
    """Local stand-in app + recording mock backend behind the middleware.

    ``backend_capabilities=None`` makes the mock 404 the endpoint (an older
    adapter without /capabilities)."""
    app = FastAPI()

    @app.get("/stats")
    async def stats():
        return {
            "version": "local-version",
            "currencies": [{"name": "btc", "no_blocks": 1}],
        }

    @app.get("/capabilities")
    async def capabilities():
        return {"networks": [{"network": "btc", "disabled": []}]}

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

    @app.post("/{currency}/bulk.csv/{operation}")
    async def bulk_csv(currency: str, operation: str):
        return {"served": "local"}

    seen: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/stats":
            return httpx.Response(200, json=backend_stats)
        if request.url.path == "/capabilities":
            if backend_capabilities is None:
                return httpx.Response(404)
            return httpx.Response(200, json=backend_capabilities)
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


def test_bulk_tag_operations_of_external_network_stay_local():
    """The bulk twins of the address tag routes are the same TagStore reads
    (the dashboard's CSV export uses them) and stay local like rule 1's
    single-address routes."""
    client, seen = make_client()
    for form in ("csv", "json"):
        for operation in ("list_tags_by_address", "get_tag_summary_by_address"):
            response = client.post(
                f"/bnb/bulk.{form}/{operation}", json={"address": ["0xabc"]}
            )
            assert response.json() == {"served": "local"}
    assert seen == []


def test_other_bulk_operations_of_external_network_proxy():
    client, seen = make_client()
    response = client.post("/bnb/bulk.json/get_address", json={"address": ["0xabc"]})
    assert response.json() == {"backend_path": "/bnb/bulk.json/get_address"}
    assert response.headers["x-served-by"] == "external-backend"


def test_capabilities_merges_and_serves_tags_locally():
    """Backend entries for configured networks are merged in; "tags" is
    removed from their disabled lists because rule 1 answers tag routes
    locally — other flags survive untouched."""
    client, seen = make_client()
    body = client.get("/capabilities").json()
    assert body["networks"] == [
        {"network": "btc", "disabled": []},
        {"network": "bnb", "disabled": ["relations", "clusters", "exact_stats"]},
    ]
    assert seen[-1].url.path == "/capabilities"


def test_capabilities_backend_404_contributes_nothing():
    """An older adapter without /capabilities degrades like an older server:
    its networks are simply absent, which consumers read as fully enabled."""
    client, seen = make_client(backend_capabilities=None)
    body = client.get("/capabilities").json()
    assert body["networks"] == [{"network": "btc", "disabled": []}]
    assert seen[-1].url.path == "/capabilities"


def test_stats_strips_legacy_capability_declarations():
    """A stale adapter still declaring the retired per-currency capabilities
    field must not leak it through the mirror-not-revalidate merge."""
    client, seen = make_client(
        backend_stats={
            "currencies": [
                {"name": "bnb", "no_blocks": 1, "capabilities": ["relations", "tags"]}
            ]
        }
    )
    assert "capabilities" not in client.get("/stats").json()["currencies"][-1]

    client, seen = make_client(
        backend_stats={"currencies": [{"name": "bnb", "no_blocks": 1}]}
    )
    assert "capabilities" not in client.get("/stats").json()["currencies"][-1]


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
    # unmodeled backend fields survive (mirrored, not re-validated) EXCEPT
    # the retired capabilities field, which is stripped (rule 2)
    assert body["currencies"] == [
        {"name": "btc", "no_blocks": 1},
        {
            "name": "bnb",
            "no_blocks": 42,
            "some_unmodeled_field": "kept",
        },
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


# ---------------------------------------------------------------------------
# Contract extensions shared with external backends (models, not middleware)
# ---------------------------------------------------------------------------


def test_currency_stats_declares_backend_extension_fields():
    """The /stats schema documents the fields external backends serve
    (network-behavior discovery), while local serialization keeps them off
    the wire (exclude_none) so baseline output is unchanged. Capability
    declaration moved to /capabilities and is no longer a stats field."""
    from graphsenselib.web.models import CurrencyStats

    props = CurrencyStats.model_json_schema()["properties"]
    for field in ("coin_ticker", "coin_decimals", "network_name"):
        assert field in props
    assert "capabilities" not in props

    local = CurrencyStats(
        name="btc",
        no_blocks=1,
        no_address_relations=1,
        no_addresses=1,
        no_entities=1,
        no_txs=1,
        no_labels=1,
        no_tagged_addresses=1,
        timestamp=1,
        network_type="utxo",
    ).model_dump(exclude_none=True)
    assert "capabilities" not in local
    assert "coin_ticker" not in local

    declared = CurrencyStats.model_validate(
        {
            "name": "arb",
            "no_blocks": 1,
            "no_address_relations": 0,
            "no_addresses": 0,
            "no_entities": 0,
            "no_txs": 0,
            "no_labels": 0,
            "no_tagged_addresses": 0,
            "timestamp": 1,
            "network_type": "account",
            "coin_ticker": "eth",
            "coin_decimals": 18,
            "network_name": "Arbitrum",
        }
    )
    assert declared.coin_ticker == "eth"
    assert declared.coin_decimals == 18


def test_address_declares_truncation_extension_fields():
    """Address bodies proxied from external backends may qualify truncated
    aggregates (aggregates_truncated/cutoff) and neighbor lists
    (neighbors_truncated); local Cassandra serving never sets them and the
    fields stay off the wire via exclude_none."""
    from graphsenselib.web.models import Address, AggregateCutoff, NeighborAddresses

    for field in ("aggregates_truncated", "cutoff"):
        assert field in Address.model_json_schema()["properties"]
    assert "neighbors_truncated" in NeighborAddresses.model_json_schema()["properties"]

    values = {"value": 1, "fiat_values": [{"code": "eur", "value": 0.1}]}
    body = {
        "currency": "bnb",
        "address": "0xabc",
        "entity": 1,
        "balance": values,
        "total_received": values,
        "total_spent": values,
        "in_degree": 5,
        "out_degree": 3,
        "no_incoming_txs": 10,
        "no_outgoing_txs": 7,
    }
    exact = Address.model_validate(body).to_dict()
    assert "aggregates_truncated" not in exact
    assert "cutoff" not in exact

    truncated = Address.model_validate(
        {
            **body,
            "aggregates_truncated": True,
            "cutoff": {"floor_fields": ["total_received", "in_degree"]},
        }
    )
    assert isinstance(truncated.cutoff, AggregateCutoff)
    assert truncated.to_dict()["cutoff"]["floor_fields"] == [
        "total_received",
        "in_degree",
    ]

    # the flat qualifiers map (the simple consumer form of cutoff) is a
    # backend-only extension too; is_possible_service IS set by local serving
    for field in ("qualifiers", "is_possible_service"):
        assert field in Address.model_json_schema()["properties"]
    assert "qualifiers" not in exact
    qualified = Address.model_validate(
        {**body, "qualifiers": {"total_received": "gt", "balance": "approx"}}
    )
    assert qualified.to_dict()["qualifiers"] == {
        "total_received": "gt",
        "balance": "approx",
    }
