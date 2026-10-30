"""External chain-data backends (middleware/external_backends.py).

Networks without a Cassandra keyspace can be served by a GraphSense-API-
compatible external backend (e.g. the iknaio external backend adapter).
These tests run without containers: a small local app stands in for the
routers and an httpx MockTransport stands in for the backend.
"""

import json
import logging
from contextlib import contextmanager
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request
from starlette.middleware.cors import CORSMiddleware
from starlette.testclient import TestClient

from graphsenselib.web.config import (
    CurrencyRolesConfig,
    ExternalBackendsConfig,
    GSRestConfig,
)
from graphsenselib.web.middleware.currency_roles import CurrencyRoleMiddleware
from graphsenselib.web.middleware import external_backends
from graphsenselib.web.middleware.external_backends import (
    DECLINED_HEADER,
    SERVED_BY_HEADER,
    SERVED_BY_VALUE,
    ExternalBackendMiddleware,
)


@contextmanager
def caplog_at_warning():
    """caplog is a fixture, and two of these tests want the records without
    taking it as an argument alongside the client they build."""
    records = []

    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("graphsenselib.web.middleware.external_backends")
    handler = Sink(level=logging.WARNING)
    logger.addHandler(handler)
    previous, logger.level = logger.level, logging.WARNING
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.level = previous


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


# what the backend answers when asked about a LOCAL network's twins (rule 5)
BACKEND_RELATED_ADDRESSES = {
    "related_addresses": [
        {"address": "0xsame", "currency": "bnb", "relation_type": "pubkey"},
        {
            "address": "0xsame",
            "currency": "arb",
            "relation_type": "pubkey",
        },  # NOT configured
        {
            "address": "0xsame",
            "currency": "eth",
            "relation_type": "pubkey",
        },  # the source itself
    ]
}

LOCAL_RELATED_ADDRESSES = {
    "related_addresses": [
        {"address": "TSAME", "currency": "trx", "relation_type": "pubkey"},
        {
            "address": "0xsame",
            "currency": "bnb",
            "relation_type": "pubkey",
        },  # already known
    ],
    "next_page": None,
}


def make_client(
    enabled=True,
    api_key="backend-key",
    backend_stats=BACKEND_STATS,
    backend_capabilities=BACKEND_CAPABILITIES,
    backend_related_addresses=BACKEND_RELATED_ADDRESSES,
    local_related_addresses=LOCAL_RELATED_ADDRESSES,
    merge_related_addresses=True,
    roles_config=None,
    gated=None,
    with_role_gate=False,
    refuse_status=None,
    refuse_headers=None,
    fail_with=None,
    with_cors=False,
    failure_skip_s=30.0,
):
    """Local stand-in app + recording mock backend behind the middleware.

    ``fail_with`` makes every backend call raise that transport error (a
    backend that is down or hangs); ``with_cors`` wraps the stack in the CORS
    middleware exactly as create_app does. ``failure_skip_s=0`` asks the
    backend on every merge, also after it failed.

    ``backend_capabilities=None`` makes the mock 404 the endpoint (an older
    adapter without /capabilities). ``local_related_addresses`` is the local
    pubkey answer, or a function of (currency, address) returning it."""
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

    @app.get("/{currency}/addresses/{address}/related_addresses")
    async def related_addresses(request: Request, currency: str, address: str):
        # the real route's dependencies read the app's state (the database)
        assert request.app.state is not None
        if callable(local_related_addresses):
            return local_related_addresses(currency, address)
        return local_related_addresses

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
        if fail_with is not None:
            raise fail_with
        if refuse_status is not None:
            return httpx.Response(
                refuse_status, json={"detail": "refused"}, headers=refuse_headers or {}
            )
        if request.url.path == "/stats":
            return httpx.Response(200, json=backend_stats)
        if request.url.path == "/capabilities":
            if backend_capabilities is None:
                return httpx.Response(404)
            return httpx.Response(200, json=backend_capabilities)
        if request.url.path == "/search":
            return httpx.Response(200, json=BACKEND_SEARCH)
        if request.url.path.endswith("/related_addresses"):
            if backend_related_addresses is None:
                return httpx.Response(404)
            if backend_related_addresses == "declines":
                return httpx.Response(501)
            return httpx.Response(200, json=backend_related_addresses)
        return httpx.Response(200, json={"backend_path": request.url.path})

    config = ExternalBackendsConfig(
        enabled=enabled,
        networks={"bnb": {"url": BACKEND_URL, "api_key": api_key}},
        merge_related_addresses=merge_related_addresses,
        failure_skip_s=failure_skip_s,
    )
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(backend),
            timeout=httpx.Timeout(config.timeout_s),
        ),
        roles_config=roles_config,
        gated=gated,
    )
    # added after, so it WRAPS the backends middleware exactly as create_app
    # stacks them -- the filter must see the merged answer
    if with_role_gate:
        app.add_middleware(
            CurrencyRoleMiddleware, config=roles_config, gated=set(gated or ())
        )
    if with_cors:
        app.add_middleware(CORSMiddleware, allow_origins=["*"])
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


def test_a_configured_network_in_upper_case_proxies_in_lower_case():
    """Core routes take a network in any case (ETH = eth); an externally
    served one 404ed when it was not written in lower case."""
    client, seen = make_client()
    response = client.get("/BNB/blocks/1?include_io=true")
    assert response.status_code == 200
    assert response.json() == {"backend_path": "/bnb/blocks/1"}
    assert response.headers["x-served-by"] == "external-backend"
    assert seen[-1].url.query == b"include_io=true"

    response = client.get(
        "/Bnb/addresses/0xsame/related_addresses?address_relation_type=pubkey"
    )
    assert response.headers["x-served-by"] == "external-backend"
    assert seen[-1].url.path == "/bnb/addresses/0xsame/related_addresses"


def test_a_configured_network_in_upper_case_keeps_its_tag_routes_local():
    client, seen = make_client()
    assert client.get("/BNB/addresses/0xabc/tags").json() == {
        "address_tags": ["local-tag"]
    }
    assert seen == []


def test_other_networks_keep_the_case_they_were_written_in():
    client, seen = make_client()
    assert client.get("/BTC/blocks/1").json() == {"served": "local", "currency": "BTC"}
    assert seen == []


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


def test_stats_overlays_tag_counts_from_the_local_tagstore():
    """Rule 2: a backend serves placeholder zeros for tag counts — the local
    TagStore owns tag data (rule 1), so its per-network numbers overwrite
    them. Networks without a TagStore row stay at zero."""
    from graphsenselib.tagstore.db import (
        NetworkStatisticsPublic,
        TagstoreStatisticsPublic,
    )

    class FakeTagstore:
        async def get_network_statistics_cached(self):
            return TagstoreStatisticsPublic(
                by_network={
                    "BNB": NetworkStatisticsPublic(
                        nr_tags=7,
                        nr_identifiers_explicit=5,
                        nr_identifiers_implicit=3572140,
                        nr_labels=11295,
                    )
                }
            )

    client, _ = make_client(
        backend_stats={
            "currencies": [
                {
                    "name": "bnb",
                    "no_blocks": 42,
                    "no_labels": 0,
                    "no_tagged_addresses": 0,
                }
            ]
        }
    )
    client.app.state.tagstore_db = FakeTagstore()
    entry = client.get("/stats").json()["currencies"][-1]
    assert entry["no_labels"] == 11295
    assert entry["no_tagged_addresses"] == 3572140
    assert entry["no_blocks"] == 42

    client.app.state.tagstore_db = None

    class EmptyTagstore:
        async def get_network_statistics_cached(self):
            return TagstoreStatisticsPublic(by_network={})

    client.app.state.tagstore_db = EmptyTagstore()
    entry = client.get("/stats").json()["currencies"][-1]
    assert entry["no_labels"] == 0 and entry["no_tagged_addresses"] == 0


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


def test_gateway_identity_is_relayed_as_consumer_username():
    client, seen = make_client()
    client.get("/bnb/blocks/1", headers={"X-Username": "alice@example.org"})
    assert seen[-1].headers["x-consumer-username"] == "alice@example.org"
    client.post(
        "/bnb/bulk.json/get_address",
        json={"address": ["0x1"]},
        headers={"X-Username": "bob@example.org"},
    )
    assert seen[-1].headers["x-consumer-username"] == "bob@example.org"


def test_no_identity_header_relays_nothing():
    client, seen = make_client()
    client.get("/bnb/blocks/1")
    assert "x-consumer-username" not in seen[-1].headers


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


def test_related_addresses_of_local_network_merge_backend_twins():
    """Rule 5: the local pubkey rows come first, the backend's rows for its
    configured networks are appended, duplicates and the source itself and
    unconfigured networks are dropped."""
    client, seen = make_client()
    response = client.get(
        "/eth/addresses/0xsame/related_addresses?address_relation_type=pubkey&pagesize=100"
    )
    assert response.status_code == 200
    assert "x-served-by" not in response.headers
    assert response.json() == {
        "related_addresses": [
            {"address": "TSAME", "currency": "trx", "relation_type": "pubkey"},
            {"address": "0xsame", "currency": "bnb", "relation_type": "pubkey"},
        ],
        "next_page": None,
    }
    assert seen[-1].url.path == "/eth/addresses/0xsame/related_addresses"
    assert seen[-1].url.query == b"address_relation_type=pubkey&pagesize=100"
    assert seen[-1].headers["Authorization"] == "backend-key"


def test_related_addresses_merge_appends_new_twins():
    client, _ = make_client(
        backend_related_addresses={
            "related_addresses": [
                {"address": "0xother", "currency": "bnb", "relation_type": "pubkey"}
            ]
        }
    )
    body = client.get("/eth/addresses/0xother/related_addresses").json()
    assert [row["currency"] for row in body["related_addresses"]] == [
        "trx",
        "bnb",
        "bnb",
    ]
    assert body["related_addresses"][-1]["address"] == "0xother"


def test_related_addresses_merge_can_be_switched_off():
    client, seen = make_client(merge_related_addresses=False)
    body = client.get("/eth/addresses/0xsame/related_addresses").json()
    assert body == LOCAL_RELATED_ADDRESSES
    assert seen == []


def test_related_addresses_of_external_network_proxy_outright_when_switched_off():
    client, seen = make_client(merge_related_addresses=False)
    response = client.get("/bnb/addresses/0xsame/related_addresses")
    assert response.headers["x-served-by"] == "external-backend"
    assert response.json() == BACKEND_RELATED_ADDRESSES


# Rule 5 the other way round: a backend cannot know a key's btc/trx twins, the
# local pubkey table cannot know its lite ones.

ETH_KEY_LOCAL_ANSWER = {
    "related_addresses": [
        {"address": "0xkey", "currency": "eth", "relation_type": "pubkey"},
        {"address": "TKEY", "currency": "trx", "relation_type": "pubkey"},
    ],
    "next_page": None,
}


def test_a_btc_address_gets_the_backend_twins_of_its_eth_twin():
    client, seen = make_client(local_related_addresses=ETH_KEY_LOCAL_ANSWER)
    response = client.get(
        "/btc/addresses/1key/related_addresses?address_relation_type=pubkey"
    )
    assert response.status_code == 200
    assert [r.url.path for r in seen] == ["/eth/addresses/0xkey/related_addresses"]
    assert response.json()["related_addresses"] == [
        *ETH_KEY_LOCAL_ANSWER["related_addresses"],
        {"address": "0xsame", "currency": "bnb", "relation_type": "pubkey"},
    ]


def test_a_key_not_used_on_ethereum_asks_no_backend():
    """Most btc keys never were, and the twin probe costs the backend
    provider calls on every network it serves."""
    client, seen = make_client()
    for path in (
        "/btc/addresses/1same/related_addresses",
        "/trx/addresses/TSAME/related_addresses",
    ):
        response = client.get(path)
        assert response.json() == LOCAL_RELATED_ADDRESSES, path
        assert DECLINED_HEADER not in response.headers, path
    assert seen == []


def test_related_addresses_of_external_network_gain_the_local_twins():
    asked = []

    def local(currency, address):
        asked.append((currency, address))
        return LOCAL_RELATED_ADDRESSES

    client, seen = make_client(local_related_addresses=local)
    response = client.get(
        "/bnb/addresses/0xsame/related_addresses?address_relation_type=pubkey"
    )
    assert response.status_code == 200
    assert response.headers["x-served-by"] == "external-backend"
    # the local bnb row is the backend's to know: only the trx twin is added
    assert response.json()["related_addresses"] == [
        *BACKEND_RELATED_ADDRESSES["related_addresses"],
        {"address": "TSAME", "currency": "trx", "relation_type": "pubkey"},
    ]
    assert [r.url.path for r in seen] == ["/bnb/addresses/0xsame/related_addresses"]
    assert asked == [("eth", "0xsame")]


def test_a_failing_local_lookup_keeps_the_backends_twins():
    def local(currency, address):
        raise RuntimeError("pubkey table unavailable")

    client, _ = make_client(local_related_addresses=local)
    with caplog_at_warning() as records:
        response = client.get("/bnb/addresses/0xsame/related_addresses")
    assert response.status_code == 200
    assert response.json() == BACKEND_RELATED_ADDRESSES
    assert ["left out" in r.getMessage() for r in records] == [True]


def test_local_twins_go_on_the_backends_last_page_only():
    paged = {
        "related_addresses": [
            {"address": "0xsame", "currency": "arb", "relation_type": "pubkey"}
        ],
        "next_page": "2",
    }
    client, _ = make_client(backend_related_addresses=paged)
    assert client.get("/bnb/addresses/0xsame/related_addresses").json() == paged


def test_a_backend_error_on_external_related_addresses_reaches_the_client():
    client, _ = make_client(refuse_status=503)
    response = client.get("/bnb/addresses/0xsame/related_addresses")
    assert response.status_code == 503
    assert response.json() == {"detail": "refused"}


def test_related_addresses_backend_404_or_501_contributes_nothing():
    for answer in (None, "declines"):
        client, _ = make_client(backend_related_addresses=answer)
        body = client.get("/eth/addresses/0xsame/related_addresses").json()
        assert body == LOCAL_RELATED_ADDRESSES


def test_related_addresses_other_relation_types_skip_the_backends():
    client, seen = make_client()
    body = client.get(
        "/btc/addresses/1same/related_addresses?address_relation_type=something_else"
    ).json()
    assert body == LOCAL_RELATED_ADDRESSES
    assert seen == []


def test_an_unreachable_backend_answers_a_proxied_request_with_503():
    """A request on an externally served network whose backend is down is a
    503 that says when to come back, not a 500 the browser cannot read."""
    client, _ = make_client(fail_with=httpx.ConnectError("down"))
    with caplog_at_warning() as records:
        response = client.get("/bnb/blocks/1")
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "30"
    assert response.headers[SERVED_BY_HEADER] == SERVED_BY_VALUE
    assert response.json() == {"detail": "The service for this network is unavailable."}
    assert [r.levelno for r in records] == [logging.ERROR]
    assert BACKEND_URL in records[0].getMessage()


def test_a_proxied_backend_error_reaches_the_client_as_it_is():
    """Only a missing answer is replaced; the backend's own answer is kept."""
    client, _ = make_client(refuse_status=500)
    response = client.get("/bnb/blocks/1")
    assert response.status_code == 500
    assert response.json() == {"detail": "refused"}


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


def test_currency_stats_has_no_extension_fields():
    """Capability declaration moved to /capabilities and the per-currency
    coin fields were dropped: CurrencyStats is exactly the core contract."""
    from graphsenselib.web.models import CurrencyStats

    props = CurrencyStats.model_json_schema()["properties"]
    for field in ("capabilities", "coin_ticker", "coin_decimals", "network_name"):
        assert field not in props


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


# --- client opt-out: X-Ikn-Currency-Opt-Out: all-light ---------------------------------

OPT_OUT = {"X-Ikn-Currency-Opt-Out": "all-light"}


def test_opt_out_header_serves_stats_locally_without_touching_the_backend():
    client, seen = make_client()
    doc = client.get("/stats", headers=OPT_OUT).json()
    assert [c["name"] for c in doc["currencies"]] == ["btc"]
    assert seen == []


def test_opt_out_header_makes_a_configured_network_a_local_miss():
    client, seen = make_client()
    r = client.get("/bnb/addresses/0xabc", headers=OPT_OUT)
    assert r.status_code == 404
    assert SERVED_BY_HEADER not in r.headers
    assert seen == []


def test_opt_out_header_skips_the_twin_merge_and_search_merge():
    client, seen = make_client()
    doc = client.get("/eth/addresses/0xsame/related_addresses", headers=OPT_OUT).json()
    assert doc == LOCAL_RELATED_ADDRESSES
    doc = client.get("/search?q=0xhit", headers=OPT_OUT).json()
    assert [c["currency"] for c in doc["currencies"]] == ["btc"]
    assert seen == []


def test_opt_out_header_value_is_case_insensitive_and_other_values_are_ignored():
    client, seen = make_client()
    doc = client.get("/stats", headers={"X-Ikn-Currency-Opt-Out": "bnb, ALL "}).json()
    assert [c["name"] for c in doc["currencies"]] == ["btc"]
    assert seen == []
    doc = client.get("/stats", headers={"X-Ikn-Currency-Opt-Out": "bnb"}).json()
    assert "bnb" in [c["name"] for c in doc["currencies"]]
    assert seen != []


# --- the fan-out must not be paid for rows the role gate will delete -------
#
# CurrencyRoleMiddleware wraps this middleware, so on a listing path it filters
# the MERGED answer: without these, a caller with no lite-currency role still
# triggered the whole fan-out and then had every row of it deleted. Measured on
# the test stack 2026-09-24: one such /search cost 2,160 provider CU and
# returned the caller nothing.

ROLES = CurrencyRolesConfig()


def test_search_does_not_reach_the_backend_without_the_role():
    client, seen = make_client(roles_config=ROLES, gated={"bnb"}, with_role_gate=True)
    response = client.get(
        "/search?q=0xdeadbeef", headers={"X-User-Roles": "currency-eth"}
    )
    assert response.status_code == 200
    assert seen == []
    # and the caller sees exactly what the role gate would have left them
    assert all(
        entry.get("currency") != "bnb"
        for entry in response.json().get("currencies", [])
    )


def test_search_reaches_the_backend_with_the_role():
    client, seen = make_client(roles_config=ROLES, gated={"bnb"}, with_role_gate=True)
    response = client.get(
        "/search?q=0xdeadbeef", headers={"X-User-Roles": "currency-bnb"}
    )
    assert response.status_code == 200
    assert [request.url.path for request in seen] == ["/search"]
    assert any(
        entry.get("currency") == "bnb"
        for entry in response.json().get("currencies", [])
    )


def test_no_roles_header_at_all_skips_the_fan_out():
    """A direct API client that sends no roles header has no roles, so the gate
    would delete every gated row anyway."""
    client, seen = make_client(roles_config=ROLES, gated={"bnb"}, with_role_gate=True)
    assert client.get("/search?q=0xdeadbeef").status_code == 200
    assert seen == []


def test_the_gate_switched_off_still_fans_out():
    """auth.enforce_currency_roles=false means no gate: nothing is filtered
    afterwards, so nothing may be skipped before."""
    client, seen = make_client(
        roles_config=CurrencyRolesConfig(enforce_currency_roles=False), gated={"bnb"}
    )
    assert client.get("/search?q=0xdeadbeef").status_code == 200
    assert [request.url.path for request in seen] == ["/search"]


def test_no_roles_config_is_unchanged_behaviour():
    """Nothing passed (an app built without the gate) fans out as before."""
    client, seen = make_client()
    assert client.get("/search?q=0xdeadbeef").status_code == 200
    assert [request.url.path for request in seen] == ["/search"]


def test_stats_and_capabilities_and_twins_skip_too():
    """The twin path here must be a LOCALLY served network.

    It used to be /bnb/..., which the role gate 403s before this middleware
    ever runs, so the assertion held whatever the fan-out did — verified
    2026-09-24 by deleting the entitlement check from _merge_related_addresses
    and watching the whole file still pass. Rule 5 asks the backends about an
    address on a network the core owns, so /eth/... is the path that actually
    reaches the merge."""
    for path in ("/stats", "/capabilities", "/eth/addresses/0xsame/related_addresses"):
        client, seen = make_client(
            roles_config=ROLES, gated={"bnb"}, with_role_gate=True
        )
        response = client.get(path, headers={"X-User-Roles": "currency-eth"})
        assert response.status_code == 200, path  # reached the merge, not a 403
        assert seen == [], path


def test_an_ungated_network_is_never_skipped():
    client, seen = make_client(roles_config=ROLES, gated=set())
    assert client.get("/search?q=0xdeadbeef").status_code == 200
    assert [request.url.path for request in seen] == ["/search"]


# --- the gateway's identity, relayed so the backend paces the real caller ---
#
# The merge fan-out used to build its headers from scratch, so every /search,
# /stats, /capabilities and twin lookup reached the backend anonymous. The
# backend then bucketed all of them together under this app's own address:
# nobody's spend was charged to them, and that one phantom bucket's share came
# out of every real caller's.

MERGE_REQUESTS = [
    ("/stats", "/stats"),
    ("/capabilities", "/capabilities"),
    ("/search?q=0x", "/search"),
    (
        "/eth/addresses/0xsame/related_addresses",
        "/eth/addresses/0xsame/related_addresses",
    ),
]


def test_every_merge_fan_out_relays_the_caller():
    for url, backend_path in MERGE_REQUESTS:
        client, seen = make_client()
        response = client.get(url, headers={"X-Username": "alice"})
        assert response.status_code == 200, url
        sent = [r for r in seen if r.url.path == backend_path]
        assert sent, url
        assert sent[0].headers.get("X-Consumer-Username") == "alice", url


def test_the_proxy_path_relays_the_same_caller():
    client, seen = make_client()
    client.get("/bnb/blocks/1", headers={"X-Username": "alice"})
    assert seen[0].headers.get("X-Consumer-Username") == "alice"


def test_no_identity_header_sends_no_identity():
    """Not an empty name: an absent one. A blank X-Consumer-Username would be
    a caller named "" rather than no caller at all."""
    client, seen = make_client()
    client.get("/search?q=0x")
    assert "X-Consumer-Username" not in seen[0].headers


def test_an_empty_consumer_header_config_switches_the_relay_off():
    app = FastAPI()

    @app.get("/search")
    async def search():
        return {"currencies": [], "labels": [], "actors": []}

    sent: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json=BACKEND_SEARCH)

    app.add_middleware(
        ExternalBackendMiddleware,
        config=ExternalBackendsConfig(
            enabled=True,
            networks={"bnb": {"url": BACKEND_URL}},
            consumer_header="",
        ),
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    TestClient(app).get("/search?q=0x", headers={"X-Username": "alice"})
    assert sent and "X-Consumer-Username" not in sent[0].headers


# --- a paced caller keeps its local answer -------------------------------
#
# Relaying the identity means the backend now paces THIS caller. Its 429 is
# backpressure on the lite networks, so it must not take out the btc/eth
# results the core served itself.


def test_a_paced_backend_does_not_fail_the_merged_search():
    client, _ = make_client(refuse_status=429)
    response = client.get("/search?q=0x", headers={"X-Username": "heavy"})
    assert response.status_code == 200
    # local hits survive; the backend contributed nothing
    assert response.json()["currencies"] == [
        {"currency": "btc", "addresses": ["1local"], "txs": []}
    ]


def test_a_paced_backend_does_not_fail_stats_or_capabilities_or_twins():
    for url, field, local in (
        ("/stats", "currencies", [{"name": "btc", "no_blocks": 1}]),
        ("/capabilities", "networks", [{"network": "btc", "disabled": []}]),
        (
            "/eth/addresses/0xsame/related_addresses",
            "related_addresses",
            LOCAL_RELATED_ADDRESSES["related_addresses"],
        ),
    ):
        client, _ = make_client(refuse_status=429)
        response = client.get(url, headers={"X-Username": "heavy"})
        assert response.status_code == 200, url
        assert response.json()[field] == local, url


# --- a failing backend never takes out the core answer ------------------
#
# The dashboard loads /stats and /capabilities at startup and searches via
# /search: a 500 there made the app unusable on every network while the
# backend was down. A failed backend is a partial answer like a 429, but it is
# a fault, so it is logged at ERROR.

MERGE_LOCAL_ANSWERS = [
    ("/stats", "currencies", [{"name": "btc", "no_blocks": 1}]),
    ("/capabilities", "networks", [{"network": "btc", "disabled": []}]),
    (
        "/search?q=0x",
        "currencies",
        [{"currency": "btc", "addresses": ["1local"], "txs": []}],
    ),
    (
        "/eth/addresses/0xsame/related_addresses",
        "related_addresses",
        LOCAL_RELATED_ADDRESSES["related_addresses"],
    ),
]


def _assert_partial_answers(client):
    levels = []
    for url, field, local in MERGE_LOCAL_ANSWERS:
        with caplog_at_warning() as records:
            response = client.get(url)
        assert response.status_code == 200, url
        assert response.json()[field] == local, url
        assert response.headers.get(DECLINED_HEADER) == BACKEND_URL, url
        levels += [r.levelno for r in records]
    # one alert for the outage, then one warning per failed call
    assert levels == [logging.ERROR] + [logging.WARNING] * (len(levels) - 1)
    assert len(levels) == len(MERGE_LOCAL_ANSWERS)


def test_an_unreachable_backend_does_not_fail_a_merge():
    client, _ = make_client(fail_with=httpx.ConnectError("down"), failure_skip_s=0)
    _assert_partial_answers(client)


def test_a_backend_that_does_not_answer_in_time_does_not_fail_a_merge():
    client, _ = make_client(fail_with=httpx.ReadTimeout("hung"), failure_skip_s=0)
    _assert_partial_answers(client)


def test_a_backend_error_answer_does_not_fail_a_merge():
    for status in (500, 502, 503, 403):
        client, _ = make_client(refuse_status=status)
        _assert_partial_answers(client)


def test_a_merge_waits_merge_timeout_s_and_a_proxy_timeout_s():
    """A hung backend held /stats for the full 60 s proxy timeout; a merge
    call now gives up after merge_timeout_s (default 5 s, the adapter answers
    these in under 1 s cold)."""
    client, seen = make_client()
    client.get("/stats")
    client.get("/bnb/blocks/1")
    merge, proxy = seen
    assert merge.extensions["timeout"]["read"] == 5.0
    assert proxy.extensions["timeout"]["read"] == 60.0


def test_a_failed_backend_answer_is_readable_by_the_browser():
    """Both answers pass the CORS middleware, which wraps this one in
    create_app: a 500 from the generic handler did not, and the dashboard
    only saw a network error."""
    client, _ = make_client(fail_with=httpx.ConnectError("down"), with_cors=True)
    origin = {"Origin": "http://localhost:3000"}
    for url in ("/stats", "/bnb/blocks/1"):
        response = client.get(url, headers=origin)
        assert response.headers["access-control-allow-origin"] == "*", url


def test_a_proxied_429_reaches_the_client_untouched():
    """On the proxy path the 429 IS the answer -- swallowing it would turn
    backpressure into a silent wrong result."""
    client, _ = make_client(refuse_status=429)
    response = client.get("/bnb/blocks/1", headers={"X-Username": "heavy"})
    assert response.status_code == 429


# --- a swallowed 429 is a PARTIAL answer and must not be silent -----------
#
# The backend charges every provider call to the caller's own bucket AND the
# account-wide one, so a 429 can equally mean "you are over your share" or
# "somebody else saturated the account". Either way the lite rows are missing
# from a response shaped exactly like a complete one.


def test_a_declined_backend_is_named_on_the_response():
    client, _ = make_client(refuse_status=429)
    for url in ("/stats", "/capabilities", "/search?q=0x"):
        response = client.get(url, headers={"X-Username": "heavy"})
        assert response.status_code == 200, url
        assert response.headers.get(DECLINED_HEADER) == BACKEND_URL, url


def test_a_declined_twin_lookup_is_named_too():
    client, _ = make_client(refuse_status=429)
    response = client.get("/eth/addresses/0xsame/related_addresses")
    assert response.status_code == 200
    assert response.headers.get(DECLINED_HEADER) == BACKEND_URL


def test_nothing_declined_asserts_nothing():
    """An empty header on every response would claim the backends all answered,
    which this middleware cannot know about one it never called."""
    client, _ = make_client()
    response = client.get("/stats")
    assert DECLINED_HEADER not in response.headers


def test_a_declined_backend_is_logged_with_the_caller(caplog):
    client, _ = make_client(refuse_status=429)
    with caplog.at_level(
        logging.WARNING, logger="graphsenselib.web.middleware.external_backends"
    ):
        client.get("/search?q=0x", headers={"X-Username": "heavy"})
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "heavy" in message and BACKEND_URL in message and "/search" in message


def test_an_anonymous_decline_is_logged_as_anonymous():
    """Not as a caller literally named "None"."""
    client, _ = make_client(refuse_status=429)
    with caplog_at_warning() as records:
        client.get("/stats")
    assert "<anonymous>" in records[0].getMessage()


# --- a proxied 429 has to say when to come back --------------------------


def test_a_proxied_429_carries_retry_after():
    """Without it the client is told to back off but not for how long, which
    is barely better than a 500."""
    client, _ = make_client(refuse_status=429, refuse_headers={"Retry-After": "17"})
    response = client.get("/bnb/blocks/1")
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "17"


def test_a_proxy_response_without_retry_after_invents_none():
    client, _ = make_client(refuse_status=429)
    response = client.get("/bnb/blocks/1")
    assert response.status_code == 429
    assert "retry-after" not in {k.lower() for k in response.headers}


def test_a_normal_proxy_response_is_unchanged():
    client, _ = make_client()
    response = client.get("/bnb/blocks/1")
    assert response.status_code == 200
    assert response.headers[SERVED_BY_HEADER] == SERVED_BY_VALUE
    assert "retry-after" not in {k.lower() for k in response.headers}


# --- partial entitlement: narrowed rows, NOT a narrowed request ----------
#
# All lite networks share one backend URL, so a caller entitled to one of them
# still triggers that backend's full multi-network query. Only the rows come
# back narrowed. Asserting it so the day someone adds a per-network filter,
# this test is what tells them the contract changed.


def _two_network_client(roles_config=None, gated=None, with_role_gate=False):
    app = FastAPI()

    @app.get("/search")
    async def search():
        return {
            "currencies": [{"currency": "btc", "addresses": ["1local"], "txs": []}],
            "labels": [],
            "actors": [],
        }

    seen: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "currencies": [
                    {"currency": "bnb", "addresses": ["0xbnb"], "txs": []},
                    {"currency": "arb", "addresses": ["0xarb"], "txs": []},
                ],
                "labels": [],
                "actors": [],
            },
        )

    app.add_middleware(
        ExternalBackendMiddleware,
        config=ExternalBackendsConfig(
            enabled=True,
            networks={"bnb": {"url": BACKEND_URL}, "arb": {"url": BACKEND_URL}},
        ),
        roles_config=roles_config,
        gated=gated,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    if with_role_gate:
        app.add_middleware(
            CurrencyRoleMiddleware, config=roles_config, gated=set(gated or ())
        )
    return TestClient(app), seen


def test_one_role_out_of_two_still_calls_the_backend_once_unnarrowed():
    client, seen = _two_network_client(roles_config=ROLES, gated={"bnb", "arb"})
    response = client.get("/search?q=0x", headers={"X-User-Roles": "currency-bnb"})
    assert response.status_code == 200
    assert len(seen) == 1
    # the request itself carries no network filter -- the backend is asked for
    # everything it serves and answers for both networks
    assert "arb" not in str(seen[0].url) and "bnb" not in str(seen[0].url)


def test_one_role_out_of_two_keeps_only_the_entitled_rows():
    client, _ = _two_network_client(roles_config=ROLES, gated={"bnb", "arb"})
    response = client.get("/search?q=0x", headers={"X-User-Roles": "currency-bnb"})
    assert [c["currency"] for c in response.json()["currencies"]] == ["btc", "bnb"]


def test_no_entitlement_to_either_skips_the_backend_entirely():
    client, seen = _two_network_client(roles_config=ROLES, gated={"bnb", "arb"})
    response = client.get("/search?q=0x", headers={"X-User-Roles": "currency-eth"})
    assert seen == []
    assert [c["currency"] for c in response.json()["currencies"]] == ["btc"]


def test_the_dashboard_can_read_the_outage_headers_with_credentials():
    """A request with credentials reads the "*" wildcard of
    Access-Control-Expose-Headers as a header literally named "*", so the
    headers the dashboard reads must be named."""
    from graphsenselib.web.app import _setup_cors_middleware

    app = FastAPI()
    # the CORS setup reads nothing else of the config
    _setup_cors_middleware(app, SimpleNamespace(ALLOWED_ORIGINS=["*"]))

    @app.get("/stats")
    async def stats():
        return {}

    response = TestClient(app).get(
        "/stats", headers={"Origin": "http://localhost:3000"}
    )
    exposed = {
        name.strip().lower()
        for name in response.headers["access-control-expose-headers"].split(",")
    }
    assert {DECLINED_HEADER.lower(), "retry-after", SERVED_BY_HEADER} <= exposed
    assert response.headers["access-control-allow-credentials"] == "true"


# --- one alert per outage, not one per request ---------------------------
#
# During an outage every dashboard page and every MCP call fails over to the
# partial answer. ERROR is the level the Slack handler forwards, so only the
# first failure of a backend that answered before may log at it.


def _flaky_client(state, failure_skip_s=0.0):
    """A backend that is down while state["down"] is true."""
    app = FastAPI()

    @app.get("/stats")
    async def stats():
        return {"currencies": [{"name": "btc", "no_blocks": 1}]}

    def backend(request: httpx.Request) -> httpx.Response:
        if state["down"]:
            raise httpx.ConnectError("down")
        if request.url.path == "/stats":
            return httpx.Response(200, json=BACKEND_STATS)
        return httpx.Response(200, json={"backend_path": request.url.path})

    config = ExternalBackendsConfig(
        enabled=True,
        networks={"bnb": {"url": BACKEND_URL}},
        failure_skip_s=failure_skip_s,
    )
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    return TestClient(app)


def _levels(client, *urls):
    with caplog_at_warning() as records:
        for url in urls:
            client.get(url)
    return [(r.levelno, r.getMessage()) for r in records]


def test_an_outage_alerts_once_and_its_end_is_logged_once():
    state = {"down": True}
    client = _flaky_client(state)

    first = _levels(client, "/stats", "/stats", "/bnb/blocks/1")
    assert [level for level, _ in first] == [
        logging.ERROR,
        logging.WARNING,
        logging.WARNING,
    ]
    assert "stopped answering" in first[0][1] and "bnb" in first[0][1]

    state["down"] = False
    recovered = _levels(client, "/stats", "/stats")
    assert [level for level, _ in recovered] == [logging.WARNING]
    assert "answers again" in recovered[0][1]

    # a new outage is a new alert
    state["down"] = True
    assert [level for level, _ in _levels(client, "/stats")] == [logging.ERROR]


def test_a_backend_error_answer_alerts_once_too():
    """A 403 (say, a wrong API key) leaves the lite networks out of every
    merged answer: an outage as well, alerted once."""
    client, _ = make_client(refuse_status=403)
    assert [level for level, _ in _levels(client, "/stats", "/search?q=0x")] == [
        logging.ERROR,
        logging.WARNING,
    ]


def test_an_overloaded_backends_503_is_no_outage():
    """The proxy hands the backend's own 503 through: it answered, so nothing
    alerts, and a pending outage ends."""
    client, _ = make_client(refuse_status=503)
    response = client.get("/bnb/blocks/1")
    assert response.status_code == 503
    assert _levels(client, "/bnb/blocks/1") == []


def test_the_outage_alert_reaches_slack(monkeypatch):
    """setup_logging gives the middleware's logger the same Slack handler as
    the app's: it is a sibling logger tree, so without it the alert would
    only reach the log."""
    from graphsenselib.utils.slack import SlackLogHandler
    from graphsenselib.web.app import setup_logging
    from graphsenselib.web.config import LoggingConfig

    names = (
        "graphsenselib.web.app",
        "graphsenselib.mcp",
        "graphsenselib.web.middleware.external_backends",
    )
    for name in (*names, "asyncio", "uvicorn"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "handlers", [])
        monkeypatch.setattr(logger, "level", logger.level)
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(logging.root, "handlers", [])

    setup_logging(
        logging.getLogger("graphsenselib.web.app"),
        SimpleNamespace(hooks=["https://hooks.slack.test/x"]),
        "test",
        LoggingConfig(),
    )

    handlers = logging.getLogger(names[2]).handlers
    assert [type(h) for h in handlers] == [SlackLogHandler]
    assert handlers[0].level == logging.ERROR


@pytest.mark.parametrize("level", ["INFO", "WARNING"])
def test_the_middleware_logs_at_the_configured_level(monkeypatch, level):
    """A sibling of the app's logger, the middleware's would otherwise keep the
    root's WARNING and drop its INFO lines (a backend that cannot answer, a
    proxied answer refused for an unindexed eth leg)."""
    from graphsenselib.web.app import setup_logging
    from graphsenselib.web.config import LoggingConfig

    names = ("graphsenselib.web.app", "graphsenselib.web.middleware.external_backends")
    for name in (*names, "asyncio", "uvicorn"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "level", logger.level)
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(logging.root, "handlers", [])

    setup_logging(logging.getLogger(names[0]), None, "test", LoggingConfig(level=level))

    assert logging.getLogger(names[1]).getEffectiveLevel() == getattr(logging, level)


# A hung backend cost every merge merge_timeout_s, and the dashboard asks for a
# twin list per address it adds: after a failure that is no answer at all,
# merges leave the backend out for failure_skip_s without asking it.


def _clock(monkeypatch, start=1000.0):
    """Replace the skip windows' clock; moving clock["now"] moves time."""
    clock = {"now": start}
    monkeypatch.setattr(external_backends, "_now", lambda: clock["now"])
    return clock


def test_a_hung_backend_is_asked_once_per_window(monkeypatch):
    _clock(monkeypatch)
    client, seen = make_client(fail_with=httpx.ReadTimeout("hung"))
    with caplog_at_warning() as records:
        for url, field, local in MERGE_LOCAL_ANSWERS:
            response = client.get(url)
            assert response.status_code == 200, url
            assert response.json()[field] == local, url
            assert response.headers.get(DECLINED_HEADER) == BACKEND_URL, url
    assert len(seen) == 1
    # the outage is alerted once; a skipped call logs nothing
    assert [r.levelno for r in records] == [logging.ERROR]


def test_the_window_ends_after_failure_skip_s(monkeypatch):
    clock = _clock(monkeypatch)
    client, seen = make_client(fail_with=httpx.ConnectError("down"))
    client.get("/stats")
    client.get("/stats")
    assert len(seen) == 1
    clock["now"] += 29.9
    client.get("/stats")
    assert len(seen) == 1
    clock["now"] += 0.2
    client.get("/stats")
    assert len(seen) == 2


def test_any_answer_ends_the_window_early(monkeypatch):
    _clock(monkeypatch)
    state = {"down": True}
    client = _flaky_client(state, failure_skip_s=30.0)
    client.get("/stats")
    state["down"] = False
    # still in the window: the backend is not asked
    assert client.get("/stats").headers.get(DECLINED_HEADER) == BACKEND_URL
    # proxies still ask, and their answer ends the window
    assert client.get("/bnb/blocks/1").status_code == 200
    response = client.get("/stats")
    assert DECLINED_HEADER not in response.headers
    assert [c["name"] for c in response.json()["currencies"]] == ["btc", "bnb"]


def test_an_error_answer_or_a_429_starts_no_window(monkeypatch):
    """The backend answered, so it is up: the next merge asks it again."""
    _clock(monkeypatch)
    for status in (500, 429):
        client, seen = make_client(refuse_status=status)
        client.get("/stats")
        client.get("/stats")
        assert len(seen) == 2, status


def test_a_proxy_that_cannot_reach_the_backend_starts_the_window(monkeypatch):
    _clock(monkeypatch)
    client, seen = make_client(fail_with=httpx.ConnectError("down"))
    assert client.get("/bnb/blocks/1").status_code == 503
    response = client.get("/stats")
    assert response.status_code == 200
    assert response.headers.get(DECLINED_HEADER) == BACKEND_URL
    assert len(seen) == 1
    # the network's own requests still ask, so they find out when it is back
    assert client.get("/bnb/blocks/1").status_code == 503
    assert len(seen) == 2
