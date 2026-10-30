"""Role-based gating of externally served currencies
(middleware/currency_roles.py).

The gateway sets ``X-User-Roles``; a gated currency needs ``currency-<code>``.
A small stand-in app replaces the routers; no containers.
"""

from fastapi import FastAPI, Request
from starlette.testclient import TestClient

from graphsenselib.web.config import CurrencyRolesConfig
from graphsenselib.web.middleware.currency_roles import (
    DENIED_DETAIL,
    CurrencyRoleMiddleware,
    parse_roles,
)

STATS = {
    "version": "v",
    "currencies": [{"name": "btc"}, {"name": "bnb"}, {"name": "arb"}],
}
CAPABILITIES = {
    "networks": [
        {"network": "btc", "disabled": []},
        {"network": "bnb", "disabled": ["relations"]},
    ]
}
SEARCH = {
    "currencies": [
        {"currency": "btc", "addresses": ["1hit"], "txs": []},
        {"currency": "bnb", "addresses": ["0xhit"], "txs": []},
    ],
    "labels": ["kept"],
    "actors": [],
}
RELATED = {
    "related_addresses": [
        {"address": "TSAME", "currency": "trx", "relation_type": "pubkey"},
        {"address": "0xsame", "currency": "bnb", "relation_type": "pubkey"},
        {"address": "0xsame", "currency": "arb", "relation_type": "pubkey"},
    ],
    "next_page": None,
}


def make_client(gated=("bnb", "arb"), config=None):
    app = FastAPI()

    @app.get("/stats")
    async def stats():
        return STATS

    @app.get("/capabilities")
    async def capabilities():
        return CAPABILITIES

    @app.get("/search")
    async def search(request: Request):
        return SEARCH

    @app.get("/{currency}/addresses/{address}")
    async def address(currency: str, address: str):
        return {"currency": currency, "address": address}

    @app.get("/{currency}/addresses/{address}/related_addresses")
    async def related(currency: str, address: str):
        return RELATED

    @app.post("/{currency}/bulk.json/{operation}")
    async def bulk(currency: str, operation: str):
        return {"currency": currency}

    app.add_middleware(
        CurrencyRoleMiddleware,
        config=config or CurrencyRolesConfig(),
        gated=set(gated),
    )
    return TestClient(app)


def roles(value):
    return {"X-User-Roles": value}


# --- the spec's test vectors -------------------------------------------------


def test_leaf_role_grants_the_currency():
    client = make_client()
    r = client.get("/bnb/addresses/0xa", headers=roles("tags-private,currency-bnb"))
    assert r.status_code == 200
    assert r.json() == {"currency": "bnb", "address": "0xa"}


def test_grouped_role_alongside_leaf_still_passes():
    client = make_client()
    r = client.get(
        "/bnb/addresses/0xa",
        headers=roles("tags-private,currencies-extended,currency-bnb"),
    )
    assert r.status_code == 200


def test_missing_leaf_role_is_403():
    client = make_client()
    r = client.get("/bnb/addresses/0xa", headers=roles("tags-private"))
    assert r.status_code == 403
    assert r.json() == {"detail": DENIED_DETAIL}


def test_absent_header_means_no_roles():
    client = make_client()
    assert client.get("/bnb/addresses/0xa").status_code == 403


def test_core_currency_is_not_gated():
    client = make_client()
    assert (
        client.get("/btc/addresses/1a", headers=roles("tags-private")).status_code
        == 200
    )
    assert client.get("/btc/addresses/1a").status_code == 200


def test_wrong_currency_role_does_not_grant():
    client = make_client()
    assert (
        client.get("/bnb/addresses/0xa", headers=roles("currency-sol")).status_code
        == 403
    )


# --- header parsing ----------------------------------------------------------


def test_parse_roles_splits_strips_and_decodes():
    assert parse_roles(" tags-private , currency-bnb,, a%2Fb ") == {
        "tags-private",
        "currency-bnb",
        "a/b",
    }
    assert parse_roles(None) == set()
    assert parse_roles("") == set()


def test_percent_encoded_role_is_decoded_before_matching():
    client = make_client()
    assert (
        client.get("/bnb/addresses/0xa", headers=roles("currency%2Dbnb")).status_code
        == 200
    )


def test_legacy_consumer_groups_header_is_ignored():
    client = make_client()
    r = client.get("/bnb/addresses/0xa", headers={"X-Consumer-Groups": "currency-bnb"})
    assert r.status_code == 403


def test_grouped_role_alone_does_not_grant():
    """Keycloak expands the bundle into leaves before the token is issued; a
    header carrying only the bundle name is not a grant."""
    client = make_client()
    assert (
        client.get(
            "/bnb/addresses/0xa", headers=roles("currencies-extended")
        ).status_code
        == 403
    )


# --- every route family under the currency prefix ---------------------------


def test_bulk_route_is_gated_too():
    client = make_client()
    assert client.post("/bnb/bulk.json/get_address", json={}).status_code == 403
    assert (
        client.post(
            "/bnb/bulk.json/get_address", json={}, headers=roles("currency-bnb")
        ).status_code
        == 200
    )


def test_path_currency_matches_case_insensitively():
    client = make_client()
    assert client.get("/BNB/addresses/0xa").status_code == 403


def test_search_with_gated_currency_filter_is_403():
    client = make_client()
    assert client.get("/search?q=0xa&currency=bnb").status_code == 403
    assert (
        client.get(
            "/search?q=0xa&currency=bnb", headers=roles("currency-bnb")
        ).status_code
        == 200
    )
    assert client.get("/search?q=0xa&currency=btc").status_code == 200


# --- listings drop what the caller lacks ------------------------------------


def test_stats_drops_gated_currencies_without_role():
    client = make_client()
    doc = client.get("/stats", headers=roles("currency-bnb")).json()
    assert [c["name"] for c in doc["currencies"]] == ["btc", "bnb"]
    assert doc["version"] == "v"
    doc = client.get("/stats").json()
    assert [c["name"] for c in doc["currencies"]] == ["btc"]


def test_capabilities_drops_gated_networks_without_role():
    client = make_client()
    doc = client.get("/capabilities").json()
    assert [n["network"] for n in doc["networks"]] == ["btc"]
    doc = client.get("/capabilities", headers=roles("currency-bnb")).json()
    assert [n["network"] for n in doc["networks"]] == ["btc", "bnb"]


def test_search_listing_drops_gated_hits_and_keeps_labels():
    client = make_client()
    doc = client.get("/search?q=hit").json()
    assert [c["currency"] for c in doc["currencies"]] == ["btc"]
    assert doc["labels"] == ["kept"]


def test_related_addresses_drops_gated_twins():
    client = make_client()
    doc = client.get(
        "/eth/addresses/0xsame/related_addresses", headers=roles("currency-arb")
    ).json()
    assert [r["currency"] for r in doc["related_addresses"]] == ["trx", "arb"]
    assert doc["next_page"] is None


def test_all_roles_keep_listings_intact():
    client = make_client()
    doc = client.get("/stats", headers=roles("currency-bnb,currency-arb")).json()
    assert doc == STATS


# --- configuration -----------------------------------------------------------


def test_custom_header_and_prefix():
    config = CurrencyRolesConfig(roles_header="X-Roles", currency_role_prefix="net-")
    client = make_client(config=config)
    assert (
        client.get("/bnb/addresses/0xa", headers={"X-Roles": "net-bnb"}).status_code
        == 200
    )
    assert (
        client.get("/bnb/addresses/0xa", headers=roles("currency-bnb")).status_code
        == 403
    )


def test_nothing_gated_passes_everything_through():
    client = make_client(gated=())
    assert client.get("/bnb/addresses/0xa").status_code == 200
    assert client.get("/stats").json() == STATS
