"""The /notifications/* routes forwarding to gs-notifications
(web/routes/notifications.py), against a recording mock service."""

import json

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from graphsenselib.web.app import _register_exception_handlers
from graphsenselib.web.config import GSRestConfig, NotificationsConfig
from graphsenselib.web.routes.notifications import (
    TAG_DESCRIPTION,
    notifications_service_info,
    order_notifications_tag,
    register_notifications_routes,
)

SERVICE_URL = "http://gs-notifications:8000"
GENESIS = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

SUBSCRIPTION = {
    "id": 7,
    "network": "btc",
    "address": GENESIS,
    "frequency": "daily",
    "filters": {"v": 1, "direction": "both", "min_value": None, "tokens": None, "include_zero_value": False},
    "label": "Genesis",
    "paused_reason": None,
    "created_at": "2026-10-12T10:00:00Z",
    "last_checked_at": "2026-10-12T10:05:00Z",
    "next_check_at": "2026-10-12T10:15:00Z",
    "check_errors": 0,
    "unread": 0,
}

FEED_ITEM = {
    "id": 3,
    "subscription_id": 7,
    "label": "Genesis",
    "network": "btc",
    "address": GENESIS,
    "tx_hash": "0" * 64,
    "height": 900000,
    "timestamp": "2026-10-12T10:00:00Z",
    "direction": "in",
    "currency": "btc",
    "token": False,
    "value": "50000000",
    "amount": "0.5",
    "fiat": {"eur": 30000.0, "usd": 32500.0},
    "counterparty": None,
    "read": False,
}

GATEWAY_HEADERS = {
    "X-Username": "alice",
    "X-User-Roles": "currency-bnb,ikn-dashboard",
    "X-Consumer-Username": "alice",
    "Authorization": "api-key-of-alice",
    "Cookie": "session=secret",
}


def make_client(answer=None, include_in_schema=False, enabled=True):
    """App with the routes behind a recording mock service. ``answer(request)``
    overrides the service's response."""
    seen: list[httpx.Request] = []

    def service(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if answer is not None:
            return answer(request)
        path, method = request.url.path, request.method
        if path == "/subscriptions" and method == "GET":
            return httpx.Response(
                200, json={"items": [SUBSCRIPTION], "next_cursor": "7"}
            )
        if path == "/subscriptions" and method == "POST":
            return httpx.Response(201, json=SUBSCRIPTION)
        if path.startswith("/subscriptions/") and method in ("GET", "PATCH"):
            return httpx.Response(200, json=SUBSCRIPTION)
        if method == "DELETE":
            return httpx.Response(204)
        if path == "/feed":
            return httpx.Response(200, json={"items": [FEED_ITEM], "next_cursor": None})
        if path == "/feed/read":
            return httpx.Response(200, json={"marked": 2})
        if path == "/meta":
            return httpx.Response(
                200,
                json={
                    "service": "gs-notifications",
                    "version": "26.10.0rc1",
                    "git_sha": "572e526",
                    "built_at": None,
                    "api": 1,
                    "limits": {"instant_mail_interval_minutes": 5, "future": "x"},
                },
            )
        if path == "/feed/unread_count":
            return httpx.Response(
                200, json={"count": 1, "capped": False, "subscriptions": 1}
            )
        return httpx.Response(404, json={"detail": "Not Found"})

    app = FastAPI()
    _register_exception_handlers(app)
    config = NotificationsConfig(
        enabled=enabled, url=SERVICE_URL, include_in_schema=include_in_schema
    )
    register_notifications_routes(
        app,
        config,
        client=httpx.AsyncClient(
            base_url=SERVICE_URL, transport=httpx.MockTransport(service)
        ),
    )

    # A network route registered after, as create_app does: must not shadow.
    @app.get("/{currency}/feed")
    async def network_route(currency: str):
        return {"served": "network"}

    return TestClient(app, raise_server_exceptions=False), seen


def body_of(request: httpx.Request) -> dict:
    return json.loads(request.content)


def test_disabled_registers_nothing():
    client, seen = make_client(enabled=False)
    assert client.get("/notifications/subscriptions").status_code == 404
    assert seen == []


def test_only_identity_and_roles_headers_are_forwarded():
    client, seen = make_client()
    response = client.get("/notifications/subscriptions", headers=GATEWAY_HEADERS)
    assert response.status_code == 200
    forwarded = seen[0].headers
    assert forwarded["x-username"] == "alice"
    assert forwarded["x-user-roles"] == "currency-bnb,ikn-dashboard"
    for header in ("authorization", "cookie", "x-consumer-username"):
        assert header not in forwarded


def test_missing_identity_is_not_invented():
    client, seen = make_client()
    client.get("/notifications/feed/unread_count")
    assert "x-username" not in seen[0].headers


def test_list_subscriptions_passes_paging():
    client, seen = make_client()
    response = client.get("/notifications/subscriptions?cursor=9&limit=20")
    assert response.json() == {"items": [SUBSCRIPTION], "next_cursor": "7"}
    assert seen[0].url.path == "/subscriptions"
    assert dict(seen[0].url.params) == {"cursor": "9", "limit": "20"}


def test_unset_query_params_are_left_out():
    client, seen = make_client()
    client.get("/notifications/feed")
    assert dict(seen[0].url.params) == {"unread": "false", "limit": "50"}


def test_limit_is_validated_here():
    client, seen = make_client()
    assert client.get("/notifications/feed?limit=500").status_code == 422
    assert seen == []


def test_create_subscription():
    client, seen = make_client()
    response = client.post(
        "/notifications/subscriptions",
        json={"network": "btc", "address": GENESIS, "label": "Genesis"},
        headers=GATEWAY_HEADERS,
    )
    assert response.status_code == 201
    assert response.json() == SUBSCRIPTION
    sent = body_of(seen[0])
    assert (sent["network"], sent["address"], sent["label"]) == (
        "btc",
        GENESIS,
        "Genesis",
    )


def test_unknown_body_fields_are_rejected_before_forwarding():
    client, seen = make_client()
    response = client.post(
        "/notifications/subscriptions",
        json={"network": "btc", "address": GENESIS, "colour": "red"},
    )
    assert response.status_code == 422
    assert seen == []


def test_patch_sends_only_the_fields_given():
    client, seen = make_client()
    client.patch("/notifications/subscriptions/7", json={"label": None})
    assert seen[0].method == "PATCH"
    assert seen[0].url.path == "/subscriptions/7"
    assert body_of(seen[0]) == {"label": None}


def test_delete_subscription_is_204():
    client, seen = make_client()
    response = client.delete("/notifications/subscriptions/7")
    assert response.status_code == 204
    assert response.content == b""
    assert (seen[0].method, seen[0].url.path) == ("DELETE", "/subscriptions/7")


def test_feed_routes():
    client, seen = make_client()
    assert client.get("/notifications/feed?unread=true").json()["items"] == [
        FEED_ITEM
    ]
    assert seen[0].url.params["unread"] == "true"
    assert client.post("/notifications/feed/read", json={"up_to": 3}).json() == {
        "marked": 2
    }
    assert body_of(seen[1]) == {"up_to": 3}
    client.get("/notifications/feed?subscription_id=7")
    assert seen[2].url.params["subscription_id"] == "7"
    assert "subscription_id" not in seen[0].url.params
    client.post("/notifications/feed/read", json={"up_to": 3, "subscription_id": 7})
    assert body_of(seen[3]) == {"up_to": 3, "subscription_id": 7}
    assert client.get("/notifications/feed/unread_count").json() == {
        "count": 1,
        "capped": False,
        "subscriptions": 1,
    }


def test_settings_are_not_exposed():
    # The service keeps per-user settings internally: no global email switch
    # for users; the timezone comes from the browser with each subscription.
    client, seen = make_client()
    assert client.get("/notifications/settings").status_code == 404
    assert client.patch("/notifications/settings", json={}).status_code in (404, 405)
    assert seen == []


def test_deleting_all_data_is_not_exposed():
    # The service's DELETE /me wipes a user's notification data; it is for
    # account deletion on the internal network, not a user-facing route.
    client, seen = make_client()
    assert client.delete("/notifications/me").status_code in (404, 405)
    assert seen == []


def test_create_passes_the_browser_timezone():
    client, seen = make_client()
    client.post(
        "/notifications/subscriptions",
        json={"network": "btc", "address": GENESIS, "timezone": "Europe/Vienna"},
    )
    assert body_of(seen[0])["timezone"] == "Europe/Vienna"


def test_invalid_timezone_is_rejected_here():
    client, seen = make_client()
    response = client.post(
        "/notifications/subscriptions",
        json={"network": "btc", "address": GENESIS, "timezone": "Mars/Base"},
    )
    assert response.status_code == 422
    assert seen == []


@pytest.mark.parametrize(
    "status, detail",
    [
        (401, "missing X-Username header"),
        (403, "network not enabled for this account"),
        (404, "subscription not found"),
        (409, "already subscribed to this address"),
        (422, "unknown network 'xyz'"),
    ],
)
def test_service_errors_reach_the_client_unchanged(status, detail):
    client, _ = make_client(
        answer=lambda r: httpx.Response(status, json={"detail": detail})
    )
    response = client.post(
        "/notifications/subscriptions", json={"network": "btc", "address": GENESIS}
    )
    assert response.status_code == status
    assert response.json() == {"detail": detail}


def test_busy_service_passes_retry_after():
    client, _ = make_client(
        answer=lambda r: httpx.Response(
            503,
            json={"detail": "graphsense is busy, try again shortly"},
            headers={"Retry-After": "12", "X-Internal": "no"},
        )
    )
    response = client.post(
        "/notifications/subscriptions", json={"network": "btc", "address": GENESIS}
    )
    assert response.status_code == 503
    assert response.headers["retry-after"] == "12"
    assert "x-internal" not in response.headers


def test_non_json_error_body_becomes_detail():
    client, _ = make_client(answer=lambda r: httpx.Response(500, text="boom"))
    response = client.get("/notifications/feed/unread_count")
    assert response.status_code == 500
    assert response.json() == {"detail": "boom"}


def test_unreachable_service_is_502():
    def down(request):
        raise httpx.ConnectError("connection refused", request=request)

    client, _ = make_client(answer=down)
    response = client.get("/notifications/feed/unread_count")
    assert response.status_code == 502
    assert response.json() == {"detail": "notifications service unavailable"}


def test_response_that_breaks_the_contract_is_an_error():
    client, _ = make_client(
        answer=lambda r: httpx.Response(200, json={"count": "many"})
    )
    assert client.get("/notifications/feed/unread_count").status_code == 500


def test_polling_state_passes_through_and_is_optional():
    polled = {
        **SUBSCRIPTION,
        "last_checked_at": "2026-10-14T12:00:00Z",
        "next_check_at": "2026-10-14T12:10:00Z",
        "check_errors": 2,
        "unread": 5,
    }
    client, _ = make_client(answer=lambda r: httpx.Response(200, json=polled))
    got = client.get("/notifications/subscriptions/7").json()
    assert (got["check_errors"], got["unread"]) == (2, 5)
    assert got["last_checked_at"].startswith("2026-10-14T12:00:00")
    # an older service without the fields still meets the contract
    polling = ("last_checked_at", "next_check_at", "check_errors", "unread")
    legacy = {k: v for k, v in SUBSCRIPTION.items() if k not in polling}
    client, _ = make_client(answer=lambda r: httpx.Response(200, json=legacy))
    got = client.get("/notifications/subscriptions/7").json()
    assert (got["last_checked_at"], got["check_errors"], got["unread"]) == (None, 0, 0)


def test_unknown_response_fields_are_dropped():
    client, _ = make_client(
        answer=lambda r: httpx.Response(
            200, json={"count": 1, "capped": False, "internal": 1}
        )
    )
    # ... and a field an older service doesn't send gets its default
    assert client.get("/notifications/feed/unread_count").json() == {
        "count": 1,
        "capped": False,
        "subscriptions": 0,
    }


def test_network_routes_are_not_shadowed():
    client, seen = make_client()
    assert client.get("/btc/feed").json() == {"served": "network"}
    assert seen == []


def test_routes_stay_out_of_the_spec_by_default():
    client, _ = make_client()
    paths = client.get("/openapi.json").json()["paths"]
    assert not any(p.startswith("/notifications") for p in paths)


def test_routes_in_the_spec_when_configured():
    client, _ = make_client(include_in_schema=True)
    spec = client.get("/openapi.json").json()
    assert set(spec["paths"]) >= {
        "/notifications/subscriptions",
        "/notifications/subscriptions/{subscription_id}",
        "/notifications/feed",
        "/notifications/feed/read",
        "/notifications/feed/unread_count",
    }
    op = spec["paths"]["/notifications/subscriptions"]["post"]
    assert op["operationId"] == "create_notification_subscription"
    assert op["tags"] == ["notifications"]


def test_tag_is_last_and_beta_in_the_spec():
    client, _ = make_client(include_in_schema=True)
    spec = client.app.openapi()
    spec["paths"]["/{currency}/feed"]["get"]["tags"] = ["txs"]
    spec["tags"] = [{"name": "txs", "description": "Transactions"}]
    spec = order_notifications_tag(spec)
    assert [t["name"] for t in spec["tags"]] == ["txs", "notifications"]
    assert spec["tags"][0] == {"name": "txs", "description": "Transactions"}
    assert spec["tags"][-1]["description"] == TAG_DESCRIPTION
    assert "Beta" in TAG_DESCRIPTION
    assert list(spec["paths"])[0] == "/{currency}/feed"
    assert all(p.startswith("/notifications") for p in list(spec["paths"])[1:])


def test_spec_without_the_routes_is_unchanged():
    client, _ = make_client()
    spec = client.app.openapi()
    before = json.dumps(spec)
    assert json.dumps(order_notifications_tag(spec)) == before
    assert "tags" not in spec


async def test_service_info_from_meta():
    client, seen = make_client()
    info = await notifications_service_info(client.app)
    assert info.model_dump() == {
        "name": "notifications",
        "available": True,
        "version": "26.10.0rc1",
        "api": 1,
        "beta": True,
        # Non-integer values are dropped.
        "limits": {"instant_mail_interval_minutes": 5},
    }
    assert (seen[0].method, seen[0].url.path) == ("GET", "/meta")
    # No identity goes to /meta.
    assert "x-username" not in seen[0].headers
    await notifications_service_info(client.app)
    assert len(seen) == 1  # cached


async def test_service_info_when_the_service_is_down():
    def down(request):
        raise httpx.ConnectError("refused", request=request)

    client, _ = make_client(answer=down)
    info = await notifications_service_info(client.app)
    assert info.available is False
    assert info.version is None and info.api is None and info.limits is None
    assert info.beta is True


async def test_service_info_ignores_a_malformed_meta():
    client, _ = make_client(
        answer=lambda r: httpx.Response(
            200, json={"version": 7, "api": True, "limits": [5]}
        )
    )
    info = await notifications_service_info(client.app)
    assert info.available is True
    assert info.version is None and info.api is None and info.limits is None


async def test_no_service_info_when_disabled():
    client, _ = make_client(enabled=False)
    assert await notifications_service_info(client.app) is None


def test_enabled_needs_a_url():
    with pytest.raises(ValidationError, match="notifications.url"):
        NotificationsConfig(enabled=True)


def test_config_section_is_optional():
    assert GSRestConfig.model_fields["notifications"].default is None
    config = NotificationsConfig(enabled=True, url=SERVICE_URL)
    assert config.forward_headers == ["X-Username", "X-User-Roles"]
    assert config.include_in_schema is False
