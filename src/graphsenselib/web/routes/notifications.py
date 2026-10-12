"""Address notifications routes (/notifications/*), forwarded to gs-notifications.

gs-notifications is a separate service on the internal network: it keeps the
subscriptions, polls this API for new transactions and fills each user's feed
and email. These routes are its public face, so the dashboard and API clients
use one API with one spec. They are typed, not a catch-all proxy: every route
here exists in the service with the same models (models/notifications.py),
and a response that does not match the model is an error, not passed on.

Identity: the gateway authenticates the caller and sets the identity and
roles headers (``forward_headers``); exactly those are passed on, nothing
else. The service trusts them because only this app can reach it.

Errors: the service's 4xx and 5xx answers reach the client unchanged (status,
``detail``, ``Retry-After``); they are its decisions, e.g. 409 "already
subscribed". A service that cannot be reached is a 502.

Registered only when ``notifications.enabled`` is set (see
``register_notifications_routes``); without it the app has no such routes.
"""

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, FastAPI, Path, Query, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from graphsenselib.web.config import NotificationsConfig
from graphsenselib.web.models.notifications import (
    FeedPage,
    FeedReadRequest,
    FeedReadResult,
    Subscription,
    SubscriptionCreate,
    SubscriptionPage,
    SubscriptionUpdate,
    UnreadCount,
    UserSettings,
    UserSettingsUpdate,
)

logger = logging.getLogger(__name__)

PREFIX = "/notifications"

# Carried back to the client on an error: a paced or busy service says when
# to come back.
_PASSED_RESPONSE_HEADERS = ("retry-after",)


class _ServiceAnswer(Exception):
    """A non-2xx answer from the service, returned to the client as is."""

    def __init__(self, response: Response):
        self.response = response


class NotificationsClient:
    """Talks to the service: one shared httpx client, identity headers only."""

    def __init__(
        self, config: NotificationsConfig, client: Optional[httpx.AsyncClient] = None
    ):
        self.config = config
        self.client = client or httpx.AsyncClient(
            base_url=config.url.rstrip("/"),  # ty: ignore[possibly-missing-attribute]
            timeout=httpx.Timeout(config.timeout_s),
        )

    async def call(
        self,
        request: Request,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
    ) -> Optional[Any]:
        """The service's JSON answer for a 2xx (None for 204); raises
        _ServiceAnswer otherwise."""
        headers = {
            name: value
            for name in self.config.forward_headers
            if (value := request.headers.get(name)) is not None
        }
        try:
            response = await self.client.request(
                method,
                path,
                params={k: v for k, v in (params or {}).items() if v is not None},
                json=body,
                headers=headers,
            )
        except httpx.HTTPError as e:
            logger.error("notifications service unreachable: %r", e)
            raise _ServiceAnswer(
                JSONResponse(
                    status_code=502,
                    content={"detail": "notifications service unavailable"},
                )
            ) from e
        if response.status_code == 204:
            return None
        if response.is_success:
            return response.json()
        try:
            content = response.json()
        except ValueError:
            content = {"detail": response.text or response.reason_phrase}
        raise _ServiceAnswer(
            JSONResponse(
                status_code=response.status_code,
                content=content,
                headers={
                    k: response.headers[k]
                    for k in _PASSED_RESPONSE_HEADERS
                    if k in response.headers
                },
            )
        )


def _client(request: Request) -> NotificationsClient:
    return request.app.state.notifications_client


SubscriptionIdPath = Path(..., ge=1, description="The subscription's id")
CursorQuery = Query(None, description="next_cursor of the previous page")
LimitQuery = Query(50, ge=1, le=200, description="Page size")

router = APIRouter(prefix=PREFIX)


@router.get(
    "/subscriptions",
    summary="List your address subscriptions",
    operation_id="list_notification_subscriptions",
    response_model=SubscriptionPage,
)
async def list_subscriptions(
    request: Request,
    cursor: Optional[str] = CursorQuery,
    limit: int = LimitQuery,
    client: NotificationsClient = Depends(_client),
):
    """Newest first."""
    return await client.call(
        request, "GET", "/subscriptions", params={"cursor": cursor, "limit": limit}
    )


@router.post(
    "/subscriptions",
    status_code=201,
    summary="Subscribe to an address",
    description=(
        "Notifies about new transactions of the address. The address is "
        "looked up first and stored in its canonical form; an address not "
        "seen on chain yet is allowed and reported from its first transaction."
    ),
    operation_id="create_notification_subscription",
    response_model=Subscription,
)
async def create_subscription(
    request: Request,
    body: SubscriptionCreate,
    client: NotificationsClient = Depends(_client),
):
    return await client.call(
        request, "POST", "/subscriptions", body=body.model_dump(mode="json")
    )


@router.get(
    "/subscriptions/{subscription_id}",
    summary="Get an address subscription",
    operation_id="get_notification_subscription",
    response_model=Subscription,
)
async def get_subscription(
    request: Request,
    subscription_id: int = SubscriptionIdPath,
    client: NotificationsClient = Depends(_client),
):
    return await client.call(request, "GET", f"/subscriptions/{subscription_id}")


@router.patch(
    "/subscriptions/{subscription_id}",
    summary="Change an address subscription",
    description="Only the fields sent are changed; `label: null` clears the label.",
    operation_id="update_notification_subscription",
    response_model=Subscription,
)
async def update_subscription(
    request: Request,
    body: SubscriptionUpdate,
    subscription_id: int = SubscriptionIdPath,
    client: NotificationsClient = Depends(_client),
):
    # exclude_unset: "not sent" and "sent as null" differ (label).
    return await client.call(
        request,
        "PATCH",
        f"/subscriptions/{subscription_id}",
        body=body.model_dump(mode="json", exclude_unset=True),
    )


@router.delete(
    "/subscriptions/{subscription_id}",
    status_code=204,
    summary="Unsubscribe from an address",
    operation_id="delete_notification_subscription",
)
async def delete_subscription(
    request: Request,
    subscription_id: int = SubscriptionIdPath,
    client: NotificationsClient = Depends(_client),
):
    await client.call(request, "DELETE", f"/subscriptions/{subscription_id}")
    return Response(status_code=204)


@router.get(
    "/feed",
    summary="List notified transactions",
    operation_id="list_notification_feed",
    response_model=FeedPage,
)
async def read_feed(
    request: Request,
    unread: bool = Query(False, description="Only unread items"),
    cursor: Optional[str] = CursorQuery,
    limit: int = LimitQuery,
    client: NotificationsClient = Depends(_client),
):
    """Newest first."""
    return await client.call(
        request,
        "GET",
        "/feed",
        params={"unread": str(unread).lower(), "cursor": cursor, "limit": limit},
    )


@router.post(
    "/feed/read",
    summary="Mark notified transactions read",
    operation_id="mark_notification_feed_read",
    response_model=FeedReadResult,
)
async def mark_read(
    request: Request,
    body: FeedReadRequest,
    client: NotificationsClient = Depends(_client),
):
    return await client.call(
        request,
        "POST",
        "/feed/read",
        body=body.model_dump(mode="json", exclude_unset=True),
    )


@router.get(
    "/feed/unread_count",
    summary="Count unread notified transactions",
    operation_id="get_notification_unread_count",
    response_model=UnreadCount,
)
async def unread_count(
    request: Request, client: NotificationsClient = Depends(_client)
):
    return await client.call(request, "GET", "/feed/unread_count")


@router.get(
    "/settings",
    summary="Get your notification settings",
    operation_id="get_notification_settings",
    response_model=UserSettings,
)
async def read_settings(
    request: Request, client: NotificationsClient = Depends(_client)
):
    return await client.call(request, "GET", "/settings")


@router.patch(
    "/settings",
    summary="Change your notification settings",
    operation_id="update_notification_settings",
    response_model=UserSettings,
)
async def update_settings(
    request: Request,
    body: UserSettingsUpdate,
    client: NotificationsClient = Depends(_client),
):
    return await client.call(
        request,
        "PATCH",
        "/settings",
        body=body.model_dump(mode="json", exclude_unset=True),
    )


@router.delete(
    "/me",
    status_code=204,
    summary="Delete all your notification data",
    description="Settings, subscriptions and the feed. Idempotent.",
    operation_id="delete_notification_data",
)
async def delete_me(request: Request, client: NotificationsClient = Depends(_client)):
    await client.call(request, "DELETE", "/me")
    return Response(status_code=204)


async def _service_answer(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, _ServiceAnswer)
    return exc.response


def register_notifications_routes(
    app: FastAPI,
    config: Optional[NotificationsConfig],
    client: Optional[httpx.AsyncClient] = None,
) -> None:
    """Add the /notifications/* routes when the feature is enabled; a no-op
    otherwise. The httpx client lives on app.state so teardown closes it."""
    if config is None or not config.enabled:
        return
    app.state.notifications_client = NotificationsClient(config, client)
    app.add_exception_handler(_ServiceAnswer, _service_answer)
    app.include_router(
        router,
        tags=["notifications"],
        include_in_schema=config.include_in_schema,
    )
    logger.info(
        "Notifications routes mounted at %s/* -> %s (in OpenAPI spec: %s)",
        PREFIX,
        config.url,
        config.include_in_schema,
    )
