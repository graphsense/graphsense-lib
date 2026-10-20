"""Address notifications routes (/notifications/*), forwarded to gs-notifications.

gs-notifications is a separate service on the internal network: it keeps the
subscriptions, polls this API for new transactions and fills each user's feed
and email. These routes are its public face, so the dashboard and API clients
use one API with one spec. They are typed, not a catch-all proxy: every route
here exists in the service with the same models (models/notifications.py),
and requests are validated with them before they are forwarded. A successful
answer is passed on as the service sent it, not validated again: a newer
service may add fields or enum values, which must not turn into a 500 here.

Identity: the gateway authenticates the caller and sets the identity and
roles headers (``forward_headers``); exactly those are passed on, nothing
else. The service trusts them because only this app can reach it.

Errors: the service's 4xx and 5xx answers reach the client unchanged (status,
``detail``, ``Retry-After``); they are its decisions, e.g. 409 "already
subscribed". A service that cannot be reached, or whose 2xx answer is not
JSON, is a 502; one that does not answer in time is a 504.

Registered only when ``notifications.enabled`` is set (see
``register_notifications_routes``); without it the app has no such routes.
"""

import asyncio
import logging
import time
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, FastAPI, Path, Query, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from graphsenselib.web.config import NotificationsConfig
from graphsenselib.web.models.general import ServiceInfo
from graphsenselib.web.models.notifications import (
    FeedPage,
    FeedReadRequest,
    FeedReadResult,
    MAX_ID,
    Subscription,
    SubscriptionCreate,
    SubscriptionPage,
    SubscriptionUpdate,
    UnreadCount,
)

logger = logging.getLogger(__name__)

PREFIX = "/notifications"
TAG = "notifications"
TAG_DESCRIPTION = (
    "**Beta.** Address notifications: subscribe to addresses and get new "
    "transactions in a feed and by email. These routes may still change."
)

# GET /meta of the service, for the services section of /capabilities: cached,
# a failure too, so a down service costs one short request a minute.
META_TTL_S = 60.0
META_TIMEOUT_S = 3.0

# Carried back to the client on an error: a paced or busy service says when
# to come back.
_PASSED_RESPONSE_HEADERS = ("retry-after",)


class _ServiceAnswer(Exception):
    """A non-2xx answer from the service, returned to the client as is."""

    def __init__(self, response: Response):
        self.response = response


def _error(status_code: int, detail: str) -> _ServiceAnswer:
    return _ServiceAnswer(
        JSONResponse(status_code=status_code, content={"detail": detail})
    )


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
        # (monotonic time, answer) of the last GET /meta
        self._meta: Optional[tuple[float, Optional[dict[str, Any]]]] = None
        # One refresh at a time: concurrent callers wait for it and share
        # its answer instead of each waiting on a dead service.
        self._meta_lock = asyncio.Lock()

    async def call(
        self,
        request: Request,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
    ) -> Response:
        """The service's 2xx answer, status and JSON body unchanged (no body
        for 204); raises _ServiceAnswer otherwise."""
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
        except httpx.TimeoutException as e:
            logger.error("notifications service timed out: %r", e)
            raise _error(504, "notifications service timed out") from e
        except httpx.HTTPError as e:
            logger.error("notifications service unreachable: %r", e)
            raise _error(502, "notifications service unavailable") from e
        if response.status_code == 204:
            return Response(status_code=204)
        if response.is_success:
            try:
                content = response.json()
            except ValueError as e:
                logger.error(
                    "notifications service: %s %s answered %d without JSON",
                    method,
                    path,
                    response.status_code,
                )
                raise _error(502, "invalid answer from notifications service") from e
            # Not re-validated against the response model (see module doc);
            # returning a Response makes FastAPI skip that.
            return JSONResponse(content=content, status_code=response.status_code)
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

    async def meta(self) -> Optional[dict[str, Any]]:
        """The service's GET /meta (no identity needed), or None when it does
        not answer; cached for META_TTL_S either way."""
        if (cached := self._cached_meta()) is not None:
            return cached[1]
        async with self._meta_lock:
            # Refreshed while we waited for the lock?
            if (cached := self._cached_meta()) is not None:
                return cached[1]
            doc: Optional[dict[str, Any]] = None
            try:
                response = await self.client.get("/meta", timeout=META_TIMEOUT_S)
                response.raise_for_status()
                answer = response.json()
                if isinstance(answer, dict):
                    doc = answer
            except (httpx.HTTPError, ValueError) as e:
                logger.warning("notifications service /meta: %r", e)
            self._meta = (time.monotonic(), doc)
            return doc

    def _cached_meta(self) -> Optional[tuple[float, Optional[dict[str, Any]]]]:
        if self._meta is not None and time.monotonic() - self._meta[0] < META_TTL_S:
            return self._meta
        return None


async def notifications_service_info(app: FastAPI) -> Optional[ServiceInfo]:
    """The notifications entry of /capabilities; None when the routes are off."""
    client: Optional[NotificationsClient] = getattr(
        app.state, "notifications_client", None
    )
    if client is None:
        return None
    meta = await client.meta()
    version = meta.get("version") if meta else None
    api = meta.get("api") if meta else None
    limits = meta.get("limits") if meta else None
    if isinstance(limits, dict):
        limits = {
            k: v
            for k, v in limits.items()
            if isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool)
        }
    else:
        limits = None
    return ServiceInfo(
        name=TAG,
        available=meta is not None,
        version=version if isinstance(version, str) else None,
        api=api if isinstance(api, int) and not isinstance(api, bool) else None,
        beta=True,
        limits=limits or None,
    )


def _client(request: Request) -> NotificationsClient:
    return request.app.state.notifications_client


SubscriptionIdPath = Path(..., ge=1, le=MAX_ID, description="The subscription's id")
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
    return await client.call(request, "DELETE", f"/subscriptions/{subscription_id}")


@router.get(
    "/feed",
    summary="List notified transactions",
    operation_id="list_notification_feed",
    response_model=FeedPage,
)
async def read_feed(
    request: Request,
    unread: bool = Query(False, description="Only unread items"),
    subscription_id: Optional[int] = Query(
        None, ge=1, le=MAX_ID, description="Only this subscription's items"
    ),
    cursor: Optional[str] = CursorQuery,
    limit: int = LimitQuery,
    client: NotificationsClient = Depends(_client),
):
    """Newest first."""
    return await client.call(
        request,
        "GET",
        "/feed",
        params={
            "unread": str(unread).lower(),
            "subscription_id": subscription_id,
            "cursor": cursor,
            "limit": limit,
        },
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


async def _service_answer(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, _ServiceAnswer)
    return exc.response


def order_notifications_tag(schema: dict[str, Any]) -> dict[str, Any]:
    """Put the notifications tag last in the spec and label it beta.

    The routes are registered before the network routers (so no
    ``/{currency}`` path shadows them), which would otherwise make them the
    first section in Swagger UI and ReDoc. Both order sections by the
    top-level ``tags`` list, so it is filled in path order with this tag
    moved to the end. A spec without the tag is returned unchanged.
    """
    paths = schema.get("paths", {})

    def is_ours(item: dict[str, Any]) -> bool:
        return any(
            TAG in op.get("tags", []) for op in item.values() if isinstance(op, dict)
        )

    if not any(is_ours(item) for item in paths.values()):
        return schema
    schema["paths"] = {
        **{p: i for p, i in paths.items() if not is_ours(i)},
        **{p: i for p, i in paths.items() if is_ours(i)},
    }
    known = {t["name"]: t for t in schema.get("tags", [])}
    names: list[str] = []
    for item in schema["paths"].values():
        for op in item.values():
            if isinstance(op, dict):
                names += [n for n in op.get("tags", []) if n not in names]
    names += [n for n in known if n not in names]
    schema["tags"] = [known.get(n, {"name": n}) for n in names if n != TAG] + [
        {**known.get(TAG, {}), "name": TAG, "description": TAG_DESCRIPTION}
    ]
    return schema


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
        tags=[TAG],
        include_in_schema=config.include_in_schema,
    )
    logger.info(
        "Notifications routes mounted at %s/* -> %s (in OpenAPI spec: %s)",
        PREFIX,
        config.url,
        config.include_in_schema,
    )
