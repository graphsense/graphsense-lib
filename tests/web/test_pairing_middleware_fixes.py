"""The bridge pairing of the external backends middleware (rule 6), after
the pre-merge review of 2026-11-10: a request that already carries
``only_bridges`` (a backend's upstream fallback) is answered locally; the
conversions route may name the tx the backend is asked about (the root trace
``_I0`` is the whole tx); and a failed pairing does not put the backend in the
skip window the merges share.

Set up as tests/web/test_delegated_conversions.py: the real txs route and
TxsService over a fake database, the middleware in front, an httpx
MockTransport for the backend.
"""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI, Request, Response
from starlette.testclient import TestClient

from graphsenselib.db.asynchronous.services.txs_service import TxsService
from graphsenselib.web.app import _register_exception_handlers
from graphsenselib.web.config import GSRestConfig
from graphsenselib.web.middleware.external_backends import (
    DECLINED_HEADER,
    ExternalBackendMiddleware,
)
from graphsenselib.web.routes import txs

from tests.defi.test_delegated_bridges import db_rows
from tests.web.test_delegated_conversions import (
    BACKEND_URL,
    BRIDGE_ROW,
    CCTP_BURN,
    CONVERSIONS,
    HALF_ROW,
    LOCAL_ROW,
    SWAP_ROW,
    fake_db,
    local_swap,  # noqa: F401 -- the autouse fixture of that module
    make_client,
    records_of_the_middleware,
)

STATS = {"currencies": [{"name": "arb", "no_blocks": 1}]}


# ---------------------------------------------------------------------------
# loop guard: a request from a backend's upstream fallback asks no backend
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("query", ["only_bridges=true", "only_bridges=false"])
def test_a_request_carrying_only_bridges_is_answered_without_a_backend(query):
    """The adapter's upstream fallback re-asks a delegating graphsense-lib
    for the conversions it cannot serve, with only_bridges; offering that
    request the pairing would ask the adapter again, and so on."""
    client, seen, _ = make_client()
    response = client.get(f"{CONVERSIONS}?{query}")
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert seen == []
    assert DECLINED_HEADER not in response.headers


# ---------------------------------------------------------------------------
# the conversions route may name the tx the backend is asked about
# ---------------------------------------------------------------------------


def pairing_client(kwargs):
    """A conversions route that calls the offered pairing with `kwargs` and
    answers the eth legs of the rows it got, behind the middleware."""
    app = FastAPI()

    @app.get("/{currency}/txs/{tx_hash}/conversions")
    async def conversions(request: Request, currency: str, tx_hash: str):
        rows = await request.state.pair_bridges(**kwargs)
        return [row.from_asset_transfer for row in rows]

    config = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "external_backends": {
                "enabled": True,
                "delegated_conversions": {"eth": {"url": BACKEND_URL}},
            },
        }
    )
    seen: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[BRIDGE_ROW, SWAP_ROW])

    app.add_middleware(
        ExternalBackendMiddleware,
        config=config.external_backends,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    return TestClient(app), seen


ROOT_TRACE = f"/eth/txs/{CCTP_BURN}_I0/conversions"


@pytest.mark.parametrize(
    "kwargs, asked",
    [
        ({}, f"{CCTP_BURN}_I0"),
        ({"tx_identifier": CCTP_BURN}, CCTP_BURN),
        # the local lookup ignores whitespace in a hash (bytes.fromhex), so a
        # control character gets this far; httpx refuses it unencoded
        ({"tx_identifier": f"{CCTP_BURN}\x0b"}, f"{CCTP_BURN}%0B"),
    ],
    ids=["as asked", "the whole tx", "quoted"],
)
def test_the_route_may_name_the_tx_the_backend_is_asked_about(kwargs, asked):
    """Without a tx_identifier the backend is asked for the request path's
    identifier, sub-tx suffix included, as before. The route passes the bare
    hash when the requested root trace (_I0) is the whole tx: the backend
    scopes its answer to the sub-tx it is asked for, and no CCTP leg is the
    root trace."""
    client, seen = pairing_client(kwargs)
    response = client.get(ROOT_TRACE)
    assert response.status_code == 200
    assert response.json() == [BRIDGE_ROW["from_asset_transfer"]]
    assert len(seen) == 1
    assert (
        seen[0].url.raw_path
        == f"/eth/txs/{asked}/conversions?only_bridges=true".encode()
    )


# ---------------------------------------------------------------------------
# a failed pairing does not hide the backend's networks from the merges
# ---------------------------------------------------------------------------


def merging_client(conversions_answer, stats_answer=None, timeout_s=None):
    """The txs routes and a local /stats behind the middleware; the backend
    serves arb AND pairs eth's bridges. `conversions_answer` is the backend's
    answer to the pairing and `stats_answer` its answer to /stats: an
    httpx.Response, an exception to raise, or an async function of the
    request."""
    app = FastAPI()
    app.include_router(txs.router, prefix="/{currency}")
    _register_exception_handlers(app)

    @app.get("/stats")
    async def stats():
        return Response(content=b'{"currencies": []}', media_type="application/json")

    backends = {
        "enabled": True,
        "networks": {"arb": {"url": BACKEND_URL}},
        "delegated_conversions": {"eth": {"url": BACKEND_URL}},
    }
    if timeout_s is not None:
        backends["merge_timeout_s"] = timeout_s
    config = GSRestConfig.from_dict(
        {
            "database": {"nodes": ["localhost"]},
            "external_backends": backends,
            "included_bridges": ["cctp"],
        }
    )
    db = fake_db(db_rows("v2_burn"), (CCTP_BURN,))
    app.state.config = config
    app.state.db = db
    app.state.services = SimpleNamespace(
        txs_service=TxsService(db=db, rates_service=MagicMock(), logger=MagicMock())
    )
    seen: list[httpx.Request] = []

    async def answer(request, what):
        if isinstance(what, Exception):
            raise what
        if callable(what):
            return await what(request)
        return what

    async def backend(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/stats":
            return await answer(
                request, stats_answer or httpx.Response(200, json=STATS)
            )
        return await answer(request, conversions_answer)

    app.add_middleware(
        ExternalBackendMiddleware,
        config=config.external_backends,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    return TestClient(app), seen


async def slow(request):
    await asyncio.sleep(3)
    return httpx.Response(200, json=[BRIDGE_ROW])


@pytest.mark.parametrize(
    "conversions_answer, timeout_s",
    [
        (httpx.ConnectError("refused"), None),
        (httpx.ReadTimeout("hung"), None),
        (slow, 0.2),
    ],
    ids=["unreachable", "hung", "over merge_timeout_s"],
)
def test_a_failed_pairing_does_not_skip_the_backend_on_the_merges(
    conversions_answer, timeout_s
):
    """The pairing only enriches an answer: its failure is named and alerted,
    but a backend a merge would still ask is asked. Before, one slow pairing
    left arb out of /stats, /search and the twin lists for failure_skip_s."""
    client, seen = merging_client(conversions_answer, timeout_s=timeout_s)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    stats = client.get("/stats")
    again = client.get(CONVERSIONS)
    # the half bridge, named and alerted as before
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    (alert,) = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert "eth conversions lack the bridges it pairs" in alert
    # the merge asked the backend and got its rows
    assert stats.status_code == 200
    assert stats.json() == STATS
    assert DECLINED_HEADER not in stats.headers
    # and the next pairing asked it too
    assert again.headers[DECLINED_HEADER] == BACKEND_URL
    assert [r.url.path for r in seen] == [
        f"/eth/txs/{CCTP_BURN}/conversions",
        "/stats",
        f"/eth/txs/{CCTP_BURN}/conversions",
    ]


def test_a_merge_that_could_not_reach_the_backend_still_skips_the_pairing():
    """The other way round is unchanged: a backend a merge could not reach is
    down, and the pairing leaves it out without asking, as every merge does
    for failure_skip_s."""
    client, seen = merging_client(
        httpx.Response(200, json=[BRIDGE_ROW]),
        stats_answer=httpx.ConnectError("refused"),
    )
    stats = client.get("/stats")
    response = client.get(CONVERSIONS)
    assert stats.headers[DECLINED_HEADER] == BACKEND_URL
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    assert [r.url.path for r in seen] == ["/stats"]


# ---------------------------------------------------------------------------
# the alert says what the failure took out
# ---------------------------------------------------------------------------


def alerts_of(records):
    return [r.getMessage() for r in records if r.levelno == logging.ERROR]


def test_a_failed_pairing_alerts_only_the_conversions_it_leaves_unpaired():
    """A pairing failure starts no skip window, so the backend's networks
    are still merged and served: the one ERROR an operator sees must not
    claim arb is left out, only that eth conversions lack its bridges."""
    client, _ = merging_client(slow, timeout_s=0.2)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    (alert,) = alerts_of(records)
    assert "eth conversions lack the bridges it pairs" in alert
    assert "arb" not in alert
    assert "left out" not in alert and "503" not in alert


def test_a_failed_merge_alerts_everything_the_skip_takes_out():
    """Unchanged: a merge failure skips the backend, pairing included, so its
    alert names both what the merges and the pairing lose."""
    client, _ = merging_client(
        httpx.Response(200, json=[BRIDGE_ROW]),
        stats_answer=httpx.ConnectError("refused"),
    )
    with records_of_the_middleware() as records:
        stats = client.get("/stats")
    assert stats.headers[DECLINED_HEADER] == BACKEND_URL
    (alert,) = alerts_of(records)
    assert "arb are left out of merged answers and their requests get 503" in alert
    assert "eth conversions lack the bridges it pairs" in alert


def test_a_merge_failure_after_a_pairing_failure_is_alerted_again():
    """The first alert named only the unpaired conversions; once a merge
    fails too, the backend's networks are left out, and the operator gets a
    second ERROR saying so. Further failures of that outage only warn."""
    client, _ = merging_client(
        slow, timeout_s=0.2, stats_answer=httpx.ConnectError("refused")
    )
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
        client.get("/stats")
        client.get(CONVERSIONS)
    first, second = alerts_of(records)
    assert "arb" not in first
    assert "arb are left out of merged answers and their requests get 503" in second
