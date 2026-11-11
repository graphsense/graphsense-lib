"""One request id per gs-rest request, so a backend's lines can be joined
with ours (middleware/external_backends.py): minted by a small ASGI
middleware (an incoming well-formed ``X-Request-Id`` is kept), echoed on
the response, put on the backends logger's lines by a logging.Filter, and
sent as ``X-Request-Id`` on every backend call, merge, pairing and proxy."""

import logging
import re

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from graphsenselib.web.app import create_app_from_dict, setup_logging
from graphsenselib.web.config import LoggingConfig
from graphsenselib.web.middleware.external_backends import (
    REQUEST_ID_HEADER,
    RequestIdFilter,
    RequestIdMiddleware,
    request_id_var,
)

from tests.web.test_bridge_pairing_logging import the_pairing_line
from tests.web.test_delegated_conversions import (
    ARB_MINT,
    BACKEND_URL,
    CONVERSIONS,
    local_swap,  # noqa: F401 -- the autouse fixture of that module
    make_client,
    records_of_the_middleware,
)

HEX32 = re.compile(r"^[0-9a-f]{32}$")
BACKENDS_LOGGER = "graphsenselib.web.middleware.external_backends"


def tiny_app():
    app = FastAPI()
    seen = []

    @app.get("/ping")
    async def ping():
        seen.append(request_id_var.get())
        return {"ok": True}

    app.add_middleware(RequestIdMiddleware)
    return TestClient(app), seen


# ---------------------------------------------------------------------------
# the middleware
# ---------------------------------------------------------------------------


def test_a_request_without_an_id_gets_one_minted_and_echoed():
    client, seen = tiny_app()
    response = client.get("/ping")
    assert response.status_code == 200
    request_id = response.headers[REQUEST_ID_HEADER]
    assert HEX32.match(request_id)
    assert seen == [request_id]


@pytest.mark.parametrize("incoming", ["abc-123.X_y", "a", "x" * 64])
def test_a_well_formed_incoming_id_is_kept(incoming):
    client, seen = tiny_app()
    response = client.get("/ping", headers={REQUEST_ID_HEADER: incoming})
    assert response.headers[REQUEST_ID_HEADER] == incoming
    assert seen == [incoming]


@pytest.mark.parametrize("incoming", ["", "x" * 65, "bad id", "a/b", "{ok}", "a=b"])
def test_a_malformed_incoming_id_is_replaced(incoming):
    client, seen = tiny_app()
    response = client.get("/ping", headers={REQUEST_ID_HEADER: incoming})
    minted = response.headers[REQUEST_ID_HEADER]
    assert HEX32.match(minted)
    assert seen == [minted]


def test_two_requests_get_two_ids_and_none_leaks_out():
    client, seen = tiny_app()
    first = client.get("/ping").headers[REQUEST_ID_HEADER]
    second = client.get("/ping").headers[REQUEST_ID_HEADER]
    assert first != second
    assert seen == [first, second]
    assert request_id_var.get() is None


def test_the_app_installs_the_middleware():
    app = create_app_from_dict({"database": {"nodes": ["localhost"]}})
    assert any(m.cls is RequestIdMiddleware for m in app.user_middleware)


# ---------------------------------------------------------------------------
# the filter
# ---------------------------------------------------------------------------


def records_through(filter_, message, *args):
    logger = logging.getLogger("tests.request_id.filter")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    records = []

    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger.addHandler(Sink())
    logger.addFilter(filter_)
    try:
        logger.info(message, *args)
    finally:
        logger.removeFilter(filter_)
        logger.handlers.clear()
    return records


def test_the_filter_adds_the_id_to_a_line():
    token = request_id_var.set("req-1")
    try:
        (record,) = records_through(RequestIdFilter(), "backend %s is down", "x")
    finally:
        request_id_var.reset(token)
    assert record.getMessage() == "backend x is down request_id=req-1"
    assert record.request_id == "req-1"


def test_the_filter_leaves_a_line_without_a_request_alone():
    assert request_id_var.get() is None
    (record,) = records_through(RequestIdFilter(), "startup done")
    assert record.getMessage() == "startup done"


def test_the_filter_does_not_repeat_an_id_the_line_names():
    token = request_id_var.set("req-1")
    try:
        (record,) = records_through(
            RequestIdFilter(), "paired | event=x request_id=req-1"
        )
    finally:
        request_id_var.reset(token)
    assert record.getMessage() == "paired | event=x request_id=req-1"


def test_setup_logging_installs_the_filter_once(monkeypatch):
    backends_logger = logging.getLogger(BACKENDS_LOGGER)
    for name in ("graphsenselib.web.app", BACKENDS_LOGGER, "asyncio", "uvicorn"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "level", logger.level)
    monkeypatch.setattr(backends_logger, "filters", list(backends_logger.filters))
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(logging.root, "handlers", [])
    app_logger = logging.getLogger("graphsenselib.web.app")

    setup_logging(app_logger, None, "test", LoggingConfig(level="INFO"))
    setup_logging(app_logger, None, "test", LoggingConfig(level="INFO"))

    installed = [f for f in backends_logger.filters if isinstance(f, RequestIdFilter)]
    assert len(installed) == 1


# ---------------------------------------------------------------------------
# the id goes to the backends
# ---------------------------------------------------------------------------


def test_the_pairing_sends_the_request_id_and_logs_it():
    client, seen, _ = make_client()
    client.app.add_middleware(RequestIdMiddleware)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS, headers={REQUEST_ID_HEADER: "gs-42"})
    assert response.headers[REQUEST_ID_HEADER] == "gs-42"
    (call,) = seen
    assert call.headers[REQUEST_ID_HEADER] == "gs-42"
    _, fields = the_pairing_line(records)
    assert fields["request_id"] == "gs-42"


def test_a_proxied_request_sends_the_request_id():
    client, seen, _ = make_client(networks={"arb": {"url": BACKEND_URL}})
    client.app.add_middleware(RequestIdMiddleware)
    response = client.get(f"/arb/txs/{ARB_MINT}/conversions")
    assert response.status_code == 200
    (call,) = seen
    assert call.headers[REQUEST_ID_HEADER] == response.headers[REQUEST_ID_HEADER]


def test_without_the_middleware_each_request_still_sends_one_id():
    """A deployment that lacks the middleware (an app built by hand) still
    joins its backend calls under one minted id."""
    client, seen, _ = make_client()
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
    (call,) = seen
    sent = call.headers[REQUEST_ID_HEADER]
    assert HEX32.match(sent)
    _, fields = the_pairing_line(records)
    assert fields["request_id"] == sent


def test_no_line_carries_the_authorization_header():
    client, _, _ = make_client()
    client.app.add_middleware(RequestIdMiddleware)
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS, headers={"Authorization": "Bearer caller-secret"})
    for record in records:
        assert "backend-key" not in record.getMessage()
        assert "caller-secret" not in record.getMessage()
