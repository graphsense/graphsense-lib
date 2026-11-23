"""Cassandra unavailable (NoHostAvailable) answers 503, logs one ERROR.

Before: an unhandled 500 per request, each logged at ERROR (and posted to
Slack). A database outage is temporary, so clients get 503 + Retry-After
(the dashboard retries those), and the log carries one ERROR per outage.
"""

import logging

from cassandra.cluster import NoHostAvailable
from fastapi import FastAPI
from fastapi.testclient import TestClient

import graphsenselib.web.app as web_app
from graphsenselib.web.app import _register_exception_handlers


def client() -> TestClient:
    app = FastAPI()
    _register_exception_handlers(app)

    @app.get("/btc/stats")
    async def stats():
        raise NoHostAvailable("Unable to complete the operation against any hosts", {})

    @app.get("/boom")
    async def boom():
        raise RuntimeError("a real bug")

    return TestClient(app, raise_server_exceptions=False)


def _levels(caplog):
    return [
        r.levelno
        for r in caplog.records
        if r.name == "graphsenselib.web.app" and "Cassandra unavailable" in r.message
    ]


def test_no_host_available_is_a_503_with_retry_after(caplog):
    r = client().get("/btc/stats")
    assert r.status_code == 503
    assert r.headers["retry-after"] == "5"
    assert r.json() == {"detail": "Database temporarily unavailable, please retry"}
    # not mistaken for an external backend's outage by the dashboard
    assert "x-served-by" not in r.headers


def test_one_error_per_outage_then_warnings(caplog):
    caplog.set_level(logging.WARNING, logger="graphsenselib.web.app")
    c = client()
    for _ in range(5):
        c.get("/btc/stats")
    assert _levels(caplog) == [logging.ERROR] + [logging.WARNING] * 4


def test_a_new_outage_after_a_quiet_period_logs_an_error_again(caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="graphsenselib.web.app")
    now = [1000.0]
    monkeypatch.setattr(web_app.time, "monotonic", lambda: now[0])
    c = client()
    c.get("/btc/stats")
    now[0] += 10
    c.get("/btc/stats")
    now[0] += web_app.DB_UNAVAILABLE_QUIET_S + 1
    c.get("/btc/stats")
    assert _levels(caplog) == [logging.ERROR, logging.WARNING, logging.ERROR]


def test_other_exceptions_stay_500():
    assert client().get("/boom").status_code == 500
