"""Request body size limit (GHSA-372j-2wgf-23ch).

FastAPI reads and parses the whole body before a route handler runs, so the
bulk endpoint's item cap cannot prevent a huge body from being materialized as
Python objects. Only a limit applied before parsing can.
"""

import json

from fastapi import FastAPI
from starlette.testclient import TestClient

from graphsenselib.web.middleware.body_size import RequestBodySizeLimitMiddleware


def make_client(max_body_bytes):
    app = FastAPI()

    @app.post("/echo")
    async def echo(body: dict):
        return {"keys": len(body)}

    app.add_middleware(
        RequestBodySizeLimitMiddleware, max_body_bytes=max_body_bytes
    )
    return TestClient(app)


def test_body_within_limit_is_passed_through():
    client = make_client(1024)
    response = client.post("/echo", json={"a": 1, "b": 2})
    assert response.status_code == 200
    assert response.json() == {"keys": 2}


def test_oversized_body_is_rejected_with_413():
    client = make_client(64)
    response = client.post("/echo", json={"a": "x" * 500})
    assert response.status_code == 413
    assert "too large" in response.json()["detail"].lower()


def test_rejection_happens_before_the_body_is_parsed():
    """The 413 must not depend on the payload being valid JSON — that would
    mean the parser already ran on it."""
    client = make_client(64)
    response = client.post(
        "/echo",
        content=b"{not valid json" + b"x" * 500,
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 413


def test_chunked_body_without_content_length_is_cut_off():
    """A body streamed without Content-Length is counted as it arrives, so the
    limit cannot be bypassed by omitting the header."""
    client = make_client(64)

    def chunks():
        for _ in range(20):
            yield b"x" * 64

    response = client.post(
        "/echo",
        content=chunks(),
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 413
    assert "too large" in response.json()["detail"].lower()


def test_zero_disables_the_check():
    client = make_client(0)
    response = client.post("/echo", json={"a": "x" * 10_000})
    assert response.status_code == 200


def test_limit_is_active_on_the_real_app(client):
    """The stock application wires the middleware, so a bulk request larger
    than the configured limit never reaches the router."""
    limit = client.app_state.config.max_request_body_bytes
    assert limit > 0
    oversized = json.dumps({"height": [0]}).encode() + b" " * (limit + 1)
    response = client.request(
        "POST",
        "/btc/bulk.json/get_block?num_pages=1",
        content=oversized,
        headers={"Content-Type": "application/json", "Authorization": "x"},
    )
    assert response.status_code == 413, response.text
