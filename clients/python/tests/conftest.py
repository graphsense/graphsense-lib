"""Shared test fixtures.

The generated client talks HTTP through `urllib3.PoolManager` inside
`graphsense.rest.RESTClientObject`, not through `requests`. We therefore
monkeypatch `RESTClientObject.request` on a real ApiClient instance and
return a fake `RESTResponse`. This gives us full control over status codes,
bodies and headers (the last matters for the deprecation-warning tests).

The `http_mock` fixture records every call so tests can assert how many
requests were made and with which body — similar in spirit to `responses`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import pytest

import graphsense.rest as rest_mod
from graphsense.api_client import ApiClient
from graphsense.configuration import Configuration
from graphsense.ext import GraphSense

TEST_HOST = "http://testserver"
FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> Any:
    return json.loads((FIXTURE_DIR / name).read_text())


@dataclass
class RecordedCall:
    method: str
    url: str
    headers: dict[str, Any]
    body: Any
    post_params: Any


class _FakeRaw:
    """Duck-type enough of a urllib3 response for RESTResponse to wrap."""

    def __init__(self, status: int, body: bytes, headers: dict[str, str]):
        self.status = status
        self.reason = "OK"
        self.data = body
        self.headers = headers


@dataclass
class HttpMock:
    """Matcher registry + call recorder."""

    rules: list[
        tuple[
            Callable[[str, str], bool],
            Callable[[RecordedCall], tuple[int, Any, dict[str, str]]],
        ]
    ] = field(default_factory=list)
    calls: list[RecordedCall] = field(default_factory=list)

    def add(
        self,
        method: str,
        url_pattern: str,
        *,
        status: int = 200,
        json_body: Any = None,
        body: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        """Register a simple match rule.

        `url_pattern` is treated as a regex searched against the request URL.
        Rules are tried in insertion order, so register the more specific
        pattern first.
        """
        wanted_method = method.upper()
        rx = re.compile(url_pattern)

        def matcher(m: str, u: str) -> bool:
            return m.upper() == wanted_method and rx.search(u) is not None

        def responder(_call: RecordedCall) -> tuple[int, Any, dict[str, str]]:
            if json_body is not None:
                return (
                    status,
                    json.dumps(json_body).encode("utf-8"),
                    headers or {"content-type": "application/json"},
                )
            if body is not None:
                return (
                    status,
                    body.encode("utf-8"),
                    headers or {"content-type": "text/plain"},
                )
            return status, b"", headers or {}

        self.rules.append((matcher, responder))

    def respond(self, call: RecordedCall) -> tuple[int, bytes, dict[str, str]]:
        for matcher, responder in self.rules:
            if matcher(call.method, call.url):
                status, body, headers = responder(call)
                if isinstance(body, str):
                    body = body.encode("utf-8")
                return status, body, headers
        raise AssertionError(f"no mock registered for {call.method} {call.url}")


@pytest.fixture
def http_mock(monkeypatch) -> HttpMock:
    mock = HttpMock()

    def fake_request(
        self,
        method,
        url,
        headers=None,
        body=None,
        post_params=None,
        _request_timeout=None,
    ):
        rec = RecordedCall(
            method=method,
            url=url,
            headers=headers or {},
            body=body,
            post_params=post_params,
        )
        mock.calls.append(rec)
        status, body_bytes, resp_headers = mock.respond(rec)
        return rest_mod.RESTResponse(
            _FakeRaw(status=status, body=body_bytes, headers=resp_headers)
        )

    monkeypatch.setattr(rest_mod.RESTClientObject, "request", fake_request)
    return mock


@pytest.fixture
def api_client() -> ApiClient:
    cfg = Configuration(host=TEST_HOST, api_key={"api_key": "test"})
    return ApiClient(cfg)


@pytest.fixture
def gs(api_client: ApiClient) -> GraphSense:
    return GraphSense(api_client=api_client, currency="btc")


@pytest.fixture
def sample_address() -> dict[str, Any]:
    return _load("address_btc.json")


@pytest.fixture
def sample_cluster() -> dict[str, Any]:
    return _load("cluster_btc.json")


@pytest.fixture
def sample_tx_account() -> dict[str, Any]:
    return _load("tx_account_eth.json")


@pytest.fixture
def sample_tx_utxo() -> dict[str, Any]:
    return _load("tx_utxo_btc.json")


@pytest.fixture
def sample_tags() -> dict[str, Any]:
    return _load("tags.json")


@pytest.fixture
def sample_tag_summary() -> dict[str, Any]:
    return _load("tag_summary.json")
