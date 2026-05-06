from __future__ import annotations

import pytest
import httpx
from fastmcp.exceptions import ToolError

from graphsenselib.mcp.config import SearchNeighborsConfig
from graphsenselib.mcp.tools.search_neighbors import (
    SearchNeighborsClient,
    _validate_network,
    _validate_task_id,
)


@pytest.fixture
def sn_config(monkeypatch) -> SearchNeighborsConfig:
    monkeypatch.setenv("TEST_SN_KEY", "test-api-key")
    return SearchNeighborsConfig(
        base_url="https://upstream.example",
        api_key_env="TEST_SN_KEY",
        poll_interval_s=0.0,  # don't actually sleep in tests
        max_poll_time_s=5.0,
    )


def test_validate_network_rejects_bad():
    with pytest.raises(ToolError):
        _validate_network("../etc")
    with pytest.raises(ToolError):
        _validate_network("BTC")  # uppercase not allowed


def test_validate_network_accepts_good():
    _validate_network("btc")
    _validate_network("eth")


def test_validate_task_id_rejects_path_traversal():
    with pytest.raises(ToolError):
        _validate_task_id("../secret")
    with pytest.raises(ToolError):
        _validate_task_id("task/with/slash")


async def test_client_without_api_key_env_sends_no_auth_header():
    cfg = SearchNeighborsConfig(base_url="https://upstream.example")
    client = SearchNeighborsClient(cfg)
    try:
        assert "Authorization" not in client._client.headers
        assert "authorization" not in client._client.headers
    finally:
        await client.aclose()


def test_client_with_api_key_env_missing_still_works(monkeypatch, caplog):
    """api_key_env is set but the env var is empty: don't raise, just warn
    and send no auth header.
    """
    monkeypatch.delenv("MISSING_KEY", raising=False)
    cfg = SearchNeighborsConfig(
        base_url="https://upstream.example",
        api_key_env="MISSING_KEY",
    )
    with caplog.at_level("WARNING"):
        client = SearchNeighborsClient(cfg)
    assert "Authorization" not in client._client.headers
    assert any("MISSING_KEY" in rec.message for rec in caplog.records)


def test_client_with_api_key_env_set_sends_auth_header(monkeypatch):
    monkeypatch.setenv("MY_API_KEY", "secret-123")
    cfg = SearchNeighborsConfig(
        base_url="https://upstream.example",
        api_key_env="MY_API_KEY",
    )
    client = SearchNeighborsClient(cfg)
    assert client._client.headers.get("authorization") == "secret-123"


async def test_poll_until_done(sn_config):
    """Upstream reports running -> running -> done; client should return the
    terminal state payload without raising.
    """
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/get_task_state/task-abc"
        call_count["n"] += 1
        if call_count["n"] < 3:
            return httpx.Response(200, json={"state": "running"})
        return httpx.Response(200, json={"state": "done", "results": [{"path": "x"}]})

    client = SearchNeighborsClient(sn_config)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://upstream.example",
    )
    try:
        result = await client.poll("task-abc")
        assert result["state"] == "done"
        assert call_count["n"] == 3
    finally:
        await client.aclose()


async def test_poll_timeout(sn_config):
    """When the state never reaches a terminal value within max_poll_time_s,
    a ToolError is raised.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"state": "running"})

    cfg = sn_config.model_copy(
        update={"poll_interval_s": 0.01, "max_poll_time_s": 0.05}
    )
    client = SearchNeighborsClient(cfg)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://upstream.example",
    )
    try:
        with pytest.raises(ToolError, match="did not reach a terminal state"):
            await client.poll("task-abc")
    finally:
        await client.aclose()


async def test_start_search_translates_http_error(sn_config):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    client = SearchNeighborsClient(sn_config)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://upstream.example",
    )
    try:
        with pytest.raises(ToolError, match="HTTP 500"):
            await client.start_search("btc", {"start_address": "x"})
    finally:
        await client.aclose()


async def test_start_search_returns_task_id(sn_config):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/find_neighbors/btc"
        return httpx.Response(200, json={"task_id": "t-123"})

    client = SearchNeighborsClient(sn_config)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://upstream.example",
    )
    try:
        task_id = await client.start_search("btc", {"start_address": "x"})
        assert task_id == "t-123"
    finally:
        await client.aclose()
