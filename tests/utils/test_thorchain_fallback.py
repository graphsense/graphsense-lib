import pytest
from typing import Optional, Any, cast

from graphsenselib.defi.bridging import thorchain
from graphsenselib.defi.bridging.models import BridgeReceiveReference
from graphsenselib.db.asynchronous.cassandra import Cassandra


class _DummyResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        json_error: Optional[Exception] = None,
    ):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class _SequenceClient:
    def __init__(self, results):
        self._results = list(results)

    async def get(self, _url):
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.asyncio
async def test_try_thornode_endpoints_raises_invalid_response(monkeypatch):
    monkeypatch.setattr(thorchain, "THORNODE_URLS", ["https://a/", "https://b/"])
    monkeypatch.setattr(
        thorchain,
        "RetryHTTPClient",
        lambda: _SequenceClient(
            [
                _DummyResponse(status_code=200, payload={}),
                _DummyResponse(status_code=200, json_error=ValueError("bad json")),
            ]
        ),
    )

    with pytest.raises(thorchain.ThornodeInvalidResponseError):
        await thorchain.try_thornode_endpoints("ABC")


@pytest.mark.asyncio
async def test_try_thornode_endpoints_raises_unavailable(monkeypatch):
    monkeypatch.setattr(thorchain, "THORNODE_URLS", ["https://a/", "https://b/"])
    monkeypatch.setattr(
        thorchain,
        "RetryHTTPClient",
        lambda: _SequenceClient(
            [
                _DummyResponse(status_code=503, payload={}),
                RuntimeError("timeout"),
            ]
        ),
    )

    with pytest.raises(thorchain.ThornodeUnavailableError):
        await thorchain.try_thornode_endpoints("ABC")


@pytest.mark.asyncio
async def test_preliminary_utxo_receive_degrades_on_invalid_response(monkeypatch):
    class _DummyDb:
        async def list_address_txs(self, *_args, **_kwargs):
            return ([{"tx_hash": bytes.fromhex("11" * 32)}], None)

    async def _fake_get_utxo_tx_with_memo(*_args, **_kwargs):
        return ({"outputs": [object()]}, None)

    async def _fake_fallback_receive(*_args, **_kwargs):
        raise thorchain.ThornodeInvalidResponseError("invalid response")

    monkeypatch.setattr(thorchain, "get_utxo_tx_with_memo", _fake_get_utxo_tx_with_memo)
    monkeypatch.setattr(thorchain, "_thornode_fallback_receive", _fake_fallback_receive)

    result = await thorchain.preliminary_utxo_handling_receive(
        cast(Cassandra, _DummyDb()),
        "btc",
        BridgeReceiveReference(
            toAddress="bc1qtestaddress",
            toNetwork="btc",
            fromTxHash="deadbeef",
        ),
    )

    assert result is None
