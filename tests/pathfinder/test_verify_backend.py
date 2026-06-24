"""Unit tests for the backend-aware pathfinder verifier.

The verifier is tested against a :class:`_FakeBackend` so the assertions
exercise the verifier's check logic — what shape of warning fires for
what spec shape — independent of any real graphsense backend. A
separate test exercises the REST adapter against a fake httpx transport
so the adapter's URL paths and response parsing don't drift from the
REST shape.
"""

from __future__ import annotations

from typing import Optional

import httpx
import pytest

from graphsenselib.pathfinder import (
    GraphsenseBackend,
    RestBackend,
    verify_against_backend,
)


class _FakeBackend:
    """In-memory backend used to drive the verifier without a real REST
    server. Pre-loaded with known-good addresses and txs; anything not
    pre-loaded is reported as missing."""

    def __init__(
        self,
        *,
        addresses: Optional[dict[tuple[str, str], bool]] = None,
        txs: Optional[dict[tuple[str, str], frozenset[str]]] = None,
    ) -> None:
        self.addresses = addresses or {}
        self.txs = txs or {}

    async def address_exists(self, network: str, address: str) -> bool:
        return self.addresses.get((network, address), False)

    async def tx_addresses(self, network: str, tx_id: str) -> Optional[frozenset[str]]:
        return self.txs.get((network, tx_id))


def test_fake_backend_satisfies_protocol() -> None:
    """Cheap sanity check: the fake we use in every other test really
    does implement the runtime-checkable Protocol. If the Protocol
    grows a method, this test breaks first."""
    backend: GraphsenseBackend = _FakeBackend()
    assert isinstance(backend, GraphsenseBackend)


async def test_clean_spec_produces_no_warnings() -> None:
    backend = _FakeBackend(
        addresses={("btc", "addrA"): True, ("btc", "addrB"): True},
        txs={("btc", "tx1"): frozenset({"addrA", "addrB"})},
    )
    spec = {
        "addresses": [{"id": "addrA"}, {"id": "addrB"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    assert warnings == []


async def test_missing_address_is_flagged() -> None:
    backend = _FakeBackend(
        addresses={("btc", "addrA"): True},  # addrB missing
        txs={("btc", "tx1"): frozenset({"addrA", "addrB"})},
    )
    spec = {
        "addresses": [{"id": "addrA"}, {"id": "addrB"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    assert any(
        "address(es) do not exist" in w and "addrB" in w and "addrA" not in w
        for w in warnings
    ), warnings


async def test_missing_tx_is_flagged_and_blocks_mediation_check() -> None:
    """When a tx doesn't exist on the backend, the verifier flags it as
    missing. It must NOT then ALSO complain about the mediation check
    for that same tx — we don't know what addresses it mediated, so
    piling on a mediation warning would be noise."""
    backend = _FakeBackend(
        addresses={("btc", "addrA"): True, ("btc", "addrB"): True},
        txs={},  # tx1 missing
    )
    spec = {
        "addresses": [{"id": "addrA"}, {"id": "addrB"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    assert any("tx hash(es) do not exist" in w and "tx1" in w for w in warnings)
    assert not any("don't actually mediate" in w for w in warnings), warnings


async def test_endpoint_mediation_mismatch_is_flagged() -> None:
    """The original incident: tx is real, addresses are real, but the
    tx doesn't actually involve the endpoints the edge claims."""
    backend = _FakeBackend(
        addresses={
            ("btc", "addrA"): True,
            ("btc", "addrB"): True,
            ("btc", "addrC"): True,
            ("btc", "addrD"): True,
        },
        # tx1 was between C and D, NOT A and B
        txs={("btc", "tx1"): frozenset({"addrC", "addrD"})},
    )
    spec = {
        "addresses": [
            {"id": "addrA"},
            {"id": "addrB"},
            {"id": "addrC"},
            {"id": "addrD"},
        ],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    assert any(
        "don't actually mediate" in w and "tx1" in w and "addrA↔addrB" in w
        for w in warnings
    ), warnings


async def test_partial_endpoint_match_is_still_flagged() -> None:
    """When one endpoint is in the tx but the other isn't, that's still
    a mismatch — both endpoints must participate, not just one."""
    backend = _FakeBackend(
        addresses={("btc", "addrA"): True, ("btc", "addrB"): True},
        txs={("btc", "tx1"): frozenset({"addrA", "addrZ"})},  # A is in, B isn't
    )
    spec = {
        "addresses": [{"id": "addrA"}, {"id": "addrB"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    assert any("don't actually mediate" in w for w in warnings), warnings


async def test_per_item_network_overrides_default() -> None:
    """The verifier must consult each item's `network` (not the spec
    default) when one is given — otherwise a multi-chain spec would
    query the wrong backend."""
    backend = _FakeBackend(
        addresses={("eth", "addrA"): True, ("btc", "addrA"): False},
        txs={},
    )
    spec = {
        "addresses": [{"id": "addrA", "network": "eth"}],
        "txs": [],
        "agg_edges": [],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    assert warnings == []


async def test_truncation_caps_at_ten_with_count_suffix() -> None:
    """A spec listing many invalid items must not bloat the warning —
    cap at 10 and emit a count suffix."""
    backend = _FakeBackend(addresses={}, txs={})
    spec = {
        "addresses": [{"id": f"addr{i}"} for i in range(15)],
        "txs": [],
        "agg_edges": [],
    }
    warnings = await verify_against_backend(
        spec, default_network="btc", backend=backend
    )
    [missing] = [w for w in warnings if "address(es) do not exist" in w]
    assert "addr0" in missing
    assert "addr9" in missing
    assert "addr10" not in missing
    assert "(+5 more)" in missing


# ---------------------------------------------------------------- adapter ---


def _transport_for(handler):
    """Wrap an inline handler as an httpx MockTransport so we can drive
    the RestBackend without a real server."""
    return httpx.MockTransport(handler)


async def test_rest_adapter_address_exists_true_on_200() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/btc/addresses/addrA"
        return httpx.Response(200, json={"address": "addrA"})

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        assert await backend.address_exists("btc", "addrA") is True


async def test_rest_adapter_address_exists_false_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        assert await backend.address_exists("btc", "addrA") is False


async def test_rest_adapter_address_exists_propagates_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "boom"})

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        with pytest.raises(httpx.HTTPStatusError):
            await backend.address_exists("btc", "addrA")


async def test_rest_adapter_tx_addresses_utxo_inputs_outputs() -> None:
    """UTXO body shape: inputs/outputs each carry an `address` list (a
    list because non-standard scripts can produce multiple addresses per
    output)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "tx_hash": "tx1",
                "inputs": [{"address": ["addrA"], "value": 100, "index": 0}],
                "outputs": [{"address": ["addrB", "addrC"], "value": 90, "index": 0}],
            },
        )

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        addrs = await backend.tx_addresses("btc", "tx1")
        assert addrs == frozenset({"addrA", "addrB", "addrC"})


async def test_rest_adapter_tx_addresses_requests_include_io() -> None:
    """Load-bearing: the UTXO inputs/outputs are excluded from the
    response body when `include_io` is not requested. Without it the
    adapter returns an empty set, producing the misleading "tx
    involves {}" mediation warning. Lock the query params in."""
    seen_params: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_params.append(dict(request.url.params))
        return httpx.Response(
            200,
            json={
                "tx_hash": "tx1",
                "inputs": [{"address": ["addrA"], "value": 100, "index": 0}],
                "outputs": [{"address": ["addrB"], "value": 90, "index": 0}],
            },
        )

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        await backend.tx_addresses("btc", "tx1")

    assert seen_params == [{"include_io": "true", "include_nonstandard_io": "true"}], (
        seen_params
    )


async def test_rest_adapter_tx_addresses_account_from_to() -> None:
    """Account-model body shape: flat from_address / to_address."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "tx_type": "account",
                "identifier": "id-1",
                "tx_hash": "0xabc",
                "from_address": "0xfrom",
                "to_address": "0xto",
            },
        )

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        addrs = await backend.tx_addresses("eth", "0xabc")
        assert addrs == frozenset({"0xfrom", "0xto"})


async def test_rest_adapter_tx_addresses_returns_none_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=_transport_for(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        assert await backend.tx_addresses("btc", "missing") is None


async def test_rest_adapter_round_trips_through_fastapi_route() -> None:
    """End-to-end against an actual FastAPI route that mimics the real
    graphsense contract: response_model_exclude_none=True, inputs /
    outputs declared Optional, populated only when include_io is
    truthy. This is the bug that produced `tx involves {}` warnings
    on every BTC build in the wild — the adapter was hitting the
    route without include_io, the route returned a body with no
    inputs/outputs, and downstream the address set was empty.

    By driving the adapter through a real FastAPI app via ASGI
    transport, we verify both halves of the fix together: the params
    arrive on the wire AND FastAPI's Pydantic bool validator
    accepts the lowercase `true` httpx writes."""
    from fastapi import FastAPI, Query
    from pydantic import BaseModel

    class TxValue(BaseModel):
        address: list[str]
        value: int

    class TxUtxo(BaseModel):
        tx_type: str = "utxo"
        tx_hash: str
        inputs: Optional[list[TxValue]] = None
        outputs: Optional[list[TxValue]] = None

    app = FastAPI()

    @app.get(
        "/btc/txs/{tx_hash}",
        response_model=TxUtxo,
        response_model_exclude_none=True,
    )
    async def get_tx(
        tx_hash: str,
        include_io: Optional[bool] = Query(None),
        include_nonstandard_io: Optional[bool] = Query(None),
    ) -> TxUtxo:
        if include_io:
            return TxUtxo(
                tx_hash=tx_hash,
                inputs=[TxValue(address=["addrA"], value=100)],
                outputs=[TxValue(address=["addrB"], value=90)],
            )
        return TxUtxo(tx_hash=tx_hash)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        addrs = await backend.tx_addresses("btc", "tx1")
    assert addrs == frozenset({"addrA", "addrB"})


async def test_checksummed_endpoint_matches_lowercase_participants() -> None:
    """The backend returns EVM addresses lowercase; a spec endpoint in
    EIP-55 checksum casing is the SAME address and must not trip the
    mediation check."""
    checksummed = "0x4e1773615dFc62A5dDc901b36223F1eAedB8F6Df"
    other = "0x" + "ab" * 20
    tx = "0x" + "cd" * 32
    backend = _FakeBackend(
        addresses={("eth", checksummed): True, ("eth", other): True},
        txs={("eth", tx): frozenset({checksummed.lower(), other})},
    )
    spec = {
        "addresses": [{"id": checksummed}, {"id": other}],
        "txs": [{"id": tx}],
        "agg_edges": [{"a": checksummed, "b": other, "tx_ids": [tx]}],
    }
    warnings = await verify_against_backend(
        spec, default_network="eth", backend=backend
    )
    assert not any("don't actually mediate" in w for w in warnings), warnings
