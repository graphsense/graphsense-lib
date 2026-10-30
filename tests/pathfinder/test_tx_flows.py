"""Tests for :func:`annotate_tx_flows` and the REST adapter's ``tx_sides``."""

from __future__ import annotations

from typing import Optional

import httpx

from graphsenselib.pathfinder import (
    ConversionsBackend,
    RestBackend,
    TxSidesBackend,
    annotate_conversions,
    annotate_tx_flows,
)
from graphsenselib.pathfinder.verify_backend import (
    tx_io_order_from_body,
    tx_sides_from_body,
)

Sides = tuple[frozenset[str], frozenset[str]]


class _FakeBackend:
    def __init__(self, txs: dict[tuple[str, str], Sides], fail: set[str] = frozenset()):
        self.txs = txs
        self.fail = fail
        self.calls: list[tuple[str, str]] = []

    async def tx_sides(self, network: str, tx_id: str) -> Optional[Sides]:
        self.calls.append((network, tx_id))
        if tx_id in self.fail:
            raise httpx.ConnectError("boom")
        return self.txs.get((network, tx_id))


def _sides(senders: list[str], receivers: list[str]) -> Sides:
    return frozenset(senders), frozenset(receivers)


def test_fake_backend_satisfies_protocol() -> None:
    assert isinstance(_FakeBackend({}), TxSidesBackend)
    assert isinstance(RestBackend(httpx.AsyncClient()), TxSidesBackend)


async def test_fills_senders_and_receivers_restricted_to_drawn_addresses() -> None:
    spec = {
        "addresses": [{"id": "A"}, {"id": "B"}],
        "txs": ["t1"],
        "agg_edges": [{"a": "A", "b": "B", "tx_ids": ["t1"]}],
    }
    backend = _FakeBackend(
        {("btc", "t1"): _sides(["A", "other_in"], ["B", "change", "A"])}
    )
    out, warnings = await annotate_tx_flows(
        spec, default_network="btc", backend=backend
    )
    assert warnings == []
    assert out["txs"] == [{"id": "t1", "senders": ["A"], "receivers": ["A", "B"]}]
    # The input spec is not modified.
    assert spec["txs"] == ["t1"]


async def test_evm_addresses_match_case_insensitively_and_keep_spec_spelling() -> None:
    checksummed = "0xAbCdEf0000000000000000000000000000000001"
    spec = {
        "addresses": [{"id": checksummed}, {"id": "0x" + "2" * 40}],
        "txs": [{"id": "t", "network": "eth"}],
        "agg_edges": [],
    }
    backend = _FakeBackend(
        {("eth", "t"): _sides([checksummed.lower()], ["0x" + "2" * 40])}
    )
    out, _ = await annotate_tx_flows(spec, default_network="btc", backend=backend)
    assert out["txs"][0]["senders"] == [checksummed]
    assert backend.calls == [("eth", "t")]


async def test_missing_and_failed_lookups_are_warnings_not_errors() -> None:
    spec = {
        "addresses": ["A", "B"],
        "txs": ["found", "missing", "broken"],
        "agg_edges": [],
    }
    backend = _FakeBackend({("btc", "found"): _sides(["A"], ["B"])}, fail={"broken"})
    out, warnings = await annotate_tx_flows(
        spec, default_network="btc", backend=backend
    )
    by_id = {t["id"]: t for t in out["txs"]}
    assert by_id["found"]["senders"] == ["A"]
    assert "senders" not in by_id["missing"] and "senders" not in by_id["broken"]
    assert len(warnings) == 2
    assert "missing" in warnings[0] and "not found" in warnings[0]
    assert "broken" in warnings[1] and "failed" in warnings[1]


async def test_tx_that_already_has_flows_is_not_looked_up() -> None:
    spec = {
        "addresses": ["A", "B"],
        "txs": [{"id": "t", "senders": ["A"], "receivers": ["B"]}],
        "agg_edges": [],
    }
    backend = _FakeBackend({})
    out, warnings = await annotate_tx_flows(
        spec, default_network="btc", backend=backend
    )
    assert backend.calls == [] and warnings == []
    assert out["txs"] == spec["txs"]


def test_tx_sides_from_utxo_and_account_bodies() -> None:
    utxo = {
        "inputs": [{"address": ["A"]}, {"address": ["A"]}],
        "outputs": [{"address": ["B", "C"]}, {"address": []}],
    }
    assert tx_sides_from_body(utxo) == _sides(["A"], ["B", "C"])
    account = {"from_address": "0xa", "to_address": "0xb"}
    assert tx_sides_from_body(account) == _sides(["0xa"], ["0xb"])


async def test_rest_adapter_tx_sides_requests_io_and_splits_sides() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/btc/txs/tx1"
        assert request.url.params["include_io"] == "true"
        return httpx.Response(
            200,
            json={"inputs": [{"address": ["A"]}], "outputs": [{"address": ["B"]}]},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://test"
    ) as client:
        assert await RestBackend(client).tx_sides("btc", "tx1") == _sides(["A"], ["B"])


async def test_rest_adapter_tx_sides_none_on_404() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(404)),
        base_url="http://test",
    ) as client:
        assert await RestBackend(client).tx_sides("btc", "tx1") is None


async def test_rest_adapters_sharing_a_cache_fetch_each_tx_once() -> None:
    hits: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        hits.append(request.url.path)
        return httpx.Response(
            200, json={"inputs": [{"address": ["A"]}], "outputs": [{"address": ["B"]}]}
        )

    cache: dict = {}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://test"
    ) as client:
        await RestBackend(client, tx_cache=cache).tx_sides("btc", "tx1")
        assert await RestBackend(client, tx_cache=cache).tx_addresses(
            "btc", "tx1"
        ) == frozenset({"A", "B"})
    assert hits == ["/btc/txs/tx1"]


# ------------------------------------------------------------ conversions --


class _FakeConversions:
    def __init__(self, conversions, io, fail=frozenset()):
        self.conversions = conversions
        self.io = io
        self.fail = fail

    async def tx_conversions(self, network: str, tx_id: str):
        if tx_id in self.fail:
            raise httpx.ConnectError("boom")
        return self.conversions.get(tx_id, [])

    async def tx_io_order(self, network: str, tx_id: str):
        return self.io.get(tx_id)


_BRIDGE = {
    "conversion_type": "bridge_tx",
    "from_asset_transfer": "ETHTX_I9",
    "to_asset_transfer": "btctx",
}


def _bridge_spec(**extra) -> dict:
    return {
        "addresses": ["hop", "contract", "vault", "recipient"],
        "txs": [{"id": "ETHTX_I9", "network": "eth"}, "btctx"],
        "agg_edges": [],
        **extra,
    }


async def test_conversion_found_from_both_legs_is_recorded_once() -> None:
    backend = _FakeConversions(
        {"ETHTX_I9": [_BRIDGE], "btctx": [_BRIDGE]},
        {
            "ETHTX_I9": (["hop"], ["contract"]),
            "btctx": (["vault", "other_input"], ["recipient", "vault"]),
        },
    )
    assert isinstance(backend, ConversionsBackend)
    out, warnings = await annotate_conversions(
        _bridge_spec(), default_network="btc", backend=backend
    )
    assert warnings == []
    assert out["conversions"] == [
        {
            "type": "bridge_tx",
            "input_tx": "ETHTX_I9",
            "output_tx": "btctx",
            "edge_from": "contract",
            "edge_to": "vault",
        }
    ]


async def test_conversion_the_ui_would_not_draw_is_skipped() -> None:
    """The UI draws the edge between the input leg's first output and the
    output leg's first input; if either isn't in the graph, no edge."""
    backend = _FakeConversions(
        {"btctx": [_BRIDGE]},
        {"ETHTX_I9": (["hop"], ["contract"]), "btctx": (["elsewhere"], ["recipient"])},
    )
    out, _ = await annotate_conversions(
        _bridge_spec(), default_network="btc", backend=backend
    )
    assert out["conversions"] == []
    # Nor when a leg tx isn't in the spec.
    spec = _bridge_spec(txs=["btctx"])
    out, _ = await annotate_conversions(spec, default_network="btc", backend=backend)
    assert out["conversions"] == []


async def test_failed_conversion_lookup_is_a_warning() -> None:
    backend = _FakeConversions({}, {}, fail={"btctx"})
    out, warnings = await annotate_conversions(
        _bridge_spec(), default_network="btc", backend=backend
    )
    assert out["conversions"] == []
    assert len(warnings) == 1 and "btctx" in warnings[0]


def test_tx_io_order_keeps_tx_order_without_repeats() -> None:
    body = {
        "inputs": [{"address": ["B"]}, {"address": ["A"]}, {"address": ["B"]}],
        "outputs": [{"address": ["C", "D"]}],
    }
    assert tx_io_order_from_body(body) == (["B", "A"], ["C", "D"])
    assert tx_io_order_from_body({"from_address": "0xa", "to_address": "0xb"}) == (
        ["0xa"],
        ["0xb"],
    )


async def test_rest_adapter_tx_conversions_uses_plain_hash() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/eth/txs/abc/conversions":
            return httpx.Response(200, json=[_BRIDGE])
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://test"
    ) as client:
        backend = RestBackend(client)
        assert await backend.tx_conversions("eth", "abc_I9") == [_BRIDGE]
        assert await backend.tx_conversions("eth", "missing") == []


async def test_conversion_ids_match_with_or_without_0x_prefix() -> None:
    """The conversions endpoint writes some account-model asset transfers
    as ``0x<hash>_I…`` while the file stores ``<hash>_I…``."""
    h = "d0" * 32
    swap = {
        "conversion_type": "dex_swap",
        "from_asset_transfer": f"0x{h}_I387",
        "to_asset_transfer": f"0x{h}_T175",
    }
    spec = {
        "addresses": ["swapper", "router", "pool"],
        "txs": [
            {"id": f"{h}_I387", "network": "eth"},
            {"id": f"{h}_T175", "network": "eth"},
        ],
        "agg_edges": [],
    }
    backend = _FakeConversions(
        {f"{h}_I387": [swap]},
        {f"{h}_I387": (["swapper"], ["router"]), f"{h}_T175": (["pool"], ["swapper"])},
    )
    out, _ = await annotate_conversions(spec, default_network="eth", backend=backend)
    assert [(c["input_tx"], c["output_tx"]) for c in out["conversions"]] == [
        (f"{h}_I387", f"{h}_T175")
    ]
