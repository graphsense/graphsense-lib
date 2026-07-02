"""Tests for `POST /graph/summary` (currency-less, mixed-network inputs).

Exercised end-to-end through the FastAPI test client. We monkeypatch the
service-layer function ``graphsenselib.web.service.graph_service.summary`` so
the test does not depend on Cassandra fixtures or DB-layer wiring.
"""

import pytest

from graphsenselib.errors import BadUserInputException
from graphsenselib.web.models import (
    GraphSummary,
    GraphTxNetworkSummary,
    GraphTxOverall,
    GraphTxSummary,
)
from tests.web.helpers import raw_request, request_with_status


def _tx_summary(networks=("btc",)) -> GraphTxSummary:
    blocks = [
        GraphTxNetworkSummary(
            network=n,
            tx_count=2,
            total_value={
                "value": 3500,
                "fiat_values": [
                    {"code": "eur", "value": 40.0},
                    {"code": "usd", "value": 42.5},
                ],
            },
            total_fee=200,
            total_inputs=3,
            total_outputs=2,
            block_min=100,
            block_max=101,
            timestamp_min=1_700_000_000,
            timestamp_max=1_700_000_500,
            notes=[],
        )
        for n in networks
    ]
    return GraphTxSummary(
        overall=GraphTxOverall(
            tx_count=2 * len(blocks),
            total_value_fiat=[
                {"code": "eur", "value": 40.0 * len(blocks)},
                {"code": "usd", "value": 42.5 * len(blocks)},
            ],
            timestamp_min=1_700_000_000,
            timestamp_max=1_700_000_500,
            notes=[],
        ),
        networks=blocks,
    )


@pytest.fixture
def patch_summary(monkeypatch):
    state = {"calls": []}

    async def _fake_summary(ctx, txs, addresses):
        state["calls"].append({"txs": list(txs), "addresses": list(addresses)})
        return GraphSummary(
            txs=_tx_summary(
                tuple(dict.fromkeys(t.network for t in txs)) or ("btc",)
            )
            if txs
            else None,
            addresses=None,
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.graph_service.summary", _fake_summary
    )
    return state


def test_summary_mixed_networks(client, patch_summary):
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "0xbb22", "network": "eth"},
        ]
    }
    result = request_with_status(client, "/graph/summary", 200, body=body)
    assert [b["network"] for b in result["txs"]["networks"]] == ["btc", "eth"]
    codes = {r["code"] for r in result["txs"]["overall"]["total_value_fiat"]}
    assert codes == {"eur", "usd"}
    # refs arrive at the service as parsed models with network preserved
    assert patch_summary["calls"][0]["txs"][1].network == "eth"


def test_summary_absent_blocks_are_omitted(client, patch_summary):
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "btc"},
        ]
    }
    result = request_with_status(client, "/graph/summary", 200, body=body)
    assert "addresses" not in result  # response_model_exclude_none


def test_summary_validation_error_maps_to_400(client, monkeypatch):
    async def _raise(ctx, txs, addresses):
        raise BadUserInputException("unsupported network 'doge'")

    monkeypatch.setattr("graphsenselib.web.service.graph_service.summary", _raise)
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "doge"},
            {"tx_hash": "bb22", "network": "doge"},
        ]
    }
    request_with_status(client, "/graph/summary", 400, body=body)


def test_summary_missing_network_field_is_422(client, patch_summary):
    body = {"txs": [{"tx_hash": "aa11"}, {"tx_hash": "bb22"}]}
    request_with_status(client, "/graph/summary", 422, body=body)


def test_summary_addresses_only(client, monkeypatch):
    from graphsenselib.web.models import (
        GraphAddressNetworkSummary,
        GraphAddressOverall,
        GraphAddressSummary,
    )

    async def _fake_summary(ctx, txs, addresses):
        block = GraphAddressNetworkSummary(
            network="btc",
            address_count=2,
            total_received={"value": 1500, "fiat_values": []},
            total_spent={"value": 500, "fiat_values": []},
            balance={"value": 1000, "fiat_values": []},
            first_usage=1_700_000_000,
            last_usage=1_700_000_500,
            tagged_address_count=1,
            actors=[{"id": "binance", "label": "Binance"}],
            notes=[],
        )
        return GraphSummary(
            txs=None,
            addresses=GraphAddressSummary(
                overall=GraphAddressOverall(
                    address_count=2,
                    tagged_address_count=1,
                    actors=[{"id": "binance", "label": "Binance"}],
                ),
                networks=[block],
            ),
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.graph_service.summary", _fake_summary
    )
    body = {
        "addresses": [
            {"address": "addr-a", "network": "btc"},
            {"address": "addr-b", "network": "btc"},
        ]
    }
    result = request_with_status(client, "/graph/summary", 200, body=body)
    # txs block omitted entirely (response_model_exclude_none).
    assert "txs" not in result
    assert result["addresses"]["overall"]["address_count"] == 2
    assert result["addresses"]["networks"][0]["actors"] == [
        {"id": "binance", "label": "Binance"}
    ]


def test_summary_not_found_propagates_404(client, monkeypatch):
    from graphsenselib.errors import NotFoundException

    async def _raise(ctx, txs, addresses):
        raise NotFoundException("tx not found")

    monkeypatch.setattr("graphsenselib.web.service.graph_service.summary", _raise)
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "btc"},
        ]
    }
    status, _ = raw_request(client, "/graph/summary", body=body)
    assert status == 404
