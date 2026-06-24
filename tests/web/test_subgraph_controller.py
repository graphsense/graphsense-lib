"""Tests for `POST /{currency}/graph/summary`.

Exercised end-to-end through the FastAPI test client. We monkeypatch the
service-layer function ``graphsenselib.web.service.subgraph_service.summary``
so the test does not depend on Cassandra fixtures or DB-layer wiring.
"""

import pytest

from graphsenselib.errors import BadUserInputException
from graphsenselib.web.models import (
    SubgraphAddressSummary,
    SubgraphSummary,
    SubgraphTxSummary,
)
from tests.web.helpers import raw_request, request_with_status


HASH_A = "aa11"
HASH_B = "bb22"
ADDR_A = "addr-a"
ADDR_B = "addr-b"


def _tx_block(fiat_currency: str = "usd") -> SubgraphTxSummary:
    return SubgraphTxSummary(
        tx_count=2,
        total_value=3500,
        total_value_fiat=42.5,
        fiat_currency=fiat_currency,
        total_fee=200,
        total_inputs=3,
        total_outputs=2,
        block_min=100,
        block_max=101,
        timestamp_min=1_700_000_000,
        timestamp_max=1_700_000_500,
        notes=[],
    )


def _address_block(fiat_currency: str = "usd") -> SubgraphAddressSummary:
    return SubgraphAddressSummary(
        address_count=2,
        total_received=1500,
        total_received_fiat=15.0,
        total_spent=500,
        total_spent_fiat=5.0,
        balance=1000,
        balance_fiat=10.0,
        fiat_currency=fiat_currency,
        first_usage=1_700_000_000,
        last_usage=1_700_000_500,
        tagged_address_count=1,
        actors=[{"id": "binance", "label": "Binance"}],
        notes=[],
    )


@pytest.fixture
def patch_summary(monkeypatch):
    state = {"calls": []}

    async def _fake_summary(ctx, currency, txs, addresses, fiat_currency="usd"):
        state["calls"].append(
            {
                "currency": currency,
                "txs": list(txs),
                "addresses": list(addresses),
                "fiat_currency": fiat_currency,
            }
        )
        # Replays the DB-layer validation messages verbatim; the max-nodes
        # cap is not replicated here, the DB-service tests own it.
        dtxs, daddrs = list(set(txs)), list(set(addresses))
        if not dtxs and not daddrs:
            raise BadUserInputException(
                "/graph/summary needs tx hashes and/or addresses."
            )
        if dtxs and len(dtxs) < 2:
            raise BadUserInputException(
                "/graph/summary needs at least 2 distinct tx hashes "
                "when txs are given."
            )
        if daddrs and len(daddrs) < 2:
            raise BadUserInputException(
                "/graph/summary needs at least 2 distinct addresses "
                "when addresses are given."
            )
        return SubgraphSummary(
            currency=currency,
            txs=_tx_block(fiat_currency) if dtxs else None,
            addresses=_address_block(fiat_currency) if daddrs else None,
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.subgraph_service.summary",
        _fake_summary,
    )
    return state


def test_subgraph_summary_happy_path(client, patch_summary):
    result = request_with_status(
        client, "/btc/graph/summary", 200, body={"txs": [HASH_A, HASH_B]}
    )
    assert result["currency"] == "btc"
    # tx-only request: no addresses block present (response_model_exclude_none).
    assert "addresses" not in result
    assert result["txs"]["tx_count"] == 2
    assert result["txs"]["total_value"] == 3500
    assert result["txs"]["total_inputs"] == 3
    assert result["txs"]["total_value_fiat"] == 42.5
    # fiat_currency defaults to usd when omitted
    assert result["txs"]["fiat_currency"] == "usd"

    call = patch_summary["calls"][0]
    assert call["currency"] == "btc"
    assert call["txs"] == [HASH_A, HASH_B]
    assert call["addresses"] == []
    assert call["fiat_currency"] == "usd"


def test_subgraph_summary_fiat_currency_passed_through(client, patch_summary):
    result = request_with_status(
        client,
        "/btc/graph/summary",
        200,
        body={"txs": [HASH_A, HASH_B], "fiat_currency": "eur"},
    )
    assert result["txs"]["fiat_currency"] == "eur"
    assert patch_summary["calls"][0]["fiat_currency"] == "eur"


def test_subgraph_summary_invalid_fiat_currency_returns_422(client, patch_summary):
    status, _ = raw_request(
        client,
        "/btc/graph/summary",
        body={"txs": [HASH_A, HASH_B], "fiat_currency": "gbp"},
    )
    assert status == 422


@pytest.mark.parametrize("currency", ["btc", "eth", "trx", "bch", "ltc", "zec"])
def test_subgraph_summary_all_chains(client, patch_summary, currency):
    # Unlike /txs/compare, the summary works for every chain.
    result = request_with_status(
        client, f"/{currency}/graph/summary", 200, body={"txs": [HASH_A, HASH_B]}
    )
    assert result["currency"] == currency


def test_subgraph_summary_single_node_rejected(client, patch_summary):
    status, _ = raw_request(client, "/btc/graph/summary", body={"txs": [HASH_A]})
    assert status == 400


def test_subgraph_summary_empty_body_rejected(client, patch_summary):
    status, _ = raw_request(client, "/btc/graph/summary", body={})
    assert status == 400


def test_subgraph_summary_empty_addresses_list_ok(client, patch_summary):
    # An explicit empty addresses list is accepted alongside txs.
    result = request_with_status(
        client,
        "/btc/graph/summary",
        200,
        body={"txs": [HASH_A, HASH_B], "addresses": []},
    )
    assert result["txs"]["tx_count"] == 2


def test_subgraph_summary_addresses_only(client, patch_summary):
    result = request_with_status(
        client, "/btc/graph/summary", 200, body={"addresses": [ADDR_A, ADDR_B]}
    )
    # txs block omitted entirely (response_model_exclude_none).
    assert "txs" not in result
    assert result["addresses"]["address_count"] == 2
    assert result["addresses"]["total_received"] == 1500
    assert result["addresses"]["tagged_address_count"] == 1
    assert result["addresses"]["actors"] == [{"id": "binance", "label": "Binance"}]
    call = patch_summary["calls"][0]
    assert call["txs"] == []
    assert call["addresses"] == [ADDR_A, ADDR_B]


def test_subgraph_summary_mixed_returns_both_blocks(client, patch_summary):
    result = request_with_status(
        client,
        "/btc/graph/summary",
        200,
        body={"txs": [HASH_A, HASH_B], "addresses": [ADDR_A, ADDR_B]},
    )
    assert result["txs"]["tx_count"] == 2
    assert result["addresses"]["address_count"] == 2


def test_subgraph_summary_single_address_rejected(client, patch_summary):
    status, body = raw_request(
        client, "/btc/graph/summary", body={"addresses": [ADDR_A]}
    )
    assert status == 400
    assert "addresses" in body


def test_subgraph_summary_one_tx_one_address_rejected(client, patch_summary):
    status, _ = raw_request(
        client,
        "/btc/graph/summary",
        body={"txs": [HASH_A], "addresses": [ADDR_A]},
    )
    assert status == 400
