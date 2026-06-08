"""Tests for `POST /{currency}/subgraph/summary`.

Exercised end-to-end through the FastAPI test client. We monkeypatch the
service-layer function ``graphsenselib.web.service.subgraph_service.summary``
so the test does not depend on Cassandra fixtures or DB-layer wiring.
"""

import pytest

from graphsenselib.errors import BadUserInputException
from graphsenselib.web.models import SubgraphSummary, SubgraphTxSummary
from tests.web.helpers import raw_request, request_with_status


HASH_A = "aa11"
HASH_B = "bb22"


def _build_summary(currency: str, fiat_currency: str = "usd") -> SubgraphSummary:
    return SubgraphSummary(
        currency=currency,
        txs=SubgraphTxSummary(
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
        ),
        addresses=None,
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
        if addresses:
            raise BadUserInputException(
                "/subgraph/summary does not support addresses yet."
            )
        if len(set(txs)) + len(addresses) < 2:
            raise BadUserInputException(
                "/subgraph/summary needs at least 2 distinct nodes."
            )
        return _build_summary(currency, fiat_currency)

    monkeypatch.setattr(
        "graphsenselib.web.service.subgraph_service.summary",
        _fake_summary,
    )
    return state


def test_subgraph_summary_happy_path(client, patch_summary):
    result = request_with_status(
        client, "/btc/subgraph/summary", 200, body={"txs": [HASH_A, HASH_B]}
    )
    assert result["currency"] == "btc"
    # addresses is reserved and omitted (response_model_exclude_none) until
    # address inputs are supported.
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
        "/btc/subgraph/summary",
        200,
        body={"txs": [HASH_A, HASH_B], "fiat_currency": "eur"},
    )
    assert result["txs"]["fiat_currency"] == "eur"
    assert patch_summary["calls"][0]["fiat_currency"] == "eur"


def test_subgraph_summary_invalid_fiat_currency_returns_422(client, patch_summary):
    status, _ = raw_request(
        client,
        "/btc/subgraph/summary",
        body={"txs": [HASH_A, HASH_B], "fiat_currency": "gbp"},
    )
    assert status == 422


@pytest.mark.parametrize("currency", ["btc", "eth", "trx", "bch", "ltc", "zec"])
def test_subgraph_summary_all_chains(client, patch_summary, currency):
    # Unlike /txs/compare, the summary works for every chain.
    result = request_with_status(
        client, f"/{currency}/subgraph/summary", 200, body={"txs": [HASH_A, HASH_B]}
    )
    assert result["currency"] == currency


def test_subgraph_summary_addresses_rejected(client, patch_summary):
    status, body = raw_request(
        client,
        "/btc/subgraph/summary",
        body={"txs": [HASH_A, HASH_B], "addresses": ["addr1"]},
    )
    assert status == 400
    assert "addresses" in body


def test_subgraph_summary_single_node_rejected(client, patch_summary):
    status, _ = raw_request(
        client, "/btc/subgraph/summary", body={"txs": [HASH_A]}
    )
    assert status == 400


def test_subgraph_summary_empty_body_rejected(client, patch_summary):
    status, _ = raw_request(client, "/btc/subgraph/summary", body={})
    assert status == 400


def test_subgraph_summary_addresses_field_accepted_in_schema(client, patch_summary):
    # The field is part of the contract even though non-empty is rejected:
    # an explicit empty list is accepted.
    result = request_with_status(
        client,
        "/btc/subgraph/summary",
        200,
        body={"txs": [HASH_A, HASH_B], "addresses": []},
    )
    assert result["txs"]["tx_count"] == 2
