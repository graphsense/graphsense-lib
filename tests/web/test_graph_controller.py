"""Tests for `POST /graph/summary` (currency-less, mixed-network inputs).

Exercised end-to-end through the FastAPI test client. We monkeypatch the
DB-layer function (``_db_summary`` as imported by the web service) so the
real web service body and the internal-to-API translator still run; only
Cassandra fixtures and DB-layer wiring are faked.
"""

import pytest

from graphsenselib.db.asynchronous.services.models import (
    GraphSummaryInternal,
    GraphTxNetworkSummaryInternal,
    GraphTxOverallInternal,
    GraphTxSummaryInternal,
)
from graphsenselib.errors import BadUserInputException
from tests.web.helpers import raw_request, request_with_status


def _tx_summary_internal(networks=("btc",)) -> GraphTxSummaryInternal:
    blocks = [
        GraphTxNetworkSummaryInternal(
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
    return GraphTxSummaryInternal(
        overall=GraphTxOverallInternal(
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

    async def _fake_db_summary(
        txs_service, addresses_service, tags_service, *, txs, addresses, tagstore_groups
    ):
        state["calls"].append({"txs": list(txs), "addresses": list(addresses)})
        return GraphSummaryInternal(
            txs=_tx_summary_internal(
                tuple(dict.fromkeys(t.network for t in txs)) or ("btc",)
            )
            if txs
            else None,
            addresses=None,
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.graph_service._db_summary", _fake_db_summary
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
    # refs arrive at the DB layer as internal refs with network preserved
    assert patch_summary["calls"][0]["txs"][1].network == "eth"
    assert patch_summary["calls"][0]["txs"][1].tx_hash == "0xbb22"


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
    async def _raise(*args, **kwargs):
        raise BadUserInputException("unsupported network 'doge'")

    monkeypatch.setattr("graphsenselib.web.service.graph_service._db_summary", _raise)
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


def test_summary_over_max_length_is_422(client, patch_summary):
    # Request-boundary cap: more than 100 tx refs is rejected before the
    # service (the combined-100 rule stays authoritative service-side).
    body = {"txs": [{"tx_hash": f"{i:064x}", "network": "btc"} for i in range(101)]}
    request_with_status(client, "/graph/summary", 422, body=body)


def test_summary_addresses_over_max_length_is_422(client, patch_summary):
    # The same per-list cap holds for the addresses list.
    body = {
        "addresses": [{"address": f"addr{i}", "network": "btc"} for i in range(101)]
    }
    request_with_status(client, "/graph/summary", 422, body=body)


def test_summary_combined_node_cap_rejected_at_boundary(client, patch_summary):
    # Each list stays under its own per-list cap, but combined they exceed
    # the shared node-limit constant; the API boundary must 422 this before
    # it ever reaches the db layer.
    txs = [{"network": "btc", "tx_hash": f"{i:064x}"} for i in range(60)]
    addresses = [{"network": "btc", "address": f"addr{i}"} for i in range(60)]
    body = {"txs": txs, "addresses": addresses}
    request_with_status(client, "/graph/summary", 422, body=body)


def test_summary_absurd_hash_length_rejected(client, patch_summary):
    body = {
        "txs": [
            {"network": "btc", "tx_hash": "a" * 5000},
            {"network": "btc", "tx_hash": "bb" * 32},
        ]
    }
    request_with_status(client, "/graph/summary", 422, body=body)


def test_summary_addresses_only(client, monkeypatch):
    from graphsenselib.db.asynchronous.services.models import (
        GraphAddressNetworkSummaryInternal,
        GraphAddressOverallInternal,
        GraphAddressSummaryInternal,
    )

    async def _fake_db_summary(
        txs_service, addresses_service, tags_service, *, txs, addresses, tagstore_groups
    ):
        block = GraphAddressNetworkSummaryInternal(
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
        return GraphSummaryInternal(
            txs=None,
            addresses=GraphAddressSummaryInternal(
                overall=GraphAddressOverallInternal(
                    address_count=2,
                    tagged_address_count=1,
                    actors=[{"id": "binance", "label": "Binance"}],
                ),
                networks=[block],
            ),
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.graph_service._db_summary", _fake_db_summary
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


def test_summary_notes_serialize_code_message_and_omit_null_network(
    client, monkeypatch
):
    from graphsenselib.db.asynchronous.services.models import GraphNoteInternal

    async def _fake_db_summary(
        txs_service, addresses_service, tags_service, *, txs, addresses, tagstore_groups
    ):
        per_network_note = GraphNoteInternal(
            code="fiat_totals_partial", message="Some transfers lack a fiat rate."
        )
        rollup_note = GraphNoteInternal(
            code="fiat_totals_partial",
            message="Some transfers lack a fiat rate.",
            network="eth",
        )
        blocks = [
            GraphTxNetworkSummaryInternal(
                network="btc",
                tx_count=2,
                total_value={"value": 3500, "fiat_values": []},
                total_fee=200,
                total_inputs=3,
                total_outputs=2,
                block_min=100,
                block_max=101,
                timestamp_min=1_700_000_000,
                timestamp_max=1_700_000_500,
                notes=[per_network_note],
            )
        ]
        return GraphSummaryInternal(
            txs=GraphTxSummaryInternal(
                overall=GraphTxOverallInternal(
                    tx_count=2,
                    total_value_fiat=[],
                    timestamp_min=1_700_000_000,
                    timestamp_max=1_700_000_500,
                    notes=[rollup_note],
                ),
                networks=blocks,
            ),
            addresses=None,
        )

    monkeypatch.setattr(
        "graphsenselib.web.service.graph_service._db_summary", _fake_db_summary
    )
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "btc"},
        ]
    }
    result = request_with_status(client, "/graph/summary", 200, body=body)

    rollup_note = result["txs"]["overall"]["notes"][0]
    assert rollup_note["code"] == "fiat_totals_partial"
    assert rollup_note["message"] == "Some transfers lack a fiat rate."
    assert rollup_note["network"] == "eth"

    per_network_note = result["txs"]["networks"][0]["notes"][0]
    assert per_network_note["code"] == "fiat_totals_partial"
    assert per_network_note["message"] == "Some transfers lack a fiat rate."
    assert "network" not in per_network_note  # response_model_exclude_none


def test_summary_combined_node_count_accepted_at_boundary(client, patch_summary):
    # 50 txs + 50 addresses = 100, exactly MAX_GRAPH_NODES: the pydantic
    # combined-cap boundary must still accept the request.
    txs = [{"network": "btc", "tx_hash": f"{i:064x}"} for i in range(50)]
    addresses = [{"network": "btc", "address": f"addr{i}"} for i in range(50)]
    body = {"txs": txs, "addresses": addresses}
    request_with_status(client, "/graph/summary", 200, body=body)


def test_summary_not_found_propagates_404(client, monkeypatch):
    from graphsenselib.errors import NotFoundException

    async def _raise(*args, **kwargs):
        raise NotFoundException("tx not found")

    monkeypatch.setattr("graphsenselib.web.service.graph_service._db_summary", _raise)
    body = {
        "txs": [
            {"tx_hash": "aa11", "network": "btc"},
            {"tx_hash": "bb22", "network": "btc"},
        ]
    }
    status, _ = raw_request(client, "/graph/summary", body=body)
    assert status == 404


def test_graph_routes_marked_beta(client):
    # Both /graph/* routes are beta: the flag must be visible in the spec
    # ((beta) in the summary, BETA lead-in, x-beta extension) until the
    # graduation decision removes it everywhere at once.
    schema = client.app.openapi()
    for path in ("/graph/summary", "/graph/compare"):
        op = schema["paths"][path]["post"]
        assert op["x-beta"] is True
        assert "(beta)" in op["summary"]
        assert op["description"].startswith("**BETA**")
