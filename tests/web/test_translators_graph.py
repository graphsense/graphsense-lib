"""Translator tests for the graph summary: internal -> API round trip."""

from graphsenselib.db.asynchronous.services.models import (
    FiatValue,
    GraphAddressNetworkSummaryInternal,
    GraphAddressOverallInternal,
    GraphAddressSummaryInternal,
    GraphSummaryInternal,
    GraphTxNetworkSummaryInternal,
    GraphTxOverallInternal,
    GraphTxSummaryInternal,
    LabeledItemRef,
    Values,
)
from graphsenselib.web.translators import to_api_graph_summary


def _internal_summary() -> GraphSummaryInternal:
    btc_txs = GraphTxNetworkSummaryInternal(
        network="btc",
        tx_count=2,
        total_value=Values(
            value=3500,
            fiat_values=[
                FiatValue(code="eur", value=40.0),
                FiatValue(code="usd", value=42.5),
            ],
        ),
        total_fee=200,
        total_inputs=3,
        total_outputs=2,
        block_min=100,
        block_max=101,
        timestamp_min=1_700_000_000,
        timestamp_max=1_700_000_500,
        notes=[],
        assets=["btc"],
    )
    addr_block = GraphAddressNetworkSummaryInternal(
        network="btc",
        address_count=2,
        total_received=Values(
            value=1500, fiat_values=[FiatValue(code="usd", value=15.0)]
        ),
        total_spent=Values(value=500, fiat_values=[FiatValue(code="usd", value=5.0)]),
        balance=Values(value=1000, fiat_values=[FiatValue(code="usd", value=10.0)]),
        first_usage=1_700_000_000,
        last_usage=1_700_000_500,
        tagged_address_count=1,
        actors=[LabeledItemRef(id="binance", label="Binance")],
        notes=[],
    )
    return GraphSummaryInternal(
        txs=GraphTxSummaryInternal(
            overall=GraphTxOverallInternal(
                tx_count=2,
                total_value_fiat=[
                    FiatValue(code="eur", value=40.0),
                    FiatValue(code="usd", value=42.5),
                ],
                timestamp_min=1_700_000_000,
                timestamp_max=1_700_000_500,
                notes=[],
            ),
            networks=[btc_txs],
        ),
        addresses=GraphAddressSummaryInternal(
            overall=GraphAddressOverallInternal(
                address_count=2,
                total_received_fiat=[FiatValue(code="usd", value=15.0)],
                total_spent_fiat=[FiatValue(code="usd", value=5.0)],
                balance_fiat=[FiatValue(code="usd", value=10.0)],
                first_usage=1_700_000_000,
                last_usage=1_700_000_500,
                tagged_address_count=1,
                actors=[LabeledItemRef(id="binance", label="Binance")],
                notes=[],
            ),
            networks=[addr_block],
        ),
    )


def test_round_trip_maps_all_fields():
    api = to_api_graph_summary(_internal_summary())
    assert api.txs.overall.tx_count == 2
    assert [n.network for n in api.txs.networks] == ["btc"]
    assert api.txs.networks[0].total_value.value == 3500
    assert {r.code: r.value for r in api.txs.networks[0].total_value.fiat_values} == {
        "eur": 40.0,
        "usd": 42.5,
    }
    assert api.addresses.overall.balance_fiat[0].code == "usd"
    assert api.addresses.networks[0].actors[0].label == "Binance"
    # assets carries through the flat model_validate round-trip
    assert api.txs.networks[0].assets == ["btc"]


def test_blocks_optional():
    api = to_api_graph_summary(GraphSummaryInternal(txs=None, addresses=None))
    assert api.txs is None and api.addresses is None
