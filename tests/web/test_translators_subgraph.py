"""Tests for the ``to_api_subgraph_summary`` translator."""

from graphsenselib.db.asynchronous.services.models import SubgraphSummaryInternal
from graphsenselib.web.translators import to_api_subgraph_summary


def _make_internal() -> SubgraphSummaryInternal:
    return SubgraphSummaryInternal(
        tx_count=2,
        currency="btc",
        total_value=2000,
        total_value_fiat=42.5,
        fiat_currency="usd",
        total_fee=150,
        total_inputs=2,
        total_outputs=2,
        block_min=100,
        block_max=101,
        timestamp_min=1_700_000_000,
        timestamp_max=1_700_000_500,
        notes=["total_value_fiat is partial: 1 of 2 txs had no USD rate"],
    )


def test_to_api_subgraph_summary_round_trip():
    internal = _make_internal()
    api = to_api_subgraph_summary(internal)

    assert api.tx_count == internal.tx_count
    assert api.currency == internal.currency
    assert api.total_value == internal.total_value
    assert api.total_value_fiat == internal.total_value_fiat
    assert api.fiat_currency == internal.fiat_currency
    assert api.total_fee == internal.total_fee
    assert api.total_inputs == internal.total_inputs
    assert api.total_outputs == internal.total_outputs
    assert api.block_min == internal.block_min
    assert api.block_max == internal.block_max
    assert api.timestamp_min == internal.timestamp_min
    assert api.timestamp_max == internal.timestamp_max
    assert api.notes == internal.notes


def test_to_api_subgraph_summary_account_omits_io_counts():
    internal = _make_internal().model_copy(
        update={"total_inputs": None, "total_outputs": None}
    )
    api = to_api_subgraph_summary(internal)
    assert api.total_inputs is None
    assert api.total_outputs is None
