"""Tests for the ``to_api_subgraph_summary`` translator."""

from graphsenselib.db.asynchronous.services.models import (
    SubgraphSummaryInternal,
    SubgraphTxSummaryInternal,
)
from graphsenselib.web.translators import to_api_subgraph_summary


def _make_tx_summary(**overrides) -> SubgraphTxSummaryInternal:
    fields = dict(
        tx_count=2,
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
    fields.update(overrides)
    return SubgraphTxSummaryInternal(**fields)


def _make_internal(**tx_overrides) -> SubgraphSummaryInternal:
    return SubgraphSummaryInternal(
        currency="btc",
        txs=_make_tx_summary(**tx_overrides),
        addresses=None,
    )


def test_to_api_subgraph_summary_round_trip():
    internal = _make_internal()
    api = to_api_subgraph_summary(internal)

    assert api.currency == internal.currency
    assert api.addresses is None
    t, it = api.txs, internal.txs
    assert t.tx_count == it.tx_count
    assert t.total_value == it.total_value
    assert t.total_value_fiat == it.total_value_fiat
    assert t.fiat_currency == it.fiat_currency
    assert t.total_fee == it.total_fee
    assert t.total_inputs == it.total_inputs
    assert t.total_outputs == it.total_outputs
    assert t.block_min == it.block_min
    assert t.block_max == it.block_max
    assert t.timestamp_min == it.timestamp_min
    assert t.timestamp_max == it.timestamp_max
    assert t.notes == it.notes


def test_to_api_subgraph_summary_account_omits_io_counts():
    internal = _make_internal(total_inputs=None, total_outputs=None)
    api = to_api_subgraph_summary(internal)
    assert api.txs.total_inputs is None
    assert api.txs.total_outputs is None
