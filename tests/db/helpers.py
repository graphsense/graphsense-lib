"""Shared tx-model builders for db-service tests.

Used by test_comparison_service.py and test_subgraph_service.py. Not a test
module itself (pytest only collects test_*.py).
"""

from __future__ import annotations

from graphsenselib.db.asynchronous.services.models import (
    FiatValue,
    TxAccount,
    TxUtxo,
    TxValue,
    UtxoHeuristics,
    Values,
)


CURRENCY = "btc"


def make_value(
    value: int, usd: float | None = None, eur: float | None = None
) -> Values:
    fiat = []
    if usd is not None:
        fiat.append(FiatValue(code="usd", value=usd))
    if eur is not None:
        fiat.append(FiatValue(code="eur", value=eur))
    return Values(value=value, fiat_values=fiat)


def make_txvalue(
    address: str,
    value: int,
    has_witness: bool | None = None,
    sequence: int | None = None,
) -> TxValue:
    return TxValue(
        address=[address] if address else [],
        value=make_value(value),
        has_witness=has_witness,
        sequence=sequence,
    )


def make_tx(
    *,
    tx_hash: str = "aa" * 32,
    inputs: list[TxValue] | None = None,
    outputs: list[TxValue] | None = None,
    coinbase: bool = False,
    height: int = 100,
    timestamp: int = 1_700_000_000,
    total_input: int | None = None,
    total_output: int | None = None,
    total_output_usd: float | None = None,
    heuristics: UtxoHeuristics | None = None,
    version: int | None = None,
    lock_time: int | None = None,
) -> TxUtxo:
    inputs = inputs if inputs is not None else []
    outputs = outputs if outputs is not None else []
    if total_input is None:
        total_input = sum(i.value.value for i in inputs)
    if total_output is None:
        total_output = sum(o.value.value for o in outputs)
    return TxUtxo(
        currency=CURRENCY,
        tx_hash=tx_hash,
        coinbase=coinbase,
        height=height,
        no_inputs=len(inputs),
        no_outputs=len(outputs),
        inputs=inputs,
        outputs=outputs,
        timestamp=timestamp,
        total_input=make_value(total_input),
        total_output=make_value(total_output, usd=total_output_usd),
        heuristics=heuristics,
        version=version,
        lock_time=lock_time,
    )


def make_account_tx(
    *,
    tx_hash: str = "aa" * 32,
    value: int = 0,
    value_usd: float | None = None,
    value_eur: float | None = None,
    fee: int | None = None,
    height: int = 100,
    timestamp: int = 1_700_000_000,
    token_tx_id: int | None = None,
    asset: str = "eth",
) -> TxAccount:
    # asset is the TxAccount.currency: the network ticker for native transfers,
    # the token ticker (e.g. "usdt") for ERC20 transfers (token_tx_id set).
    return TxAccount(
        currency=asset,
        network="eth",
        identifier=tx_hash,
        tx_hash=tx_hash,
        timestamp=timestamp,
        height=height,
        from_address="0xfrom",
        to_address="0xto",
        value=make_value(value, usd=value_usd, eur=value_eur),
        fee=make_value(fee) if fee is not None else None,
        token_tx_id=token_tx_id,
    )
