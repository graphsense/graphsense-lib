"""Aggregate summary over a subgraph (a set of transactions).

The summary is derived straight from tx headers (value, fee, counts, height,
timestamp), so it is chain-agnostic and cheap: no IO decomposition, cluster
lookups, or fingerprinting analysis. Address inputs are reserved for a future
extension; for now the subgraph is defined by transaction hashes only.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Union

from graphsenselib.errors import BadUserInputException
from graphsenselib.db.asynchronous.services.models import (
    SubgraphSummaryInternal,
    SubgraphTxSummaryInternal,
    TxAccount,
    TxUtxo,
)
from graphsenselib.utils.rest_utils import is_eth_like


def _fiat(values, code: str) -> Optional[float]:
    """Pull the fiat amount for ``code`` (e.g. "usd") off a Values object, or
    None if that currency has no rate. ``code`` must be lower-case."""
    for fv in values.fiat_values:
        if fv.code.lower() == code:
            return fv.value
    return None


def build_summary(
    currency: str,
    txs: list[Union[TxUtxo, TxAccount]],
    fiat_currency: str = "usd",
) -> SubgraphTxSummaryInternal:
    """Aggregate stats over a set of txs, derived straight from the tx headers
    (value, fee, counts, height, timestamp). Needs nothing beyond the headers,
    so it can be built without fetching IO or running any analysis.

    Currency-aware. ``total_value`` is the queried currency's native base
    unit (UTXO: summed outputs; account: summed native transfers). Account
    token transfers (``token_tx_id`` set) carry no native-unit amount, so
    they are excluded from ``total_value`` and that exclusion is recorded in
    ``notes``. ``total_value_fiat`` sums the ``fiat_currency`` value across
    every tx (native and token) and so is comparable across assets; if some
    txs lack a rate for that currency the available ones are summed and a note
    flags the partial total. ``fiat_currency`` echoes which currency the fiat
    total is in. ``total_fee`` stays in the native unit (gas is always
    native)."""
    fiat = fiat_currency.lower()
    fiat_label = fiat.upper()
    notes: list[str] = []
    if is_eth_like(currency):
        native_txs = [t for t in txs if t.token_tx_id is None]
        total_value = sum(t.value.value for t in native_txs)
        fees = [t.fee.value for t in txs if t.fee is not None]
        total_inputs = None
        total_outputs = None
        fiat_values = [_fiat(t.value, fiat) for t in txs]
        n_token = len(txs) - len(native_txs)
        if n_token:
            notes.append(
                f"total_value covers native transfers only; {n_token} token "
                "transfer(s) excluded (their value is in total_value_fiat)"
            )
    else:
        total_value = sum(t.total_output.value for t in txs)
        fees = [
            t.total_input.value - t.total_output.value for t in txs if not t.coinbase
        ]
        total_inputs = sum(t.no_inputs for t in txs)
        total_outputs = sum(t.no_outputs for t in txs)
        fiat_values = [_fiat(t.total_output, fiat) for t in txs]

    present = [v for v in fiat_values if v is not None]
    n_missing = len(fiat_values) - len(present)
    if not present:
        total_value_fiat = None
        notes.append(f"total_value_fiat unavailable: no {fiat_label} rate for any tx")
    else:
        total_value_fiat = sum(present)
        if n_missing:
            notes.append(
                f"total_value_fiat is partial: {n_missing} of {len(fiat_values)} "
                f"txs had no {fiat_label} rate"
            )

    return SubgraphTxSummaryInternal(
        tx_count=len(txs),
        total_value=total_value,
        total_value_fiat=total_value_fiat,
        fiat_currency=fiat,
        total_fee=sum(fees) if fees else None,
        total_inputs=total_inputs,
        total_outputs=total_outputs,
        block_min=min(t.height for t in txs),
        block_max=max(t.height for t in txs),
        timestamp_min=min(t.timestamp for t in txs),
        timestamp_max=max(t.timestamp for t in txs),
        notes=notes,
    )


# Combined cap on the subgraph node set (txs + addresses), matching the
# /txs/compare hash cap. Keeps the per-request DB work bounded.
_MAX_NODES = 100


async def summary(
    txs_service,
    currency: str,
    txs: list[str],
    addresses: list[str],
    tagstore_groups: list[str],
    fiat_currency: str = "usd",
) -> SubgraphSummaryInternal:
    """Aggregate stats over the set of transactions ``txs``.

    ``addresses`` is reserved for a future extension; a non-empty list is
    rejected for now rather than silently ignored, so the field name is
    locked in the API contract. The node set (txs + addresses) must hold at
    least 2 and at most 100 distinct nodes. ``fiat_currency`` selects the
    currency for ``total_value_fiat`` (default USD).
    """
    if addresses:
        raise BadUserInputException(
            "/subgraph/summary does not support addresses yet; "
            "pass transaction hashes only."
        )

    # Dedup hashes (order-preserving) so a repeated hash is fetched once and
    # not double-counted in the aggregates.
    txs = list(dict.fromkeys(txs))
    n_nodes = len(txs) + len(addresses)
    if n_nodes < 2:
        raise BadUserInputException(
            "/subgraph/summary needs at least 2 distinct nodes."
        )
    if n_nodes > _MAX_NODES:
        raise BadUserInputException(
            f"/subgraph/summary accepts at most {_MAX_NODES} nodes."
        )

    if is_eth_like(currency):
        # Account chains: ``get_tx`` returns only the base/native transaction,
        # so its token-transfer legs (and their fiat) would be invisible to
        # ``build_summary``. Fetch the full asset-flow set per hash (base tx +
        # token transfers) so token fiat is folded into ``total_value_fiat``.
        flow_lists = await asyncio.gather(
            *[
                txs_service.get_asset_flows_within_tx(
                    currency,
                    h,
                    include_internal_txs=False,
                    include_token_txs=True,
                    include_base_transaction=True,
                )
                for h in txs
            ]
        )
        summary_txs: list[Union[TxUtxo, TxAccount]] = [
            leg for fl in flow_lists for leg in fl.txs
        ]
    else:
        # UTXO chains: the tx header already carries the aggregate fields the
        # summary needs (total_input/total_output, no_inputs/no_outputs), so a
        # header-only fetch (no IO, no heuristics) is enough.
        summary_txs = await asyncio.gather(
            *[
                txs_service.get_tx(
                    currency,
                    h,
                    None,
                    include_io=False,
                    include_nonstandard_io=False,
                    include_io_index=False,
                    include_heuristics=[],
                    tagstore_groups=tagstore_groups,
                )
                for h in txs
            ]
        )

    return SubgraphSummaryInternal(
        currency=currency,
        txs=build_summary(currency, summary_txs, fiat_currency),
        addresses=None,
    )
