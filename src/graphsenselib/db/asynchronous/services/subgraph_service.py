"""Aggregate summary over a subgraph (a set of transactions and/or addresses).

The summary is derived straight from tx headers (value, fee, counts, height,
timestamp) and address header fields, so it is chain-agnostic and cheap: no
IO decomposition, cluster lookups, or fingerprinting analysis. The subgraph
is defined by transaction hashes and/or addresses.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Union

from graphsenselib.config.tagstore_config import get_tagstore_max_concurrency
from graphsenselib.errors import BadUserInputException
from graphsenselib.tagstore.db import TagPublic
from graphsenselib.db.asynchronous.services.common import gather_bounded
from graphsenselib.db.asynchronous.services.models import (
    Address,
    AddressTagQueryInput,
    LabeledItemRef,
    SubgraphAddressSummaryInternal,
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
    every tx (native and token), rounded to 2 decimals, and so is
    comparable across assets; if some
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
        # Round to cents: float summation otherwise leaks representation
        # noise (e.g. 26.990000000000002) into the response.
        total_value_fiat = round(sum(present), 2)
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


def build_address_summary(
    currency: str,
    addresses: list[Address],
    tags: list[TagPublic],
    actors: list[LabeledItemRef],
    fiat_currency: str = "usd",
) -> SubgraphAddressSummaryInternal:
    """Aggregate stats over a set of address rows, derived straight from
    the per-address header fields (totals, balance, activity timestamps).

    ``tags`` are the TagPublic rows of the batched tagstore query over the
    set (only ``identifier`` is read); ``actors`` are the
    already-resolved distinct actor refs. Native totals exclude
    account-chain token holdings (noted). Fiat sums follow the tx block:
    rounded to 2 decimals, partial sums get a note, all-missing yields
    None plus a note."""
    fiat = fiat_currency.lower()
    fiat_label = fiat.upper()
    notes: list[str] = []

    def _fiat_sum(values_list):
        present = [v for v in (_fiat(vs, fiat) for vs in values_list) if v is not None]
        # Round to cents: float summation otherwise leaks representation
        # noise (e.g. 26.990000000000002) into the response.
        return (round(sum(present), 2) if present else None), len(values_list) - len(
            present
        )

    # Rates apply uniformly to received/spent/balance on an address row, so
    # the received missing-count stands for all three fiat sums.
    total_received_fiat, n_missing = _fiat_sum([a.total_received for a in addresses])
    total_spent_fiat, _ = _fiat_sum([a.total_spent for a in addresses])
    balance_fiat, _ = _fiat_sum([a.balance for a in addresses])
    if n_missing == len(addresses):
        notes.append(f"fiat totals unavailable: no {fiat_label} rate for any address")
    elif n_missing:
        notes.append(
            f"fiat totals are partial: {n_missing} of {len(addresses)} "
            f"addresses had no {fiat_label} rate"
        )

    first_usages = [a.first_tx.timestamp for a in addresses if a.first_tx]
    last_usages = [a.last_tx.timestamp for a in addresses if a.last_tx]
    if not first_usages:
        notes.append("usage span unavailable: no selected address has any activity")

    n_token = sum(
        1
        for a in addresses
        if a.total_tokens_received or a.total_tokens_spent or a.token_balances
    )
    if n_token:
        notes.append(
            f"native totals exclude token holdings; {n_token} address(es) "
            "hold or moved tokens"
        )

    return SubgraphAddressSummaryInternal(
        address_count=len(addresses),
        total_received=sum(a.total_received.value for a in addresses),
        total_received_fiat=total_received_fiat,
        total_spent=sum(a.total_spent.value for a in addresses),
        total_spent_fiat=total_spent_fiat,
        balance=sum(a.balance.value for a in addresses),
        balance_fiat=balance_fiat,
        fiat_currency=fiat,
        first_usage=min(first_usages) if first_usages else None,
        last_usage=max(last_usages) if last_usages else None,
        tagged_address_count=len({t.identifier for t in tags}),
        actors=actors,
        notes=notes,
    )


# Combined cap on the subgraph node set (txs + addresses), matching the
# /txs/compare hash cap. Keeps the per-request DB work bounded.
_MAX_NODES = 100


async def summary(
    txs_service,
    addresses_service,
    tags_service,
    currency: str,
    txs: list[str],
    addresses: list[str],
    tagstore_groups: list[str],
    fiat_currency: str = "usd",
) -> SubgraphSummaryInternal:
    """Aggregate stats over the node set ``txs`` and/or ``addresses``.

    Each non-empty list must hold at least 2 distinct entries (each block
    is an aggregate over its own node type); together at most 100. Each
    block of the result is present iff its input list was non-empty.
    Unknown nodes fail the whole request (404); ``fiat_currency`` selects
    the currency for the fiat totals (default USD).
    """
    # Dedup (order-preserving) so repeated nodes are fetched once and not
    # double-counted in the aggregates.
    txs = list(dict.fromkeys(txs))
    addresses = list(dict.fromkeys(addresses))
    if not txs and not addresses:
        raise BadUserInputException(
            "/subgraph/summary needs tx hashes and/or addresses."
        )
    if txs and len(txs) < 2:
        raise BadUserInputException(
            "/subgraph/summary needs at least 2 distinct tx hashes when txs are given."
        )
    if addresses and len(addresses) < 2:
        raise BadUserInputException(
            "/subgraph/summary needs at least 2 distinct addresses "
            "when addresses are given."
        )
    if len(txs) + len(addresses) > _MAX_NODES:
        raise BadUserInputException(
            f"/subgraph/summary accepts at most {_MAX_NODES} nodes."
        )

    tx_block = None
    if txs:
        if is_eth_like(currency):
            # Account chains: ``get_tx`` returns only the base/native
            # transaction, so its token-transfer legs (and their fiat)
            # would be invisible to ``build_summary``. Fetch the full
            # asset-flow set per hash (base tx + token transfers) so token
            # fiat is folded into ``total_value_fiat``.
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
            # UTXO chains: the tx header already carries the aggregate
            # fields the summary needs (total_input/total_output,
            # no_inputs/no_outputs), so a header-only fetch (no IO, no
            # heuristics) is enough.
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
        tx_block = build_summary(currency, summary_txs, fiat_currency)

    address_block = None
    if addresses:
        # Header-level rows: no per-address actor query (actors come from
        # the batched tag query below) and no new-address fallback, so an
        # unknown address fails the request instead of contributing zeros.
        address_rows = await asyncio.gather(
            *[
                addresses_service.get_address(
                    currency,
                    a,
                    tagstore_groups,
                    include_actors=False,
                    new_address_fallback=False,
                )
                for a in addresses
            ]
        )
        tags, _ = await tags_service.list_tags_by_addresses_raw(
            [AddressTagQueryInput(network=currency, address=a) for a in addresses],
            tagstore_groups,
        )
        actor_ids = list(dict.fromkeys(t.actor for t in tags if t.actor))
        # Bound the actor fan-out: each get_actor() opens a Postgres
        # session via tagstore.
        sem = asyncio.Semaphore(get_tagstore_max_concurrency())
        actor_objs = await gather_bounded(
            sem, *[tags_service.get_actor(aid) for aid in actor_ids]
        )
        actors = [LabeledItemRef(id=a.id, label=a.label) for a in actor_objs]
        address_block = build_address_summary(
            currency, address_rows, tags, actors, fiat_currency
        )

    return SubgraphSummaryInternal(
        currency=currency, txs=tx_block, addresses=address_block
    )
