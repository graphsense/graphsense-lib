"""Aggregate summary over a graph node set (transactions and/or addresses)
spanning one or more networks.

The summary is derived straight from tx headers (value, fee, counts, height,
timestamp) and address header fields, so it is chain-agnostic and cheap: no
IO decomposition, cluster lookups, or fingerprinting analysis. Inputs carry
their network per item; results are grouped per network plus a
network-agnostic overall block (fiat and timestamps only, since base units
and block heights are not comparable across chains).
"""

from __future__ import annotations

import asyncio
from typing import Union

from graphsenselib.config.tagstore_config import get_tagstore_max_concurrency
from graphsenselib.errors import BadUserInputException
from graphsenselib.tagstore.db import TagPublic
from graphsenselib.db.asynchronous.services.common import gather_bounded
from graphsenselib.db.asynchronous.services.models import (
    Address,
    AddressRefInternal,
    AddressTagQueryInput,
    FiatValue,
    GraphAddressNetworkSummaryInternal,
    GraphAddressOverallInternal,
    GraphAddressSummaryInternal,
    GraphSummaryInternal,
    GraphTxNetworkSummaryInternal,
    GraphTxOverallInternal,
    GraphTxSummaryInternal,
    LabeledItemRef,
    TxAccount,
    TxRefInternal,
    TxUtxo,
    Values,
)
from graphsenselib.utils.rest_utils import is_eth_like

_MAX_NODES = 100


def _fiat_sums(values_list) -> tuple[list[FiatValue], int]:
    """Sum fiat amounts per code across Values objects, rounded to cents
    (float summation otherwise leaks representation noise into the
    response). Returns (per-code sums, sorted by code, and the number of
    rows carrying no rate at all)."""
    sums: dict[str, float] = {}
    n_missing = 0
    for vs in values_list:
        if not vs.fiat_values:
            n_missing += 1
            continue
        for fv in vs.fiat_values:
            code = fv.code.lower()
            sums[code] = sums.get(code, 0.0) + fv.value
    fiat = [FiatValue(code=c, value=round(v, 2)) for c, v in sorted(sums.items())]
    return fiat, n_missing


def _missing_rate_notes(n_missing: int, n_total: int, unit: str) -> list[str]:
    if n_missing == n_total:
        return [f"fiat totals unavailable: no rate for any {unit}"]
    if n_missing:
        return [
            f"fiat totals are partial: {n_missing} of {n_total} {unit}s had no rate"
        ]
    return []


def build_network_tx_summary(
    network: str,
    txs: list[Union[TxUtxo, TxAccount]],
) -> GraphTxNetworkSummaryInternal:
    """Aggregate stats over one network's txs, derived straight from the tx
    headers. total_value.value sums native transfers only (account token
    transfers carry no native-unit amount, noted); total_value.fiat_values
    sum per fiat code across every tx (native and token)."""
    notes: list[str] = []
    if is_eth_like(network):
        native_txs = [t for t in txs if t.token_tx_id is None]
        total_value = sum(t.value.value for t in native_txs)
        fees = [t.fee.value for t in txs if t.fee is not None]
        total_inputs = None
        total_outputs = None
        fiat_values, n_missing = _fiat_sums([t.value for t in txs])
        n_token = len(txs) - len(native_txs)
        if n_token:
            notes.append(
                f"total_value covers native transfers only; {n_token} token "
                "transfer(s) excluded (their value is in the fiat totals)"
            )
    else:
        total_value = sum(t.total_output.value for t in txs)
        fees = [
            t.total_input.value - t.total_output.value for t in txs if not t.coinbase
        ]
        total_inputs = sum(t.no_inputs for t in txs)
        total_outputs = sum(t.no_outputs for t in txs)
        fiat_values, n_missing = _fiat_sums([t.total_output for t in txs])

    notes.extend(_missing_rate_notes(n_missing, len(txs), "tx"))

    return GraphTxNetworkSummaryInternal(
        network=network,
        tx_count=len(txs),
        total_value=Values(value=total_value, fiat_values=fiat_values),
        total_fee=sum(fees) if fees else None,
        total_inputs=total_inputs,
        total_outputs=total_outputs,
        block_min=min(t.height for t in txs),
        block_max=max(t.height for t in txs),
        timestamp_min=min(t.timestamp for t in txs),
        timestamp_max=max(t.timestamp for t in txs),
        notes=notes,
    )


def build_network_address_summary(
    network: str,
    addresses: list[Address],
    tags: list[TagPublic],
    actors: list[LabeledItemRef],
) -> GraphAddressNetworkSummaryInternal:
    """Aggregate stats over one network's address rows. ``tags`` are the
    TagPublic rows of the batched tagstore query for this network (only
    ``identifier`` is read); ``actors`` are the already-resolved distinct
    actor refs for this network."""
    notes: list[str] = []

    def _values(values_list) -> tuple[Values, int]:
        fiat, n_missing = _fiat_sums(values_list)
        return (
            Values(value=sum(v.value for v in values_list), fiat_values=fiat),
            n_missing,
        )

    # Rates apply uniformly to received/spent/balance on an address row, so
    # the received missing-count stands for all three fiat sums.
    total_received, n_missing = _values([a.total_received for a in addresses])
    total_spent, _ = _values([a.total_spent for a in addresses])
    balance, _ = _values([a.balance for a in addresses])
    notes.extend(_missing_rate_notes(n_missing, len(addresses), "address"))

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

    return GraphAddressNetworkSummaryInternal(
        network=network,
        address_count=len(addresses),
        total_received=total_received,
        total_spent=total_spent,
        balance=balance,
        first_usage=min(first_usages) if first_usages else None,
        last_usage=max(last_usages) if last_usages else None,
        tagged_address_count=len({t.identifier for t in tags}),
        actors=actors,
        notes=notes,
    )


def _sum_fiat_lists(lists: list[list[FiatValue]]) -> list[FiatValue]:
    sums: dict[str, float] = {}
    for fvs in lists:
        for fv in fvs:
            sums[fv.code] = sums.get(fv.code, 0.0) + fv.value
    return [FiatValue(code=c, value=round(v, 2)) for c, v in sorted(sums.items())]


def _prefixed_notes(blocks) -> list[str]:
    return [f"{b.network}: {n}" for b in blocks for n in b.notes]


def build_tx_overall(
    blocks: list[GraphTxNetworkSummaryInternal],
) -> GraphTxOverallInternal:
    """Network-agnostic rollup: fiat sums per code and timestamp span only
    (base units and block heights are not comparable across chains).
    Per-network notes carry their network as prefix."""
    return GraphTxOverallInternal(
        tx_count=sum(b.tx_count for b in blocks),
        total_value_fiat=_sum_fiat_lists([b.total_value.fiat_values for b in blocks]),
        timestamp_min=min(b.timestamp_min for b in blocks),
        timestamp_max=max(b.timestamp_max for b in blocks),
        notes=_prefixed_notes(blocks),
    )


def build_address_overall(
    blocks: list[GraphAddressNetworkSummaryInternal],
) -> GraphAddressOverallInternal:
    first_usages = [b.first_usage for b in blocks if b.first_usage is not None]
    last_usages = [b.last_usage for b in blocks if b.last_usage is not None]
    seen: set[str] = set()
    actors: list[LabeledItemRef] = []
    for b in blocks:
        for a in b.actors:
            if a.id not in seen:
                seen.add(a.id)
                actors.append(a)
    return GraphAddressOverallInternal(
        address_count=sum(b.address_count for b in blocks),
        total_received_fiat=_sum_fiat_lists(
            [b.total_received.fiat_values for b in blocks]
        ),
        total_spent_fiat=_sum_fiat_lists([b.total_spent.fiat_values for b in blocks]),
        balance_fiat=_sum_fiat_lists([b.balance.fiat_values for b in blocks]),
        first_usage=min(first_usages) if first_usages else None,
        last_usage=max(last_usages) if last_usages else None,
        tagged_address_count=sum(b.tagged_address_count for b in blocks),
        actors=actors,
        notes=_prefixed_notes(blocks),
    )


def _dedup(refs, key):
    seen = set()
    out = []
    for r in refs:
        k = key(r)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def _group_by_network(refs, value) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for r in refs:
        groups.setdefault(r.network, []).append(value(r))
    return groups


async def _fetch_network_txs(txs_service, network, hashes, tagstore_groups):
    if is_eth_like(network):
        # Account chains: get_tx returns only the base/native transaction,
        # so its token-transfer legs (and their fiat) would be invisible to
        # the summary. Fetch the full asset-flow set per hash instead.
        flow_lists = await asyncio.gather(
            *[
                txs_service.get_asset_flows_within_tx(
                    network,
                    h,
                    include_internal_txs=False,
                    include_token_txs=True,
                    include_base_transaction=True,
                )
                for h in hashes
            ]
        )
        return [leg for fl in flow_lists for leg in fl.txs]
    # UTXO chains: the tx header already carries the aggregate fields the
    # summary needs, so a header-only fetch is enough.
    return await asyncio.gather(
        *[
            txs_service.get_tx(
                network,
                h,
                None,
                include_io=False,
                include_nonstandard_io=False,
                include_io_index=False,
                include_heuristics=[],
                tagstore_groups=tagstore_groups,
            )
            for h in hashes
        ]
    )


async def summary(
    txs_service,
    addresses_service,
    tags_service,
    txs: list[TxRefInternal],
    addresses: list[AddressRefInternal],
    tagstore_groups: list[str],
) -> GraphSummaryInternal:
    """Aggregate stats over the node set ``txs`` and/or ``addresses``.

    Each non-empty list must hold at least 2 distinct entries (distinctness
    keyed on (network, hash)); together at most 100. Every ref's network
    must be a supported currency (400 otherwise); unknown nodes fail the
    whole request (404). Each block of the result is present iff its input
    list was non-empty.
    """
    txs = _dedup(
        [TxRefInternal(network=r.network.lower(), tx_hash=r.tx_hash) for r in txs],
        key=lambda r: (r.network, r.tx_hash),
    )
    addresses = _dedup(
        [
            AddressRefInternal(network=r.network.lower(), address=r.address)
            for r in addresses
        ],
        key=lambda r: (r.network, r.address),
    )
    if not txs and not addresses:
        raise BadUserInputException("/graph/summary needs tx refs and/or addresses.")
    if txs and len(txs) < 2:
        raise BadUserInputException(
            "/graph/summary needs at least 2 distinct tx refs when txs are given."
        )
    if addresses and len(addresses) < 2:
        raise BadUserInputException(
            "/graph/summary needs at least 2 distinct addresses "
            "when addresses are given."
        )
    if len(txs) + len(addresses) > _MAX_NODES:
        raise BadUserInputException(
            f"/graph/summary accepts at most {_MAX_NODES} nodes."
        )

    supported = {c.lower() for c in txs_service.db.get_supported_currencies()}
    for net in {r.network for r in txs} | {r.network for r in addresses}:
        if net not in supported:
            raise BadUserInputException(f"unsupported network '{net}'")

    tx_block = None
    if txs:
        tx_groups = _group_by_network(txs, lambda r: r.tx_hash)
        fetched = await asyncio.gather(
            *[
                _fetch_network_txs(txs_service, net, hashes, tagstore_groups)
                for net, hashes in tx_groups.items()
            ]
        )
        blocks = [
            build_network_tx_summary(net, net_txs)
            for net, net_txs in zip(tx_groups, fetched)
        ]
        tx_block = GraphTxSummaryInternal(
            overall=build_tx_overall(blocks), networks=blocks
        )

    address_block = None
    if addresses:
        addr_groups = _group_by_network(addresses, lambda r: r.address)

        async def _rows_and_tags(net, addrs):
            # Header-level rows: no per-address actor query (actors come
            # from the batched tag query) and no new-address fallback, so an
            # unknown address fails the request instead of contributing
            # zeros.
            rows = await asyncio.gather(
                *[
                    addresses_service.get_address(
                        net,
                        a,
                        tagstore_groups,
                        include_actors=False,
                        new_address_fallback=False,
                    )
                    for a in addrs
                ]
            )
            tags, _ = await tags_service.list_tags_by_addresses_raw(
                [AddressTagQueryInput(network=net, address=a) for a in addrs],
                tagstore_groups,
            )
            return rows, tags

        per_net = await asyncio.gather(
            *[_rows_and_tags(net, addrs) for net, addrs in addr_groups.items()]
        )

        # Resolve each distinct actor id once across all networks. Bound the
        # fan-out: each get_actor() opens a Postgres session via tagstore.
        actor_ids_per_net = [
            list(dict.fromkeys(t.actor for t in tags if t.actor)) for _, tags in per_net
        ]
        all_actor_ids = list(
            dict.fromkeys(aid for ids in actor_ids_per_net for aid in ids)
        )
        sem = asyncio.Semaphore(get_tagstore_max_concurrency())
        actor_objs = await gather_bounded(
            sem, *[tags_service.get_actor(aid) for aid in all_actor_ids]
        )
        actor_by_id = {a.id: LabeledItemRef(id=a.id, label=a.label) for a in actor_objs}

        blocks = [
            build_network_address_summary(
                net,
                rows,
                tags,
                [actor_by_id[aid] for aid in ids],
            )
            for (net, _), (rows, tags), ids in zip(
                addr_groups.items(), per_net, actor_ids_per_net
            )
        ]
        address_block = GraphAddressSummaryInternal(
            overall=build_address_overall(blocks), networks=blocks
        )

    return GraphSummaryInternal(txs=tx_block, addresses=address_block)
