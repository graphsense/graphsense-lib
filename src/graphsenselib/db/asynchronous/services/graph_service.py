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
from graphsenselib.errors import (
    AddressNotFoundException,
    BadUserInputException,
    NotFoundException,
    TransactionNotFoundException,
)
from graphsenselib.tagstore.db import TagPublic
from graphsenselib.db.asynchronous.services.common import (
    cannonicalize_address,
    canonical_tx_hash,
    dedup_refs,
    gather_bounded,
    partition_not_found,
)
from graphsenselib.db.asynchronous.services.models import (
    Address,
    AddressRefInternal,
    AddressTagQueryInput,
    FiatValue,
    GraphAddressNetworkSummaryInternal,
    GraphAddressOverallInternal,
    GraphAddressSummaryInternal,
    GraphNoteInternal,
    GraphSummaryInternal,
    GraphTxNetworkSummaryInternal,
    GraphTxOverallInternal,
    GraphTxSummaryInternal,
    LabeledItemRef,
    MAX_GRAPH_NODES,
    TxAccount,
    TxRefInternal,
    TxUtxo,
    Values,
)
from graphsenselib.utils.rest_utils import is_eth_like


def _sum_fiat_lists(lists: list[list[FiatValue]]) -> list[FiatValue]:
    """Accumulate fiat amounts per code, rounded to cents (float summation
    otherwise leaks representation noise into the response). Sorted by code
    for stable output."""
    sums: dict[str, float] = {}
    for fvs in lists:
        for fv in fvs:
            code = fv.code.lower()
            sums[code] = sums.get(code, 0.0) + fv.value
    return [FiatValue(code=c, value=round(v, 2)) for c, v in sorted(sums.items())]


def _fiat_sums(values_list) -> tuple[list[FiatValue], int]:
    """Per-code fiat sums across Values objects plus the number of rows
    carrying no rate at all."""
    n_missing = sum(1 for vs in values_list if not vs.fiat_values)
    fiat = _sum_fiat_lists([vs.fiat_values for vs in values_list if vs.fiat_values])
    return fiat, n_missing


def _missing_rate_notes(
    n_missing: int, n_total: int, singular: str, plural: str
) -> list[GraphNoteInternal]:
    if n_missing == n_total:
        return [
            GraphNoteInternal(
                code="fiat_totals_missing",
                message=f"fiat totals unavailable: no rate for any {singular}",
            )
        ]
    if n_missing:
        return [
            GraphNoteInternal(
                code="fiat_totals_partial",
                message=(
                    f"fiat totals are partial: {n_missing} of {n_total} "
                    f"{plural} had no rate"
                ),
            )
        ]
    return []


def _order_assets(native: str, symbols: set[str]) -> list[str]:
    """Native asset first, remaining tokens sorted alphabetically. All
    values lowercase. Native is always included even if absent from
    ``symbols``."""
    rest = sorted(s for s in symbols if s != native)
    return [native, *rest]


def build_network_tx_summary(
    network: str,
    txs: list[Union[TxUtxo, TxAccount]],
) -> GraphTxNetworkSummaryInternal:
    """Aggregate stats over one network's txs, derived straight from the tx
    headers. total_value.value sums native transfers only (account token
    transfers carry no native-unit amount, noted); total_value.fiat_values
    sum per fiat code across every tx (native and token)."""
    notes: list[GraphNoteInternal] = []
    if is_eth_like(network):
        # ``txs`` holds one leg per asset flow (base tx plus token-transfer
        # legs); the submitted transactions are exactly the base legs.
        native_txs = [t for t in txs if t.token_tx_id is None]
        tx_count = len(native_txs)
        total_value = sum(t.value.value for t in native_txs)
        # Token-transfer legs carry fee=None, so only the base transaction
        # contributes one fee per tx (no double count). A schema change adding
        # fees to token rows would break this invariant.
        fees = [t.fee.value for t in txs if t.fee is not None]
        # Account fee data can be absent per tx; None marks "unknown" and a
        # partial sum would silently understate, so only emit a total when
        # every base tx carries its fee.
        total_fee = sum(fees) if len(fees) == tx_count else None
        total_inputs = None
        total_outputs = None
        fiat_values, n_missing = _fiat_sums([t.value for t in txs])
        # Fiat is summed per leg, so missing rates are counted in transfers.
        notes.extend(_missing_rate_notes(n_missing, len(txs), "transfer", "transfers"))
        n_token = len(txs) - len(native_txs)
        if n_token:
            notes.append(
                GraphNoteInternal(
                    code="token_value_excluded",
                    message=(
                        f"total_value covers native transfers only; {n_token} "
                        "token transfer(s) excluded (their value is in the "
                        "fiat totals)"
                    ),
                )
            )
    else:
        tx_count = len(txs)
        total_value = sum(t.total_output.value for t in txs)
        fees = [
            t.total_input.value - t.total_output.value for t in txs if not t.coinbase
        ]
        # UTXO fees are always derivable and coinbase txs pay none, so the
        # total is always known: 0 for an all-coinbase set, never None.
        # (None is reserved for "fee data unavailable" on account chains.)
        total_fee = sum(fees)
        total_inputs = sum(t.no_inputs for t in txs)
        total_outputs = sum(t.no_outputs for t in txs)
        fiat_values, n_missing = _fiat_sums([t.total_output for t in txs])
        notes.extend(_missing_rate_notes(n_missing, len(txs), "tx", "txs"))

    assets = _order_assets(network, {t.currency.lower() for t in txs})

    return GraphTxNetworkSummaryInternal(
        network=network,
        tx_count=tx_count,
        total_value=Values(value=total_value, fiat_values=fiat_values),
        total_fee=total_fee,
        total_inputs=total_inputs,
        total_outputs=total_outputs,
        block_min=min(t.height for t in txs),
        block_max=max(t.height for t in txs),
        timestamp_min=min(t.timestamp for t in txs),
        timestamp_max=max(t.timestamp for t in txs),
        notes=notes,
        assets=assets,
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
    notes: list[GraphNoteInternal] = []

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
    notes.extend(_missing_rate_notes(n_missing, len(addresses), "address", "addresses"))

    first_usages = [a.first_tx.timestamp for a in addresses if a.first_tx]
    last_usages = [a.last_tx.timestamp for a in addresses if a.last_tx]
    if not first_usages:
        notes.append(
            GraphNoteInternal(
                code="usage_span_unavailable",
                message="usage span unavailable: no selected address has any activity",
            )
        )

    n_token = sum(
        1
        for a in addresses
        if a.total_tokens_received or a.total_tokens_spent or a.token_balances
    )
    if n_token:
        notes.append(
            GraphNoteInternal(
                code="token_holdings_excluded",
                message=(
                    f"native totals exclude token holdings; {n_token} "
                    "address(es) hold or moved tokens"
                ),
            )
        )

    token_symbols: set[str] = set()
    for a in addresses:
        for d in (a.total_tokens_received, a.total_tokens_spent, a.token_balances):
            if d:
                token_symbols.update(k.lower() for k in d)
    assets = _order_assets(network, token_symbols)

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
        assets=assets,
    )


def _rollup_notes(blocks) -> list[GraphNoteInternal]:
    """Per-network notes lifted into the overall rollup, tagged with their
    source network."""
    return [
        n.model_copy(update={"network": b.network}) for b in blocks for n in b.notes
    ]


def build_tx_overall(
    blocks: list[GraphTxNetworkSummaryInternal],
) -> GraphTxOverallInternal:
    """Network-agnostic rollup: fiat sums per code and timestamp span only
    (base units and block heights are not comparable across chains).
    Per-network notes carry their source network in ``network``."""
    return GraphTxOverallInternal(
        tx_count=sum(b.tx_count for b in blocks),
        total_value_fiat=_sum_fiat_lists([b.total_value.fiat_values for b in blocks]),
        timestamp_min=min(b.timestamp_min for b in blocks),
        timestamp_max=max(b.timestamp_max for b in blocks),
        notes=_rollup_notes(blocks),
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
        notes=_rollup_notes(blocks),
    )


def _canonical_tx_key(ref: TxRefInternal) -> tuple[str, str]:
    # Hashes are canonicalized (lowercase, no 0x) at ref construction, so
    # network + hash is the dedup identity.
    return (ref.network, ref.tx_hash)


def _canonical_address_key(ref: AddressRefInternal) -> tuple[str, object]:
    # Same reasoning as for tx hashes: get_address canonicalizes (eth hex
    # decode, trx base58 to evm, bch cashaddr to legacy) before the lookup.
    return (ref.network, cannonicalize_address(ref.network, ref.address))


def _group_by_network(refs, value) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for r in refs:
        groups.setdefault(r.network, []).append(value(r))
    return groups


def _nodes_not_found_note(
    network: str, missing: list[str], singular: str, plural: str
) -> GraphNoteInternal:
    word = singular if len(missing) == 1 else plural
    return GraphNoteInternal(
        code="nodes_not_found",
        message=(
            f"{len(missing)} requested {word} not found on {network}; "
            "excluded from all totals"
        ),
        network=network,
        items=sorted(missing),
    )


async def _fetch_network_txs(txs_service, network, hashes, tagstore_groups):
    """Fetch one network's txs; unknown hashes are dropped and returned
    separately instead of failing the request. Returns ``(legs, missing)``
    where ``legs`` holds one entry per asset flow (account chains) or per
    tx (UTXO chains)."""
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
            ],
            return_exceptions=True,
        )
        found, missing = partition_not_found(
            hashes, flow_lists, TransactionNotFoundException
        )
        return [leg for fl in found for leg in fl.txs], missing
    # UTXO chains: the tx header already carries the aggregate fields the
    # summary needs, so a header-only fetch is enough.
    fetched = await asyncio.gather(
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
        ],
        return_exceptions=True,
    )
    return partition_not_found(hashes, fetched, TransactionNotFoundException)


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
    keyed on (network, canonical hash/address), so spelling variants of one
    node count once); together at most MAX_GRAPH_NODES. Every ref's network
    must be a supported currency (400 otherwise). Unknown nodes are dropped
    and reported per network in a ``nodes_not_found`` note on the block's
    overall rollup (``items`` carries the refs); the request fails with 404
    only when fewer than 2 of a list's refs exist. Each block of the result
    is present iff its input list was non-empty; a network whose refs are
    all unknown gets no per-network block.
    """
    txs = dedup_refs(
        [
            TxRefInternal(
                network=r.network.lower(), tx_hash=canonical_tx_hash(r.tx_hash)
            )
            for r in txs
        ],
        key=_canonical_tx_key,
    )
    addresses = dedup_refs(
        [
            AddressRefInternal(network=r.network.lower(), address=r.address)
            for r in addresses
        ],
        key=_canonical_address_key,
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
    if len(txs) + len(addresses) > MAX_GRAPH_NODES:
        raise BadUserInputException(
            f"/graph/summary accepts at most {MAX_GRAPH_NODES} nodes."
        )

    supported = {c.lower() for c in txs_service.db.get_supported_currencies()}
    for net in {r.network for r in txs} | {r.network for r in addresses}:
        if net not in supported:
            raise BadUserInputException(f"unsupported network '{net}'")

    async def _tx_block():
        tx_groups = _group_by_network(txs, lambda r: r.tx_hash)
        fetched = await asyncio.gather(
            *[
                _fetch_network_txs(txs_service, net, hashes, tagstore_groups)
                for net, hashes in tx_groups.items()
            ]
        )
        missing_by_net = {
            net: missing for net, (_, missing) in zip(tx_groups, fetched) if missing
        }
        n_missing = sum(len(m) for m in missing_by_net.values())
        if len(txs) - n_missing < 2:
            raise NotFoundException(
                "fewer than 2 of the requested txs exist; not found: "
                + ", ".join(
                    f"{net}:{h}" for net, m in missing_by_net.items() for h in m
                )
            )
        blocks = [
            build_network_tx_summary(net, net_txs)
            for net, (net_txs, _) in zip(tx_groups, fetched)
            if net_txs
        ]
        overall = build_tx_overall(blocks)
        overall.notes.extend(
            _nodes_not_found_note(net, m, "tx", "txs")
            for net, m in missing_by_net.items()
        )
        return GraphTxSummaryInternal(overall=overall, networks=blocks)

    async def _address_block():
        addr_groups = _group_by_network(addresses, lambda r: r.address)

        async def _rows_and_tags(net, addrs):
            # Header-level rows: no per-address actor query (actors come
            # from the batched tag query) and no new-address fallback, so an
            # unknown address is dropped from the summary (reported via a
            # nodes_not_found note) instead of contributing zeros. Rows and
            # the batched tag query are independent, so they run
            # concurrently; tag rows for dropped addresses simply don't
            # exist.
            results, (tags, _) = await asyncio.gather(
                asyncio.gather(
                    *[
                        addresses_service.get_address(
                            net,
                            a,
                            tagstore_groups,
                            include_actors=False,
                            new_address_fallback=False,
                        )
                        for a in addrs
                    ],
                    return_exceptions=True,
                ),
                tags_service.list_tags_by_addresses_raw(
                    [AddressTagQueryInput(network=net, address=a) for a in addrs],
                    tagstore_groups,
                ),
            )
            rows, missing = partition_not_found(
                addrs, results, AddressNotFoundException
            )
            # The tagstore is independent of Cassandra and may carry tags
            # for a dropped address; those must not leak into tag counts or
            # actor lists of a summary that excludes the address itself.
            missing_set = set(missing)
            tags = [t for t in tags if t.identifier not in missing_set]
            return rows, tags, missing

        per_net = await asyncio.gather(
            *[_rows_and_tags(net, addrs) for net, addrs in addr_groups.items()]
        )
        missing_by_net = {
            net: missing
            for net, (_, _, missing) in zip(addr_groups, per_net)
            if missing
        }
        n_missing = sum(len(m) for m in missing_by_net.values())
        if len(addresses) - n_missing < 2:
            raise NotFoundException(
                "fewer than 2 of the requested addresses exist; not found: "
                + ", ".join(
                    f"{net}:{a}" for net, m in missing_by_net.items() for a in m
                )
            )

        # Resolve each distinct actor id once across all networks. Bound the
        # fan-out: each get_actor() opens a Postgres session via tagstore.
        actor_ids_per_net = [
            list(dict.fromkeys(t.actor for t in tags if t.actor))
            for _, tags, _ in per_net
        ]
        all_actor_ids = list(
            dict.fromkeys(aid for ids in actor_ids_per_net for aid in ids)
        )
        sem = asyncio.Semaphore(get_tagstore_max_concurrency())

        async def _resolve_actor(aid):
            # A tag can reference an actor pack that is not loaded; a missing
            # actor must not fail the summary (every tx/address still exists).
            try:
                return await tags_service.get_actor(aid)
            except NotFoundException:
                return None

        actor_objs = await gather_bounded(
            sem, *[_resolve_actor(aid) for aid in all_actor_ids]
        )
        actor_by_id = {
            a.id: LabeledItemRef(id=a.id, label=a.label)
            for a in actor_objs
            if a is not None
        }

        blocks = [
            build_network_address_summary(
                net,
                rows,
                tags,
                [actor_by_id[aid] for aid in ids if aid in actor_by_id],
            )
            for (net, _), (rows, tags, _), ids in zip(
                addr_groups.items(), per_net, actor_ids_per_net
            )
            if rows
        ]
        overall = build_address_overall(blocks)
        overall.notes.extend(
            _nodes_not_found_note(net, m, "address", "addresses")
            for net, m in missing_by_net.items()
        )
        return GraphAddressSummaryInternal(overall=overall, networks=blocks)

    async def _none():
        return None

    tx_task = asyncio.ensure_future(_tx_block() if txs else _none())
    address_task = asyncio.ensure_future(_address_block() if addresses else _none())
    try:
        tx_block, address_block = await asyncio.gather(tx_task, address_task)
    except BaseException:
        # A failed phase must not leave the sibling's db queries running in
        # the background: cancel the survivor and drain it before re-raising.
        tx_task.cancel()
        address_task.cancel()
        await asyncio.gather(tx_task, address_task, return_exceptions=True)
        raise

    return GraphSummaryInternal(txs=tx_block, addresses=address_block)
