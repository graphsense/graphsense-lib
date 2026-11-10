import logging
from collections import Counter
from typing import Awaitable, Callable, Optional, Sequence, Set

from graphsenselib.db.asynchronous.services.models import (
    ExternalConversion as ServiceExternalConversion,
)
from graphsenselib.defi.bridging.delegated import PENDING, UNKNOWN
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.web.middleware.external_backends import logfmt
from graphsenselib.web.models import ExternalConversion
from graphsenselib.web.service import parse_page_int_optional
from graphsenselib.web.translators import (
    pydantic_to_openapi,
    to_api_external_conversion,
    to_api_tx,
    to_api_tx_account,
    to_api_tx_ref,
    to_api_tx_value,
)

# what a half or partial bridge names in place of a transfer (delegated.py)
_PLACEHOLDERS = frozenset({PENDING, UNKNOWN})

# The bridge merge is the gs-rest half of the external backends middleware's
# rule 6, and its line is read next to that middleware's bridge_pairing line
# (same request, same Loki query), so it goes through that module's logger:
# the one web/app.py:setup_logging gives the configured level, the request
# id filter and the Slack handler. A logger of this module's own name would
# keep the root's WARNING and drop the line.
_merge_logger = logging.getLogger("graphsenselib.web.middleware.external_backends")


async def get_tx(
    ctx,
    currency,
    tx_hash,
    token_tx_id=None,
    include_io=False,
    include_nonstandard_io=False,
    include_io_index=False,
    include_heuristics=[],
):
    result = await ctx.services.txs_service.get_tx(
        currency,
        tx_hash,
        token_tx_id,
        include_io,
        include_nonstandard_io,
        include_io_index,
        include_heuristics,
        tagstore_groups=ctx.tagstore_groups,
    )
    return to_api_tx(result)


async def get_tx_io(
    ctx, currency, tx_hash, io, include_nonstandard_io, include_io_index
):
    result = await ctx.services.txs_service.get_tx_io(
        currency, tx_hash, io, include_nonstandard_io, include_io_index
    )
    return [to_api_tx_value(tx_value) for tx_value in result or []]


async def list_token_txs(ctx, currency, tx_hash, token_tx_id=None):
    results = await ctx.services.txs_service.list_token_txs(
        currency, tx_hash, token_tx_id
    )
    return [to_api_tx_account(tx) for tx in results]


async def get_spent_in_txs(ctx, currency: str, tx_hash: str, io_index: Optional[int]):
    results = await ctx.services.txs_service.get_spent_in_txs(
        currency, tx_hash, io_index
    )
    return [to_api_tx_ref(tx_ref) for tx_ref in results]


async def get_spending_txs(ctx, currency: str, tx_hash: str, io_index: Optional[int]):
    results = await ctx.services.txs_service.get_spending_txs(
        currency, tx_hash, io_index
    )
    return [to_api_tx_ref(tx_ref) for tx_ref in results]


async def list_matching_txs(ctx, currency, expression):
    return await ctx.services.txs_service.list_matching_txs(currency, expression)


async def get_tx_conversions(
    ctx,
    currency,
    tx_hash,
    pair_bridges: Optional[
        Callable[..., Awaitable[Sequence[ServiceExternalConversion]]]
    ] = None,
):
    """The tx's conversions as API rows. The bridges graphsense-lib finds only
    half of (defi/bridging/delegated.py) are half rows, unless pair_bridges
    (the external backends middleware's rule 6) pairs them. A paired row
    replaces the one half row whose known leg it names (same transfer and
    network on that side, or a bare tx with matching asset and amount);
    a half no row names stays, a row naming no half
    was not asked for and is dropped, and an ambiguous match keeps the half.
    So with several bridges in a tx, one bridge's pairing never hides
    another's half. A root-trace request (<hash>_I<n> that is the whole tx)
    asks the backend for the bare hash."""
    result = await ctx.services.txs_service.get_conversions(
        currency,
        tx_hash,
        included_bridges=ctx.config.included_bridges,
        cow_protocol_swaps=ctx.config.cow_protocol_swaps,
    )
    rows = [to_api_external_conversion(conv) for conv in result.conversions]
    halves = [to_api_external_conversion(conv) for conv in result.half_bridges]
    candidates = []
    if halves and pair_bridges is not None:
        identifier = SubTransactionIdentifier.from_string(tx_hash)
        if result.whole_tx and identifier.tx_type is SubTransactionType.InternalTx:
            bridges = await pair_bridges(tx_identifier=identifier.tx_hash)
        else:
            bridges = await pair_bridges()
        candidates = [ExternalConversion(**r.model_dump()) for r in bridges]

    matches = [
        [i for i, half in enumerate(halves) if _matches_half(row, half)]
        for row in candidates
    ]
    counts = Counter(i for indices in matches for i in indices)
    accepted = [
        (row, indices[0])
        for row, indices in zip(candidates, matches)
        if len(indices) == 1 and counts[indices[0]] == 1
    ]
    paired = [row for row, _ in accepted]
    replaced = {i for _, i in accepted}
    # Only accepted pairs may remove local conversions naming their legs.
    named = set().union(*map(_asset_transfers, paired))
    local = [row for row in rows if not _asset_transfers(row) & named]
    kept = [half for i, half in enumerate(halves) if i not in replaced]
    if halves:
        _log_merge(
            tx_hash,
            halves=len(halves),
            paired_rows=len(candidates),
            accepted=len(accepted),
            no_match=sum(1 for indices in matches if not indices),
            local_dropped=len(rows) - len(local),
            kept_halves=len(kept),
        )
    return local + kept + paired


def _log_merge(
    tx_hash: str,
    *,
    halves: int,
    paired_rows: int,
    accepted: int,
    no_match: int,
    local_dropped: int,
    kept_halves: int,
) -> None:
    """One INFO ``event=bridge_merge`` line per conversions request with
    half bridges, logfmt tail (LogQL ``| logfmt``): how many halves there
    were, how many rows the pairing answered, how many replaced a half, how
    many named no half or an ambiguous one (several rows for one half, or
    one row for several), how many local rows an accepted pair removed, and
    how many halves stayed."""
    ambiguous = paired_rows - accepted - no_match
    _merge_logger.info(
        "bridge merge of %s: %d of %d paired rows accepted, %d of %d halves kept | %s",
        tx_hash,
        accepted,
        paired_rows,
        kept_halves,
        halves,
        logfmt(
            {
                "event": "bridge_merge",
                "tx": tx_hash,
                "halves": halves,
                "paired_rows": paired_rows,
                "accepted": accepted,
                "ambiguous": ambiguous,
                "no_match": no_match,
                "local_dropped": local_dropped,
                "kept_halves": kept_halves,
            }
        ),
    )


def _matches_half(row: ExternalConversion, half: ExternalConversion) -> bool:
    for side in ("from", "to"):
        transfer = getattr(half, f"{side}_asset_transfer").lower().removeprefix("0x")
        if transfer in _PLACEHOLDERS or getattr(half, f"{side}_network") != getattr(
            row, f"{side}_network"
        ):
            continue
        paired_transfer = (
            getattr(row, f"{side}_asset_transfer").lower().removeprefix("0x")
        )
        if transfer == paired_transfer:
            return True
        # A bare leg has no unique Transfer; its asset and amount must agree.
        if (
            "_" not in transfer
            and transfer == paired_transfer.partition("_")[0]
            and getattr(half, f"{side}_asset") == getattr(row, f"{side}_asset")
            and getattr(half, f"{side}_amount") == getattr(row, f"{side}_amount")
        ):
            return True
    return False


def _asset_transfers(row: ExternalConversion) -> Set[str]:
    """The asset transfers a row names, in lower case and without 0x. The
    placeholders of a half or partial bridge ("pending", "unknown") name none."""
    transfers = (
        transfer.lower().removeprefix("0x")
        for transfer in (row.from_asset_transfer, row.to_asset_transfer)
    )
    return {transfer for transfer in transfers if transfer not in _PLACEHOLDERS}


async def list_tx_flows(
    ctx,
    currency: str,
    tx_hash: str,
    strip_zero_value_txs: bool,
    only_token_txs: bool,
    token_currency: Optional[str],
    page: Optional[int],
    pagesize: Optional[int],
):
    page = parse_page_int_optional(page)

    if page is None and pagesize is not None:
        page = 1

    include_token_txs = True
    include_internal_txs = True
    include_base_tx = True

    if only_token_txs:
        include_internal_txs = False
        include_base_tx = False

    result = await ctx.services.txs_service.get_asset_flows_within_tx(
        currency,
        tx_hash,
        include_zero_value=not strip_zero_value_txs,
        include_token_txs=include_token_txs,
        include_internal_txs=include_internal_txs,
        include_base_transaction=include_base_tx,
        asset=token_currency,
        page=page,
        page_size=pagesize,
    )

    return pydantic_to_openapi(result)
