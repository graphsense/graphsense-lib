from typing import Awaitable, Callable, Optional, Sequence, Set

from graphsenselib.db.asynchronous.services.models import (
    ExternalConversion as ServiceExternalConversion,
)
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
        Callable[[], Awaitable[Sequence[ServiceExternalConversion]]]
    ] = None,
):
    """The tx's conversions as API rows. The bridges graphsense-lib finds only
    half of (defi/bridging/delegated.py) are half rows, unless pair_bridges
    (the external backends middleware's rule 6) answers the tx's bridges:
    those then take the half rows' place. A backend pairs all of a tx's
    bridges or none, and may name a leg otherwise than the half row does."""
    result = await ctx.services.txs_service.get_conversions(
        currency,
        tx_hash,
        included_bridges=ctx.config.included_bridges,
        cow_protocol_swaps=ctx.config.cow_protocol_swaps,
    )
    rows = [to_api_external_conversion(conv) for conv in result.conversions]
    halves = [to_api_external_conversion(conv) for conv in result.half_bridges]
    paired = []
    if halves and pair_bridges is not None:
        paired = [ExternalConversion(**r.model_dump()) for r in await pair_bridges()]
    if not paired:
        return rows + halves
    # a local row naming a leg of a paired bridge is that bridge too
    named = set().union(*map(_asset_transfers, paired))
    return [row for row in rows if not _asset_transfers(row) & named] + paired


def _asset_transfers(row: ExternalConversion) -> Set[str]:
    """The asset transfers a row names, in lower case and without 0x."""
    return {
        transfer.lower().removeprefix("0x")
        for transfer in (row.from_asset_transfer, row.to_asset_transfer)
    }


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
