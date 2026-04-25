"""Transaction API routes"""

from typing import List, Literal, Optional, Union

from fastapi import APIRouter, Depends, Path, Query, Request

from graphsenselib.web.service import ServiceContext
from graphsenselib.web.models import (
    ExternalConversion,
    TxAccount,
    TxRef,
    TxUtxo,
    TxValue,
)
from graphsenselib.web.routes.base import (
    PluginRoute,
    get_ctx,
    normalize_page,
)
from graphsenselib.web.routes.params import (
    CurrencyPath,
    PageQuery,
    PagesizeQuery,
    TokenCurrencyQuery,
    TxHashPath,
)
import graphsenselib.web.service.txs_service as service

router = APIRouter(route_class=PluginRoute)


@router.get(
    "/token_txs/{tx_hash}",
    summary="List token transfers in a transaction",
    description="Returns token transfer records associated with the given transaction hash.",
    operation_id="list_token_txs",
    deprecated=True,
    response_model=list[TxAccount],
    response_model_exclude_none=True,
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def list_token_txs(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns all token transactions in a given transaction"""
    result = await service.list_token_txs(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
    )
    return result


@router.get(
    "/txs/{tx_hash}",
    summary="Get transaction details by hash",
    description=(
        "Returns a transaction, including optional input/output details for UTXO-like "
        "currencies and token transaction selection for account-like currencies."
    ),
    operation_id="get_tx",
    response_model=Union[TxUtxo, TxAccount],
    response_model_exclude_none=True,
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def get_tx(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    token_tx_id: Optional[int] = Query(
        None, description="Token transaction ID for account-model currencies"
    ),
    include_io: Optional[bool] = Query(
        None, description="Include transaction inputs/outputs"
    ),
    include_nonstandard_io: Optional[bool] = Query(
        None, description="Include non-standard inputs/outputs"
    ),
    include_io_index: Optional[bool] = Query(
        None, description="Include input/output indices"
    ),
    include_heuristics: List[
        Literal[
            "all",
            "one_time_change",
            "direct_change",
            "multi_input_change",
            "all_change",
            "all_coinjoin",
            "whirlpool_coinjoin",
            "wasabi_coinjoin",
            "wasabi_1_0_coinjoin",
            "wasabi_1_1_coinjoin",
            "wasabi_2_0_coinjoin",
            "joinmarket_coinjoin",
        ]
    ] = Query(
        default=[],
        description="Heuristics to compute (e.g. one_time_change, all_coinjoin) as list, or simply all",
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get a transaction by its hash"""
    result = await service.get_tx(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        token_tx_id=token_tx_id,
        include_io=include_io,
        include_nonstandard_io=include_nonstandard_io,
        include_io_index=include_io_index,
        include_heuristics=include_heuristics,
    )
    return result


@router.get(
    "/txs/{tx_hash}/spent_in",
    summary="List spending transactions",
    description=(
        "Returns references to transactions that spend outputs created by this "
        "transaction."
    ),
    operation_id="get_spent_in_txs",
    response_model=list[TxRef],
    response_model_exclude_none=True,
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def get_spent_in(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    io_index: Optional[int] = Query(
        None, description="Output index to check", examples=[0]
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """List transactions that spent outputs from this transaction."""
    result = await service.get_spent_in_txs(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        io_index=io_index,
    )
    return result


@router.get(
    "/txs/{tx_hash}/spending",
    summary="List source transactions",
    description=(
        "Returns references to transactions whose outputs are consumed by this "
        "transaction."
    ),
    operation_id="get_spending_txs",
    response_model=list[TxRef],
    response_model_exclude_none=True,
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def get_spending(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    io_index: Optional[int] = Query(
        None, description="Input index to check", examples=[0]
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """List transactions that this transaction is spending from."""
    result = await service.get_spending_txs(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        io_index=io_index,
    )
    return result


@router.get(
    "/txs/{tx_hash}/conversions",
    summary="List DeFi conversions in a transaction",
    description=(
        "Returns detected DeFi conversion events contained in the transaction."
    ),
    operation_id="get_tx_conversions",
    response_model=list[ExternalConversion],
    response_model_exclude_none=True,
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def get_tx_conversions(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    ctx: ServiceContext = Depends(get_ctx),
):
    """List DeFi conversions for a transaction."""
    result = await service.get_tx_conversions(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
    )
    return result


@router.get(
    "/txs/{tx_hash}/flows",
    summary="List transaction asset flows",
    description=(
        "Returns paginated asset flow events within the transaction, optionally "
        "filtered to token transfers."
    ),
    operation_id="list_tx_flows",
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def list_tx_flows(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    strip_zero_value_txs: Optional[bool] = Query(
        None, description="Strip zero value transactions"
    ),
    only_token_txs: Optional[bool] = Query(
        None, description="Only return token transactions", examples=[False]
    ),
    token_currency: TokenCurrencyQuery = None,
    page: PageQuery = None,
    pagesize: PagesizeQuery = None,
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get asset flows within a transaction"""
    result = await service.list_tx_flows(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        strip_zero_value_txs=strip_zero_value_txs or False,
        only_token_txs=only_token_txs or False,
        token_currency=token_currency,
        page=normalize_page(page),
        pagesize=pagesize,
    )
    return result


# NOTE: This route MUST be defined AFTER all other /txs/{tx_hash}/... routes
# because {io} is a catch-all that would match "spent_in", "spending", etc.
@router.get(
    "/txs/{tx_hash}/{io}",
    summary="List transaction inputs or outputs",
    description=(
        "Returns transaction input or output values, including optional index and "
        "non-standard entries."
    ),
    operation_id="get_tx_io",
    response_model=list[TxValue],
    response_model_exclude_none=True,
    responses={
        404: {"description": "Transaction not found for the selected currency."}
    },
)
async def get_tx_io(
    request: Request,
    currency: CurrencyPath,
    tx_hash: TxHashPath,
    io: str = Path(
        ...,
        description="Input or output values of a transaction (inputs or outputs)",
        examples=["outputs"],
    ),
    include_nonstandard_io: Optional[bool] = Query(
        None, description="Include non-standard inputs/outputs"
    ),
    include_io_index: Optional[bool] = Query(
        None, description="Include input/output indices"
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """List transaction inputs or outputs."""
    result = await service.get_tx_io(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        io=io,
        include_nonstandard_io=include_nonstandard_io,
        include_io_index=include_io_index,
    )
    return result
