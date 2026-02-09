"""Transaction API routes"""

from typing import Optional, Union

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
    get_ctx,
    normalize_page,
    respond,
)
import graphsenselib.web.service.txs_service as service

router = APIRouter()


@router.get(
    "/token_txs/{tx_hash}",
    summary="Returns all token transactions in a given transaction",
    operation_id="list_token_txs",
    deprecated=True,
    response_model=list[TxAccount],
    response_model_exclude_none=True,
)
async def list_token_txs(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., eth)", examples=["eth"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Returns all token transactions in a given transaction"""
    result = await service.list_token_txs(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
    )
    return respond(request, result)


@router.get(
    "/txs/{tx_hash}",
    summary="Get a transaction by its hash",
    operation_id="get_tx",
    response_model=Union[TxUtxo, TxAccount],
    response_model_exclude_none=True,
)
async def get_tx(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
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
    )
    return respond(request, result)


@router.get(
    "/txs/{tx_hash}/spent_in",
    summary="Get transactions that spent outputs from this transaction",
    operation_id="get_spent_in_txs",
    response_model=list[TxRef],
    response_model_exclude_none=True,
)
async def get_spent_in(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
    io_index: Optional[int] = Query(
        None, description="Output index to check", examples=[0]
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get transactions that spent outputs from this transaction"""
    result = await service.get_spent_in_txs(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        io_index=io_index,
    )
    return respond(request, result)


@router.get(
    "/txs/{tx_hash}/spending",
    summary="Get transactions that this transaction is spending from",
    operation_id="get_spending_txs",
    response_model=list[TxRef],
    response_model_exclude_none=True,
)
async def get_spending(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
    io_index: Optional[int] = Query(
        None, description="Input index to check", examples=[0]
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get transactions that this transaction is spending from"""
    result = await service.get_spending_txs(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        io_index=io_index,
    )
    return respond(request, result)


@router.get(
    "/txs/{tx_hash}/conversions",
    summary="Get DeFi conversions for a transaction",
    operation_id="get_tx_conversions",
    response_model=list[ExternalConversion],
    response_model_exclude_none=True,
)
async def get_tx_conversions(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., eth)", examples=["eth"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
    ctx: ServiceContext = Depends(get_ctx),
):
    """Get DeFi conversions for a transaction"""
    result = await service.get_tx_conversions(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
    )
    return respond(request, result)


@router.get(
    "/txs/{tx_hash}/flows",
    summary="Get asset flows within a transaction",
    operation_id="list_tx_flows",
)
async def list_tx_flows(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., eth)", examples=["eth"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
    strip_zero_value_txs: Optional[bool] = Query(
        None, description="Strip zero value transactions"
    ),
    only_token_txs: Optional[bool] = Query(
        None, description="Only return token transactions", examples=[False]
    ),
    token_currency: Optional[str] = Query(
        None, description="Filter by token currency", examples=["WETH"]
    ),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None,
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
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
    return respond(request, result)


# NOTE: This route MUST be defined AFTER all other /txs/{tx_hash}/... routes
# because {io} is a catch-all that would match "spent_in", "spending", etc.
@router.get(
    "/txs/{tx_hash}/{io}",
    summary="Get transaction inputs or outputs",
    operation_id="get_tx_io",
    response_model=list[TxValue],
    response_model_exclude_none=True,
)
async def get_tx_io(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., btc)", examples=["btc"]
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
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
    """Get transaction inputs or outputs"""
    result = await service.get_tx_io(
        ctx,
        currency=currency.lower(),
        tx_hash=tx_hash,
        io=io,
        include_nonstandard_io=include_nonstandard_io,
        include_io_index=include_io_index,
    )
    return respond(request, result)
