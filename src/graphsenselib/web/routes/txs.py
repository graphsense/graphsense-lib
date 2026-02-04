"""Transaction API routes"""

from typing import Optional, Union

from fastapi import APIRouter, Depends, Path, Query, Request

from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.models import (
    ExternalConversion,
    TxAccount,
    TxRef,
    TxUtxo,
    TxValue,
)
from graphsenselib.web.routes.base import (
    RequestAdapter,
    apply_plugin_hooks,
    get_services,
    get_tagstore_access_groups,
    to_json_response,
)
import graphsenselib.web.service.txs_service as service

router = APIRouter()


def _normalize_page(page: Optional[str]) -> Optional[str]:
    """Convert empty string to None for pagination parameter."""
    if page is not None and page.strip() == "":
        return None
    return page


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
        ..., description="The cryptocurrency code (e.g., eth)", example="eth"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Returns all token transactions in a given transaction"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_token_txs(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


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
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
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
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get a transaction by its hash"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_tx(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
        token_tx_id=token_tx_id,
        include_io=include_io,
        include_nonstandard_io=include_nonstandard_io,
        include_io_index=include_io_index,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


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
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    ),
    io_index: Optional[int] = Query(
        None, description="Output index to check", example=0
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get transactions that spent outputs from this transaction"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_spent_in_txs(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
        io_index=io_index,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


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
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    ),
    io_index: Optional[int] = Query(
        None, description="Input index to check", example=0
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get transactions that this transaction is spending from"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_spending_txs(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
        io_index=io_index,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


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
        ..., description="The cryptocurrency code (e.g., eth)", example="eth"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get DeFi conversions for a transaction"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_tx_conversions(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


@router.get(
    "/txs/{tx_hash}/flows",
    summary="Get asset flows within a transaction",
    operation_id="list_tx_flows",
)
async def list_tx_flows(
    request: Request,
    currency: str = Path(
        ..., description="The cryptocurrency code (e.g., eth)", example="eth"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    ),
    strip_zero_value_txs: Optional[bool] = Query(
        None, description="Strip zero value transactions"
    ),
    only_token_txs: Optional[bool] = Query(
        None, description="Only return token transactions", example=False
    ),
    token_currency: Optional[str] = Query(
        None, description="Filter by token currency", example="WETH"
    ),
    page: Optional[str] = Query(
        None, description="Resumption token for retrieving the next page"
    ),
    pagesize: Optional[int] = Query(
        None, ge=1, description="Number of items returned in a single page", example=10
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get asset flows within a transaction"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.list_tx_flows(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
        strip_zero_value_txs=strip_zero_value_txs or False,
        only_token_txs=only_token_txs or False,
        token_currency=token_currency,
        page=_normalize_page(page),
        pagesize=pagesize,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)


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
        ..., description="The cryptocurrency code (e.g., btc)", example="btc"
    ),
    tx_hash: str = Path(
        ...,
        description="The transaction hash",
        example="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
    ),
    io: str = Path(
        ...,
        description="Input or output values of a transaction (inputs or outputs)",
        example="outputs",
    ),
    include_nonstandard_io: Optional[bool] = Query(
        None, description="Include non-standard inputs/outputs"
    ),
    include_io_index: Optional[bool] = Query(
        None, description="Include input/output indices"
    ),
    services: ServiceContainer = Depends(get_services),
    tagstore_groups: list[str] = Depends(get_tagstore_access_groups),
):
    """Get transaction inputs or outputs"""
    currency = currency.lower()
    adapted_request = RequestAdapter(request, services, tagstore_groups)

    result = await service.get_tx_io(
        adapted_request,
        currency=currency,
        tx_hash=tx_hash,
        io=io,
        include_nonstandard_io=include_nonstandard_io,
        include_io_index=include_io_index,
    )

    apply_plugin_hooks(request, result)
    return to_json_response(result)
