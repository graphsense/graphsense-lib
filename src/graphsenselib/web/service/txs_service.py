from typing import Optional

from graphsenselib.web.dependencies import get_service_container
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
    request,
    currency,
    tx_hash,
    token_tx_id=None,
    include_io=False,
    include_nonstandard_io=False,
    include_io_index=False,
):
    services = get_service_container(request)

    result = await services.txs_service.get_tx(
        currency,
        tx_hash,
        token_tx_id,
        include_io,
        include_nonstandard_io,
        include_io_index,
    )
    return to_api_tx(result)


async def get_tx_io(
    request, currency, tx_hash, io, include_nonstandard_io, include_io_index
):
    services = get_service_container(request)
    result = await services.txs_service.get_tx_io(
        currency, tx_hash, io, include_nonstandard_io, include_io_index
    )
    return [to_api_tx_value(tx_value) for tx_value in result or []]


async def list_token_txs(request, currency, tx_hash, token_tx_id=None):
    services = get_service_container(request)
    results = await services.txs_service.list_token_txs(currency, tx_hash, token_tx_id)
    return [to_api_tx_account(tx) for tx in results]


async def get_spent_in_txs(
    request, currency: str, tx_hash: str, io_index: Optional[int]
):
    services = get_service_container(request)
    results = await services.txs_service.get_spent_in_txs(currency, tx_hash, io_index)
    return [to_api_tx_ref(tx_ref) for tx_ref in results]


async def get_spending_txs(
    request, currency: str, tx_hash: str, io_index: Optional[int]
):
    services = get_service_container(request)
    results = await services.txs_service.get_spending_txs(currency, tx_hash, io_index)
    return [to_api_tx_ref(tx_ref) for tx_ref in results]


async def list_matching_txs(request, currency, expression):
    services = get_service_container(request)
    return await services.txs_service.list_matching_txs(currency, expression)


async def get_tx_conversions(request, currency, tx_hash):
    services = get_service_container(request)
    included_bridges = request.app["config"].included_bridges
    result = await services.txs_service.get_conversions(
        currency, tx_hash, included_bridges=included_bridges
    )
    return [to_api_external_conversion(conv) for conv in result]


async def list_tx_flows(
    request,
    currency: str,
    tx_hash: str,
    strip_zero_value_txs: bool,
    only_token_txs: bool,
    token_currency: Optional[str],
    page: Optional[int],
    pagesize: Optional[int],
):
    services = get_service_container(request)

    page = parse_page_int_optional(page)

    if page is None and pagesize is not None:
        page = 1

    include_token_txs = True
    include_internal_txs = True
    include_base_tx = True

    if only_token_txs:
        include_internal_txs = False
        include_base_tx = False

    result = await services.txs_service.get_asset_flows_within_tx(
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
