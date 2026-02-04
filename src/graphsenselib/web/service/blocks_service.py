from graphsenselib.web.dependencies import get_service_container
from graphsenselib.web.translators import (
    pydantic_block_at_date_to_openapi,
    pydantic_block_to_openapi,
    pydantic_tx_account_to_openapi,
    pydantic_tx_utxo_to_openapi,
)


# Updated functions using new service layer
async def get_block(request, currency, height):
    services = get_service_container(request)

    pydantic_result = await services.blocks_service.get_block(currency, height)

    return pydantic_block_to_openapi(pydantic_result)


async def list_block_txs(request, currency, height):
    services = get_service_container(request)

    pydantic_results = await services.blocks_service.list_block_txs(currency, height)

    # Convert each transaction result based on its type
    openapi_results = []
    for tx in pydantic_results:
        if hasattr(tx, "network"):  # TxAccount
            openapi_results.append(pydantic_tx_account_to_openapi(tx))
        else:  # TxUtxo
            openapi_results.append(pydantic_tx_utxo_to_openapi(tx))

    return openapi_results


async def get_block_by_date(request, currency, date):
    services = get_service_container(request)

    pydantic_result = await services.blocks_service.get_block_by_date(currency, date)

    return pydantic_block_at_date_to_openapi(pydantic_result)
