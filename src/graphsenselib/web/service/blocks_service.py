from graphsenselib.web.translators import (
    to_api_block,
    to_api_block_at_date,
    to_api_tx_account,
    to_api_tx_utxo,
)


async def get_block(ctx, currency, height):
    pydantic_result = await ctx.services.blocks_service.get_block(currency, height)

    return to_api_block(pydantic_result)


async def list_block_txs(ctx, currency, height):
    pydantic_results = await ctx.services.blocks_service.list_block_txs(
        currency, height
    )

    # Convert each transaction result based on its type
    openapi_results = []
    for tx in pydantic_results:
        if hasattr(tx, "network"):  # TxAccount
            openapi_results.append(to_api_tx_account(tx))
        else:  # TxUtxo
            openapi_results.append(to_api_tx_utxo(tx))

    return openapi_results


async def get_block_by_date(ctx, currency, date):
    pydantic_result = await ctx.services.blocks_service.get_block_by_date(
        currency, date
    )

    return to_api_block_at_date(pydantic_result)
