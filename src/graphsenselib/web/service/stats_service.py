from graphsenselib.web.translators import to_api_currency_stats


async def get_currency_statistics(ctx, currency):
    pydantic_result = await ctx.services.stats_service.get_currency_statistics(currency)

    return to_api_currency_stats(pydantic_result)


async def get_no_blocks(ctx, currency):
    return await ctx.services.stats_service.get_no_blocks(currency)
