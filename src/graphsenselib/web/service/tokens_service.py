from graphsenselib.web.translators import to_api_token_configs


async def list_supported_tokens(ctx, currency):
    pydantic_result = await ctx.services.tokens_service.list_supported_tokens(currency)

    return to_api_token_configs(pydantic_result)
