from graphsenselib.web.models import Rates
from graphsenselib.web.translators import to_api_rates


async def get_exchange_rates(ctx, currency, height) -> Rates:
    pydantic_result = await ctx.services.rates_service.get_rates(currency, height)

    return to_api_rates(pydantic_result)


async def get_rates(ctx, currency, height=None):
    rates_response = await ctx.services.rates_service.get_rates(currency, height)

    # Return in the original format for backward compatibility
    return {"rates": rates_response.rates}


async def list_rates(ctx, currency, heights):
    return await ctx.services.rates_service.list_rates(currency, heights)
