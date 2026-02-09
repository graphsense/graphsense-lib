from graphsenselib.web.dependencies import get_service_container
from graphsenselib.web.translators import to_api_currency_stats


async def get_currency_statistics(request, currency):
    services = get_service_container(request)

    pydantic_result = await services.stats_service.get_currency_statistics(currency)

    return to_api_currency_stats(pydantic_result)


async def get_no_blocks(request, currency):
    services = get_service_container(request)

    return await services.stats_service.get_no_blocks(currency)
