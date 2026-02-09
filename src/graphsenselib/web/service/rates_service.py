from graphsenselib.web.dependencies import get_service_container
from graphsenselib.web.models import Rates
from graphsenselib.web.translators import to_api_rates


async def get_exchange_rates(request, currency, height) -> Rates:
    services = get_service_container(request)

    pydantic_result = await services.rates_service.get_rates(currency, height)

    return to_api_rates(pydantic_result)


async def get_rates(request, currency, height=None):
    services = get_service_container(request)

    rates_response = await services.rates_service.get_rates(currency, height)

    # Return in the original format for backward compatibility
    return {"rates": rates_response.rates}


async def list_rates(request, currency, heights):
    services = get_service_container(request)

    return await services.rates_service.list_rates(currency, heights)
