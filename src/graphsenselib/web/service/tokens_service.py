from graphsenselib.web.dependencies import get_service_container
from graphsenselib.web.translators import to_api_token_configs


async def list_supported_tokens(request, currency):
    services = get_service_container(request)

    pydantic_result = await services.tokens_service.list_supported_tokens(currency)

    return to_api_token_configs(pydantic_result)
