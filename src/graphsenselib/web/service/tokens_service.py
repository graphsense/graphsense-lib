from graphsenselib.web.dependencies import get_service_container
from graphsenselib.web.translators import pydantic_token_configs_to_openapi


async def list_supported_tokens(request, currency):
    services = get_service_container(request)

    pydantic_result = await services.tokens_service.list_supported_tokens(currency)

    return pydantic_token_configs_to_openapi(pydantic_result)
