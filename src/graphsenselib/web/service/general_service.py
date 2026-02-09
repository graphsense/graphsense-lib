from graphsenselib.db.asynchronous.services.models import SearchRequestConfig

from graphsenselib.web.dependencies import (
    get_service_container,
    get_tagstore_access_groups,
)
from graphsenselib.web.translators import (
    to_api_search_result,
    to_api_search_result_by_currency,
    to_api_stats,
)


async def get_statistics(request):
    """Returns summary statistics on all available currencies"""
    services = get_service_container(request)
    version = request.app["version"]

    pydantic_result = await services.general_service.get_statistics(version)

    return to_api_stats(pydantic_result)


async def search_by_currency(request, currency, q, limit=10):
    services = get_service_container(request)

    pydantic_result = await services.general_service.search_by_currency(
        currency, q, limit
    )

    return to_api_search_result_by_currency(pydantic_result)


async def search(
    request,
    q,
    currency=None,
    limit=10,
    include_sub_tx_identifiers=False,
    include_labels=True,
    include_actors=True,
    include_txs=True,
    include_addresses=True,
):
    services = get_service_container(request)
    tagstore_groups = get_tagstore_access_groups(request)

    search_config = SearchRequestConfig(
        include_sub_tx_identifiers=include_sub_tx_identifiers,
        include_labels=include_labels,
        include_actors=include_actors,
        include_txs=include_txs,
        include_addresses=include_addresses,
    )

    pydantic_result = await services.general_service.search(
        q,
        tagstore_groups,
        currency,
        limit,
        config=search_config,
    )

    return to_api_search_result(pydantic_result)
