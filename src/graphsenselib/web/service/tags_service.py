from graphsenselib.tagstore.db.queries import UserReportedAddressTag

from graphsenselib.web.dependencies import (
    get_service_container,
    get_tagstore_access_groups,
    get_user_tags_acl_group,
    get_username,
)
from graphsenselib.web.service import parse_page_int_optional
from graphsenselib.web.models import UserTagReportResponse
from graphsenselib.web.translators import (
    to_api_actor,
    to_api_address_tag_result,
    to_api_concept,
    to_api_taxonomy,
)


async def get_actor(request, actor):
    services = get_service_container(request)

    pydantic_result = await services.tags_service.get_actor(actor)

    return to_api_actor(pydantic_result)


async def get_actor_tags(request, actor, page=None, pagesize=None):
    services = get_service_container(request)
    tagstore_groups = get_tagstore_access_groups(request)

    page = parse_page_int_optional(page)

    pydantic_result = await services.tags_service.get_actor_tags(
        actor, tagstore_groups, page, pagesize
    )

    return to_api_address_tag_result(pydantic_result)


async def list_address_tags(request, label, page=None, pagesize=None):
    services = get_service_container(request)
    tagstore_groups = get_tagstore_access_groups(request)

    page = parse_page_int_optional(page)

    pydantic_result = await services.tags_service.list_address_tags_by_label(
        label, tagstore_groups, page, pagesize
    )

    return to_api_address_tag_result(pydantic_result)


async def list_concepts(request, taxonomy):
    services = get_service_container(request)

    pydantic_results = await services.tags_service.list_concepts(taxonomy)

    return [to_api_concept(concept) for concept in pydantic_results]


async def list_taxonomies(request):
    services = get_service_container(request)

    pydantic_results = await services.tags_service.list_taxonomies()

    return [to_api_taxonomy(taxonomy) for taxonomy in pydantic_results]


async def report_tag(request, body):
    services = get_service_container(request)
    config = request.app["config"]
    tag_acl_group = get_user_tags_acl_group(request)

    tag_to_report = UserReportedAddressTag(
        address=body.address,
        network=body.network,
        actor=body.actor,
        label=body.label,
        description=body.description,
        user=get_username(request),
    )

    derId = await services.tags_service.report_tag(tag_to_report, config, tag_acl_group)

    return UserTagReportResponse(id=derId)
