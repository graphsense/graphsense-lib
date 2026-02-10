import re
from functools import partial
from typing import Any

from fastapi import Request
from graphsenselib.tagstore.algorithms.obfuscate import (
    obfuscate_entity_actor,
    obfuscate_tag_if_not_public,
)

from graphsenselib.web.models import (
    AddressTags,
    Entity,
    NeighborEntities,
    SearchResultLeaf,
    SearchResultLevel1,
    SearchResultLevel2,
    SearchResultLevel3,
    SearchResultLevel4,
    SearchResultLevel5,
    SearchResultLevel6,
)
from graphsenselib.web.plugins import (
    Plugin,
    get_request_header,
    get_request_path,
    get_request_query_string,
)

GROUPS_HEADER_NAME = "X-Consumer-Groups"
NO_OBFUSCATION_MARKER_PATTERN = re.compile(r"tags-private")
OBFUSCATION_MARKER_GROUP = "obfuscate"


def has_no_obfuscation_group(groups):
    """Check if any group matches the no obfuscation pattern."""
    for group in groups:
        if NO_OBFUSCATION_MARKER_PATTERN.match(group):
            return True
    return False


def obfuscate_tagpack_uri_by_rule(rule, tags):
    if not tags:
        return
    if isinstance(tags, list):
        for tag in tags:
            obfuscate_tagpack_uri_by_rule(rule, tag)
    else:
        # use regex in rule to check if uri needs to be redacted
        if tags.tagpack_uri is None:
            return
        pattern = re.compile(rule)
        if pattern.match(tags.tagpack_uri):
            tags.tagpack_uri = ""


def obfuscate_private_tags(tags):
    if not tags:
        return
    if isinstance(tags, list):
        for tag in tags:
            obfuscate_tag_if_not_public(tag)
    else:
        obfuscate_tag_if_not_public(tags)


class ObfuscateTags(Plugin):
    @classmethod
    def before_request(cls, context: dict, request: Request) -> dict | None:
        groups = [
            x.strip()
            for x in get_request_header(request, GROUPS_HEADER_NAME, "").split(",")
        ]

        path = get_request_path(request)
        query_string = get_request_query_string(request)

        if has_no_obfuscation_group(groups):
            return None
        if "include_labels=true" in query_string.lower():
            return None
        if "/search" == path:
            return None
        if "/bulk" in path:
            return None
        if re.match(re.compile("/tags"), path):
            return None
        if re.match(re.compile("/[a-z]{3}/addresses/[^/]+$"), path):
            # to avoid loading actors for address
            return None

        return {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}

    @classmethod
    def before_response(cls, context: dict, request: Request, result: Any) -> None:
        # Get groups from headers (check for header modifications first)
        header_mods = getattr(request.state, "plugin_state", {})
        if GROUPS_HEADER_NAME in header_mods:
            groups = [header_mods[GROUPS_HEADER_NAME]]
        else:
            groups = [
                x.strip()
                for x in get_request_header(request, GROUPS_HEADER_NAME, "").split(",")
            ]

        obfuscate_tagpack_uri_rule = (context.get("config") or {}).get(
            "obfuscate_tagpack_uri_rule", None
        )

        if obfuscate_tagpack_uri_rule is not None:
            cls.obfuscate_tags_in_objects(
                context,
                request,
                result,
                partial(obfuscate_tagpack_uri_by_rule, obfuscate_tagpack_uri_rule),
            )

        if has_no_obfuscation_group(groups):
            return

        else:
            cls.obfuscate_tags_in_objects(
                context, request, result, obfuscate_private_tags
            )

    @classmethod
    def obfuscate_tags_in_objects(cls, context, request, result, tag_obfuscation_func):
        if isinstance(result, Entity):
            tag_obfuscation_func(result.best_address_tag)
            obfuscate_entity_actor(result)
            return
        if isinstance(result, AddressTags):
            tag_obfuscation_func(result.address_tags)
            return
        if isinstance(result, NeighborEntities):
            for neighbor in result.neighbors:
                tag_obfuscation_func(neighbor.entity.best_address_tag)
                obfuscate_entity_actor(neighbor.entity)
        if (
            isinstance(result, SearchResultLevel1)
            or isinstance(result, SearchResultLevel2)
            or isinstance(result, SearchResultLevel3)
            or isinstance(result, SearchResultLevel4)
            or isinstance(result, SearchResultLevel5)
            or isinstance(result, SearchResultLevel6)
            or isinstance(result, SearchResultLeaf)
        ):
            if result.neighbor:
                tag_obfuscation_func(result.neighbor.entity.best_address_tag)
            if not isinstance(result, SearchResultLeaf) and result.paths:
                for path in result.paths:
                    cls.before_response(context, request, path)
            return
        if isinstance(result, list):
            for r in result:
                cls.before_response(context, request, r)
            return
