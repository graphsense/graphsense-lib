from __future__ import annotations

import logging
import re
from functools import partial
from typing import Any

from fastapi import Request
from graphsenselib.tagstore.algorithms.obfuscate import (
    obfuscate_entity_actor,
    obfuscate_tag_if_not_public,
)

from graphsenselib.web.models import (
    Address,
    AddressTags,
    Cluster,
    Entity,
    EntityAddresses,
    NeighborAddress,
    NeighborAddresses,
    NeighborClusters,
    NeighborEntities,
    SearchResult,
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
OBFUSCATION_MODE_CONFIG_KEY = "obfuscation_mode"
OBFUSCATION_MODE_DEFAULT = "default"
OBFUSCATION_MODE_FORCE_ENABLE = "force_enable"
OBFUSCATION_MODE_FORCE_DISABLE = "force_disable"


logger = logging.getLogger(__name__)


def get_obfuscation_mode(context: dict) -> str:
    """Return effective obfuscation mode.

    Supported values:
    - default: header/path based logic
    - force_enable: always obfuscate private tags
    - force_disable: never obfuscate private tags
    """
    config = context.get("config") or {}
    mode = str(
        config.get(OBFUSCATION_MODE_CONFIG_KEY, OBFUSCATION_MODE_DEFAULT)
    ).lower()
    valid_modes = {
        OBFUSCATION_MODE_DEFAULT,
        OBFUSCATION_MODE_FORCE_ENABLE,
        OBFUSCATION_MODE_FORCE_DISABLE,
    }
    if mode not in valid_modes:
        logger.warning(
            "Unknown obfuscation mode '%s', falling back to '%s'",
            mode,
            OBFUSCATION_MODE_DEFAULT,
        )
        return OBFUSCATION_MODE_DEFAULT
    return mode


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


def expanded_entity(entity_ref):
    """Return the neighbor reference only if it is an expanded entity object.

    With `relations_only=true` a neighbor's `entity` is the bare cluster id, not
    an `Entity`/`Cluster`. Such a reference carries no tags or actors to
    obfuscate.
    """
    if isinstance(entity_ref, (Entity, Cluster)):
        return entity_ref
    return None


def obfuscate_private_tags(tags):
    if not tags:
        return
    if isinstance(tags, list):
        for tag in tags:
            obfuscate_tag_if_not_public(tag)
    else:
        obfuscate_tag_if_not_public(tags)


def obfuscate_address_actors(address):
    """Blank actor attribution on an address response.

    Unlike ``Entity``/``Cluster``, ``Address`` carries no ``best_address_tag``
    with a publicity flag, so the per-tag gate used by
    :func:`obfuscate_entity_actor` is unavailable here. Under obfuscation the
    whole actor list is hidden — the same stance the single-address route
    takes by not loading address actors for obfuscated consumers.
    """
    if not address or not address.actors:
        return
    for actor in address.actors:
        actor.id = ""
        actor.label = ""


def obfuscate_label_strings(labels):
    """Blank plain label strings (no publicity info survives on them)."""
    if not labels:
        return
    for i in range(len(labels)):
        labels[i] = ""


def obfuscate_search_result_actors(result):
    """Blank actor attribution and label strings on a top-level search result."""
    if result.actors:
        for actor in result.actors:
            actor.id = ""
            actor.label = ""
    obfuscate_label_strings(result.labels)


class ObfuscateTags(Plugin):
    @classmethod
    def before_request(cls, context: dict, request: Request) -> dict | None:
        mode = get_obfuscation_mode(context)
        if mode == OBFUSCATION_MODE_FORCE_DISABLE:
            return None
        if mode == OBFUSCATION_MODE_FORCE_ENABLE:
            return {GROUPS_HEADER_NAME: OBFUSCATION_MARKER_GROUP}

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
        mode = get_obfuscation_mode(context)

        if mode == OBFUSCATION_MODE_FORCE_DISABLE:
            return

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

        if mode == OBFUSCATION_MODE_FORCE_ENABLE:
            # Ignore group-based bypass when force_enable mode is active.
            cls.obfuscate_tags_in_objects(
                context, request, result, obfuscate_private_tags
            )
        elif has_no_obfuscation_group(groups):
            return
        else:
            cls.obfuscate_tags_in_objects(
                context, request, result, obfuscate_private_tags
            )

    @classmethod
    def obfuscate_tags_in_objects(cls, context, request, result, tag_obfuscation_func):
        if isinstance(result, (Entity, Cluster)):
            tag_obfuscation_func(result.best_address_tag)
            obfuscate_entity_actor(result)
            return
        if isinstance(result, Address):
            obfuscate_address_actors(result)
            return
        if isinstance(result, NeighborAddress):
            obfuscate_address_actors(result.address)
            return
        if isinstance(result, NeighborAddresses):
            for neighbor in result.neighbors:
                obfuscate_address_actors(neighbor.address)
            return
        if isinstance(result, EntityAddresses):
            for address in result.addresses:
                obfuscate_address_actors(address)
            return
        if isinstance(result, SearchResult):
            obfuscate_search_result_actors(result)
            return
        if isinstance(result, AddressTags):
            tag_obfuscation_func(result.address_tags)
            return
        if isinstance(result, (NeighborEntities, NeighborClusters)):
            for neighbor in result.neighbors:
                entity = expanded_entity(neighbor.entity)
                if entity is None:
                    continue
                tag_obfuscation_func(entity.best_address_tag)
                obfuscate_entity_actor(entity)
            return
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
                entity = expanded_entity(result.neighbor.entity)
                if entity is not None:
                    tag_obfuscation_func(entity.best_address_tag)
                    obfuscate_entity_actor(entity)
            for address in result.matching_addresses:
                obfuscate_address_actors(address)
            if not isinstance(result, SearchResultLeaf) and result.paths:
                for path in result.paths:
                    cls.before_response(context, request, path)
            return
        if isinstance(result, list):
            for r in result:
                cls.before_response(context, request, r)
            return
