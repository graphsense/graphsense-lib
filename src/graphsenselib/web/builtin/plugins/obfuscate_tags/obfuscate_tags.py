from __future__ import annotations

import logging
import re
from functools import partial
from typing import Any

from fastapi import Request
from graphsenselib.tagstore.algorithms.obfuscate import (
    obfuscate_entity_actor,
    obfuscate_tag,
    obfuscate_tag_if_not_public,
)

from graphsenselib.web.models import (
    AddressTags,
    Cluster,
    Entity,
    NeighborClusters,
    NeighborEntities,
    SearchResultLeaf,
    SearchResultLevel1,
    SearchResultLevel2,
    SearchResultLevel3,
    SearchResultLevel4,
    SearchResultLevel5,
    SearchResultLevel6,
    TagSummary,
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


def suppress_tags_by_uri_rule(rule, tags, label_rule=None):
    """Blind leak/investigation tagpacks selected by ``rule`` (matched on
    tagpack_uri). Runs unconditionally in before_response — including for
    tags-private callers who bypass the not-public obfuscation — so the
    selected packs stay blinded for every consumer, while non-matching packs
    (legit attribution) pass through untouched.

    Two modes, toggled by ``label_rule``:

    - Strict (``label_rule is None``): fully blank every matching tag
      (label/source/actor/uri), so the pack is entirely invisible.
    - Lenient (``label_rule`` given): only fully blank matching tags whose
      *label* also matches ``label_rule`` — the investigation-revealing labels
      (DOJ case numbers, "perpetrator address", court pseudonyms). Benign
      labels from the same pack (e.g. "Binance deposit address") survive with
      just the uri redacted, exactly like ``obfuscate_tagpack_uri_by_rule``.
    """
    if not tags:
        return
    if isinstance(tags, list):
        for tag in tags:
            suppress_tags_by_uri_rule(rule, tag, label_rule)
        return
    if tags.tagpack_uri is None:
        return
    if not re.compile(rule).match(tags.tagpack_uri):
        return
    if label_rule is None or (tags.label and re.compile(label_rule).search(tags.label)):
        # ponytail: blanks in place, leaving a hollow tag; dropping it and
        # recomputing best_address_tag would be more correct but touches
        # ranking/list handling — revisit if an empty best_label bites.
        obfuscate_tag(tags)
        # obfuscate_tag() does NOT clear tagpack_title, and for the court-filing
        # packs the TITLE is the answer key: "Court filing: DOJ case
        # 1:20-cv-02228 (DCD)". Blanking label/uri/source while still serving
        # that is no blinding at all - models quoted it verbatim and built
        # their terminal on it. Bench path only; prod obfuscate_tag untouched.
        if getattr(tags, "tagpack_title", None):
            tags.tagpack_title = ""
    else:
        # lenient + benign label: keep attribution, hide the pack source repo.
        tags.tagpack_uri = ""
        # the title names the pack, and for court packs that names the case
        if getattr(tags, "tagpack_title", None) and re.compile(
            label_rule, re.IGNORECASE
        ).search(tags.tagpack_title):
            tags.tagpack_title = ""


def suppress_tag_summary(label_rule, summary):
    """Apply the same blinding to an aggregated TagSummary.

    TagSummary carries no tagpack_uri - it is already collapsed per label - so
    the uri rule cannot be applied here and we filter on the label rule instead.
    Without this, lookup_address leaks through tag_summary.best_label exactly
    what list_tags_by_address blanks (e.g. "DOJ case 2:22-mj-00161 (WIED)").

    Strict (label_rule is None): we cannot tell which labels came from the
    suppressed packs, so blank the whole summary - conservative by design.
    """
    if summary is None:
        return
    if label_rule is None:
        summary.best_label = None
        summary.best_actor = None
        summary.label_summary = {}
        summary.concept_tag_cloud = {}
        return
    rx = re.compile(label_rule)
    kept = {
        k: v
        for k, v in (summary.label_summary or {}).items()
        if not rx.search(getattr(v, "label", None) or k)
    }
    if len(kept) != len(summary.label_summary or {}):
        summary.label_summary = kept
        if summary.best_label and rx.search(summary.best_label):
            best = max(
                kept.values(), key=lambda v: getattr(v, "relevance", 0), default=None
            )
            summary.best_label = getattr(best, "label", None) if best else None
            if best is None:
                summary.best_actor = None


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

        suppress_tagpack_uri_rule = (context.get("config") or {}).get(
            "suppress_tagpack_uri_rule", None
        )
        # Toggle: absent => strict (drop whole matching tag); present =>
        # lenient (drop only investigation-revealing labels, keep benign ones).
        suppress_label_rule = (context.get("config") or {}).get(
            "suppress_label_rule", None
        )

        if suppress_tagpack_uri_rule is not None:
            cls.obfuscate_tags_in_objects(
                context,
                request,
                result,
                partial(
                    suppress_tags_by_uri_rule,
                    suppress_tagpack_uri_rule,
                    label_rule=suppress_label_rule,
                ),
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
        if isinstance(result, AddressTags):
            tag_obfuscation_func(result.address_tags)
            return
        if isinstance(result, TagSummary):
            # only the suppression pass carries a label_rule keyword
            kw = getattr(tag_obfuscation_func, "keywords", None) or {}
            if "label_rule" in kw:
                suppress_tag_summary(kw["label_rule"], result)
            return
        if isinstance(result, (NeighborEntities, NeighborClusters)):
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
