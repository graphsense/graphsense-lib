"""Base utilities for FastAPI routes"""

import logging
import re
from datetime import datetime
from functools import wraps
from typing import Annotated, Any, Optional

from fastapi import Depends, Header, Request

from graphsenselib.web.config import GSRestConfig
from graphsenselib.web.dependencies import ServiceContainer
from graphsenselib.web.service import ServiceContext

logger = logging.getLogger(__name__)


def make_ctx(
    request: Request,
    services: ServiceContainer,
    tagstore_groups: list[str],
    **kwargs,
) -> ServiceContext:
    return ServiceContext(
        services=services,
        tagstore_groups=tagstore_groups,
        config=request.app.state.config,
        **kwargs,
    )


def apply_plugin_hooks(request: Request, result):
    """Apply plugin response hooks to a result.

    This function iterates through registered plugins and calls their
    before_response hooks, allowing plugins to modify the response.
    """
    plugins = getattr(request.app.state, "plugins", [])
    plugin_contexts = getattr(request.app.state, "plugin_contexts", {})
    for plugin in plugins:
        if hasattr(plugin, "before_response"):
            ctx = plugin_contexts.get(plugin.__module__, {})
            plugin.before_response(ctx, request, result)


def get_config(request: Request) -> GSRestConfig:
    """Get application config"""
    return request.app.state.config


def get_services(request: Request) -> ServiceContainer:
    """Get service container"""
    return request.app.state.services


def get_username(
    x_consumer_username: Annotated[Optional[str], Header()] = None,
) -> Optional[str]:
    """Extract username from header"""
    return x_consumer_username


def get_show_private_tags(
    request: Request,
) -> bool:
    """Determine if private tags should be shown based on config and headers"""
    config = request.app.state.config
    show_private_tags_conf = config.show_private_tags or False

    if not show_private_tags_conf:
        return False

    # Get header modifications from plugin middleware (if any)
    header_mods = getattr(request.state, "header_modifications", {})

    show_private_tags = True
    for k, v in show_private_tags_conf.get("on_header", {}).items():
        # Check both actual headers and plugin-set header modifications
        hval = header_mods.get(k) or request.headers.get(k, None)
        if not hval:
            return False
        show_private_tags = show_private_tags and bool(re.match(re.compile(v), hval))

    # Store in request state for other dependencies
    request.state.show_private_tags = show_private_tags
    return show_private_tags


def get_tagstore_access_groups(
    request: Request,
    show_private: bool = Depends(get_show_private_tags),
) -> list[str]:
    """Get tagstore access groups based on request"""
    config = request.app.state.config
    groups = ["public"]
    if show_private:
        groups.append("private")
    groups.append(config.user_tag_reporting_acl_group)
    return groups


def should_obfuscate_private_tags(request: Request) -> bool:
    """Check if private tags should be obfuscated"""
    from graphsenselib.web.builtin.plugins.obfuscate_tags.obfuscate_tags import (
        GROUPS_HEADER_NAME,
        OBFUSCATION_MARKER_GROUP,
    )

    # Check header modifications from middleware
    header_mods = getattr(request.state, "header_modifications", {})
    if header_mods.get(GROUPS_HEADER_NAME) == OBFUSCATION_MARKER_GROUP:
        return True

    return request.headers.get(GROUPS_HEADER_NAME, "") == OBFUSCATION_MARKER_GROUP


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse datetime string to datetime object"""
    if dt_str is None:
        return None
    from dateutil import parser

    return parser.parse(dt_str)


def with_plugin_response_hooks(func):
    """Decorator to apply plugin before_response hooks to route handlers.

    This decorator must wrap async route handlers that need plugin response processing.
    The route handler must accept a 'request: Request' parameter.
    """

    @wraps(func)
    async def wrapper(*args, request: Request, **kwargs):
        result = await func(*args, request=request, **kwargs)

        plugins = getattr(request.app.state, "plugins", [])
        plugin_contexts = getattr(request.app.state, "plugin_contexts", {})

        for plugin in plugins:
            if hasattr(plugin, "before_response"):
                ctx = plugin_contexts.get(plugin.__module__, {})
                plugin.before_response(ctx, request, result)

        return result

    return wrapper


def to_json_response(result: Any) -> dict:
    """Convert API model result to JSON-serializable dict.

    Handles both old OpenAPI models (with to_dict()) and new Pydantic models
    (with model_dump()).
    """
    if result is None:
        return {}
    elif isinstance(result, list):
        return [_model_to_dict(d) for d in result]
    else:
        return _model_to_dict(result)


def _model_to_dict(obj: Any) -> Any:
    """Convert a single model to dict."""
    # Prefer to_dict() for compatibility with both old and new models
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    # Fallback for other Pydantic models
    elif hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    return obj


def parse_comma_separated_ints(value: Optional[str]) -> Optional[list[int]]:
    """Parse comma-separated string of integers into a list of integers.

    Used for query params like only_ids that accept CSV format.
    """
    if value is None:
        return None
    if value.strip() == "":
        return None
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def parse_comma_separated_strings(value: Optional[str]) -> Optional[list[str]]:
    """Parse comma-separated string into a list of strings.

    Used for query params like only_ids that accept CSV format.
    """
    if value is None:
        return None
    if value.strip() == "":
        return None
    return [x.strip() for x in value.split(",") if x.strip()]


def normalize_page(page: Optional[str]) -> Optional[str]:
    """Convert empty string to None for pagination parameter.

    FastAPI doesn't distinguish between missing params and empty strings,
    so this normalizes empty strings to None for consistent pagination handling.
    """
    if page is not None and page.strip() == "":
        return None
    return page
