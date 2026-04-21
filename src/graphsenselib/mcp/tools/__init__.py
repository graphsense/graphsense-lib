from __future__ import annotations

import importlib
import logging
from contextlib import AsyncExitStack
from typing import Any

from graphsenselib.mcp.config import GSMCPConfig
from graphsenselib.mcp.curation import CurationFile, CurationError

logger = logging.getLogger(__name__)


def _resolve(module_spec: str) -> Any:
    """Resolve a 'package.module:callable' string to the callable object."""
    if ":" not in module_spec:
        raise CurationError(
            f"Module spec must be of the form 'package.module:callable': {module_spec!r}"
        )
    module_name, attr = module_spec.split(":", 1)
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise CurationError(
            f"Callable {attr!r} not found in module {module_name!r}"
        ) from exc


def register_custom_tools(
    mcp,
    app,
    curation: CurationFile,
    config: GSMCPConfig,
    stack: AsyncExitStack,
) -> None:
    """Register hand-written consolidated tools and (optionally) external tools.

    Every tool listed under curation.consolidated_tools is registered via its
    module:callable reference. External tools (e.g. search_neighbors) are
    registered only if both enabled in curation AND configured in
    GSMCPConfig.
    """
    for tool in curation.consolidated_tools:
        register_fn = _resolve(tool.module)
        register_fn(mcp, app, stack)
        logger.info("registered consolidated tool: %s", tool.name)

    for name, spec in curation.external_tools.items():
        if not spec.enabled:
            logger.info("external tool %s disabled via curation", name)
            continue
        if name == "search_neighbors":
            if config.search_neighbors is None:
                logger.warning(
                    "external tool 'search_neighbors' is enabled in curation but "
                    "GS_MCP_SEARCH_NEIGHBORS__* is not configured; skipping."
                )
                continue
            register_fn = _resolve(spec.module)
            register_fn(mcp, config.search_neighbors, stack)
            logger.info("registered external tool: %s", name)
        else:
            logger.warning("unknown external tool %r in curation; skipping", name)
