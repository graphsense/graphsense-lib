from __future__ import annotations

from fastmcp.server.providers.openapi import MCPType
from fastmcp.server.providers.openapi.routing import HTTPRoute

from graphsenselib.mcp.curation import CurationFile


def make_route_map_fn(curation: CurationFile):
    """Return a route_map_fn for FastMCP.from_fastapi that keeps only curated ops.

    Every op_id listed in curation.include stays as a TOOL. Every op_id listed
    as a consolidated tool's 'replaces' entry is explicitly excluded (the
    hand-written wrapper supersedes it). Everything else is excluded.
    """
    included = curation.included_op_ids()
    replaced = curation.replaced_op_ids()

    def route_map_fn(route: HTTPRoute, mcp_type: MCPType) -> MCPType | None:
        op_id = route.operation_id
        if not op_id:
            return MCPType.EXCLUDE
        if op_id in included:
            return MCPType.TOOL
        if op_id in replaced:
            return MCPType.EXCLUDE
        return MCPType.EXCLUDE

    return route_map_fn


def make_component_fn(curation: CurationFile):
    """Return an mcp_component_fn that applies description + tag overrides
    from the curation YAML.
    """
    include = curation.include
    tag_prefix = curation.defaults.tag_prefix

    def component_fn(route: HTTPRoute, component) -> None:
        op_id = route.operation_id
        if not op_id or op_id not in include:
            return
        entry = include[op_id]
        endpoint_info = f"{route.method} {route.path}"
        if entry.description:
            component.description = (
                f"{entry.description.rstrip()}\n\nAPI: {endpoint_info}"
            )
        elif component.description:
            component.description = f"{component.description}\n\nAPI: {endpoint_info}"
        else:
            component.description = f"API: {endpoint_info}"
        for tag in entry.tags:
            component.tags.add(f"{tag_prefix}{tag}")

    return component_fn
