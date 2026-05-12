from types import SimpleNamespace
from typing import Optional

from fastmcp.server.providers.openapi import MCPType

from graphsenselib.mcp import curation as curation_mod
from graphsenselib.mcp.routes import make_component_fn, make_route_map_fn


def _fake_route(
    op_id: Optional[str], method: str = "GET", path: str = "/x"
) -> SimpleNamespace:
    return SimpleNamespace(operation_id=op_id, method=method, path=path, tags=set())


def _fake_component():
    return SimpleNamespace(description=None, tags=set())


def test_route_map_fn_keeps_included(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    fn = make_route_map_fn(c)
    assert fn(_fake_route("get_statistics"), MCPType.TOOL) is MCPType.TOOL


def test_route_map_fn_excludes_replaced(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    fn = make_route_map_fn(c)
    assert fn(_fake_route("get_address"), MCPType.TOOL) is MCPType.EXCLUDE
    assert fn(_fake_route("get_address_entity"), MCPType.TOOL) is MCPType.EXCLUDE


def test_route_map_fn_excludes_unknown(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    fn = make_route_map_fn(c)
    assert fn(_fake_route("some_random_op"), MCPType.TOOL) is MCPType.EXCLUDE


def test_route_map_fn_excludes_routes_without_operation_id(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    fn = make_route_map_fn(c)
    assert fn(_fake_route(None), MCPType.TOOL) is MCPType.EXCLUDE


def test_component_fn_applies_description_override(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    fn = make_component_fn(c)
    comp = _fake_component()
    fn(_fake_route("get_statistics", method="GET", path="/stats"), comp)
    assert "snapshot" in comp.description
    assert "API: GET /stats" in comp.description
    assert "gs_overview" in comp.tags


def test_component_fn_skips_unlisted(sample_curation_file):
    c = curation_mod.load(sample_curation_file)
    fn = make_component_fn(c)
    comp = _fake_component()
    fn(_fake_route("not_included"), comp)
    assert comp.description is None  # left alone
    assert comp.tags == set()
