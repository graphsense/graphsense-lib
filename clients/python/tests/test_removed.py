"""Removed client names fail with a pointer to their replacement."""

from __future__ import annotations

import importlib

import pytest

import graphsense
from graphsense.api_client import ApiClient
from graphsense.ext.client import GraphSense
from graphsense.removed import ENTITIES_API_METHODS, RemovedError


def test_from_import_removed_api_names_replacement():
    with pytest.raises(ImportError) as exc:
        from graphsense import EntitiesApi  # noqa: F401
    message = str(exc.value)
    assert "ClustersApi" in message
    assert "`entity=` to `cluster=`" in message
    for old, new in ENTITIES_API_METHODS.items():
        assert f"EntitiesApi.{old}(...) -> ClustersApi.{new}(...)" in message


@pytest.mark.parametrize(
    "statement, replacement",
    [
        ("from graphsense.api import EntitiesApi", "ClustersApi"),
        ("from graphsense.models import EntityAddresses", "ClusterAddresses"),
        ("from graphsense import NeighborEntities", "NeighborClusters"),
        ("from graphsense.api.entities_api import EntitiesApi", "ClustersApi"),
        (
            "from graphsense.models.neighbor_entities import NeighborEntities",
            "NeighborClusters",
        ),
        (
            "from graphsense.model.entity_addresses import EntityAddresses",
            "ClusterAddresses",
        ),
    ],
)
def test_removed_names_and_modules(statement, replacement):
    with pytest.raises(RemovedError, match=replacement):
        exec(statement, {})


def test_unknown_names_still_plain_errors():
    with pytest.raises(AttributeError):
        graphsense.NoSuchThing  # noqa: B018
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("graphsense.api.no_such_api")


@pytest.mark.parametrize(
    "method", ["get_address_entity", "get_address_entity_with_http_info"]
)
def test_removed_method_points_to_replacement(method):
    api = graphsense.AddressesApi(ApiClient())
    suffix = method[len("get_address_entity") :]
    with pytest.raises(AttributeError, match=f"get_address_cluster{suffix}"):
        getattr(api, method)
    assert not hasattr(api, method)
    assert method not in dir(api)
    # Present methods are untouched.
    assert callable(api.get_address_cluster)


def test_raw_namespace_points_to_clusters(api_client: ApiClient):
    gs = GraphSense(api_client=api_client)
    with pytest.raises(AttributeError, match="ClustersApi"):
        gs.raw.entities  # noqa: B018
    with pytest.raises(AttributeError):
        gs.raw.no_such_group  # noqa: B018
