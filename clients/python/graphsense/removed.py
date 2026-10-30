"""Pointers for client surfaces removed together with their REST routes.

Hand-written (listed in `.openapi-generator-ignore`); `compat/patch_compat.py`
imports it at the end of the generated `graphsense/__init__.py`. Using a
removed name raises an error that says what to use instead, rather than a bare
"cannot import name" / "has no attribute".

To retire a stub, delete its entry below. To add one when a later release
removes an API class, model or method, add it to the matching table.
"""

from __future__ import annotations

import sys
import types
from typing import Dict

_ENTITIES_REMOVED = (
    "was removed together with the /{currency}/entities/... routes "
    "(sunset 2026-10-31)"
)

# EntitiesApi method -> ClustersApi method. The id parameter is renamed too:
# `entity=` becomes `cluster=`.
ENTITIES_API_METHODS: Dict[str, str] = {
    "get_entity": "get_cluster",
    "list_entity_addresses": "list_cluster_addresses",
    "list_entity_neighbors": "list_cluster_neighbors",
    "list_entity_links": "list_cluster_links",
    "list_address_tags_by_entity": "list_address_tags_by_cluster",
    "list_entity_txs": "list_cluster_txs",
    "search_entity_neighbors": "search_cluster_neighbors",
}


def _entities_api_hint() -> str:
    lines = "\n".join(
        f"  EntitiesApi.{old}(...) -> ClustersApi.{new}(...)"
        for old, new in ENTITIES_API_METHODS.items()
    )
    return (
        f"EntitiesApi {_ENTITIES_REMOVED}. Use ClustersApi instead; it returns "
        f"the same data. Rename the id argument `entity=` to `cluster=`:\n{lines}"
    )


# Removed class/model name -> message. Looked up on `graphsense`,
# `graphsense.api` and `graphsense.models`.
REMOVED_NAMES: Dict[str, str] = {
    "EntitiesApi": _entities_api_hint(),
    "EntityAddresses": (
        f"EntityAddresses {_ENTITIES_REMOVED}. Use ClusterAddresses "
        "(returned by ClustersApi.list_cluster_addresses)."
    ),
    "NeighborEntities": (
        f"NeighborEntities {_ENTITIES_REMOVED}. Use NeighborClusters "
        "(returned by ClustersApi.list_cluster_neighbors)."
    ),
}

# Removed module path -> the removed name it used to provide.
REMOVED_MODULES: Dict[str, str] = {
    "graphsense.api.entities_api": "EntitiesApi",
    "graphsense.models.entity_addresses": "EntityAddresses",
    "graphsense.models.neighbor_entities": "NeighborEntities",
    "graphsense.model.entity_addresses": "EntityAddresses",
    "graphsense.model.neighbor_entities": "NeighborEntities",
}

# (API class name, removed method) -> replacement method on the same class.
# The `_with_http_info` / `_without_preload_content` variants are covered too.
REMOVED_METHODS: Dict[tuple, str] = {
    ("AddressesApi", "get_address_entity"): "get_address_cluster",
}
_METHOD_SUFFIXES = ("", "_with_http_info", "_without_preload_content")


class RemovedError(ImportError):
    """A removed client name was used. The message says what replaces it.

    An ImportError because Python keeps its message on
    `from graphsense import EntitiesApi`; an AttributeError would be replaced
    by a generic "cannot import name".
    """


def _module_getattr(module_name: str):
    def __getattr__(name: str):
        if name in REMOVED_NAMES:
            raise RemovedError(REMOVED_NAMES[name])
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")

    return __getattr__


class _RemovedModule(types.ModuleType):
    """Stand-in for a removed module: any name imported from it raises."""

    def __init__(self, name: str, removed_name: str):
        super().__init__(name)
        self._removed_name = removed_name

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        raise RemovedError(REMOVED_NAMES[self._removed_name])


def _api_class_getattr(cls_name: str):
    def __getattr__(self, name: str):
        for suffix in _METHOD_SUFFIXES:
            if not name.endswith(suffix):
                continue
            base = name[: -len(suffix)] if suffix else name
            new = REMOVED_METHODS.get((cls_name, base))
            if new is not None:
                raise AttributeError(
                    f"{cls_name}.{name} {_ENTITIES_REMOVED}. "
                    f"Use {cls_name}.{new}{suffix}(...) instead; it takes the "
                    "same arguments and returns the same data."
                )
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    return __getattr__


def install() -> None:
    for module_name in ("graphsense", "graphsense.api", "graphsense.models"):
        module = sys.modules.get(module_name)
        if module is not None and "__getattr__" not in vars(module):
            module.__getattr__ = _module_getattr(module_name)

    for module_name, removed_name in REMOVED_MODULES.items():
        sys.modules.setdefault(module_name, _RemovedModule(module_name, removed_name))

    import graphsense.api

    for cls_name in {cls for cls, _ in REMOVED_METHODS}:
        cls = getattr(graphsense.api, cls_name, None)
        if cls is not None and "__getattr__" not in vars(cls):
            cls.__getattr__ = _api_class_getattr(cls_name)


install()
