"""`entity` is no longer required in the spec, though the server still sends it.

Clients (the dashboard's decoders, generated clients) must be able to drop the
deprecated `entity` alias before it is removed, so the OpenAPI schema stops
promising it while `cluster` stays required. At runtime the models still
require `entity`, because `cluster` is computed from it.
"""

import pytest
from pydantic import ValidationError

from graphsenselib.web.models import (
    Address,
    AddressTag,
    Cluster,
    Entity,
    NeighborCluster,
)

MODELS = [Address, Cluster, Entity, NeighborCluster, AddressTag]


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_entity_not_required_cluster_required_in_response_schema(model):
    required = model.model_json_schema(mode="serialization").get("required", [])
    assert "entity" not in required
    assert "cluster" in required


@pytest.mark.parametrize("model", [Address, Cluster, Entity], ids=lambda m: m.__name__)
def test_schema_keeps_example_and_other_required_fields(model):
    schema = model.model_json_schema(mode="serialization")
    assert "example" in schema
    assert "currency" in schema["required"]


def test_runtime_still_requires_entity():
    with pytest.raises(ValidationError, match="entity"):
        Address.model_validate({"currency": "btc", "address": "x"})
