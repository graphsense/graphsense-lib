"""Guard the API-doc clarifications for issue #58.

The neighbor endpoints were ambiguous about which fields are edge-scoped (the
relationship between the queried address/entity and a neighbor) versus the
neighbor's own lifetime attributes — most notably the embedded address/entity's
`first_tx`/`last_tx`. This test pins descriptions on exactly that curated set of
historically-ambiguous fields, so the clarification can't silently regress. It
is deliberately NOT a "every field must be documented" gate.
"""

import pytest

from graphsenselib.web.models.addresses import Address, NeighborAddress
from graphsenselib.web.models.entities import (
    Cluster,
    Entity,
    NeighborCluster,
    NeighborEntity,
)

# model -> fields that must carry a non-empty OpenAPI description.
CURATED_DOCUMENTED_FIELDS = {
    Address: ["first_tx", "last_tx"],
    Entity: ["first_tx", "last_tx"],
    Cluster: ["first_tx", "last_tx"],
    NeighborAddress: ["value", "no_txs", "address"],
    NeighborEntity: ["value", "no_txs"],
    NeighborCluster: ["value", "no_txs"],
}


@pytest.mark.parametrize(
    "model,field",
    [(m, f) for m, fields in CURATED_DOCUMENTED_FIELDS.items() for f in fields],
)
def test_ambiguous_field_has_description(model, field):
    props = model.model_json_schema()["properties"]
    assert field in props, f"{model.__name__}.{field} missing from schema"
    description = props[field].get("description")
    assert description, f"{model.__name__}.{field} must carry an API description"


def test_neighbor_edge_scope_is_spelled_out():
    # The specific confusion in #58: edge-scoped vs lifetime. Make sure the
    # wording actually distinguishes them rather than being a generic blurb.
    na = NeighborAddress.model_json_schema()["properties"]
    assert "edge" in na["value"]["description"].lower()
    assert "edge" in na["no_txs"]["description"].lower()
    assert "lifetime" in na["address"]["description"].lower()

    addr = Address.model_json_schema()["properties"]
    assert "entire history" in addr["first_tx"]["description"].lower()
