"""Entity-related API models.

Note: "entity" terminology is deprecated in favor of "cluster". The classes here
remain named `Entity*` for backwards compatibility and are re-exported from
`graphsenselib.web.models` under both `Entity*` and `Cluster*` names. The
`entity` field on responses is dual-emitted alongside a new `cluster` field.
"""

from typing import Any, Optional, Union

from pydantic import Field, computed_field

from graphsenselib.web.models.addresses import Address
from graphsenselib.web.models.base import APIModel, api_model_config
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.tags import AddressTag
from graphsenselib.web.models.transactions import TX_SUMMARY_EXAMPLE, TxSummary
from graphsenselib.web.models.values import VALUES_EXAMPLE, Values

ENTITY_EXAMPLE = {
    "currency": "btc",
    "entity": 264711,
    "cluster": 264711,
    "root_address": "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
    "balance": VALUES_EXAMPLE,
    "total_received": VALUES_EXAMPLE,
    "total_spent": VALUES_EXAMPLE,
    "first_tx": TX_SUMMARY_EXAMPLE,
    "last_tx": TX_SUMMARY_EXAMPLE,
    "in_degree": 100,
    "out_degree": 50,
    "no_addresses": 25,
    "no_incoming_txs": 200,
    "no_outgoing_txs": 100,
    "no_address_tags": 3,
}


class Entity(APIModel):
    """Cluster model (legacy name: Entity).

    The `entity` field is a deprecated alias of `cluster` kept for backwards
    compatibility. New consumers should read `cluster`.
    """

    model_config = api_model_config(ENTITY_EXAMPLE)

    currency: str
    entity: int = Field(
        description="Deprecated alias of `cluster`. Use `cluster` instead; this "
        "field is retained for backwards compatibility and will be removed in a "
        "future release.",
        json_schema_extra={"deprecated": True},
    )
    root_address: str
    balance: Values
    total_received: Values
    total_spent: Values
    first_tx: TxSummary
    last_tx: TxSummary
    in_degree: int
    out_degree: int
    no_addresses: int
    no_incoming_txs: int
    no_outgoing_txs: int
    no_address_tags: int
    token_balances: Optional[dict[str, Values]] = None
    total_tokens_received: Optional[dict[str, Values]] = None
    total_tokens_spent: Optional[dict[str, Values]] = None
    actors: Optional[list[LabeledItemRef]] = None
    best_address_tag: Optional[AddressTag] = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def cluster(self) -> int:
        """Cluster ID (preferred alias for the deprecated `entity` field)."""
        return self.entity

    def to_dict(self, shallow: bool = False) -> dict[str, Any]:
        result = super().to_dict(shallow=shallow)
        if shallow and "cluster" not in result:
            result["cluster"] = self.entity
        return result


class NeighborEntity(APIModel):
    """Neighbor cluster model (legacy name: NeighborEntity).

    Note: unlike the top-level `Entity`/`Cluster` models, this class does NOT
    dual-emit a `cluster` key at the neighbor level. The nested `entity` value
    is either an integer ID or a full `Entity` object (which itself already
    exposes both `entity` and `cluster` keys), so adding a sibling `cluster`
    key here would duplicate either an int or an entire object for no gain
    and introduce an OpenAPI schema name collision with the top-level
    `Cluster` type.
    """

    model_config = api_model_config(
        {
            "value": VALUES_EXAMPLE,
            "no_txs": 5,
            "entity": ENTITY_EXAMPLE,
            "labels": ["internet archive"],
        }
    )

    value: Values
    no_txs: int
    entity: Optional[Union[Entity, int]] = Field(
        default=None,
        description="Legacy field name. When this carries a full `Entity`/`Cluster` "
        "object, prefer reading the `cluster` field on that nested object. The "
        "field name `entity` at the neighbor level is retained for backwards "
        "compatibility.",
        json_schema_extra={"deprecated": True},
    )
    labels: Optional[list[str]] = None
    token_values: Optional[dict[str, Values]] = None


class NeighborEntities(APIModel):
    """Paginated list of neighbor clusters (legacy name: NeighborEntities)."""

    neighbors: list[NeighborEntity]
    next_page: Optional[str] = None


class EntityAddresses(APIModel):
    """Paginated list of addresses in a cluster (legacy name: EntityAddresses)."""

    addresses: list[Address]
    next_page: Optional[str] = None


# Canonical cluster-named subclasses. These exist so the new `/clusters/...`
# endpoints advertise a distinct `Cluster*` schema in the OpenAPI spec (and
# therefore in generated clients) rather than reusing the legacy `Entity*`
# schema names. Pydantic v2 key-schemas by class name, so an empty subclass is
# enough to produce a separate schema while inheriting all field definitions,
# validators, and the computed `cluster` field.


class Cluster(Entity):
    """Address cluster (canonical name, supersedes `Entity`)."""


class NeighborCluster(NeighborEntity):
    """Neighbor cluster (canonical name, supersedes `NeighborEntity`)."""


class NeighborClusters(APIModel):
    """Paginated list of neighbor clusters (canonical name)."""

    neighbors: list[NeighborCluster]
    next_page: Optional[str] = None


class ClusterAddresses(EntityAddresses):
    """Paginated list of addresses in a cluster (canonical name)."""
