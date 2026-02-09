"""Entity-related API models."""

from typing import Optional, Union

from graphsenselib.web.models.addresses import Address
from graphsenselib.web.models.base import APIModel, api_model_config
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.tags import AddressTag
from graphsenselib.web.models.transactions import TX_SUMMARY_EXAMPLE, TxSummary
from graphsenselib.web.models.values import VALUES_EXAMPLE, Values

ENTITY_EXAMPLE = {
    "currency": "btc",
    "entity": 264711,
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
    """Entity model."""

    model_config = api_model_config(ENTITY_EXAMPLE)

    currency: str
    entity: int
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


class NeighborEntity(APIModel):
    """Neighbor entity model."""

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
    entity: Optional[Union[Entity, int]] = None
    labels: Optional[list[str]] = None
    token_values: Optional[dict[str, Values]] = None


class NeighborEntities(APIModel):
    """Paginated list of neighbor entities."""

    neighbors: list[NeighborEntity]
    next_page: Optional[str] = None


class EntityAddresses(APIModel):
    """Paginated list of addresses in an entity."""

    addresses: list[Address]
    next_page: Optional[str] = None
