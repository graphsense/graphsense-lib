"""Entity-related API models."""

from typing import Optional, Union

from graphsenselib.web.models.addresses import Address
from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.tags import AddressTag
from graphsenselib.web.models.transactions import TxSummary
from graphsenselib.web.models.values import Values


class Entity(APIModel):
    """Entity model."""

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
