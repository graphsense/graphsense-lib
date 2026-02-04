"""Address-related API models."""

from typing import Any, Optional

from pydantic import Field

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.transactions import TxSummary
from graphsenselib.web.models.values import Values


class Address(APIModel):
    """Address model."""

    currency: str
    address: str
    entity: int
    balance: Values
    total_received: Values
    total_spent: Values
    first_tx: TxSummary
    last_tx: TxSummary
    in_degree: int
    out_degree: int
    no_incoming_txs: int
    no_outgoing_txs: int
    token_balances: Optional[dict[str, Values]] = None
    total_tokens_received: Optional[dict[str, Values]] = None
    total_tokens_spent: Optional[dict[str, Values]] = None
    actors: Optional[list[LabeledItemRef]] = None
    is_contract: Optional[bool] = None
    status: Optional[str] = None
    # tags field used by test fixtures only, excluded from serialization and schema
    tags: Optional[list[Any]] = Field(default=None, exclude=True)

    def to_dict(self, shallow: bool = False) -> dict[str, Any]:
        """Override to exclude test-only fields from serialization."""
        result = super().to_dict(shallow=shallow)
        result.pop("tags", None)
        return result


class NeighborAddress(APIModel):
    """Neighbor address model."""

    value: Values
    no_txs: int
    address: Address
    labels: Optional[list[str]] = None
    token_values: Optional[dict[str, Values]] = None


class NeighborAddresses(APIModel):
    """Paginated list of neighbor addresses."""

    neighbors: list[NeighborAddress]
    next_page: Optional[str] = None
