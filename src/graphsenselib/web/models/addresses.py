"""Address-related API models."""

from typing import Any, Optional

from pydantic import ConfigDict, Field

from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.transactions import TX_SUMMARY_EXAMPLE, TxSummary
from graphsenselib.web.models.values import VALUES_EXAMPLE, Values


class Address(APIModel):
    """Address model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "currency": "btc",
                "address": "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
                "entity": 264711,
                "balance": VALUES_EXAMPLE,
                "total_received": VALUES_EXAMPLE,
                "total_spent": VALUES_EXAMPLE,
                "first_tx": TX_SUMMARY_EXAMPLE,
                "last_tx": TX_SUMMARY_EXAMPLE,
                "in_degree": 100,
                "out_degree": 50,
                "no_incoming_txs": 200,
                "no_outgoing_txs": 100,
                "status": "clean",
            }
        },
    )

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

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "value": VALUES_EXAMPLE,
                "no_txs": 5,
                "address": {
                    "currency": "btc",
                    "address": "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
                    "entity": 264711,
                    "balance": VALUES_EXAMPLE,
                    "total_received": VALUES_EXAMPLE,
                    "total_spent": VALUES_EXAMPLE,
                    "first_tx": TX_SUMMARY_EXAMPLE,
                    "last_tx": TX_SUMMARY_EXAMPLE,
                    "in_degree": 100,
                    "out_degree": 50,
                    "no_incoming_txs": 200,
                    "no_outgoing_txs": 100,
                },
                "labels": ["internet archive"],
            }
        },
    )

    value: Values
    no_txs: int
    address: Address
    labels: Optional[list[str]] = None
    token_values: Optional[dict[str, Values]] = None


class NeighborAddresses(APIModel):
    """Paginated list of neighbor addresses."""

    neighbors: list[NeighborAddress]
    next_page: Optional[str] = None
