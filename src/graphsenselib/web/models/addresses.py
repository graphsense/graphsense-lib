"""Address-related API models."""

from typing import Any, Optional

from pydantic import Field, computed_field

from graphsenselib.web.models.base import APIModel, api_model_config
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.transactions import TX_SUMMARY_EXAMPLE, TxSummary
from graphsenselib.web.models.values import VALUES_EXAMPLE, Values

ADDRESS_EXAMPLE = {
    "currency": "btc",
    "address": "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
    "entity": 264711,
    "cluster": 264711,
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


class Address(APIModel):
    """Address model."""

    model_config = api_model_config(ADDRESS_EXAMPLE)

    currency: str
    address: str
    entity: int = Field(
        description="Deprecated alias of `cluster`. Use `cluster` instead; this "
        "field is retained for backwards compatibility and will be removed in a "
        "future release.",
        json_schema_extra={"deprecated": True},
    )
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
    status: Optional[str] = Field(
        default=None,
        description="Legacy field. Do not use — retained only for backwards "
        "compatibility and will be removed in a future release.",
        json_schema_extra={"deprecated": True},
    )
    # tags field used by test fixtures only, excluded from serialization and schema
    tags: Optional[list[Any]] = Field(default=None, exclude=True)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def cluster(self) -> int:
        """Address cluster ID (preferred alias for the deprecated `entity` field)."""
        return self.entity

    def to_dict(self, shallow: bool = False) -> dict[str, Any]:
        """Override to exclude test-only fields from serialization."""
        result = super().to_dict(shallow=shallow)
        result.pop("tags", None)
        if shallow and "cluster" not in result:
            result["cluster"] = self.entity
        return result


class NeighborAddress(APIModel):
    """Neighbor address model."""

    model_config = api_model_config(
        {
            "value": VALUES_EXAMPLE,
            "no_txs": 5,
            "address": ADDRESS_EXAMPLE,
            "labels": ["internet archive"],
        }
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
