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
    "fresh_cluster_id": 264800,
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
    fresh_cluster_id: Optional[int] = None
    balance: Values
    total_received: Values
    total_spent: Values
    # None when the address has no transactions of its own (first_tx_id == -1),
    # e.g. an account address that only ever paid a failed-tx gas fee or only
    # received a coinbase/miner reward.
    first_tx: Optional[TxSummary] = Field(
        default=None,
        description="First transaction in which this address appears, over its "
        "entire history — independent of any neighbor, direction, or date "
        "filter. Null if the address has no transactions of its own.",
    )
    last_tx: Optional[TxSummary] = Field(
        default=None,
        description="Last transaction in which this address appears, over its "
        "entire history — independent of any neighbor, direction, or date "
        "filter. Null if the address has no transactions of its own.",
    )
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

    value: Values = Field(
        description="Total value transferred on the edge between the queried "
        "address and this neighbor (edge-scoped, not the neighbor's lifetime "
        "total)."
    )
    no_txs: int = Field(
        description="Number of transactions on the edge between the queried "
        "address and this neighbor (edge-scoped, not the neighbor's lifetime "
        "transaction count)."
    )
    address: Address = Field(
        description="The neighbor address with its own address-level attributes. "
        "These are the neighbor's lifetime values (its own balance, degrees, "
        "first_tx/last_tx, …), NOT relative to the queried address or the edge "
        "between them."
    )
    labels: Optional[list[str]] = None
    token_values: Optional[dict[str, Values]] = Field(
        default=None,
        description="Per-token value transferred on this edge (edge-scoped).",
    )


class NeighborAddresses(APIModel):
    """Paginated list of neighbor addresses."""

    neighbors: list[NeighborAddress]
    next_page: Optional[str] = None
