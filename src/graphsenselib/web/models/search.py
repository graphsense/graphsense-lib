"""Search-related API models."""

from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict

from graphsenselib.web.models.addresses import Address
from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.entities import NeighborEntity


class SearchResultByCurrency(APIModel):
    """Search result by currency."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "currency": "btc",
                "addresses": [
                    "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
                    "1ArchiveisY6i4Hpostivemate1sVRhQ71",
                ],
                "txs": [],
            }
        },
    )

    currency: str
    addresses: list[str]
    txs: list[str]


class SearchResult(APIModel):
    """Search result model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={
            "example": {
                "currencies": [
                    {
                        "currency": "btc",
                        "addresses": [
                            "1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
                            "1ArchiveisY6i4Hpostivemate1sVRhQ71",
                        ],
                        "txs": [],
                    }
                ],
                "labels": [],
            }
        },
    )

    currencies: list[SearchResultByCurrency]
    labels: list[str]
    actors: Optional[list[LabeledItemRef]] = None


class SearchResultLeaf(APIModel):
    """Search result leaf node (no further paths)."""

    neighbor: NeighborEntity
    matching_addresses: list[Address]


class SearchResultLevel6(SearchResultLeaf):
    """Search result at depth 6 (leaf)."""

    pass


class SearchResultLevel5(APIModel):
    """Search result at depth 5."""

    neighbor: NeighborEntity
    matching_addresses: list[Address]
    paths: list[SearchResultLevel6]


class SearchResultLevel4(APIModel):
    """Search result at depth 4."""

    neighbor: NeighborEntity
    matching_addresses: list[Address]
    paths: list[SearchResultLevel5]


class SearchResultLevel3(APIModel):
    """Search result at depth 3."""

    neighbor: NeighborEntity
    matching_addresses: list[Address]
    paths: list[SearchResultLevel4]


class SearchResultLevel2(APIModel):
    """Search result at depth 2."""

    neighbor: NeighborEntity
    matching_addresses: list[Address]
    paths: list[SearchResultLevel3]


class SearchResultLevel1(APIModel):
    """Search result at depth 1."""

    neighbor: NeighborEntity
    matching_addresses: list[Address]
    paths: list[SearchResultLevel2]
