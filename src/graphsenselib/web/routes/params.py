"""Reusable Annotated parameter types for route definitions."""

from typing import Annotated, Literal, Optional

from fastapi import Path, Query

# Path parameters

CurrencyPath = Annotated[
    str,
    Path(description="The cryptocurrency code (e.g., btc)", examples=["btc"]),
]

AddressPath = Annotated[
    str,
    Path(
        description="The cryptocurrency address",
        examples=["1Archive1n2C579dMsAu3iC6tWzuQJz8dN"],
    ),
]

EntityPath = Annotated[
    int,
    Path(description="The entity ID", examples=[67065]),
]

TxHashPath = Annotated[
    str,
    Path(
        description="The transaction hash",
        examples=["04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd"],
    ),
]

HeightPath = Annotated[
    int,
    Path(description="The block height", examples=[1]),
]

# Query parameters

PageQuery = Annotated[
    Optional[str],
    Query(description="Resumption token for retrieving the next page"),
]

PagesizeQuery = Annotated[
    Optional[int],
    Query(
        ge=1,
        description="Number of items returned in a single page",
        examples=[10],
    ),
]

MinHeightQuery = Annotated[
    Optional[int],
    Query(description="Return transactions starting from given height", examples=[1]),
]

MaxHeightQuery = Annotated[
    Optional[int],
    Query(
        description="Return transactions up to (including) given height",
        examples=[2],
    ),
]

MinDateQuery = Annotated[
    Optional[str],
    Query(description="Min date of txs", examples=["2017-07-21T17:32:28Z"]),
]

MaxDateQuery = Annotated[
    Optional[str],
    Query(description="Max date of txs", examples=["2017-07-21T17:32:28Z"]),
]

OrderQuery = Annotated[
    Optional[str],
    Query(description="Sorting order", examples=["desc"]),
]

TokenCurrencyQuery = Annotated[
    Optional[str],
    Query(
        description="Return transactions of given token or base currency",
        examples=["WETH"],
    ),
]

DirectionQuery = Annotated[
    Literal["in", "out"],
    Query(description="Incoming or outgoing neighbors", examples=["out"]),
]

OptionalDirectionQuery = Annotated[
    Optional[str],
    Query(description="Incoming or outgoing transactions", examples=["out"]),
]

IncludeActorsQuery = Annotated[
    bool,
    Query(description="Whether to include actor information", examples=[True]),
]

IncludeLabelsQuery = Annotated[
    Optional[bool],
    Query(
        description="Whether to include labels of first page of address tags",
        examples=[True],
    ),
]

IncludeBestClusterTagQuery = Annotated[
    Optional[bool],
    Query(
        description="If the best cluster tag should be inherited to the address level",
    ),
]

ExcludeBestAddressTagQuery = Annotated[
    Optional[bool],
    Query(description="Whether to exclude best address tag"),
]
