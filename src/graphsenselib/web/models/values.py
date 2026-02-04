"""Value-related API models."""

from graphsenselib.web.models.base import APIModel


class Rate(APIModel):
    """Exchange rate model."""

    code: str
    value: float


class Values(APIModel):
    """Values model with fiat conversion."""

    fiat_values: list[Rate]
    value: int
