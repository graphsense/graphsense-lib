"""Value-related API models."""

from pydantic import ConfigDict

from graphsenselib.web.models.base import APIModel

RATE_EXAMPLE = {"code": "eur", "value": 0.1234}
FIAT_VALUES_EXAMPLE = [{"code": "eur", "value": 10}, {"code": "usd", "value": 20}]
VALUES_EXAMPLE = {"fiat_values": FIAT_VALUES_EXAMPLE, "value": 1000000}


class Rate(APIModel):
    """Exchange rate model."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={"example": RATE_EXAMPLE},
    )

    code: str
    value: float


class Values(APIModel):
    """Values model with fiat conversion."""

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        json_schema_extra={"example": VALUES_EXAMPLE},
    )

    fiat_values: list[Rate]
    value: int
