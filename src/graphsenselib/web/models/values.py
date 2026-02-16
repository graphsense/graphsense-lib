"""Value-related API models."""

from graphsenselib.web.models.base import APIModel, api_model_config

RATE_EXAMPLE = {"code": "eur", "value": 0.1234}
FIAT_VALUES_EXAMPLE = [{"code": "eur", "value": 10}, {"code": "usd", "value": 20}]
VALUES_EXAMPLE = {"fiat_values": FIAT_VALUES_EXAMPLE, "value": 1000000}


class Rate(APIModel):
    """Exchange rate model."""

    model_config = api_model_config(RATE_EXAMPLE)

    code: str
    value: float


class Values(APIModel):
    """Values model with fiat conversion."""

    model_config = api_model_config(VALUES_EXAMPLE)

    fiat_values: list[Rate]
    value: int
