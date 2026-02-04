from graphsenselib.db.asynchronous.services.common import (
    convert_value as internal_convert_value,
)
from graphsenselib.db.asynchronous.services.common import (
    make_values as internal_make_values,
)

from graphsenselib.web.translators import pydantic_values_to_openapi


def make_values(value, eur, usd):
    """Legacy wrapper that returns OpenAPI Values"""
    internal_result = internal_make_values(value, eur, usd)
    return pydantic_values_to_openapi(internal_result)


def convert_value(currency, value, rates):
    """Legacy wrapper that returns OpenAPI Values"""
    internal_result = internal_convert_value(currency, value, rates)
    return pydantic_values_to_openapi(internal_result)


# Re-export functions that don't need conversion
__all__ = [
    "make_values",
    "convert_value",
]
