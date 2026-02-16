from graphsenselib.db.asynchronous.services.common import (
    convert_value as internal_convert_value,
)
from graphsenselib.db.asynchronous.services.common import (
    make_values as internal_make_values,
)

from graphsenselib.web.translators import to_api_values


def make_values(value, eur, usd):
    """Legacy wrapper that returns OpenAPI Values"""
    internal_result = internal_make_values(value, eur, usd)
    return to_api_values(internal_result)


def convert_value(currency, value, rates):
    """Legacy wrapper that returns OpenAPI Values"""
    internal_result = internal_convert_value(currency, value, rates)
    return to_api_values(internal_result)


# Re-export functions that don't need conversion
__all__ = [
    "make_values",
    "convert_value",
]
