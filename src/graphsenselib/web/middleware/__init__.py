"""FastAPI middleware for gsrest"""

from graphsenselib.web.middleware.deprecation import DeprecationHeaderMiddleware
from graphsenselib.web.middleware.empty_params import EmptyQueryParamsMiddleware
from graphsenselib.web.middleware.plugins import PluginMiddleware

__all__ = [
    "PluginMiddleware",
    "EmptyQueryParamsMiddleware",
    "DeprecationHeaderMiddleware",
]
