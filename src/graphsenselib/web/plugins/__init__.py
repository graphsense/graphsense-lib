import abc
import inspect
from typing import Any

from fastapi import Request


class Plugin(abc.ABC):
    """Base class for plugins.

    Plugins can implement before_request and before_response hooks to modify
    request handling and responses.
    """

    @classmethod
    @abc.abstractmethod
    def before_request(cls, context: dict, request: Request) -> dict | None:
        """Called before request processing.

        Returns None to skip modifications, or a dict of header modifications.
        """
        return None

    @classmethod
    @abc.abstractmethod
    def before_response(cls, context: dict, request: Request, result: Any) -> None:
        """Called after response is prepared but before serialization.

        Can modify the result object in place.
        """
        return


def get_subclass(module):
    klasses = inspect.getmembers(module, inspect.isclass)
    for name, kls in klasses:
        if kls is Plugin:
            continue
        if issubclass(kls, Plugin):
            return kls
    raise TypeError(
        f"{module.__name__} does not implement graphsenselib.web.plugins.Plugin"
    )


def get_request_path(request: Request) -> str:
    """Get the request path."""
    return request.url.path


def get_request_query_string(request: Request) -> str:
    """Get the query string."""
    return str(request.query_params)


def get_request_header(request: Request, name: str, default: str = "") -> str:
    """Get a header value."""
    return request.headers.get(name, default)
