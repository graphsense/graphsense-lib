"""The adapter against the service layer's actual demands.

Every failure in the first live back-to-back run was one of these, and none
needed a cluster to find: a method the protocols declare and the adapter did not
have, a method declared `def` and implemented `async def`, a parameter passed by
keyword under a name the adapter spelled differently. They surfaced as
`TypeError` and `KeyError` from deep inside the service layer, where the cause
is invisible.

So the contract is checked from the service layer's own source rather than from
a hand-written list, which cannot go stale as the services change:

* **presence and async-ness** come from the ``DatabaseProtocol`` declarations;
* **keyword names** come from the real call sites, because a parameter name is
  only load-bearing where the caller actually uses it as a keyword.
"""

import ast
import importlib
import inspect
import pkgutil
from pathlib import Path

import pytest

import graphsenselib.db.asynchronous.services as services_pkg
from graphsense_v3.db.legacy import LegacyAdapter


def protocol_methods() -> dict:
    """``{method: (is_async, [modules declaring it])}`` from every
    ``DatabaseProtocol`` in the services package."""
    found: dict = {}
    for module in pkgutil.iter_modules(services_pkg.__path__):
        loaded = importlib.import_module(f"{services_pkg.__name__}.{module.name}")
        protocol = getattr(loaded, "DatabaseProtocol", None)
        if protocol is None or not inspect.isclass(protocol):
            continue
        for name, fn in vars(protocol).items():
            if name.startswith("_") or not callable(fn):
                continue
            is_async = inspect.iscoroutinefunction(fn)
            entry = found.setdefault(name, (is_async, []))
            entry[1].append(module.name)
    return found


def db_call_keywords() -> dict:
    """``{method: {keyword names}}`` from every ``db.<method>(...)`` call site.

    Read from source because this is the only thing that says whether a
    parameter's NAME is part of the contract. `list_address_links` is called
    with ``token_currency=``; `get_block_timestamp` is called positionally, and
    renaming its parameter would break nothing.
    """
    keywords: dict = {}
    for path in Path(services_pkg.__path__[0]).glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute):
                continue
            owner = func.value
            is_db = (isinstance(owner, ast.Name) and owner.id == "db") or (
                isinstance(owner, ast.Attribute) and owner.attr == "db"
            )
            if not is_db:
                continue
            names = {kw.arg for kw in node.keywords if kw.arg}
            keywords.setdefault(func.attr, set()).update(names)
    return keywords


PROTOCOLS = protocol_methods()
KEYWORDS = db_call_keywords()


def test_the_services_were_actually_scanned() -> None:
    """A guard on the guards: if the introspection silently found nothing,
    every test below would pass vacuously and prove nothing."""
    assert len(PROTOCOLS) > 10
    assert "list_address_links" in KEYWORDS
    assert "token_currency" in KEYWORDS["list_address_links"]


@pytest.mark.parametrize("method", sorted(PROTOCOLS))
def test_the_adapter_implements_every_protocol_method(method) -> None:
    """A missing method is not a clean failure -- the service layer raises
    AttributeError somewhere unrelated to what the caller asked for."""
    _, where = PROTOCOLS[method]
    assert hasattr(LegacyAdapter, method), (
        f"{method} is declared by {', '.join(where)} but the adapter has no "
        "implementation"
    )


@pytest.mark.parametrize("method", sorted(PROTOCOLS))
def test_sync_and_async_match_the_protocol(method) -> None:
    """`get_token_configuration` is declared `def` in seven protocols and the
    services call it without awaiting. Implemented `async`, it returns a
    coroutine that is then subscripted -- a TypeError nowhere near its cause,
    plus a "never awaited" warning."""
    want_async, where = PROTOCOLS[method]
    impl = getattr(LegacyAdapter, method, None)
    if impl is None:
        pytest.skip("covered by the presence test")
    assert inspect.iscoroutinefunction(impl) == want_async, (
        f"{method}: {', '.join(where)} declares it "
        f"{'async' if want_async else 'sync'}, the adapter is "
        f"{'async' if inspect.iscoroutinefunction(impl) else 'sync'}"
    )


@pytest.mark.parametrize("method", sorted(KEYWORDS))
def test_every_keyword_a_service_passes_is_accepted(method) -> None:
    """Only keyword call sites make a parameter NAME part of the contract.
    `list_address_links(..., token_currency=...)` is the one that bit."""
    impl = getattr(LegacyAdapter, method, None)
    if impl is None:
        pytest.skip("not an adapter method")
    params = inspect.signature(impl).parameters
    if any(p.kind is p.VAR_KEYWORD for p in params.values()):
        return
    for name in KEYWORDS[method]:
        assert name in params, (
            f"a service calls db.{method}({name}=...) but the adapter's "
            f"signature has no such parameter"
        )
