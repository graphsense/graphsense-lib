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


# --------------------------------------------------------------------------- #
# What the services READ off a result -- the axis the checks above miss        #
# --------------------------------------------------------------------------- #


def result_requirements() -> dict:
    """``{method: {kind: (values...)}}`` -- what each ``db.<m>()`` RESULT must be.

    The checks above are derived from the call SITE: which methods exist, which
    are awaited, which parameter names are passed by keyword. None of them look
    at what comes back -- and three of the four bugs found in the first week of
    back-to-back testing were return-SHAPE bugs, each dormant behind a path the
    backtest does not exercise:

    * `get_block_timestamp` returned an int where the service calls ``.get``
    * `get_block_by_date_allow_filtering` returned an int where the service
      subscripts ``["block_id"]`` and ``["timestamp"]``
    * `get_spending_txs` returned a list where the service iterates
      ``.current_rows``

    None of them were wrong when written. They were written against a contract
    nobody was watching.
    """

    def db_method(node):
        call = node.value if isinstance(node, ast.Await) else node
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
            return None
        owner = call.func.value
        is_db = (isinstance(owner, ast.Name) and owner.id == "db") or (
            isinstance(owner, ast.Attribute) and owner.attr == "db"
        )
        return call.func.attr if is_db else None

    found: dict = {}

    def note(method, kind, value):
        found.setdefault(method, {}).setdefault(kind, set()).add(value)

    class Fn(ast.NodeVisitor):
        """One function: bind names to db methods, then record how they are used."""

        def __init__(self):
            self.bound = {}

        def visit_Assign(self, node):
            method = db_method(node.value)
            if method:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    self.bound[target.id] = method
                elif isinstance(target, (ast.Tuple, ast.List)):
                    note(method, "unpacks_to", str(len(target.elts)))
                    for index, element in enumerate(target.elts):
                        if isinstance(element, ast.Name):
                            self.bound[element.id] = f"{method}[{index}]"
            self.generic_visit(node)

        def _owner(self, node):
            direct = db_method(node)
            if direct:
                return direct
            if isinstance(node, ast.Name):
                return self.bound.get(node.id)
            return None

        def visit_Subscript(self, node):
            owner = self._owner(node.value)
            if owner:
                key = (
                    repr(node.slice.value)
                    if isinstance(node.slice, ast.Constant)
                    else "<expr>"
                )
                # READ and WRITE are different contracts, and conflating them
                # reads as "the service needs this key" when it is the service
                # PUTTING the key there. The write is the stronger requirement:
                # `txs_service` does `result["type"] = ...` on a get_tx result,
                # so a Row or a namedtuple would raise where a dict works.
                kind = "assigns" if isinstance(node.ctx, ast.Store) else "subscript"
                note(owner, kind, key)
            self.generic_visit(node)

        def visit_Attribute(self, node):
            owner = self._owner(node.value)
            if owner:
                note(owner, "attribute", node.attr)
            self.generic_visit(node)

        def visit_For(self, node):
            owner = self._owner(node.iter)
            if owner:
                note(owner, "iterated", "yes")
            self.generic_visit(node)

    for path in sorted(Path(services_pkg.__path__[0]).glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                visitor = Fn()
                for statement in node.body:
                    visitor.visit(statement)

    return {
        method: {kind: tuple(sorted(values)) for kind, values in sorted(kinds.items())}
        for method, kinds in sorted(found.items())
    }


#: What every service does with a ``db.<method>()`` result TODAY.
#:
#: A CHANGE DETECTOR, not a correctness check -- it says nothing about whether
#: the adapter satisfies any of this. What it does is fail the moment a service
#: starts reading a key or an attribute it did not before, and name it, which is
#: the moment to go and check the adapter returns it.
#:
#: That is where the leverage is. `current_rows`, `get_block_timestamp` and
#: `get_block_by_date_allow_filtering` were not wrong when they were written --
#: they were written against a contract nobody was watching, and each was found
#: months later by accident.
#:
#: Regenerate by calling :func:`result_requirements` and pretty-printing it.
RESULT_CONTRACT: dict = {
    "fetch_transaction_trace": {"subscript": ("'tx_hash'",)},
    "fetch_transaction_traces": {"iterated": ("yes",)},
    "get_address_id_id_group": {"unpacks_to": ("2",)},
    "get_addresses_light": {"attribute": ("get",)},
    "get_block_below_block_allow_filtering": {
        "subscript": ("'block_id'", "'timestamp'")
    },
    "get_block_by_date_allow_filtering": {"subscript": ("'block_id'", "'timestamp'")},
    "get_block_timestamp": {"attribute": ("get",)},
    "get_currency_statistics": {
        "subscript": (
            "'no_address_relations'",
            "'no_addresses'",
            "'no_blocks'",
            "'no_clusters'",
            "'no_transactions'",
            "'timestamp'",
        )
    },
    "get_entities_by_ids": {"subscript": ("<expr>",)},
    "get_rates": {
        "assigns": ("'rates'",),
        "attribute": ("copy",),
        "subscript": ("'rates'",),
    },
    "get_spending_txs": {"attribute": ("current_rows",)},
    "get_spent_in_txs": {"attribute": ("current_rows",)},
    "get_token_configuration": {
        "attribute": ("get", "items"),
        "subscript": ("<expr>",),
    },
    "get_tx": {
        "assigns": ("'heuristics'", "'type'"),
        "subscript": ("'block_id'", "'block_timestamp'"),
    },
    "list_address_txs": {"unpacks_to": ("2",)},
    "list_block_txs": {"iterated": ("yes",)},
    "list_entity_addresses": {"unpacks_to": ("2",)},
    "list_entity_txs": {"unpacks_to": ("2",)},
    "list_neighbors": {"unpacks_to": ("2",)},
    "list_neighbors[0]": {"iterated": ("yes",)},
    "list_rates": {"iterated": ("yes",)},
    "list_token_txs": {"iterated": ("yes",)},
    "new_address": {"attribute": ("get",), "subscript": ("'address_id'",)},
}


def test_the_services_read_what_this_snapshot_says_they_read() -> None:
    """A service that starts consuming a result differently must be noticed.

    Updating the snapshot is the correct response to a failure here -- AFTER
    checking that `LegacyAdapter` returns what the new line asks for. Updating
    it without that check is how this stops being worth anything.
    """
    current = result_requirements()

    changed = {
        method: kinds
        for method, kinds in current.items()
        if kinds != RESULT_CONTRACT.get(method)
    }
    removed = sorted(set(RESULT_CONTRACT) - set(current))

    assert not changed, (
        "a service now consumes these results differently -- check the adapter "
        f"returns what each asks for, THEN update RESULT_CONTRACT: {changed}"
    )
    assert not removed, (
        "these methods are no longer consumed by any service; drop them from "
        f"RESULT_CONTRACT (and consider dropping the adapter method): {removed}"
    )


def test_the_snapshot_is_not_vacuous() -> None:
    """The guard on the guard: an extraction that silently found nothing would
    make the test above pass forever while checking nothing."""
    assert len(RESULT_CONTRACT) > 15
    assert RESULT_CONTRACT["get_spent_in_txs"] == {"attribute": ("current_rows",)}
