"""`graphsense raw <group> <method> ...` — auto-mirror of `graphsense.api.*Api`.

Survives regeneration: the command tree is built at CLI startup from whatever
the generator produces. New endpoints show up as new subcommands for free;
removed ones disappear.
"""

from __future__ import annotations

import inspect
import json
import os
import typing as t
from typing import Any, Optional

import rich_click as click

import graphsense
from graphsense.cli.context import CliContext
from graphsense.ext import output as out_mod

pass_ctx = click.make_pass_decorator(CliContext)

_DEPRECATED_APIS = {"EntitiesApi"}
_INTERNAL_SUFFIXES = ("_with_http_info", "_without_preload_content")
_PRIVATE_PREFIXES = ("_",)


def build_raw_group() -> click.Group:
    """Assemble the `raw` group by reflecting on `graphsense.api` classes."""

    show_deprecated = (
        os.environ.get("GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS") == "1"
    )

    @click.group(
        name="raw",
        help="Auto-generated wrapper over every REST endpoint in the "
        "current graphsense-python build. Shape follows the generated "
        "API classes and therefore survives regeneration.",
    )
    def raw_group() -> None:
        pass

    for cls_name in sorted(_iter_api_class_names()):
        if cls_name in _DEPRECATED_APIS and not show_deprecated:
            continue
        cls = getattr(graphsense, cls_name)
        subgroup = _make_api_subgroup(cls_name, cls)
        raw_group.add_command(subgroup)

    return raw_group


def _iter_api_class_names() -> t.Iterator[str]:
    for name in dir(graphsense):
        if name.endswith("Api") and inspect.isclass(getattr(graphsense, name)):
            yield name


def _make_api_subgroup(cls_name: str, cls: type) -> click.Group:
    key = cls_name[: -len("Api")].lower()

    @click.group(
        name=key,
        help=f"Access to {cls_name} methods (auto-generated).",
    )
    def group() -> None:
        pass

    for method_name, method in _iter_public_methods(cls):
        cmd = _make_method_command(key, method_name, method)
        if cmd is not None:
            group.add_command(cmd)
    return group


def _iter_public_methods(cls: type) -> t.Iterator[tuple[str, Any]]:
    for name, obj in inspect.getmembers(cls, predicate=inspect.isfunction):
        if name.startswith(_PRIVATE_PREFIXES):
            continue
        if name.endswith(_INTERNAL_SUFFIXES):
            continue
        yield name, obj


def _make_method_command(
    api_key: str, method_name: str, method: Any
) -> Optional[click.Command]:
    try:
        sig = inspect.signature(method)
    except (TypeError, ValueError):  # pragma: no cover
        return None

    params = [
        p
        for p in sig.parameters.values()
        if p.name not in ("self",) and not p.name.startswith("_")
    ]

    click_params: list[click.Parameter] = []
    conversions: list[tuple[str, t.Callable[[Any], Any]]] = []
    for p in params:
        cp, conv = _param_to_click(p)
        click_params.append(cp)
        conversions.append((p.name, conv))

    dashed_name = method_name.replace("_", "-")

    @click.pass_context
    def invoke(
        click_ctx: click.Context,
        *,
        _method_name: str = method_name,
        _conversions: list[tuple[str, t.Callable[[Any], Any]]] = conversions,
        **kwargs: Any,
    ) -> None:
        cli_ctx = _find_cli_context(click_ctx)
        gs = cli_ctx.gs()
        api_instance = getattr(gs.raw, api_key, None)
        if api_instance is None:
            # deprecated API; surface via GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS
            raise click.UsageError(
                f"API group {api_key!r} not available (deprecated? "
                f"try GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS=1)"
            )
        fn = getattr(api_instance, _method_name)
        call_kwargs: dict[str, Any] = {}
        for name, conv in _conversions:
            value = kwargs.get(name)
            if value is None or value == ():
                continue
            call_kwargs[name] = conv(value)
        result = fn(**call_kwargs)
        out_mod.write(
            result,
            output=cli_ctx.output,
            directory=cli_ctx.directory,
            format=cli_ctx.format,
            color=cli_ctx.color,
        )

    cmd = click.Command(
        name=dashed_name,
        params=click_params,
        callback=invoke,
        short_help=_short_help(method),
        help=(inspect.getdoc(method) or "").strip() or None,
    )
    return cmd


def _find_cli_context(click_ctx: click.Context) -> CliContext:
    node: Optional[click.Context] = click_ctx
    while node is not None:
        if isinstance(node.obj, CliContext):
            return node.obj
        node = node.parent
    raise click.UsageError("internal: could not locate graphsense CLI context")


def _short_help(method: Any) -> Optional[str]:
    doc = inspect.getdoc(method) or ""
    if not doc:
        return None
    first = doc.splitlines()[0].strip()
    return first or None


def _param_to_click(
    p: inspect.Parameter,
) -> tuple[click.Parameter, t.Callable[[Any], Any]]:
    """Map one method parameter to a click Argument/Option + conversion fn."""
    name = p.name
    required = p.default is inspect.Parameter.empty
    annotation = _unwrap_annotated(p.annotation)
    click_type, conv, multiple = _click_type_for(annotation)

    if required:
        arg = click.Argument([name], type=click_type, required=True, nargs=1)
        return arg, conv

    opt_name = "--" + name.replace("_", "-")
    if multiple:
        opt = click.Option(
            [opt_name, name],
            type=click_type,
            multiple=True,
            default=(),
        )
        return opt, conv
    if click_type is click.BOOL:
        # Render as --flag / --no-flag pair.
        opt = click.Option(
            [opt_name + "/--no-" + name.replace("_", "-"), name],
            default=None,
        )
        return opt, lambda v: bool(v)

    opt = click.Option([opt_name, name], type=click_type, default=None)
    return opt, conv


def _unwrap_annotated(ann: Any) -> Any:
    """Peel `Annotated[T, ...]` and `Optional[T]` down to a usable type."""
    if ann is inspect.Parameter.empty:
        return str

    origin = t.get_origin(ann)
    args = t.get_args(ann)

    # Annotated[T, ...]  (pydantic Field metadata)
    if origin is not None and repr(origin).endswith("Annotated"):
        return _unwrap_annotated(args[0])

    # Union / Optional
    if origin in (t.Union, getattr(__import__("types"), "UnionType", None)):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _unwrap_annotated(non_none[0])
        # Pick first for heterogeneous unions
        return _unwrap_annotated(non_none[0]) if non_none else str

    return ann


def _click_type_for(
    ann: Any,
) -> tuple[click.ParamType, t.Callable[[Any], Any], bool]:
    origin = t.get_origin(ann)
    args = t.get_args(ann)

    if origin in (list, tuple):
        inner = args[0] if args else str
        inner_unwrapped = _unwrap_annotated(inner)
        inner_ct, inner_conv, _ = _click_type_for(inner_unwrapped)
        return inner_ct, lambda v, conv=inner_conv: [conv(x) for x in v], True

    if origin is dict or ann is dict:
        # Accept a JSON blob on the command line.
        return click.STRING, _parse_json, False

    if ann is int:
        return click.INT, int, False
    if ann is float:
        return click.FLOAT, float, False
    if ann is bool:
        return click.BOOL, bool, False
    return click.STRING, str, False


def _parse_json(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    s = str(v)
    if s.startswith("@"):
        with open(s[1:], "r", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(s)
