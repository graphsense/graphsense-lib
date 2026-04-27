"""Public helpers for inspecting where each ``Settings`` field came from.

Used by ``gs config show --resolved [--source]``. Sits in its own module
so the CLI doesn't have to import private internals of ``_sources.py``.
"""

from __future__ import annotations

from typing import Any, Iterable, Tuple


def _walk_model(obj: Any, prefix: str = "") -> Iterable[Tuple[str, Any]]:
    """Yield (dotted_path, value) for every leaf field in a model dump."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                yield from _walk_model(v, key)
            else:
                yield key, v
    else:
        yield prefix, obj


def iter_field_sources(settings: Any) -> Iterable[Tuple[str, Any, str]]:
    """Yield (dotted_path, value, source_label) for each effective field.

    Source labels:
        - ``init`` — kwargs to ``Settings()``
        - ``env`` — picked up from an env var
        - ``dotenv`` — ``.env`` file
        - ``yaml:/abs/path`` — YAML file at the given path
        - ``secrets`` — pydantic file_secret_settings
        - ``default`` — pydantic field default (no source produced a value)

    The default label is filled in here, by walking the validated model
    dump and labelling any path that doesn't appear in the source-tracking
    sink as ``default``.
    """
    from ._sources import get_sink

    sink = get_sink(settings)
    sink_data = sink.data if sink is not None else {}

    dump = settings.model_dump(exclude={"legacy_web_dict"})
    for path, value in _walk_model(dump):
        if path in sink_data:
            recorded_value, label = sink_data[path]
            yield path, value, label
        else:
            yield path, value, "default"
