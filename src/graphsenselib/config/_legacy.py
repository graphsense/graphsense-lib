"""Deprecation-window quarantine.

This module hosts everything that disappears when the deprecation
window for the legacy config layout closes. Removing the file plus
the call sites tagged ``# TODO(deprecation): remove with _legacy.py``
in:

- ``config/settings.py`` (one ``_apply_legacy_env_aliases`` call)
- ``config/cassandra_async_config.py``
- ``config/tagstore_config.py``
- ``tagstore/config/__init__.py``
- ``web/config.py``
- ``mcp/config.py``

…is the entirety of the removal PR.

What lives here:

- ``LEGACY_PREFIX_MAP`` — five legacy env prefixes → new
  ``GRAPHSENSE_<sub>__<field>`` form.
- ``_apply_legacy_env_aliases()`` — promotes legacy-prefixed env vars
  into ``os.environ`` with ``setdefault`` (so new-prefix vars always
  win when both are set), emitting one ``DeprecationWarning`` per
  unique legacy variable.
- ``_emit_class_deprecation(class_name, replacement)`` — helper used
  by each legacy class's ``__init__`` to emit a one-shot construction
  warning steering users to the new ``Settings.<sub>`` field.
"""

from __future__ import annotations

import os
import warnings
from typing import Dict, Mapping, Optional, Tuple


# Mapping from legacy prefix → new prefix (with the nested ``__`` already
# baked in). Lookups are case-insensitive: ``gs_tagstore_DB_URL`` matches
# ``GS_TAGSTORE_`` here. Order doesn't matter — prefixes don't overlap.
LEGACY_PREFIX_MAP: Dict[str, str] = {
    "GS_CASSANDRA_ASYNC_": "GRAPHSENSE_CASSANDRA__",
    "GRAPHSENSE_TAGSTORE_READ_": "GRAPHSENSE_TAGSTORE__",
    "GSREST_": "GRAPHSENSE_WEB__",
    "GS_MCP_": "GRAPHSENSE_MCP__",
}

# Exact-var renames that take precedence over the prefix map. Covers the
# cases where the field name itself changed (not just the prefix). The
# new consolidated Settings collapses ``tagstore_db.db_url`` into
# ``tagstore.url``, so ``GS_TAGSTORE_DB_URL`` maps directly to
# ``GRAPHSENSE_TAGSTORE__URL``.
LEGACY_VAR_MAP: Dict[str, str] = {
    "GS_TAGSTORE_DB_URL": "GRAPHSENSE_TAGSTORE__URL",
}


# One-shot dedup so the same legacy var only warns once per process.
_warned_legacy_vars: set[str] = set()
_warned_legacy_classes: set[str] = set()


def _matches_legacy_prefix(name: str) -> Optional[Tuple[str, str]]:
    upper = name.upper()
    for old, new in LEGACY_PREFIX_MAP.items():
        if upper.startswith(old.upper()):
            return old, new
    return None


def _resolve_legacy_name(env_name: str) -> Optional[str]:
    """Return the new-prefix equivalent for a legacy env var, or None if
    it doesn't match any legacy form.

    Exact-var renames win over prefix matches — needed for field
    renames like ``GS_TAGSTORE_DB_URL`` → ``GRAPHSENSE_TAGSTORE__URL``
    where only the prefix rewrite would produce the wrong field.
    """
    upper = env_name.upper()
    if upper in LEGACY_VAR_MAP:
        return LEGACY_VAR_MAP[upper]
    match = _matches_legacy_prefix(env_name)
    if match is None:
        return None
    old, new = match
    return new + env_name[len(old) :]


def _apply_legacy_env_aliases(env: Optional[Mapping[str, str]] = None) -> None:
    """Mirror legacy-prefixed env vars to their new ``GRAPHSENSE_<sub>__<field>``
    equivalents in ``os.environ``.

    ``setdefault`` semantics — if the user has explicitly set the new
    name too, it wins (we don't overwrite it). One ``DeprecationWarning``
    per unique legacy var per process.
    """
    source: Mapping[str, str] = env if env is not None else os.environ

    for env_name, env_value in list(source.items()):
        new_name = _resolve_legacy_name(env_name)
        if new_name is None:
            continue

        if new_name in os.environ:
            continue

        if env_name not in _warned_legacy_vars:
            warnings.warn(
                f"Env var {env_name!r} is deprecated; use {new_name!r} instead.",
                DeprecationWarning,
                stacklevel=3,
            )
            _warned_legacy_vars.add(env_name)

        os.environ[new_name] = env_value


def _emit_class_deprecation(class_name: str, replacement: str) -> None:
    """Emit a one-shot ``DeprecationWarning`` for a legacy class.

    Call from the legacy class's ``__init__``. ``replacement`` is the
    user-facing description of what to switch to (e.g.
    ``"graphsenselib.config.Settings.cassandra"``).
    """
    if class_name in _warned_legacy_classes:
        return
    _warned_legacy_classes.add(class_name)
    warnings.warn(
        f"{class_name} is deprecated; use {replacement} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def _reset_warning_dedup() -> None:
    """Test-only: clear the per-process warning dedup sets."""
    _warned_legacy_vars.clear()
    _warned_legacy_classes.clear()
