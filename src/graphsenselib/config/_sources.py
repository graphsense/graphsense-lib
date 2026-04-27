"""Custom pydantic-settings sources for the new ``Settings`` model.

Two responsibilities:

1. ``YamlConfigSource`` — load a YAML config with the same default-file
   lookup the legacy ``AppConfig`` used, plus support for a **layered**
   base + per-environment overlay (``graphsense.yaml`` +
   ``graphsense.<env>.yaml``). Legacy monolithic YAMLs with a
   top-level ``environments.<env>`` section keep working — their
   contents are lifted to root at load time and a deprecation warning
   is emitted nudging toward per-env files.
2. ``ProvenanceTrackingSource`` — wraps any source and records, per
   field path, which source produced the final value, so that
   ``gs config show --resolved --source`` can answer
   "where did this value come from?".

The legacy env-prefix aliasing logic lives in ``_legacy.py`` so the
removal PR doesn't have to touch this file.
"""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic.fields import FieldInfo
from pydantic_settings import PydanticBaseSettingsSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# YAML source
# ---------------------------------------------------------------------------

# Default lookup for the *base* file, in order. Matches today's
# AppConfig.GoodConfConfigDict plus the REST-deployment convention of
# ``./instance/config.yaml``.
_DEFAULT_YAML_FILES = (
    ".graphsense.yaml",
    "./instance/config.yaml",
    "~/.graphsense.yaml",
)
_YAML_FILE_ENV_VAR = "GRAPHSENSE_CONFIG_YAML"
# Legacy env var that the REST (gsrest) entrypoint honored for an
# explicit config path. Kept for operator-level back-compat so a Docker
# deployment that sets CONFIG_FILE=/srv/graphsense-rest/instance/config.yaml
# still loads the expected file with zero change.
_LEGACY_REST_FILE_ENV_VAR = "CONFIG_FILE"
_YAML_ENV_VAR = "GRAPHSENSE_ENV"


def _overlay_candidates(base: Path, env: str) -> List[Path]:
    """Return the per-env overlay file candidates for a given base file.

    Given ``./some/graphsense.yaml`` + env ``prod``, the candidate is
    ``./some/graphsense.prod.yaml``. The stem is split on the first
    ``.`` so both ``graphsense.yaml`` and ``.graphsense.yaml`` work.
    """
    parent = base.parent
    name = base.name
    # Strip leading dot so ".graphsense.yaml" → "graphsense.prod.yaml"
    if name.startswith("."):
        stem_name = name[1:]
        prefix = "."
    else:
        stem_name = name
        prefix = ""
    # Split off the final suffix (.yaml / .yml) so the env tag slots in
    # before it.
    if "." in stem_name:
        stem, suffix = stem_name.rsplit(".", 1)
        return [parent / f"{prefix}{stem}.{env}.{suffix}"]
    return []


def resolve_yaml_paths(
    explicit: Optional[str] = None, env: Optional[str] = None
) -> Tuple[Optional[Path], Optional[Path]]:
    """Return ``(base_path, overlay_path)`` — either may be None.

    Precedence for the base file: explicit arg → ``GRAPHSENSE_CONFIG_YAML``
    env → ``./.graphsense.yaml`` → ``./instance/config.yaml`` →
    ``~/.graphsense.yaml``.

    Overlay: if ``env`` is given, a ``<base-stem>.<env>.<ext>`` file
    next to the base, if it exists.
    """
    base: Optional[Path] = None

    if explicit:
        p = Path(explicit).expanduser()
        if p.exists():
            base = p
    elif os.environ.get(_YAML_FILE_ENV_VAR):
        p = Path(os.environ[_YAML_FILE_ENV_VAR]).expanduser()
        if p.exists():
            base = p
    elif os.environ.get(_LEGACY_REST_FILE_ENV_VAR):
        # Back-compat for REST deployments that use CONFIG_FILE.
        p = Path(os.environ[_LEGACY_REST_FILE_ENV_VAR]).expanduser()
        if p.exists():
            base = p
    else:
        for candidate in _DEFAULT_YAML_FILES:
            p = Path(candidate).expanduser()
            if p.exists():
                base = p
                break

    overlay: Optional[Path] = None
    if env and base is not None:
        for cand in _overlay_candidates(base, env):
            if cand.exists():
                overlay = cand
                break

    return base, overlay


# Back-compat alias for anything that used to import resolve_yaml_path.
def resolve_yaml_path(explicit: Optional[str] = None) -> Optional[Path]:
    base, _ = resolve_yaml_paths(explicit)
    return base


def _deep_merge(lo: Dict[str, Any], hi: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``lo`` deep-merged with ``hi``. ``hi`` wins on scalar
    conflicts; dicts merge recursively; lists are replaced wholesale."""
    out = dict(lo)
    for k, v in hi.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _lift_legacy_web_subkeys(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Hoist legacy ``web.database`` → root ``cassandra`` and
    ``web.gs-tagstore`` → root ``tagstore``.

    These were the GSRestConfig-era nested fields. In the new model
    both live at root. This function applies the same translation the
    ``gs config migrate`` command does so loading a legacy YAML
    produces byte-identical Settings to loading the migrated
    per-env-split equivalent.

    Pure-ish: returns a shallow copy with the lifted fields moved.
    Existing root-level ``cassandra`` / ``tagstore`` win over the
    lifted values.
    """
    legacy_web = raw.get("web")
    if not isinstance(legacy_web, dict):
        return raw
    if "database" not in legacy_web and "gs-tagstore" not in legacy_web:
        return raw

    out = dict(raw)
    web_out = dict(legacy_web)

    if "database" in web_out:
        db = web_out.pop("database")
        if isinstance(db, dict):
            existing_cas = out.get("cassandra")
            if isinstance(existing_cas, dict):
                out["cassandra"] = _deep_merge(db, existing_cas)  # existing wins
            else:
                out["cassandra"] = db

    if "gs-tagstore" in web_out:
        ts = web_out.pop("gs-tagstore")
        if isinstance(ts, dict):
            existing_ts = out.get("tagstore")
            if isinstance(existing_ts, dict):
                out["tagstore"] = _deep_merge(ts, existing_ts)  # existing wins
            else:
                out["tagstore"] = ts

    out["web"] = web_out
    return out


def _lift_legacy_environments(
    raw: Dict[str, Any], env: Optional[str], source_label: str
) -> Dict[str, Any]:
    """Promote ``environments.<env>`` contents to root.

    Legacy YAMLs nest per-environment config under ``environments.<env>``.
    The new model has a root-level ``keyspaces`` / ``cassandra`` /
    ``environment`` instead. This function applies the translation and
    emits a ``DeprecationWarning`` nudging toward the per-env file
    split — but only when the legacy nested block is actually present.

    Mutates and returns a *copy* of ``raw``.
    """
    if "environments" not in raw or not isinstance(raw["environments"], dict):
        return raw

    envs = raw["environments"]
    selected_env = env or raw.get("default_environment")
    if selected_env is None or selected_env not in envs:
        return raw

    env_block = envs[selected_env]
    if not isinstance(env_block, dict):
        return raw

    warnings.warn(
        f"{source_label}: nested 'environments.{selected_env}' block is "
        "deprecated. Split into graphsense.yaml (shared) + "
        f"graphsense.{selected_env}.yaml (per-env overlay). The legacy "
        "shape will be dropped together with AppConfig.",
        DeprecationWarning,
        stacklevel=4,
    )

    out = dict(raw)
    out.setdefault("environment", selected_env)

    # cassandra_nodes + creds → cassandra.<field>
    cassandra_overlay: Dict[str, Any] = {}
    if "cassandra_nodes" in env_block:
        cassandra_overlay["nodes"] = env_block["cassandra_nodes"]
    for k in ("username", "password", "readonly_username", "readonly_password"):
        if k in env_block:
            cassandra_overlay[k] = env_block[k]
    if cassandra_overlay:
        existing_cas = out.get("cassandra")
        existing_cas_dict: Dict[str, Any] = (
            existing_cas if isinstance(existing_cas, dict) else {}
        )
        out["cassandra"] = _deep_merge(cassandra_overlay, existing_cas_dict)

    # keyspaces → root-level keyspaces (existing wins)
    if "keyspaces" in env_block and isinstance(env_block["keyspaces"], dict):
        existing_ks = out.get("keyspaces")
        existing_ks_dict: Dict[str, Any] = (
            existing_ks if isinstance(existing_ks, dict) else {}
        )
        out["keyspaces"] = _deep_merge(env_block["keyspaces"], existing_ks_dict)

    return out


class YamlConfigSource(PydanticBaseSettingsSource):
    """Load YAML and present it as a flat dict for pydantic-settings.

    Resolves a *base* file (e.g. ``graphsense.yaml``) and, when an env
    is specified, a *per-env overlay* file
    (e.g. ``graphsense.prod.yaml``) next to it. Deep-merges base under
    overlay, with overlay winning on conflicts.

    Side effects on the merged dict:

    - A top-level ``web:`` key is mirrored verbatim into
      ``legacy_web_dict`` so the loose fallback path in
      ``web/app.py:resolve_rest_config()`` keeps working even if
      ``WebSettings`` would have rejected one of the fields.
    - A legacy ``environments.<env>`` nested block is lifted to root
      (``keyspaces``, ``cassandra.nodes``, ``cassandra.<creds>``) with
      a ``DeprecationWarning`` nudging toward per-env files.
    """

    def __init__(
        self,
        settings_cls,
        explicit_file: Optional[str] = None,
        env: Optional[str] = None,
    ):
        super().__init__(settings_cls)
        self._explicit_file = explicit_file
        self._env = env or os.environ.get(_YAML_ENV_VAR)
        self._base_path: Optional[Path] = None
        self._overlay_path: Optional[Path] = None
        self._cache: Optional[Dict[str, Any]] = None

    @property
    def loaded_path(self) -> Optional[Path]:
        """The primary file path for provenance display. Overlay path
        is used when present, otherwise the base path."""
        return self._overlay_path or self._base_path

    @property
    def base_path(self) -> Optional[Path]:
        return self._base_path

    @property
    def overlay_path(self) -> Optional[Path]:
        return self._overlay_path

    def _read_yaml(self, path: Path) -> Dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning("Failed to parse YAML config %s: %s", path, e)
            return {}
        if not isinstance(raw, dict):
            logger.warning(
                "YAML config %s did not parse to a mapping (got %s); ignoring.",
                path,
                type(raw).__name__,
            )
            return {}
        return raw

    def _load(self) -> Dict[str, Any]:
        if self._cache is not None:
            return self._cache

        base_path, overlay_path = resolve_yaml_paths(self._explicit_file, self._env)
        self._base_path = base_path
        self._overlay_path = overlay_path

        if base_path is None and overlay_path is None:
            self._cache = {}
            return self._cache

        base_raw = self._read_yaml(base_path) if base_path is not None else {}
        overlay_raw = self._read_yaml(overlay_path) if overlay_path is not None else {}

        # Apply legacy lifts to each file independently so the warning
        # (when emitted) quotes the actual file it came from.
        #
        # Order matters: environments.<env> lift first, then
        # web-sub-keys. This matches the `gs config migrate` behavior
        # where per-env `cassandra_nodes` is authoritative — when a
        # deployment has a separate `web.database:` cluster the migrator
        # warns and the per-env cluster wins. If we lifted web-sub-keys
        # first, the per-env cluster would be silently discarded by the
        # "existing wins" merge in _lift_legacy_environments.
        if base_path is not None:
            base_raw = _lift_legacy_environments(
                base_raw, self._env, f"yaml:{base_path}"
            )
            base_raw = _lift_legacy_web_subkeys(base_raw)
        if overlay_path is not None:
            overlay_raw = _lift_legacy_environments(
                overlay_raw, self._env, f"yaml:{overlay_path}"
            )
            overlay_raw = _lift_legacy_web_subkeys(overlay_raw)

        merged = _deep_merge(base_raw, overlay_raw)

        # Mirror top-level `web:` into `legacy_web_dict` for the loose
        # fallback path in web/app.py:resolve_rest_config().
        if "web" in merged and isinstance(merged["web"], dict):
            merged.setdefault("legacy_web_dict", merged["web"])

        # Carry the env name through if not set explicitly in YAML.
        if self._env and "environment" not in merged:
            merged["environment"] = self._env

        self._cache = merged
        return self._cache

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        data = self._load()
        if field_name in data:
            return data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> Dict[str, Any]:
        data = self._load()
        known = set(self.settings_cls.model_fields.keys())
        return {k: v for k, v in data.items() if k in known}


# ---------------------------------------------------------------------------
# Per-field provenance
# ---------------------------------------------------------------------------

# Maps id(settings_instance) -> _Sink. A plain dict (rather than
# WeakValueDictionary) because pydantic v2 models are not always weak-
# referenceable. The CLI lifecycle is short and the singleton-replace
# rate is low, so the small leak is acceptable. ``set_settings(None)``
# (and ``reset_settings``) trigger ``_drop_sink`` to keep this tidy.
_provenance_store: "Dict[int, _Sink]" = {}


class _Sink:
    """Holds the {dotted_path: (value, source_label)} dict for one
    Settings instance."""

    __slots__ = ("data",)

    def __init__(self) -> None:
        self.data: Dict[str, Tuple[Any, str]] = {}


def _walk(obj: Any, prefix: str = "") -> "list[tuple[str, Any]]":
    out: list[tuple[str, Any]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                out.extend(_walk(v, key))
            else:
                out.append((key, v))
    return out


class ProvenanceTrackingSource(PydanticBaseSettingsSource):
    """Wraps another source; records which fields it produced.

    A single shared ``_Sink`` is passed in by the customise-sources hook,
    so all wrappers write to the same dict. Higher-priority sources are
    invoked first (pydantic-settings ordering), and we use ``setdefault``
    to record the *first* source that produced each path — which is the
    one whose value wins.
    """

    def __init__(
        self,
        inner: PydanticBaseSettingsSource,
        label: str,
        sink: _Sink,
    ) -> None:
        super().__init__(inner.settings_cls)
        self._inner = inner
        self._label = label
        self._sink = sink

    @property
    def label(self) -> str:
        return self._label

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        return self._inner.get_field_value(field, field_name)

    def __call__(self) -> Dict[str, Any]:
        data = self._inner()
        for path, value in _walk(data):
            self._sink.data.setdefault(path, (value, self._label))
        # Also record top-level scalar fields (those not covered by _walk
        # because they're not dicts).
        for k, v in data.items():
            if not isinstance(v, dict):
                self._sink.data.setdefault(k, (v, self._label))
        return data


def attach_sink(settings: Any, sink: _Sink) -> None:
    """Bind a provenance sink to a Settings instance."""
    _provenance_store[id(settings)] = sink


def get_sink(settings: Any) -> Optional[_Sink]:
    return _provenance_store.get(id(settings))


def drop_sink(settings: Any) -> None:
    """Drop the provenance sink for a Settings instance, if any."""
    _provenance_store.pop(id(settings), None)
