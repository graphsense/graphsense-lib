"""Consolidated graphsenselib settings.

One root `Settings` model on pydantic-settings, single env prefix
`GRAPHSENSE_` with nested `__` delimiter. Replaces the five legacy env
prefixes (`GS_CASSANDRA_ASYNC_`, `GRAPHSENSE_TAGSTORE_READ_`,
`gs_tagstore_`, `GSREST_`, `GS_MCP_`) — see ``_legacy.py`` and
``_sources.py`` for the back-compat shim layer.

Submodels are plain ``BaseModel`` so the root ``Settings`` is the only
class that scans env vars; nested env vars resolve via
``env_nested_delimiter='__'``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    field_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cassandra (formerly CassandraConfig / GS_CASSANDRA_ASYNC_*)
# ---------------------------------------------------------------------------


class CurrencySettings(BaseModel):
    """Per-currency keyspace pointers used by the async Cassandra client."""

    raw: Optional[str] = None
    transformed: Optional[str] = None
    balance_provider: Optional[str] = None


class CassandraSettings(BaseModel):
    """Async Cassandra client configuration. New env: GRAPHSENSE_CASSANDRA__*."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    driver: str = Field(default="cassandra", description="Database driver")
    currencies: Dict[str, Optional[CurrencySettings]] = Field(
        default_factory=lambda: {
            "btc": None,
            "bch": None,
            "ltc": None,
            "zec": None,
            "eth": None,
            "trx": None,
        },
        description="Dictionary of currency configurations",
    )
    nodes: List[str] = Field(
        default_factory=list, description="List of Cassandra node addresses"
    )
    port: int = Field(default=9042, description="Cassandra port number")
    username: Optional[str] = Field(
        default=None, description="Username for authentication"
    )
    password: Optional[str] = Field(
        default=None, description="Password for authentication"
    )
    readonly_username: Optional[str] = Field(
        default=None, description="Read-only username (optional, used by REST)"
    )
    readonly_password: Optional[str] = Field(
        default=None, description="Read-only password (optional, used by REST)"
    )
    consistency_level: str = Field(
        default="LOCAL_ONE", description="Cassandra consistency level"
    )
    strict_data_validation: bool = Field(
        default=True, description="Enable strict data validation"
    )
    retry_interval: Optional[int] = Field(
        default=5, description="Retry interval in seconds when connection fails"
    )
    list_address_txs_ordered_legacy: bool = Field(
        default=False, description="Use legacy address transaction ordering"
    )
    cross_chain_pubkey_mapping_keyspace: Optional[str] = Field(
        default="pubkey", description="Keyspace for cross-chain public key mapping"
    )
    ignore_traces_not_found_in_list_txs: bool = Field(
        default=True,
        description=(
            "Ignore missing traces in list_address_txs for Ethereum-like currencies"
        ),
    )

    @field_validator("currencies", mode="before")
    @classmethod
    def _validate_currencies(cls, v):
        if not isinstance(v, dict):
            raise ValueError("currencies must be a dictionary")
        result: Dict[str, Optional[CurrencySettings]] = {}
        for currency, config in v.items():
            if config is None:
                result[currency] = CurrencySettings()
            elif isinstance(config, dict):
                result[currency] = CurrencySettings(**config)
            elif isinstance(config, CurrencySettings):
                result[currency] = config
            else:
                raise ValueError(f"Invalid config type for currency {currency}")
        return result

    @field_validator("consistency_level")
    @classmethod
    def _validate_consistency_level(cls, v):
        valid = {
            "ANY",
            "ONE",
            "TWO",
            "THREE",
            "QUORUM",
            "ALL",
            "LOCAL_QUORUM",
            "EACH_QUORUM",
            "SERIAL",
            "LOCAL_SERIAL",
            "LOCAL_ONE",
        }
        if v not in valid:
            raise ValueError(f"consistency_level must be one of {valid}")
        return v


# ---------------------------------------------------------------------------
# TagStore reader (formerly TagStoreReaderConfig / GRAPHSENSE_TAGSTORE_READ_*)
# ---------------------------------------------------------------------------


class TagStoreSettings(BaseModel):
    """REST/MCP-side tagstore reader. New env: GRAPHSENSE_TAGSTORE__*."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    url: str = Field(default="", description="Database connection URL")
    pool_size: int = Field(default=50, ge=1)
    pool_timeout: int = Field(default=300, ge=1)
    max_overflow: int = Field(default=10, ge=0)
    pool_recycle: int = Field(default=3600, ge=1)
    enable_prepared_statements_cache: bool = Field(default=False)

    def get_connection_url(self) -> str:
        if not self.enable_prepared_statements_cache:
            separator = "&" if "?" in self.url else "?"
            return f"{self.url}{separator}prepared_statement_cache_size=0"
        return self.url


# ---------------------------------------------------------------------------
# Logging / SMTP (shared between web and mcp; formerly in web/config.py)
# ---------------------------------------------------------------------------


class SMTPSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    host: str = Field(..., description="SMTP server host")
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    from_addr: str = Field(..., alias="from")
    to: List[str] = Field(...)
    subject: str = Field(...)
    secure: Optional[bool] = None
    timeout: Optional[float] = None
    level: str = Field(default="CRITICAL")


class LoggingSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    level: str = Field(default="INFO")
    smtp: Optional[SMTPSettings] = None


class TagAccessLoggerSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    enabled: bool = Field(default=False)
    prefix: str = Field(default="tag_access")
    redis_url: Optional[str] = None


class SlackTopicSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    hooks: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Web/REST (formerly GSRestConfig / GSREST_*)
# ---------------------------------------------------------------------------


class WebSettings(BaseModel):
    """REST API configuration. New env: GRAPHSENSE_WEB__*."""

    # extra="allow" preserves today's GSRestConfig behaviour where
    # plugin configs and arbitrary YAML keys land as attributes.
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    environment: Optional[str] = None
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    # Cassandra and tagstore are intentionally NOT nested here. They live
    # at Settings.cassandra / Settings.tagstore so the REST, MCP, and
    # ingest subsystems share a single source of truth instead of each
    # carrying their own copy. Legacy GSRestConfig.database /
    # GSRestConfig.tagstore keep working via _legacy.py.

    ALLOWED_ORIGINS: Union[str, List[str]] = Field(default="*")
    hide_private_tags: bool = Field(default=False)
    show_private_tags: Optional[Dict[str, Any]] = None
    address_links_request_timeout: float = Field(default=30)
    include_pubkey_derived_tags: bool = Field(default=True)
    tag_summary_only_propagate_high_confidence_actors: bool = Field(default=True)
    user_tag_reporting_acl_group: str = Field(
        default="develop", alias="user-tag-reporting-acl-group"
    )
    enable_user_tag_reporting: bool = Field(
        default=False, alias="enable-user-tag-reporting"
    )
    privacy_preserving_tag_notifications: bool = Field(default=True)
    included_bridges: Tuple[str, ...] = Field(default_factory=tuple)
    block_by_date_use_linear_search: bool = Field(default=False)
    disable_auth: bool = Field(default=False)
    ensure_tagstore_schema_on_startup: bool = Field(
        default=False, alias="ensure-tagstore-schema-on-startup"
    )

    docs_logo_url: Optional[str] = None
    docs_favicon_url: Optional[str] = None
    docs_swagger_crosslink_url: Optional[str] = "/docs"
    docs_swagger_crosslink_label: str = "ReDoc"
    docs_redoc_crosslink_url: Optional[str] = "/ui"
    docs_redoc_crosslink_label: str = "Try the API"
    docs_external_url: Optional[str] = None
    docs_external_label: str = "External Docs"
    docs_python_client_url: Optional[str] = (
        "https://github.com/graphsense/graphsense-lib/tree/master/clients/python"
    )
    docs_python_client_label: str = "Python Client Docs"
    docs_contact_name: str = "Iknaio Cryptoasset Analytics GmbH"
    docs_contact_email: str = "contact@iknaio.com"
    docs_contact_url: str = "https://www.iknaio.com/"

    plugins: List[str] = Field(default_factory=list)
    slack_topics: Dict[str, SlackTopicSettings] = Field(default_factory=dict)
    slack_info_hook: Dict[str, SlackTopicSettings] = Field(default_factory=dict)
    tag_access_logger: Optional[TagAccessLoggerSettings] = None


# ---------------------------------------------------------------------------
# MCP (formerly GSMCPConfig / GS_MCP_*)
# ---------------------------------------------------------------------------


class SearchNeighborsSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    base_url: str = Field(..., description="Base URL of the external search service")
    api_key_env: Optional[str] = None
    auth_header: str = Field(default="Authorization")
    timeout_s: float = Field(default=660.0)
    poll_interval_s: float = Field(default=1.0)
    max_poll_time_s: float = Field(default=600.0)


class MCPSettings(BaseModel):
    """MCP server configuration. New env: GRAPHSENSE_MCP__*."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    enabled: bool = Field(default=True)
    path: str = Field(default="/mcp")
    stateless_http: bool = Field(default=True)

    curation_file: Optional[Path] = None
    strict_validation: bool = Field(default=True)

    instructions: Optional[str] = None
    instructions_file: Optional[Path] = None

    search_neighbors: Optional[SearchNeighborsSettings] = None
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    def bundled_curation_path(self) -> Path:
        # Mirror legacy GSMCPConfig.bundled_curation_path: anchored at mcp/.
        from graphsenselib import mcp as _mcp_pkg

        return Path(_mcp_pkg.__file__).parent / "curation" / "tools.yaml"

    def resolved_curation_path(self) -> Path:
        return self.curation_file or self.bundled_curation_path()

    def bundled_instructions_path(self) -> Path:
        from graphsenselib import mcp as _mcp_pkg

        return Path(_mcp_pkg.__file__).parent / "curation" / "instructions.md"

    def resolved_instructions(self) -> Optional[str]:
        if self.instructions is not None:
            return self.instructions or None
        path = self.instructions_file or self.bundled_instructions_path()
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8").strip() or None


# ---------------------------------------------------------------------------
# Per-keyspace topology
#
# In the new model there is NO ``environments`` dict — a Settings instance
# represents a single environment. Switch environments by loading a
# different overlay file (``graphsense.<env>.yaml``). Legacy YAML with a
# nested ``environments.<env>`` section gets lifted to root by the YAML
# source, so old files keep working; see ``_sources.py``.
# ---------------------------------------------------------------------------


class FileSinkSettings(BaseModel):
    directory: str


class IngestSettings(BaseModel):
    node_reference: str = Field(default="")
    secondary_node_references: List[str] = Field(default_factory=list)
    raw_keyspace_file_sinks: Dict[str, FileSinkSettings] = Field(default_factory=dict)


class KeyspaceSetupSettings(BaseModel):
    replication_config: str = Field(
        default="{'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    data_configuration: Dict[str, object] = Field(default_factory=dict)


class KeyspaceSettings(BaseModel):
    """Per-currency keyspace topology. Same shape as legacy KeyspaceConfig."""

    raw_keyspace_name: str
    transformed_keyspace_name: str
    schema_type: str
    disable_delta_updates: bool = Field(default=False)
    ingest_config: Optional[IngestSettings] = None
    keyspace_setup_config: Dict[str, KeyspaceSetupSettings] = Field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Root Settings
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Single front door for all graphsenselib configuration.

    Sources, highest priority first (see ``settings_customise_sources``):
        1. init kwargs
        2. env vars (``GRAPHSENSE_*`` and ``GRAPHSENSE_<sub>__<field>``)
        3. .env file
        4. YAML file (``GRAPHSENSE_CONFIG_YAML`` env var, or
           ``./.graphsense.yaml`` / ``~/.graphsense.yaml``)
        5. file_secret defaults

    Construction is partial-friendly: missing required submodel fields
    leave the submodel ``None`` rather than raising. Use
    :meth:`Settings.try_load` to capture validation errors instead.
    """

    model_config = SettingsConfigDict(
        env_prefix="GRAPHSENSE_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow",
    )

    # Subsystems. Each lives at root level — no duplication between
    # subsystems (e.g. REST and MCP both read `cassandra` rather than
    # each nesting their own copy).
    cassandra: Optional[CassandraSettings] = None
    tagstore: Optional[TagStoreSettings] = None
    web: Optional[WebSettings] = None
    mcp: MCPSettings = Field(default_factory=MCPSettings)

    # Per-currency keyspace topology. Lives at root — selecting an
    # environment is done at YAML-load time by picking the right overlay
    # file, not by keying into a dict here.
    keyspaces: Dict[str, KeyspaceSettings] = Field(default_factory=dict)

    # Name of the currently-loaded environment, for display. Populated
    # by the CLI from the ``--env`` flag; informational only.
    environment: Optional[str] = None

    slack_topics: Dict[str, SlackTopicSettings] = Field(default_factory=dict)
    cache_directory: str = Field(default="~/.graphsense/cache")
    coingecko_api_key: str = Field(default="")
    coinmarketcap_api_key: str = Field(default="")
    s3_credentials: Optional[Dict[str, str]] = None
    use_redis_locks: bool = Field(default=False)
    redis_url: Optional[str] = None

    # Free-form mirror of YAML `web:` block — populated by YamlConfigSource so
    # that web/app.py:resolve_rest_config()'s loose fallback path keeps working
    # even when WebSettings would have rejected a field. See plan R3.
    legacy_web_dict: Optional[Dict[str, Any]] = Field(default=None, exclude=True)

    # Informational: path of the YAML file the current Settings was loaded
    # from (overlay wins if both base and overlay were resolved). Set by
    # model_post_init after the YAML source runs. Not part of the schema —
    # excluded from model_dump / show --resolved.
    _yaml_loaded_path: Optional[Path] = PrivateAttr(default=None)

    def __init__(
        self, env: Optional[str] = None, _yaml_file: Optional[str] = None, **kwargs: Any
    ) -> None:
        # Promote legacy-prefixed env vars *before* pydantic-settings reads
        # the env. setdefault means new-prefix vars always win when both
        # are present. Emits one DeprecationWarning per legacy var.
        # TODO(deprecation): remove with _legacy.py
        from ._legacy import _apply_legacy_env_aliases

        _apply_legacy_env_aliases()

        # Stash env / yaml-file on the class so settings_customise_sources
        # can pick them up. pydantic-settings doesn't pass init kwargs
        # through to the source chain.
        setattr(type(self), "_pending_env", env)
        setattr(type(self), "_pending_yaml_file", _yaml_file)

        super().__init__(**kwargs)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        from ._sources import (
            ProvenanceTrackingSource,
            YamlConfigSource,
            _Sink,
        )

        # YAML source uses the same default-file lookup as today's
        # AppConfig (.graphsense.yaml, ./instance/config.yaml,
        # ~/.graphsense.yaml, GRAPHSENSE_CONFIG_YAML). Sits below env
        # so env always wins. When an env name is supplied it layers a
        # per-env overlay (graphsense.<env>.yaml) on top of the base.
        env = getattr(cls, "_pending_env", None)
        explicit_yaml = getattr(cls, "_pending_yaml_file", None)
        yaml_source = YamlConfigSource(
            settings_cls, explicit_file=explicit_yaml, env=env
        )

        sink = _Sink()
        # Stash the sink + yaml source on the class so model_post_init
        # can bind them to the instance. settings_customise_sources runs
        # exactly once per Settings() construction, before validation.
        cls._pending_sink = sink  # type: ignore[attr-defined]
        cls._pending_yaml_source = yaml_source  # type: ignore[attr-defined]

        return (
            ProvenanceTrackingSource(init_settings, "init", sink),
            ProvenanceTrackingSource(env_settings, "env", sink),
            ProvenanceTrackingSource(dotenv_settings, "dotenv", sink),
            ProvenanceTrackingSource(yaml_source, "_yaml_label_filled_in", sink),
            ProvenanceTrackingSource(file_secret_settings, "secrets", sink),
        )

    def model_post_init(self, __context: Any) -> None:
        from ._sources import attach_sink

        cls = type(self)
        sink = getattr(cls, "_pending_sink", None)
        yaml_source = getattr(cls, "_pending_yaml_source", None)

        # Replace the placeholder yaml label in sink entries with one
        # that includes the resolved path (or 'yaml' if no file).
        if sink is not None and yaml_source is not None:
            yaml_label = (
                f"yaml:{yaml_source.loaded_path}"
                if yaml_source.loaded_path is not None
                else "yaml"
            )
            for k, (v, label) in list(sink.data.items()):
                if label == "_yaml_label_filled_in":
                    sink.data[k] = (v, yaml_label)

        if sink is not None:
            attach_sink(self, sink)

        self._yaml_loaded_path = (
            yaml_source.loaded_path if yaml_source is not None else None
        )

        # Clear the per-class scratch so a subsequent construction starts
        # fresh; otherwise two Settings() in a row would share the sink.
        for attr in (
            "_pending_sink",
            "_pending_yaml_source",
            "_pending_env",
            "_pending_yaml_file",
        ):
            if hasattr(cls, attr):
                delattr(cls, attr)

    @property
    def yaml_loaded_path(self) -> Optional[Path]:
        """Path of the YAML file the current Settings was loaded from
        (overlay if both base and overlay were resolved). None if no
        YAML file was found."""
        return self._yaml_loaded_path

    @classmethod
    def try_load(
        cls,
        filename: Optional[str] = None,
        env: Optional[str] = None,
        **init_kwargs: Any,
    ) -> Tuple[Optional["Settings"], List[str]]:
        """Build a Settings instance, returning (None, errors) on
        ValidationError instead of raising.

        ``filename`` pins the base YAML path (bypassing the default
        lookup). ``env`` selects a per-environment overlay file and
        triggers the legacy-monolithic environments-lift.
        """
        try:
            return cls(env=env, _yaml_file=filename, **init_kwargs), []
        except ValidationError as e:
            return None, [str(err) for err in e.errors()]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_settings_singleton: Optional[Settings] = None


def get_settings() -> Settings:
    """Return the process-wide :class:`Settings`. Lazy on first call.

    Keep this lazy: the module is imported eagerly in many places, but
    construction triggers env scans that should happen *after* the user
    has had a chance to set them (CLI entrypoint, web app factory, etc.).
    """
    global _settings_singleton
    if _settings_singleton is None:
        _settings_singleton = Settings()
    return _settings_singleton


def set_settings(s: Optional[Settings]) -> None:
    """Override the singleton. ``None`` resets to lazy reload."""
    global _settings_singleton
    if _settings_singleton is not None and _settings_singleton is not s:
        from ._sources import drop_sink

        drop_sink(_settings_singleton)
    _settings_singleton = s


def reset_settings() -> None:
    """Force the next ``get_settings()`` call to rebuild from sources."""
    set_settings(None)
