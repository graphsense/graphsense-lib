from typing import Any, Dict, List, Optional, Tuple, Union

from graphsenselib.config.cassandra_async_config import CassandraConfig
from graphsenselib.config.config import SlackTopic
from graphsenselib.config.tagstore_config import TagStoreReaderConfig
from pydantic import ConfigDict, Field
from pydantic_settings import BaseSettings


class SMTPConfig(BaseSettings):
    model_config = ConfigDict(populate_by_name=True)

    host: str = Field(..., description="SMTP server host")
    port: Optional[int] = Field(default=None, description="SMTP server port")
    username: Optional[str] = Field(default=None, description="SMTP username")
    password: Optional[str] = Field(default=None, description="SMTP password")
    from_addr: str = Field(..., alias="from", description="From email address")
    to: List[str] = Field(..., description="List of recipient email addresses")
    subject: str = Field(..., description="Email subject")
    secure: Optional[bool] = Field(default=None, description="Use secure connection")
    timeout: Optional[float] = Field(default=None, description="Connection timeout")
    level: str = Field(default="CRITICAL", description="Log level for SMTP handler")


class LoggingConfig(BaseSettings):
    level: str = Field(default="INFO", description="Logging level")
    smtp: Optional[SMTPConfig] = Field(
        default=None, description="SMTP configuration for email logging"
    )


class TagAccessLoggerConfig(BaseSettings):
    enabled: bool = Field(default=False, description="Enable tag access logging")
    prefix: str = Field(
        default="tag_access",
        description="Prefix for Redis keys used in tag access logging",
    )
    redis_url: Optional[str] = Field(
        default=None, description="Redis URL for tag access logging"
    )


class GSRestConfig(BaseSettings):
    model_config = ConfigDict(env_prefix="GSREST_", case_sensitive=False, extra="allow")

    environment: Optional[str] = Field(default=None, description="Environment name")
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig, description="Logging configuration"
    )
    database: Optional[CassandraConfig] = Field(
        default=None, description="Database configuration"
    )
    tagstore: Optional[TagStoreReaderConfig] = Field(
        default=None, alias="gs-tagstore", description="Tagstore configuration"
    )
    ALLOWED_ORIGINS: Union[str, List[str]] = Field(
        default="*", description="CORS allowed origins"
    )
    hide_private_tags: bool = Field(
        default=False, description="Whether to hide private tags"
    )
    show_private_tags: Optional[Dict[str, Any]] = Field(
        default=None, description="Show private tags configuration"
    )
    address_links_request_timeout: float = Field(
        default=30, description="Timeout for address links requests endpoint"
    )
    include_pubkey_derived_tags: bool = Field(
        default=True,
        description="Include pubkey derived tags in tag summaries and lists",
    )
    tag_summary_only_propagate_high_confidence_actors: bool = Field(
        default=True,
        description="Only propagate high confidence actors in tag summaries",
    )
    user_tag_reporting_acl_group: str = Field(
        default="develop",
        alias="user-tag-reporting-acl-group",
        description="ACL group for user tag reporting",
    )
    enable_user_tag_reporting: bool = Field(
        default=False,
        alias="enable-user-tag-reporting",
        description="Enable user tag reporting functionality",
    )
    privacy_preserving_tag_notifications: bool = Field(
        default=True,
        description="Enable privacy preserving tag notifications",
    )
    included_bridges: Tuple[str, ...] = Field(
        default_factory=tuple,
        description="Included bridges in tx conversions",
    )
    block_by_date_use_linear_search: bool = Field(
        default=False,
        description="Use linear search for block by date queries",
    )

    disable_auth: bool = Field(
        default=False,
        description="Disable API key authentication (removes security scheme from OpenAPI spec)",
    )

    plugins: List[str] = Field(
        default_factory=list, description="List of plugin modules to load"
    )
    slack_info_hook: Dict[str, SlackTopic] = Field(
        default_factory=dict, description="Slack info hook"
    )

    tag_access_logger: Optional[TagAccessLoggerConfig] = Field(
        default=None, description="Tag access logger configuration"
    )

    def get_plugin_config(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific plugin"""
        return getattr(self, plugin_name, None)

    def get_max_concurrency_bulk(self, operation_name: str, default: int = 10) -> int:
        """Get max concurrency for bulk operations with fallback to defaults"""
        config_key = f"max_concurrency_bulk_{operation_name}"
        # First check if it's set as a direct attribute (from extra fields)
        if hasattr(self, config_key):
            return getattr(self, config_key)
        # Then check database config if it exists
        if self.database and hasattr(self.database, config_key):
            return getattr(self.database, config_key)
        # Fall back to default
        return default

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "GSRestConfig":
        """Create config from dictionary (for backward compatibility with YAML loading)"""
        # Handle nested logging config
        if "logging" in config_dict and isinstance(config_dict["logging"], dict):
            logging_config = config_dict["logging"].copy()
            # Handle nested SMTP config
            if "smtp" in logging_config and isinstance(logging_config["smtp"], dict):
                logging_config["smtp"] = SMTPConfig(**logging_config["smtp"])
            config_dict["logging"] = LoggingConfig(**logging_config)

        # Handle nested database config
        if "database" in config_dict and isinstance(config_dict["database"], dict):
            config_dict["database"] = CassandraConfig(**config_dict["database"])

        # Handle nested tagstore config (with gs-tagstore key)
        if "gs-tagstore" in config_dict and isinstance(
            config_dict["gs-tagstore"], dict
        ):
            config_dict["gs-tagstore"] = TagStoreReaderConfig(
                **config_dict["gs-tagstore"]
            )

        return cls(**config_dict)
