from typing import Optional

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Fallback fan-out cap used by get_tagstore_max_concurrency() when no
# TagStoreReaderConfig has been registered (CLI tools, tests). Live
# deployments derive max_concurrency from pool_size — see
# _derive_max_concurrency below.
#
# The cap exists because each tagstore call checks out its own pool
# connection (see _inject_session in tagstore/db/queries.py); without
# bounding, a single request can fan out into hundreds of concurrent
# sessions and exhaust the pool — root cause of the 2026-05-04 gs-rest
# pool exhaustion incident.
#
# Used by clusters_service (BFS), entities_service (neighbors),
# heuristics_service (input-is-exchange), and the shared _add_labels helper.
_DEFAULT_TAGSTORE_MAX_CONCURRENCY = 8


def _derive_max_concurrency(pool_size: int) -> int:
    """Per-request fan-out cap given a steady pool size.

    Caps a single request to ~1/3 of the steady pool, so up to three
    concurrent wide requests fit before traffic spills into max_overflow.
    Overflow stays as system burst budget. Floor of 2 so tiny test pools
    still allow some parallelism.
    """
    return max(2, pool_size // 3)


# Holds the active TagStoreReaderConfig registered at REST startup. The
# get_tagstore_max_concurrency() accessor reads from it directly, so the
# config field is the single source of truth — no mirror, no drift.
_active_config: Optional["TagStoreReaderConfig"] = None


def set_active_tagstore_config(config: Optional["TagStoreReaderConfig"]) -> None:
    """Register the active TagStoreReaderConfig as runtime source of truth.

    Pass None to clear (useful in tests). Called once during REST startup
    from setup_database() with the resolved tagstore config.
    """
    global _active_config
    _active_config = config


def get_active_tagstore_config() -> Optional["TagStoreReaderConfig"]:
    """Return the currently active TagStoreReaderConfig, or None if unset."""
    return _active_config


def get_tagstore_max_concurrency() -> int:
    """Return the active fan-out cap.

    Reads `max_concurrency` directly from the registered TagStoreReaderConfig.
    Falls back to the package default when no config has been activated (e.g.
    in CLI tools or tests that don't go through REST startup).
    """
    if _active_config is not None and _active_config.max_concurrency is not None:
        return _active_config.max_concurrency
    return _DEFAULT_TAGSTORE_MAX_CONCURRENCY


class TagStoreReaderConfig(BaseSettings):
    """Configuration for TagStore database connection and settings."""

    # Required fields
    url: str = Field(..., description="Database connection URL")

    # Optional connection pool settings
    pool_size: int = Field(
        default=50, description="Number of connections to maintain in pool"
    )
    pool_timeout: int = Field(
        default=10, description="Timeout in seconds to get connection from pool"
    )
    max_overflow: int = Field(
        default=10, description="Maximum overflow connections beyond pool_size"
    )
    pool_recycle: int = Field(
        default=3600, description="Time in seconds to recycle connections"
    )
    max_concurrency: Optional[int] = Field(
        default=None,
        description=(
            "Cap on concurrent Postgres-touching coroutines per request. "
            "Defaults to max(2, pool_size // 3) — caps each request at ~1/3 "
            "of the steady pool so up to three concurrent wide requests fit "
            "before spilling into max_overflow. Must be <= pool_size + "
            "max_overflow."
        ),
    )

    # Optional performance settings
    enable_prepared_statements_cache: bool = Field(
        default=False, description="Enable prepared statements cache"
    )

    @field_validator("pool_size")
    @classmethod
    def validate_pool_size(cls, v):
        """Validate pool size is positive."""
        if v < 1:
            raise ValueError("pool_size must be at least 1")
        return v

    @field_validator("pool_timeout")
    @classmethod
    def validate_pool_timeout(cls, v):
        """Validate pool timeout is positive."""
        if v < 1:
            raise ValueError("pool_timeout must be at least 1")
        return v

    @field_validator("max_overflow")
    @classmethod
    def validate_max_overflow(cls, v):
        """Validate max overflow is non-negative."""
        if v < 0:
            raise ValueError("max_overflow must be non-negative")
        return v

    @field_validator("pool_recycle")
    @classmethod
    def validate_pool_recycle(cls, v):
        """Validate pool recycle is positive."""
        if v < 1:
            raise ValueError("pool_recycle must be at least 1")
        return v

    @field_validator("max_concurrency")
    @classmethod
    def validate_max_concurrency(cls, v):
        """Validate explicit max_concurrency is positive (None means derive)."""
        if v is not None and v < 1:
            raise ValueError("max_concurrency must be at least 1")
        return v

    @model_validator(mode="after")
    def derive_and_validate_max_concurrency(self):
        """Fill in derived max_concurrency when unset, then enforce that the
        pool can satisfy it. Wide requests would otherwise deadlock on pool
        checkout."""
        if self.max_concurrency is None:
            self.max_concurrency = _derive_max_concurrency(self.pool_size)
        capacity = self.pool_size + self.max_overflow
        if capacity < self.max_concurrency:
            raise ValueError(
                f"pool_size ({self.pool_size}) + max_overflow ({self.max_overflow}) "
                f"= {capacity} must be >= max_concurrency ({self.max_concurrency})"
            )
        return self

    def get_connection_url(self) -> str:
        """Get the connection URL with prepared statement cache setting."""
        if not self.enable_prepared_statements_cache:
            separator = "&" if "?" in self.url else "?"
            return f"{self.url}{separator}prepared_statement_cache_size=0"
        return self.url

    model_config = SettingsConfigDict(
        extra="allow",
        env_prefix="GRAPHSENSE_TAGSTORE_READ_",
    )
