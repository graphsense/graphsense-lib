from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Default cap on concurrent Postgres-touching coroutines per request.
# Each tagstore call checks out its own pool connection (see _inject_session
# in tagstore/db/queries.py); without bounding, a single request can fan out
# into hundreds of concurrent sessions and exhaust the pool — root cause of
# the 2026-05-04 gs-rest pool exhaustion incident.
#
# Used by clusters_service (BFS), entities_service (neighbors),
# heuristics_service (input-is-exchange), and the shared _add_labels helper.
# Override per-deployment via TagStoreReaderConfig.max_concurrency. Runtime
# value is read by callers via get_tagstore_max_concurrency().
_DEFAULT_TAGSTORE_MAX_CONCURRENCY = 8

_runtime_max_concurrency = _DEFAULT_TAGSTORE_MAX_CONCURRENCY


def get_tagstore_max_concurrency() -> int:
    """Return the active fan-out cap; defaults until set at app startup."""
    return _runtime_max_concurrency


def set_tagstore_max_concurrency(value: int) -> None:
    """Set the runtime fan-out cap. Called once during REST startup from the
    resolved TagStoreReaderConfig."""
    if value < 1:
        raise ValueError("max_concurrency must be at least 1")
    global _runtime_max_concurrency
    _runtime_max_concurrency = value


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
    max_concurrency: int = Field(
        default=_DEFAULT_TAGSTORE_MAX_CONCURRENCY,
        description=(
            "Cap on concurrent Postgres-touching coroutines per request. "
            "Must be <= pool_size + max_overflow."
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
        """Validate max concurrency is positive."""
        if v < 1:
            raise ValueError("max_concurrency must be at least 1")
        return v

    @model_validator(mode="after")
    def validate_pool_capacity(self):
        """Ensure the pool can satisfy the bounded fan-out cap. Otherwise wide
        requests deadlock on pool checkout."""
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
