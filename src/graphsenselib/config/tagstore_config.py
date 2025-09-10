from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class TagStoreReaderConfig(BaseSettings):
    """Configuration for TagStore database connection and settings."""

    # Required fields
    url: str = Field(..., description="Database connection URL")

    # Optional connection pool settings
    pool_size: int = Field(
        default=50, description="Number of connections to maintain in pool"
    )
    pool_timeout: int = Field(
        default=300, description="Timeout in seconds to get connection from pool"
    )
    max_overflow: int = Field(
        default=10, description="Maximum overflow connections beyond pool_size"
    )
    pool_recycle: int = Field(
        default=3600, description="Time in seconds to recycle connections"
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
