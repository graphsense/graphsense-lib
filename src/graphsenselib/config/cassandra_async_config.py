from typing import Dict, List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CurrencyConfig(BaseSettings):
    """Configuration for a specific currency/network."""

    raw: Optional[str] = None
    transformed: Optional[str] = None
    balance_provider: Optional[str] = None


class CassandraConfig(BaseSettings):
    """Configuration for Cassandra database connection and settings."""

    # Required fields
    currencies: Dict[str, Optional[CurrencyConfig]] = Field(
        ..., description="Dictionary of currency configurations"
    )
    nodes: List[str] = Field(..., description="List of Cassandra node addresses")

    # Optional connection settings
    port: int = Field(default=9042, description="Cassandra port number")
    username: Optional[str] = Field(
        default=None, description="Username for authentication"
    )
    password: Optional[str] = Field(
        default=None, description="Password for authentication"
    )
    consistency_level: str = Field(
        default="LOCAL_ONE", description="Cassandra consistency level"
    )

    # Optional operational settings
    retry_interval: Optional[int] = Field(
        default=5, description="Retry interval in seconds when connection fails"
    )
    list_address_txs_ordered_legacy: bool = Field(
        default=False, description="Use legacy address transaction ordering"
    )

    cross_chain_pubkey_mapping_keyspace: Optional[str] = Field(
        default="pubkey", description="Keyspace for cross-chain public key mapping"
    )

    @field_validator("currencies", mode="before")
    @classmethod
    def validate_currencies(cls, v):
        """Convert None values to empty CurrencyConfig objects."""
        if not isinstance(v, dict):
            raise ValueError("currencies must be a dictionary")

        result = {}
        for currency, config in v.items():
            if config is None:
                result[currency] = CurrencyConfig()
            elif isinstance(config, dict):
                result[currency] = CurrencyConfig(**config)
            elif isinstance(config, CurrencyConfig):
                result[currency] = config
            else:
                raise ValueError(f"Invalid config type for currency {currency}")

        return result

    @field_validator("nodes")
    @classmethod
    def validate_nodes_not_empty(cls, v):
        """Ensure nodes list is not empty."""
        if not v:
            raise ValueError("nodes list cannot be empty")
        return v

    @field_validator("consistency_level")
    @classmethod
    def validate_consistency_level(cls, v):
        """Validate consistency level is a known Cassandra consistency level."""
        valid_levels = {
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
        if v not in valid_levels:
            raise ValueError(f"consistency_level must be one of {valid_levels}")
        return v

    model_config = SettingsConfigDict(
        extra="allow",
        env_prefix="GRAPHSENSE_CASSANDRA_ASYNC_",
    )  # Allow additional fields for extensibility
