from typing import Dict, List, Optional, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CurrencyConfig(BaseSettings):
    """Configuration for a specific currency/network."""

    raw: Optional[str] = None
    transformed: Optional[str] = None
    balance_provider: Optional[str] = None


class CassandraConfig(BaseSettings):
    """Configuration for Cassandra database connection and settings."""

    # Driver field (accessed by setup_database)
    driver: str = Field(default="cassandra", description="Database driver")

    # Connection fields
    currencies: Dict[str, Optional[CurrencyConfig]] = Field(
        default={
            "btc": None,
            "bch": None,
            "ltc": None,
            "zec": None,
            "eth": None,
            "trx": None,
        },
        description="Dictionary of currency configurations",
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
    consistency_level_fallback: bool = Field(
        default=False,
        description=(
            "If true and consistency_level=LOCAL_QUORUM, allow the read path "
            "to downgrade to LOCAL_ONE on the first Unavailable / ReadTimeout "
            "when at least one replica is alive. Lets the web tier survive "
            "rolling restarts on RF=2 at the cost of read-after-write guarantees."
        ),
    )

    strict_data_validation: bool = Field(
        default=True, description="Enable strict data validation"
    )

    # Optional operational settings
    retry_interval: Optional[int] = Field(
        default=5, description="Retry interval in seconds when connection fails"
    )
    list_address_txs_ordered_legacy: bool = Field(
        default=False, description="Use legacy address transaction ordering"
    )
    fanout_bounding_and_links_precheck_enabled: bool = Field(
        default=True,
        description=(
            "Master switch for the serving-path query optimizations that "
            "trust precomputed aggregate data. (1) Token fan-out bounding: "
            "address tx listings and links only query the tokens an address "
            "actually used (derived from the address rows' "
            "total_tokens_received/total_tokens_spent maps) instead of every "
            "configured token. (2) Links pre-check: links queries point-look "
            "up the directed edge in the relations tables to return "
            "immediately when no edge exists and to stop paging once all of "
            "the edge's no_transactions txs are found. Disable to restore "
            "the previous unbounded/full-scan behavior if those aggregates "
            "are suspected to be incomplete (token txs missing from "
            "listings, links missing txs or coming back empty). Note that "
            "tokens absent from token_configuration are never queried "
            "regardless of this setting (a warning is logged when an "
            "address used such tokens)."
        ),
    )

    cross_chain_pubkey_mapping_keyspace: Optional[Union[str, List[str]]] = Field(
        default="pubkey",
        description=(
            "Keyspace(s) the REST API READS cross-chain pubkey→address mappings "
            "from. Defaults to the legacy 'pubkey' keyspace. The pubkey-update "
            "job writes to a fresh keyspace by default (pubkey_v2); point this "
            "there once that data is validated, or set to null to disable the "
            "lookup. May also be a LIST of keyspaces (e.g. [pubkey_v2, pubkey]) "
            "— the reader looks the address up in each and merges the derived "
            "addresses, so the legacy keyspace can still contribute keys the new "
            "pipeline cannot reproduce (e.g. doge-sourced). Only keyspaces that "
            "actually contain a 'pubkey_by_address' table are used; the feature "
            "auto-enables if at least one does."
        ),
    )

    def get_cross_chain_pubkey_keyspaces(self) -> List[str]:
        """Normalise cross_chain_pubkey_mapping_keyspace to a list of keyspaces."""
        ks = self.cross_chain_pubkey_mapping_keyspace
        if ks is None:
            return []
        if isinstance(ks, str):
            return [ks]
        return list(ks)

    ignore_traces_not_found_in_list_txs: bool = Field(
        default=True,
        description="Ignore missing traces in list_address_txs for Ethereum-like currencies",
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
        env_prefix="GS_CASSANDRA_ASYNC_",
    )  # Allow additional fields for extensibility
