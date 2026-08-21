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

    links_adaptive_fetch_size_cap: int = Field(
        default=8192,
        description=(
            "Upper bound for the adaptive candidate-batch ramp in list_links. "
            "When > 0, the candidate scan starts at max(FETCH_SIZE_MIN, "
            "pagesize) and doubles after every iteration that does not fill "
            "the requested page, up to this cap — turning the "
            "needle-in-haystack case (few links inside a huge tx history) "
            "from thousands of sequential round trips into dozens. 0 keeps "
            "the fixed batch size (previous behavior). Batch size never "
            "affects which links are returned or pagination tokens (tokens "
            "are absolute tx positions)."
        ),
    )
    links_per_tx_asset_probe_enabled: bool = Field(
        default=True,
        description=(
            "For eth-like networks, probe each links candidate tx only in "
            "the asset of the candidate transfer itself instead of the "
            "whole batch's asset union. A link is the same transfer seen "
            "from both sides, so the second side's row carries the same "
            "currency — probing other assets can never match."
        ),
    )
    txs_secondary_group_scan_window: int = Field(
        default=64,
        description=(
            "Maximum number of consecutive secondary id groups (block "
            "buckets) list_address_txs_ordered may fetch concurrently per "
            "round when scanning an eth-like address's history. 1 keeps the "
            "previous one-bucket-at-a-time behavior. With > 1 the window "
            "starts at 1 and doubles after every round that fails to fill "
            "its batch, so sparse histories cross empty buckets in "
            "logarithmically many round trips instead of one per bucket. "
            "Results and page tokens are unaffected (tokens are absolute "
            "tx positions; a row's bucket is derived from its tx id)."
        ),
    )
    links_probe_in_batch_size: int = Field(
        default=200,
        description=(
            "Transport-level batching for the links candidate probes: instead "
            "of one point read per candidate tx, up to this many tx ids that "
            "share a partition, direction and asset are fetched with a single "
            "'transaction_id IN (...)' query (single-partition named slices — "
            "no ALLOW FILTERING; the tx_reference filter stays client-side). "
            "Results are redistributed into the exact per-candidate sub-result "
            "lists the point probes would produce, so merge semantics, "
            "results and page tokens are unchanged. Benchmarked 17-30x faster "
            "on the probe phase against prod. Ablation (2026-08): 200 is "
            "wall-time-equal to 100 but issues ~30% fewer queries; 50 issued "
            "~55% more queries with no wall gain — don't lower this. 0 or 1 "
            "restores per-candidate point probes."
        ),
    )
    links_sparse_direction_race_enabled: bool = Field(
        default=True,
        description=(
            "For eth-like first-page links queries where the relations "
            "pre-check shows the whole edge fits in one page "
            "(no_transactions < pagesize), additionally run the scan in the "
            "opposite direction and take whichever completes first. The "
            "flipped scan's result is only used when it provably found the "
            "complete link set (ended with no next page and fewer rows than "
            "the requested pagesize); it is then re-sorted into the "
            "requested order, so results and page tokens are unchanged. "
            "Fixes the asc/desc asymmetry where links sit at the far end of "
            "the scanned history (an ascending scan otherwise walks the "
            "whole history before finding them). Costs at most a second, "
            "concurrent, early-cancelled scan for these sparse-edge queries."
        ),
    )
    links_probe_in_min_statements: int = Field(
        default=500,
        description=(
            "Probe rounds with at most this many statements keep per-candidate "
            "point reads (they fly fully concurrently and spread over all "
            "replicas); IN-batching only activates above it, where client-side "
            "per-query overhead dominates. Matches the probe concurrency "
            "window by default. Ablation (2026-08): lowering to 200 was only "
            "~5-8% faster on huge scans and was not adopted — IN-batching "
            "small probe rounds regressed fast requests badly (0.3s -> 2.8s "
            "when applied unconditionally), so keep this at or above the "
            "probe concurrency window."
        ),
    )
    links_direction_race_hedge_delay_ms: int = Field(
        default=1000,
        description=(
            "The sparse-edge direction race only launches its "
            "opposite-direction scan after the requested direction has failed "
            "to answer within this delay (hedged request): fast queries never "
            "pay for a second scan, while the pathological wrong-direction "
            "scans it exists for run for minutes. Ablation (2026-08): 250ms/"
            "500ms sped the pathological case up ~15-30% but fire spuriously "
            "for any moderately slow request (extra scan + contention), and "
            "2000ms bought the moderate band nothing (its regression was "
            "duplicated prechecks, since fixed by passing _precheck into the "
            "raced scans) — 1000ms kept."
        ),
    )
    links_candidate_prefetch_enabled: bool = Field(
        default=True,
        description=(
            "Fetch the next candidate page of a links scan concurrently with "
            "probing the current one (pipelining the two serial phases). "
            "Candidate pages are deterministic in (page token, batch size), "
            "so results are unchanged; on early termination the in-flight "
            "prefetch is discarded."
        ),
    )
    links_slim_candidate_columns_enabled: bool = Field(
        default=True,
        description=(
            "Fetch only the columns the links intersection actually reads "
            "from the candidate side (tx id, tx_reference and currency for "
            "eth-like; tx id and value for UTXO) instead of the full row, "
            "cutting client-side row decoding on scans over huge histories. "
            "Ablation (2026-08): worth little on its own (~3%) but compounds "
            "with candidate prefetch (decode competes with probe processing "
            "for the interpreter once the phases overlap): prefetch alone "
            "0.75x, prefetch+slim 0.62x on the worst scans."
        ),
    )
    links_directed_probe_enabled: bool = Field(
        default=True,
        description=(
            "For eth-like networks, probe the second (larger) side of a "
            "links query only in the direction the link can actually have "
            "(candidates are the first side's outgoing txs, so the second "
            "side can only hold them as incoming, and vice versa) instead "
            "of both directions — halving the point lookups against the "
            "large partition. UTXO always probes both directions because "
            "its address_transactions rows carry only the net direction."
        ),
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
