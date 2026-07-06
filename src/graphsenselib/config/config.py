from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from goodconf import Field, GoodConf, GoodConfConfigDict
from goodconf import FileConfigSettingsSource, _load_config
from pydantic import BaseModel, field_validator, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from ..utils import first_or_default, flatten, resolve_env_vars

logger = logging.getLogger(__name__)

supported_base_currencies = ["btc", "eth", "ltc", "bch", "zec", "trx"]
default_environments = ["prod", "test", "dev", "exp1", "exp2", "exp3", "pytest"]
schema_types = ["utxo", "account", "account_trx"]
keyspace_types = ["raw", "transformed"]
currency_to_schema_type = {
    cur: "account_trx" if cur == "trx" else "account" if cur == "eth" else "utxo"
    for cur in supported_base_currencies
}
currency_to_public_schema_type = {
    cur: "account" if cur in ["eth", "trx"] else "utxo"
    for cur in supported_base_currencies
}
supported_fiat_currencies = ["USD", "EUR"]
EXCHANGE_RATES_PROVIDERS = ["coingecko", "coinmarketcap", "cryptocompare"]
avg_blocktimes_by_currencies = {
    "trx": 7,
    "eth": 15,
    "btc": 600,
    "bch": 600,
    "zec": 75,
    "ltc": 150,
}

reorg_backoff_blocks = {
    "trx": 20,
    "eth": 70,
    "btc": 3,
    "bch": 15,
    "zec": 150,
    "ltc": 12,
}

# Chains that forked off another chain and therefore share the base chain's
# entire history up to the fork block (e.g. BCH split from BTC at block 478558 —
# blocks 0..478558 are byte-identical on both). Keyed by the fork chain ->
# {"base": <parent chain>, "fork_block": <last shared block height>}.
# Single source of truth for fork-awareness: the REST fork-overlap handler uses
# the (base, fork) pair, and the pubkey job uses fork_block to skip re-extracting
# the shared pre-fork history. These are immutable protocol facts — keep them
# here in code, NOT in graphsense.yaml (a per-env typo would silently corrupt
# cross-chain handling).
chain_forks: Dict[str, Dict[str, Any]] = {
    "bch": {"base": "btc", "fork_block": 478558},
}


GRAPHSENSE_DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
GRAPHSENSE_VERBOSE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

VALID_CONSISTENCY_LEVELS = {
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
VALID_SERIAL_CONSISTENCY_LEVELS = {"SERIAL", "LOCAL_SERIAL"}


def is_fresh_clustering_enabled(currency: str) -> bool:
    """Per-currency fresh-clustering rollout switch.

    ``GRAPHSENSE_FRESH_CLUSTERING_CURRENCIES`` holds a comma-separated list of
    network codes (e.g. ``ltc`` or ``ltc,btc``); fresh clustering is enabled
    for a currency iff it is in the list. Read by the REST entity endpoints,
    the delta updater and the one-off clustering commands, so currencies can
    be cut over one at a time.
    """
    enabled = os.environ.get("GRAPHSENSE_FRESH_CLUSTERING_CURRENCIES", "")
    return currency.strip().lower() in {
        c.strip().lower() for c in enabled.split(",") if c.strip()
    }


def is_tagstore_fresh_clusters_enabled() -> bool:
    """Tagstore fresh-clustering switch — flips the whole cluster-tag path,
    read **and** feed, to the ``*_v2`` relations in one move:

    - REST reads resolve to ``address_cluster_mapping_v2`` / ``*_v2`` MVs;
    - the tagpack ``sync`` feeder reads cluster membership from
      ``fresh_address_cluster`` / ``fresh_cluster_stats`` (Cassandra) and writes
      ``address_cluster_mapping_v2`` (Postgres), refreshing the ``*_v2`` MVs.

    Exclusive switch: while on, the legacy ``address_cluster_mapping`` is no
    longer maintained. Independent of ``is_fresh_clustering_enabled`` (the
    Cassandra write side / one-off clustering job). Flip on only after
    ``address_cluster_mapping_v2`` has been populated by a full feeder rerun,
    else cluster-tag reads come back empty."""
    return os.environ.get("GRAPHSENSE_TAGSTORE_FRESH_CLUSTERS", "false").lower() in (
        "1",
        "true",
        "yes",
    )


CASSANDRA_DEFAULT_REPLICATION_CONFIG = (
    "{'class': 'SimpleStrategy', 'replication_factor': 1}"
)


def get_default_data_configuration(
    currency: str, keyspace_type: str
) -> Dict[str, object]:
    """Get default data_configuration for a specific currency and keyspace type.

    Args:
        currency: The currency/network code (e.g., 'btc', 'eth', 'trx')
        keyspace_type: Either 'raw' or 'transformed'

    Returns:
        Dictionary with default data_configuration for the given currency and keyspace type
    """
    currency = currency.lower()

    # The PK column ("id" for raw, "keyspace_name" for transformed) is
    # intentionally omitted: it must match the actual target keyspace name and
    # is injected at seed time in schema.create_keyspace_if_not_exist.
    # Hardcoding it here used to write a prefix-only row (e.g. id="zec_raw")
    # into dated keyspaces (e.g. zec_raw_20260423), producing two configuration
    # rows after the first real ingest.

    # Configuration for account-based currencies (eth, trx)
    if currency == "eth":
        if keyspace_type == "raw":
            return {
                "block_bucket_size": 1000,
                "tx_prefix_length": 5,
            }
        else:  # transformed
            return {
                "address_prefix_length": 5,
                "bucket_size": 25000,
                "tx_prefix_length": 5,
                "fiat_currencies": ["EUR", "USD"],
            }
    elif currency == "trx":
        if keyspace_type == "raw":
            return {
                "block_bucket_size": 1000,
                "tx_prefix_length": 5,
            }
        else:  # transformed
            return {
                "address_prefix_length": 5,
                "bucket_size": 10000,
                "tx_prefix_length": 5,
                "fiat_currencies": ["EUR", "USD"],
            }

    # Configuration for UTXO-based currencies (btc, bch, ltc, zec)
    elif currency in ["btc", "bch", "ltc", "zec"]:
        if keyspace_type == "raw":
            return {
                "block_bucket_size": 100,
                "tx_bucket_size": 25000,
                "tx_prefix_length": 5,
            }
        else:  # transformed
            bech32_prefixes = {
                "btc": "bc",
                "bch": "",
                "ltc": "ltc1",
                "zec": "",
            }
            return {
                "address_prefix_length": 4,
                "bech_32_prefix": bech32_prefixes.get(currency, ""),
                "bucket_size": 5000,
                "coinjoin_filtering": True,
                "fiat_currencies": ["EUR", "USD"],
            }

    return {}


def is_account_based_currency(currency: str) -> bool:
    return currency_to_public_schema_type.get(currency.lower()) == "account"


def get_reorg_backoff_blocks(network: str) -> int:
    """For imports we do not want to always catch up with the latest block
    since we want to avoid reorgs lead to spurious data in the database.

    Default conservative estimate is 2h worth of blocks lag.

    Args:
        network (str): currency/network label
    """
    return reorg_backoff_blocks[network.lower()]


class _WarnExtraModel(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def _warn_unknown_keys(cls, data):
        if isinstance(data, dict):
            known = set(cls.model_fields.keys())
            unknown = set(data.keys()) - known
            for key in sorted(unknown):
                logger.warning(
                    f"Unknown key '{key}' in {cls.__name__} config — ignoring"
                )
        return data


class FileSink(_WarnExtraModel):
    directory: str
    s3_config: Optional[str] = None


class IngestConfig(_WarnExtraModel):
    node_reference: str = Field(default_factory=lambda: "")
    secondary_node_references: List[str] = Field(default_factory=lambda: [])
    raw_keyspace_file_sinks: Dict[str, FileSink] = Field(default_factory=lambda: {})
    source_max_workers: int = 5
    raw_ingest_staleness_threshold: Optional[int] = Field(
        default=None,
        description=(
            "Staleness tolerance in hours for the post-ingest check. When set, "
            "`ingest from-node` verifies after every run that the timestamp of "
            "the highest ingested raw block is not older than this many hours "
            "and sends a notification otherwise (same check as `monitoring "
            "monitor-raw-ingest`). Unset disables the automatic check."
        ),
    )
    exchange_rates_provider: Optional[str] = Field(
        default=None,
        description=(
            "When set, `ingest from-node` ingests the latest exchange rates "
            "from this provider into the raw keyspace before ingesting blocks "
            "(same as `exchange-rates <provider> ingest --abort-on-gaps`). "
            "Supported: coingecko, coinmarketcap, cryptocompare (each needs "
            "its API key in the config). Unset disables the step."
        ),
    )

    @field_validator("exchange_rates_provider")
    @classmethod
    def _validate_exchange_rates_provider(cls, v):
        if v is not None and v not in EXCHANGE_RATES_PROVIDERS:
            raise ValueError(
                f"exchange_rates_provider must be one of {EXCHANGE_RATES_PROVIDERS}"
            )
        return v

    @property
    def all_node_references(self) -> List[str]:
        return [self.node_reference] + self.secondary_node_references

    def get_first_node_reference(self, protocol: str = "http") -> Optional[str]:
        return first_or_default(
            self.all_node_references, lambda x: x.startswith(protocol)
        )


class KeyspaceSetupConfig(_WarnExtraModel):
    replication_config: str = Field(
        default_factory=lambda: CASSANDRA_DEFAULT_REPLICATION_CONFIG
    )
    data_configuration: Dict[str, object] = Field(default_factory=lambda: {})


class DeltaUpdaterConfig(BaseModel):
    delta_sink: Optional[FileSink]
    currency: str
    s3_credentials: Optional[Dict[str, str]]


class KeyspaceConfig(_WarnExtraModel):
    raw_keyspace_name: str
    transformed_keyspace_name: str
    schema_type: str
    disable_delta_updates: bool = Field(default_factory=lambda: False)
    ingest_config: Optional[IngestConfig]
    keyspace_setup_config: Dict[str, KeyspaceSetupConfig] = Field(
        default_factory=lambda: {kst: KeyspaceSetupConfig() for kst in keyspace_types}
    )

    @field_validator("schema_type")
    def schema_type_in_range(cls, v):
        assert v.lower() in schema_types, (
            f"Schema must be either {', '.join(schema_types)}"
        )
        return v.lower()

    @field_validator("transformed_keyspace_name")
    def keyspace_prefix_match(cls, v, info):
        raw = info.data["raw_keyspace_name"]

        if v[:3] != raw[:3]:
            raise ValueError(f"Keyspace prefix do not match {raw} and {v}")

        return v

    @field_validator("schema_type")
    def keyspace_prefix_matches_schema(cls, v, info):
        raw = info.data["raw_keyspace_name"]
        key = raw[:3].lower()
        if key in currency_to_schema_type:
            schema = currency_to_schema_type[key]

            if v != schema:
                raise ValueError(
                    "Configured schema type does not match schema "
                    f"type of currency {schema} != {v}"
                )

        return v

    def model_post_init(self, __context):
        """Populate data_configuration with defaults if not provided.

        This is called after model initialization to populate empty
        data_configuration dictionaries with defaults based on currency.
        """
        # Extract currency from raw_keyspace_name
        currency = self.raw_keyspace_name[:3].lower() if self.raw_keyspace_name else ""

        if currency:
            for keyspace_type in keyspace_types:
                if keyspace_type in self.keyspace_setup_config:
                    # Only populate if data_configuration is empty
                    if not self.keyspace_setup_config[keyspace_type].data_configuration:
                        self.keyspace_setup_config[
                            keyspace_type
                        ].data_configuration = get_default_data_configuration(
                            currency, keyspace_type
                        )


class PubkeyConfig(_WarnExtraModel):
    """Defaults for the cross-chain pubkey-update job for this environment.

    Supplies the shared sink path and backend so ``transformation
    pubkey-update`` need not pass ``--sink-path`` every run; explicit CLI
    flags still override. The Cassandra-overwrite warning fires regardless
    of where the value came from.
    """

    sink_path: str
    sink_type: str = "cassandra"
    keyspace: Optional[str] = Field(
        default=None,
        description=(
            "Cassandra keyspace the pubkey-update job WRITES to. Defaults to "
            "the job's fresh default (pubkey_v2) when unset, so it never appends "
            "into a legacy 'pubkey' table. The REST reader selects its source "
            "separately via cross_chain_pubkey_mapping_keyspace."
        ),
    )

    @field_validator("sink_type")
    @classmethod
    def _validate_sink_type(cls, v):
        if v not in ("cassandra", "delta"):
            raise ValueError("sink_type must be 'cassandra' or 'delta'")
        return v


class Environment(_WarnExtraModel):
    cassandra_nodes: List[str]
    username: Optional[str] = Field(default_factory=lambda: None)
    password: Optional[str] = Field(default_factory=lambda: None)
    readonly_username: Optional[str] = Field(default_factory=lambda: None)
    readonly_password: Optional[str] = Field(default_factory=lambda: None)
    consistency_level: str = Field(
        default="LOCAL_QUORUM",
        description=(
            "Cassandra consistency level for the synchronous (ingest/write) "
            "connection. Applies to the whole ExecutionProfile."
        ),
    )
    serial_consistency_level: str = Field(
        default="LOCAL_SERIAL",
        description=(
            "Cassandra serial consistency level for the synchronous connection. "
            "Only affects lightweight transactions (IF NOT EXISTS upserts)."
        ),
    )
    keyspaces: Dict[str, KeyspaceConfig]
    pubkey: Optional[PubkeyConfig] = None

    @field_validator("consistency_level")
    @classmethod
    def _validate_consistency_level(cls, v):
        if v not in VALID_CONSISTENCY_LEVELS:
            raise ValueError(
                f"consistency_level must be one of {sorted(VALID_CONSISTENCY_LEVELS)}"
            )
        return v

    @field_validator("serial_consistency_level")
    @classmethod
    def _validate_serial_consistency_level(cls, v):
        if v not in VALID_SERIAL_CONSISTENCY_LEVELS:
            raise ValueError(
                "serial_consistency_level must be one of "
                f"{sorted(VALID_SERIAL_CONSISTENCY_LEVELS)}"
            )
        return v

    def get_configured_currencies(self) -> List[str]:
        return list(self.keyspaces.keys())

    def get_keyspace(self, currency: str) -> KeyspaceConfig:
        return self.keyspaces[currency]


class SlackTopic(_WarnExtraModel):
    hooks: List[str] = Field(default_factory=lambda: [])


# Maven coordinates of the graphsense-spark job's runtime dependencies. Only
# needed for the "slim" artifact; the "fat" (assembly) jar bundles these. The
# slim path also needs the spark-packages resolver for graphframes.
DEFAULT_SCALA_JOB_PACKAGES = [
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
    "org.rogach:scallop_2.12:4.1.0",
    "joda-time:joda-time:2.10.10",
    "org.web3j:core:4.8.7",
    "org.web3j:abi:4.8.7",
    "graphframes:graphframes:0.8.3-spark3.5-s_2.12",
]


class SidecarConfig(BaseModel):
    """Opt-in Cassandra Sidecar bulk-write path for the full transform.

    When enabled the runner adds the cassandra-analytics package (needed even
    with the fat jar, where it is not bundled), the SSTable-writer JVM module
    flags + temp-dir redirect, and the job's --writer/--sidecar-* arguments.
    """

    enabled: bool = False
    contact_points: List[str] = Field(default_factory=list)
    local_dc: Optional[str] = None
    consistency_level: str = "LOCAL_QUORUM"


class FullTransformArgs(BaseModel):
    """Arguments for the `transformation raw-to-transformed` command.

    Drives the raw -> transformed ("full transform") graph computation. The
    command itself and its neutral options (env, currency, suffix, keyspaces,
    spark profile, dry-run) are intentionally backend-agnostic so that a future
    native-PySpark implementation can be selected via `backend: pyspark` (or
    --backend) without changing how the command is invoked. The remaining
    fields (repo/version/artifact/main_class/packages/repositories/jar_args/
    extra_submit_args) are specific to the current Scala spark-submit backend.
    """

    # Implementation backend. "scala" launches the external graphsense-spark jar
    # via spark-submit; "pyspark" is reserved for a future native rewrite.
    backend: str = "scala"

    # spark_config profile to use per currency (falls back to baseline/flat).
    # Shared across backends — both resolve Spark properties from spark_config.
    spark_profile: Dict[str, str] = Field(default_factory=dict)

    # --- Scala (spark-submit) backend ---------------------------------------
    repo: str = "graphsense/graphsense-spark"
    # Release tag, e.g. "v26.06.0". Empty or "latest" resolves the latest stable
    # (non-prerelease) release from the GitHub API at run time.
    version: str = ""
    version_overrides: Dict[str, str] = Field(default_factory=dict)
    artifact: str = "fat"  # "fat" (assembly, self-contained) | "slim" (+packages)
    main_class: str = "org.graphsense.TransformationJob"
    packages: List[str] = Field(
        default_factory=lambda: list(DEFAULT_SCALA_JOB_PACKAGES)
    )
    repositories: List[str] = Field(
        default_factory=lambda: ["https://repos.spark-packages.org/"]
    )
    jar_args: Dict[str, List[str]] = Field(default_factory=dict)
    sidecar: SidecarConfig = Field(default_factory=SidecarConfig)
    extra_submit_args: List[str] = Field(default_factory=list)

    def version_for(self, currency: str) -> str:
        return self.version_overrides.get(currency, self.version)

    def profile_for(self, currency: str) -> Optional[str]:
        return self.spark_profile.get(currency)


class _EnvResolvingFileConfigSource(FileConfigSettingsSource):
    """goodconf file source that resolves ${VAR} placeholders via the
    environment before the values reach pydantic.

    This covers the native goodconf ``load()`` path; the explicit
    ``load_partial`` path resolves separately (both share the same helper).
    """

    def __call__(self) -> Dict[str, Any]:
        return resolve_env_vars(super().__call__())


class AppConfig(GoodConf):
    """Graphsenselib config file"""

    default_environment: Optional[str] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        # Mirror goodconf's source ordering but swap in the env-resolving file
        # source so ${VAR} placeholders are expanded on the native load() path.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _EnvResolvingFileConfigSource(settings_cls),
            file_secret_settings,
        )

    @model_validator(mode="before")
    @classmethod
    def _warn_unknown_keys(cls, data):
        if isinstance(data, dict):
            known = set(cls.model_fields.keys())
            unknown = set(data.keys()) - known
            for key in sorted(unknown):
                logger.warning(f"Unknown key '{key}' in config — ignoring")
        return data

    model_config = GoodConfConfigDict(
        extra="ignore",
        env_prefix="GRAPHSENSE_",
        file_env_var="GRAPHSENSE_CONFIG_YAML",
        default_files=[".graphsense.yaml", os.path.expanduser("~/.graphsense.yaml")],
    )

    environments: Dict[str, Environment] = Field(
        default_factory=lambda: {
            env: {
                "cassandra_nodes": ["enter your cassandra hosts here."],
                "keyspaces": {
                    cur: {
                        "raw_keyspace_name": f"{cur}_raw_{env}",
                        "transformed_keyspace_name": f"{cur}_transformed_{env}",
                        "schema_type": currency_to_schema_type[cur],
                        "disable_delta_updates": False,
                        "keyspace_setup_config": {
                            kst: {
                                "replication_config": CASSANDRA_DEFAULT_REPLICATION_CONFIG,  # noqa
                                "data_configuration": get_default_data_configuration(
                                    cur, kst
                                ),
                            }
                            for kst in keyspace_types
                        },
                        "ingest_config": {"node_reference": "localhost:8545"},
                    }
                    for cur in supported_base_currencies
                },
            }
            for env in default_environments
        },
        description="Config per environment",
    )

    slack_topics: Dict[str, SlackTopic] = Field(
        default_factory=lambda: {"exceptions": {"hooks": []}}
    )

    cache_directory: str = Field(
        # initial=lambda: "~/.graphsense/cache",
        default_factory=lambda: "~/.graphsense/cache",
    )

    coingecko_api_key: str = Field(
        # initial=lambda: "",
        default_factory=lambda: "",
    )

    coinmarketcap_api_key: str = Field(
        # initial=lambda: "",
        default_factory=lambda: "",
    )

    cryptocompare_api_key: str = Field(
        default_factory=lambda: "",
        description=(
            "API key for min-api.cryptocompare.com (required since June 2026, "
            "see https://developers.coindesk.com/)."
        ),
    )

    s3_credentials: Optional[Dict[str, str]] = Field(default_factory=lambda: None)

    s3_configs: Dict[str, Dict[str, str]] = Field(default_factory=lambda: {})

    spark_config: Dict[str, Any] = Field(
        default_factory=lambda: {},
        description=(
            "Spark configuration properties passed to SparkSession.builder. "
            "Two supported shapes: (1) flat — keys are Spark property names "
            "and values are strings, e.g. {'spark.master': 'spark://host:7077', "
            "'spark.executor.memory': '8g'}; (2) nested — keys are profile "
            "names with dict values, with a reserved 'baseline' profile that "
            "other profiles inherit from and may override. Access via "
            "get_spark_config(profile_name)."
        ),
    )

    spark_packages: Dict[str, str] = Field(
        default_factory=lambda: {},
        description=(
            "Per-package Maven coordinate overrides for the Spark transformation "
            "session, keyed by logical name (cassandra_connector, joda_time, "
            "delta_spark, hadoop_aws). Merged over the built-in defaults, so only "
            "the packages you want to change need to be listed, e.g. "
            "{'hadoop_aws': 'org.apache.hadoop:hadoop-aws:3.3.4'}. Defaults stay "
            "the same when this is empty."
        ),
    )

    full_transform_args: Optional[FullTransformArgs] = Field(
        default=None,
        description=(
            "Arguments for the raw -> transformed full-transform command "
            "(`transformation raw-to-transformed`). Spark properties themselves "
            "live in spark_config (selected per currency via spark_profile)."
        ),
    )

    legacy_ingest: bool = Field(
        default=False,
        description="Use the legacy ingest pipeline instead of the new IngestRunner pipeline.",
    )

    resolve_inputs_via_cassandra: bool = Field(
        default=False,
        description=(
            "Use Cassandra to resolve UTXO input values instead of RPC verbosity 3 "
            "or explicit transaction fetching for LTC and ZEC."
        ),
    )

    fill_unresolved_inputs: bool = Field(
        default=False,
        description=(
            "Fill unresolved UTXO inputs with dummy values (value=0, type=nonstandard) "
            "instead of failing. Useful for mid-chain delta-only ingests where the "
            "node lacks txindex and no Cassandra is available."
        ),
    )

    use_redis_locks: bool = Field(
        default=False,
        description="Use Redis for distributed locking instead of file locks.",
    )

    redis_url: Optional[str] = Field(
        default=None,
        description="Redis URL for distributed locking (e.g. redis://localhost:6379).",
    )

    delta_updater_wal_enabled: bool = Field(
        default=True,
        description=(
            "Enable the crash-safe write-ahead log for delta updates. When on, "
            "each batch's resolved writes are staged durably (delta_updater_wal "
            "table in the transformed keyspace) before being applied and replayed "
            "on the next run if a crash left them partially applied. On by "
            "default as a mandatory safety net; set to false (or pass "
            "--no-enable-wal) to opt out. The --enable-wal/--no-enable-wal CLI "
            "flag overrides this."
        ),
    )

    web: Optional[Dict] = Field(
        default=None,
        description="Optional REST API (gsrest) configuration. Read by the web app.",
    )

    def __init__(
        self, load: bool = False, config_file: str | None = None, **kwargs
    ) -> None:
        super().__init__(load, config_file, **kwargs)
        self.model_config["explicit_config_file"] = config_file  # ty: ignore[invalid-key]

    def is_loaded(self) -> bool:
        return hasattr(self, "environments")

    @property
    def underlying_file(self) -> Optional[str]:
        env_overwrite_file_env = self.model_config.get("file_env_var")
        env_overwrite_file = (
            os.environ.get(env_overwrite_file_env) if env_overwrite_file_env else None
        )
        explicit_config_file = self.model_config.get("explicit_config_file")

        if explicit_config_file is not None:
            return explicit_config_file
        elif env_overwrite_file_env and env_overwrite_file:
            return env_overwrite_file
        else:
            default_files = self.model_config.get("default_files", []) or []
            for f in default_files:
                if os.path.exists(f):
                    return f

        logger.debug("No config file found in default locations.")
        return None

    def text(self) -> str:
        if self.underlying_file:
            with open(self.underlying_file, "r") as f:
                return f.read()
        else:
            return ""

    def path(self):
        return self.underlying_file

    def get_configured_environments(self):
        if self.is_loaded():
            return self.environments.keys()
        else:
            return []

    def get_configured_currencies(self):
        if self.is_loaded():
            return list(
                set(
                    flatten(
                        [ec.keyspaces.keys() for e, ec in self.environments.items()]
                    )
                )
            )
        else:
            return []

    def get_configured_slack_topics(self) -> List[str]:
        return list(self.slack_topics.keys())

    def get_environment(self, env: str) -> Environment:
        if not self.is_loaded():
            self.load()
        return self.environments[env]

    def get_slack_exception_notification_hook_urls(self) -> List[str]:
        if "exceptions" in self.slack_topics:
            return self.slack_topics["exceptions"].hooks
        else:
            return []

    def get_slack_hooks_by_topic(self, topic: str) -> Optional[SlackTopic]:
        if topic in self.slack_topics:
            return self.slack_topics[topic]
        else:
            return None

    def get_spark_config(self, profile_name: Optional[str] = None) -> Dict[str, str]:
        """Resolve the Spark properties dict to pass to SparkSession.builder.

        Supports two YAML shapes (see the spark_config field docstring):
        - flat (legacy): values are Spark property strings; returned as-is.
        - nested: values are per-profile dicts; the reserved 'baseline' profile
          is merged under the requested profile (profile keys win).
        """
        raw = self.spark_config or {}
        nested = any(isinstance(v, dict) for v in raw.values())

        if not nested:
            if profile_name is not None:
                raise ValueError(
                    f"spark_config profile '{profile_name}' requested but "
                    f"spark_config is in flat (legacy) form. Convert to the "
                    f"nested form with a 'baseline' key and named profiles."
                )
            return dict(raw)

        baseline = raw.get("baseline") or {}
        if profile_name is None:
            return dict(baseline)
        profile = raw.get(profile_name)
        if profile is None:
            available = sorted(k for k in raw.keys() if k != "baseline")
            raise ValueError(
                f"spark_config profile '{profile_name}' not found. "
                f"Available: {available}"
            )
        if profile_name == "baseline":
            return dict(profile)
        return {**baseline, **profile}

    def get_spark_packages(self) -> Dict[str, str]:
        """Per-package Maven coordinate overrides for the Spark session.

        Returned as-is; merged over the built-in defaults in create_spark_session.
        """
        return dict(self.spark_packages or {})

    def get_full_transform_args(self) -> FullTransformArgs:
        """Args for the raw -> transformed full-transform command.

        Returns defaults when no `full_transform_args` section is configured; a
        missing release version then surfaces as a clear error at jar-fetch time.
        """
        return self.full_transform_args or FullTransformArgs()

    def get_s3_credentials(
        self, config_name: Optional[str] = None
    ) -> Optional[Dict[str, str]]:
        if config_name is not None:
            creds = self.s3_configs.get(config_name)
            if creds is None:
                raise ValueError(
                    f"s3_config '{config_name}' not found. "
                    f"Available: {list(self.s3_configs.keys())}"
                )
            baseline = self.s3_configs.get("baseline")
            if baseline and config_name != "baseline":
                return {**baseline, **creds}
            return creds
        return self.s3_credentials

    def get_keyspace_config(self, env: str, currency: str) -> KeyspaceConfig:
        return self.get_environment(env).get_keyspace(currency)

    def load_partial(self, filename: Optional[str] = None) -> Tuple[bool, List[str]]:
        errors = []

        self._init_with_field_defaults()

        config_file = filename or self.underlying_file

        if config_file and os.path.exists(config_file):
            raw_config = resolve_env_vars(_load_config(config_file))
        else:
            logger.warning(
                f"Config file not found: {config_file}. Continuing with defaults."
            )
            raw_config = {}

        if raw_config:
            for field_name, value in raw_config.items():
                try:
                    if field_name == "slack_topics" and isinstance(value, dict):
                        setattr(self, field_name, self._parse_slack_topics(value))
                    else:
                        setattr(self, field_name, value)
                except Exception as e:
                    errors.append(f"{field_name}: {str(e)}")

        env_slack_topics = os.environ.get("GRAPHSENSE_SLACK_TOPICS")
        if env_slack_topics:
            try:
                parsed_env_topics = json.loads(env_slack_topics)
                if not isinstance(parsed_env_topics, dict):
                    raise ValueError("GRAPHSENSE_SLACK_TOPICS must be a JSON object")
                self.slack_topics = self._parse_slack_topics(parsed_env_topics)
            except Exception as e:
                errors.append(f"GRAPHSENSE_SLACK_TOPICS: {str(e)}")

        return len(errors) == 0, errors

    @staticmethod
    def _parse_slack_topics(raw_topics: Dict) -> Dict[str, SlackTopic]:
        converted_topics = {}
        for topic_name, topic_data in raw_topics.items():
            if isinstance(topic_data, dict):
                converted_topics[topic_name] = SlackTopic(**topic_data)
            else:
                converted_topics[topic_name] = topic_data
        return converted_topics

    def _init_with_field_defaults(self):
        """Initialize config using field default factories."""
        defaults = self.__class__.get_initial()

        super().__init__(**defaults)

    def get_deltaupdater_config(
        self, env: str, currency: str
    ) -> Optional[DeltaUpdaterConfig]:
        delta_sink = (
            self.get_environment(env)
            .keyspaces[currency]
            .ingest_config.raw_keyspace_file_sinks.get("delta")  # ty: ignore[unresolved-attribute]
        )
        if delta_sink is None:
            logger.debug(f"Delta sink not configured for {currency} in {env}")
            return None
        s3_config_name = delta_sink.s3_config
        return DeltaUpdaterConfig(
            delta_sink=delta_sink,
            currency=currency,
            s3_credentials=self.get_s3_credentials(s3_config_name),
        )


def get_config() -> AppConfig:
    return _app_config


def set_config(cfg: AppConfig):
    global _app_config
    _app_config = cfg


_app_config = AppConfig(load=False)
