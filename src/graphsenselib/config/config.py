from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional, Tuple

from goodconf import Field, GoodConf, GoodConfConfigDict
from goodconf import _load_config
from pydantic import BaseModel, field_validator

from ..utils import first_or_default, flatten

logger = logging.getLogger(__name__)

supported_base_currencies = ["btc", "eth", "ltc", "bch", "zec", "trx"]
default_environments = ["prod", "test", "dev", "exp1", "exp2", "exp3", "pytest"]
schema_types = ["utxo", "account", "account_trx"]
keyspace_types = ["raw", "transformed"]
currency_to_schema_type = {
    cur: "account_trx" if cur == "trx" else "account" if cur == "eth" else "utxo"
    for cur in supported_base_currencies
}
supported_fiat_currencies = ["USD", "EUR"]
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


GRAPHSENSE_DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
GRAPHSENSE_VERBOSE_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

CASSANDRA_DEFAULT_REPLICATION_CONFIG = (
    "{'class': 'SimpleStrategy', 'replication_factor': 1}"
)


def get_reorg_backoff_blocks(network: str) -> int:
    """For imports we do not want to always catch up with the latest block
    since we want to avoid reorgs lead to spurious data in the database.

    Default conservative estimate is 2h worth of blocks lag.

    Args:
        network (str): currency/network label
    """
    return reorg_backoff_blocks[network.lower()]


class FileSink(BaseModel):
    directory: str


class IngestConfig(BaseModel):
    node_reference: str = Field(default_factory=lambda: "")
    secondary_node_references: List[str] = Field(default_factory=lambda: [])
    raw_keyspace_file_sinks: Dict[str, FileSink] = Field(default_factory=lambda: {})

    @property
    def all_node_references(self) -> List[str]:
        return [self.node_reference] + self.secondary_node_references

    def get_first_node_reference(self, protocol: str = "http") -> Optional[str]:
        return first_or_default(
            self.all_node_references, lambda x: x.startswith(protocol)
        )


class KeyspaceSetupConfig(BaseModel):
    replication_config: str = Field(
        default_factory=lambda: CASSANDRA_DEFAULT_REPLICATION_CONFIG
    )
    data_configuration: Dict[str, object] = Field(default_factory=lambda: {})


class DeltaUpdaterConfig(BaseModel):
    delta_sink: Optional[FileSink]
    currency: str
    s3_credentials: Optional[Dict[str, str]]


class KeyspaceConfig(BaseModel):
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


class Environment(BaseModel):
    cassandra_nodes: List[str]
    username: Optional[str] = Field(default_factory=lambda: None)
    password: Optional[str] = Field(default_factory=lambda: None)
    readonly_username: Optional[str] = Field(default_factory=lambda: None)
    readonly_password: Optional[str] = Field(default_factory=lambda: None)
    keyspaces: Dict[str, KeyspaceConfig]

    def get_configured_currencies(self) -> List[str]:
        return self.keyspaces.keys()

    def get_keyspace(self, currency: str) -> KeyspaceConfig:
        return self.keyspaces[currency]


class SlackTopic(BaseModel):
    hooks: List[str] = Field(default_factory=lambda: [])


class AppConfig(GoodConf):
    """Graphsenselib config file"""

    default_environment: Optional[str] = None

    model_config = GoodConfConfigDict(
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
                                "replication_config": CASSANDRA_DEFAULT_REPLICATION_CONFIG  # noqa
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
        default_factory=lambda: {"exceptions": SlackTopic(hooks=[])}
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

    s3_credentials: Optional[Dict[str, str]] = Field(default_factory=lambda: None)

    def __init__(
        self, load: bool = False, config_file: str | None = None, **kwargs
    ) -> None:
        super().__init__(load, config_file, **kwargs)
        self.model_config["explicit_config_file"] = config_file

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
            default_files = self.model_config.get("default_files", [])
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
        return self.slack_topics.keys()

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

    def get_s3_credentials(self) -> Optional[Dict[str, str]]:
        return self.s3_credentials

    def get_keyspace_config(self, env: str, currency: str) -> KeyspaceConfig:
        return self.get_environment(env).get_keyspace(currency)

    def load_partial(self, filename: Optional[str] = None) -> Tuple[bool, List[str]]:
        errors = []

        self._init_with_field_defaults()

        config_file = filename or self.underlying_file

        if config_file and os.path.exists(config_file):
            raw_config = _load_config(config_file)
        else:
            logger.warning(
                f"Config file not found: {config_file}. Continuing with defaults."
            )
            raw_config = {}

        if raw_config:
            for field_name, value in raw_config.items():
                try:
                    if field_name == "slack_topics" and isinstance(value, dict):
                        converted_topics = {}
                        for topic_name, topic_data in value.items():
                            if isinstance(topic_data, dict):
                                converted_topics[topic_name] = SlackTopic(**topic_data)
                            else:
                                converted_topics[topic_name] = topic_data
                        setattr(self, field_name, converted_topics)
                    else:
                        setattr(self, field_name, value)
                except Exception as e:
                    errors.append(f"{field_name}: {str(e)}")

        return len(errors) == 0, errors

    def _init_with_field_defaults(self):
        """Initialize config using field default factories."""
        defaults = self.__class__.get_initial()

        super().__init__(**defaults)

    def get_deltaupdater_config(self, env: str, currency: str) -> DeltaUpdaterConfig:
        delta_sink = (
            self.get_environment(env)
            .keyspaces[currency]
            .ingest_config.raw_keyspace_file_sinks.get("delta")
        )
        if delta_sink is None:
            logger.debug(f"Delta sink not configured for {currency} in {env}")
        return DeltaUpdaterConfig(
            delta_sink=delta_sink,
            currency=currency,
            s3_credentials=self.get_s3_credentials(),
        )


def get_config() -> AppConfig:
    return _app_config


def set_config(cfg: AppConfig):
    global _app_config
    _app_config = cfg


_app_config = AppConfig(load=False)
