import math
import os
from typing import Dict, List, Optional

from goodconf import Field, GoodConf
from pydantic import BaseModel, validator

from ..utils import flatten

supported_base_currencies = ["btc", "eth", "ltc", "bch", "zec", "trx"]
default_environments = ["prod", "test", "dev", "exp1", "exp2", "exp3"]
schema_types = ["utxo", "account"]
keyspace_types = ["raw", "transformed"]
currency_to_schema_type = {
    cur: "account" if cur == "eth" or cur == "trx" else "utxo"
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

GRAPHSENSE_DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

CASSANDRA_DEFAULT_REPLICATION_CONFIG = (
    "{'class': 'SimpleStrategy', 'replication_factor': 1}"
)


def get_approx_reorg_backoff_blocks(network: str, lag_in_hours: int = 2) -> int:
    """For imports we do not want to always catch up with the latest block
    since we want to avoid reorgs lead to spurious data in the database.

    Default conservative estimate is 2h worth of blocks lag.

    Args:
        network (str): currency/network label
    """
    return math.ceil(
        (lag_in_hours * 3600) / avg_blocktimes_by_currencies[network.lower()]
    )


class FileSink(BaseModel):
    directory: str


class IngestConfig(BaseModel):
    node_reference: str = Field(default_factory=lambda: "")
    raw_keyspace_file_sinks: Dict[str, FileSink] = Field(default_factory=lambda: {})
    # raw_keyspace_file_sink_directory: str = Field(default_factory=lambda: None)


class KeyspaceSetupConfig(BaseModel):
    replication_config: str = Field(
        default_factory=lambda: CASSANDRA_DEFAULT_REPLICATION_CONFIG
    )
    data_configuration: Dict[str, object] = Field(default_factory=lambda: {})


class KeyspaceConfig(BaseModel):
    raw_keyspace_name: str
    transformed_keyspace_name: str
    schema_type: str
    disable_delta_updates: bool = Field(default_factory=lambda: False)
    ingest_config: Optional[IngestConfig]
    keyspace_setup_config: Dict[str, KeyspaceSetupConfig] = Field(
        default_factory=lambda: {kst: KeyspaceSetupConfig() for kst in keyspace_types}
    )

    @validator("schema_type", allow_reuse=True)
    def schema_type_in_range(cls, v):
        assert (
            v.lower() in schema_types
        ), f'Schema must be either {", ".join(schema_types)}'
        return v.lower()

    @validator("transformed_keyspace_name", allow_reuse=True)
    def keyspace_prefix_match(cls, v, values, **kwargs):
        raw = values["raw_keyspace_name"]

        if v[:3] != raw[:3]:
            raise ValueError(f"Keyspace prefix do not match {raw} and {v}")

        return v

    @validator("schema_type", allow_reuse=True)
    def keyspace_prefix_matches_schema(cls, v, values, **kwargs):
        raw = values["raw_keyspace_name"]
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
    keyspaces: Dict[str, KeyspaceConfig]

    def get_configured_currencies(self) -> List[str]:
        return self.keyspaces.keys()

    def get_keyspace(self, currency: str) -> KeyspaceConfig:
        return self.keyspaces[currency]


class SlackTopic(BaseModel):
    hooks: List[str] = Field(default_factory=lambda: [])


class AppConfig(GoodConf):

    """Graphsenselib config file"""

    environments: Dict[str, Environment] = Field(
        initial=lambda: {
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
        initial=lambda: {}, default_factory=lambda: {}
    )

    class Config:
        env_prefix = "GRAPHSENSE_"
        file_env_var = "GRAPHSENSE_CONFIG_YAML"
        default_files = [".graphsense.yaml", os.path.expanduser("~/.graphsense.yaml")]

    def is_loaded(self) -> bool:
        return hasattr(self, "environments")

    @property
    def underlying_file(self) -> Optional[str]:
        if hasattr(self.__config__, "_config_file"):
            return self.__config__._config_file
        else:
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

    def get_keyspace_config(self, env: str, currency: str) -> KeyspaceConfig:
        return self.get_environment(env).get_keyspace(currency)


config = AppConfig(load=False)
