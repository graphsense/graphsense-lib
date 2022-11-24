import os
from typing import Dict, List

from goodconf import Field, GoodConf
from pydantic import BaseModel, validator

supported_base_currencies = ["btc", "eth", "ltc", "bch", "zec"]
default_environments = ["prod", "test"]
schema_types = ["utxo", "account"]
keyspace_types = ["raw", "transformed"]
currency_to_schema_type = {
    cur: "account" if cur == "eth" else "utxo" for cur in supported_base_currencies
}
supported_fiat_currencies = ["USD", "EUR"]

GRAPHSENSE_DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class KeyspaceConfig(BaseModel):
    raw_keyspace_name: str
    transformed_keyspace_name: str
    schema_type: str
    disable_delta_updates: bool = Field(default_factory=lambda: False)

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
                    }
                    for cur in supported_base_currencies
                },
            }
            for env in default_environments
        },
        description="Separate configs per environment",
    )

    slack_topics: Dict[str, SlackTopic] = Field(
        initial=lambda: {}, default_factory=lambda: {}
    )

    class Config:
        file_env_var = "GRAPHSENSE_CONFIG_YAML"
        default_files = [".graphsense.yaml", os.path.expanduser("~/.graphsense.yaml")]

    def is_loaded(self) -> bool:
        return hasattr(self, "environments")

    def text(self):
        if self.Config._config_file:
            with open(self.Config._config_file, "r") as f:
                return f.read()
        else:
            return ""

    def path(self):
        return self.Config._config_file

    def get_environment(self, env: str) -> Environment:
        if not self.is_loaded():
            self.load()
        return self.environments[env]

    def get_slack_exception_notification_hook_urls(self) -> List[str]:
        if "exceptions" in self.slack_topics:
            return self.slack_topics["exceptions"].hooks
        else:
            return []

    def get_keyspace_config(self, env: str, currency: str) -> KeyspaceConfig:
        return self.get_environment(env).get_keyspace(currency)


config = AppConfig(load=False)
