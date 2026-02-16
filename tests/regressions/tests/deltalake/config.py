"""Test configuration loaded from environment variables and .graphsense.yaml."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


NETWORK_DEFAULTS: dict[str, dict] = {
    "btc": {"start_block": 200000, "base_blocks": 50, "append_blocks": 50},
    "eth": {"start_block": 2000000, "base_blocks": 50, "append_blocks": 50},
    "ltc": {"start_block": 500000, "base_blocks": 50, "append_blocks": 50},
    "bch": {"start_block": 200000, "base_blocks": 50, "append_blocks": 50},
    "zec": {"start_block": 500000, "base_blocks": 50, "append_blocks": 50},
    "trx": {"start_block": 50000000, "base_blocks": 50, "append_blocks": 50},
}

ALL_CURRENCIES = list(NETWORK_DEFAULTS.keys())

SCHEMA_TYPE_MAP = {
    "btc": "utxo",
    "ltc": "utxo",
    "bch": "utxo",
    "zec": "utxo",
    "eth": "account",
    "trx": "account_trx",
}

GRAPHSENSE_CONFIG_PATHS = [
    Path(".graphsense.yaml"),
    Path.home() / ".graphsense.yaml",
]


def load_ingest_configs(environment: str = "dev") -> dict[str, dict]:
    """Load ingest configs from .graphsense.yaml, keyed by currency.

    Returns a dict like::

        {"eth": {"node_url": "http://...", "secondary_node_references": [...]}, ...}
    """
    config_file = os.environ.get("GRAPHSENSE_CONFIG_YAML")
    if config_file:
        paths = [Path(config_file)]
    else:
        paths = GRAPHSENSE_CONFIG_PATHS

    for p in paths:
        if p.exists():
            with open(p) as f:
                config = yaml.safe_load(f)
            keyspaces = (
                config.get("environments", {})
                .get(environment, {})
                .get("keyspaces", {})
            )
            result = {}
            for currency, ks in keyspaces.items():
                ic = ks.get("ingest_config", {})
                node_url = ic.get("node_reference", "")
                if node_url:
                    result[currency] = {
                        "node_url": node_url,
                        "secondary_node_references": ic.get(
                            "secondary_node_references", []
                        ),
                    }
            return result
    return {}


@dataclass
class DeltaTestConfig:
    """Configuration for a single currency's Delta Lake cross-version test."""

    ref_version: str = "v25.11.18"
    currency: str = "eth"
    start_block: int = 2000000
    base_blocks: int = 50
    append_blocks: int = 50
    node_url: str = ""
    secondary_node_references: list[str] = field(default_factory=list)
    gslib_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[4]
    )

    @property
    def base_end_block(self) -> int:
        return self.start_block + self.base_blocks

    @property
    def append_start_block(self) -> int:
        return self.base_end_block + 1

    @property
    def append_end_block(self) -> int:
        return self.append_start_block + self.append_blocks - 1

    @property
    def tables(self) -> list[str]:
        """Tables to compare — currency-dependent."""
        if self.currency == "eth":
            return ["block", "transaction", "log", "trace"]
        elif self.currency == "trx":
            return ["block", "transaction", "trace"]
        else:
            # UTXO currencies
            return ["block", "transaction"]


def build_delta_configs() -> list[DeltaTestConfig]:
    """Build a DeltaTestConfig per requested currency.

    Reads ``DELTA_CURRENCIES`` (comma-separated, default: all) and
    resolves node URLs from ``.graphsense.yaml``.  Currencies without
    a configured node URL are silently skipped.
    """
    ref_version = os.environ.get("DELTA_REF_VERSION", "v25.11.18")
    currencies_str = os.environ.get("DELTA_CURRENCIES", ",".join(ALL_CURRENCIES))
    currencies = [c.strip() for c in currencies_str.split(",") if c.strip()]
    gslib_path = Path(
        os.environ.get("GSLIB_PATH", str(Path(__file__).resolve().parents[4]))
    )
    ingest_configs = load_ingest_configs()

    configs = []
    for currency in currencies:
        ic = ingest_configs.get(currency)
        if not ic:
            continue
        defaults = NETWORK_DEFAULTS.get(
            currency, {"start_block": 1000000, "base_blocks": 50, "append_blocks": 50}
        )
        configs.append(
            DeltaTestConfig(
                ref_version=ref_version,
                currency=currency,
                start_block=defaults["start_block"],
                base_blocks=defaults["base_blocks"],
                append_blocks=defaults["append_blocks"],
                node_url=ic["node_url"],
                secondary_node_references=ic.get("secondary_node_references", []),
                gslib_path=gslib_path,
            )
        )
    return configs


def get_minio_storage_options(
    endpoint: str, access_key: str, secret_key: str
) -> dict[str, str]:
    """Build AWS-compatible storage options dict for deltalake / S3."""
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
