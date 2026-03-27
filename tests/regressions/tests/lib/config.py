"""Shared configuration infrastructure for regression tests.

Contains:
- .graphsense.yaml loading (node URLs, ingest configs)
- Schema type/table mappings
- Base dataclasses for test configs and block ranges
- MinIO storage options builder
"""

import os
from pathlib import Path

import yaml


GRAPHSENSE_CONFIG_PATHS = [
    Path(".graphsense.yaml"),
    Path.home() / ".graphsense.yaml",
]

# Maps currency codes to their schema types.
SCHEMA_TYPE_MAP = {
    "btc": "utxo",
    "ltc": "utxo",
    "bch": "utxo",
    "zec": "utxo",
    "eth": "account",
    "trx": "account_trx",
}

SCHEMA_TABLES_MAP_KEYS = {
    "utxo",
    "account",
    "account_trx",
}


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


def tables_for_currency(currency: str) -> list[str]:
    """Return the raw parquet table names for the given currency."""
    from graphsenselib.schema.resources.parquet.account import ACCOUNT_SCHEMA_RAW
    from graphsenselib.schema.resources.parquet.account_trx import (
        ACCOUNT_TRX_SCHEMA_RAW,
    )
    from graphsenselib.schema.resources.parquet.utxo import UTXO_SCHEMA_RAW

    schema_tables_map = {
        "utxo": list(UTXO_SCHEMA_RAW.keys()),
        "account": list(ACCOUNT_SCHEMA_RAW.keys()),
        "account_trx": list(ACCOUNT_TRX_SCHEMA_RAW.keys()),
    }
    schema_type = SCHEMA_TYPE_MAP[currency]
    return schema_tables_map[schema_type]


def resolve_gslib_path() -> Path:
    """Resolve the graphsense-lib repo root from env or filesystem."""
    return Path(
        os.environ.get("GSLIB_PATH", str(Path(__file__).resolve().parents[5]))
    )


def parse_currencies(env_var: str, all_currencies: list[str]) -> list[str]:
    """Parse a comma-separated currency list from an env var."""
    raw = os.environ.get(env_var, ",".join(all_currencies))
    return [c.strip() for c in raw.split(",") if c.strip()]
