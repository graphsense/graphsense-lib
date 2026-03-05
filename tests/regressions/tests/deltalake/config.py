"""Test configuration loaded from environment variables and .graphsense.yaml."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from graphsenselib.schema.resources.parquet.account import ACCOUNT_SCHEMA_RAW
from graphsenselib.schema.resources.parquet.account_trx import ACCOUNT_TRX_SCHEMA_RAW
from graphsenselib.schema.resources.parquet.utxo import UTXO_SCHEMA_RAW


@dataclass(frozen=True)
class BlockRangeProfile:
    """Block range profile for a currency regression scenario."""

    profile_id: str
    start_block: int
    base_blocks: int = 5
    append_blocks: int = 5
    category: str = "mid"
    note: str = ""
    perf_blocks: int | None = None


GENESIS_PROFILE = BlockRangeProfile(
    "genesis",
    0,
    base_blocks=50,
    append_blocks=50,
    category="genesis",
    note="genesis era",
    perf_blocks=1000,
)


NETWORK_RANGE_PROFILES: dict[str, list[BlockRangeProfile]] = {
    "btc": [
        GENESIS_PROFILE,
        BlockRangeProfile("old", 210000, category="old", note="first halving era"),
        BlockRangeProfile(
            "mid",
            600000,
            category="mid",
            note="post-segwit era",
            perf_blocks=10,
        ),
        BlockRangeProfile(
            "new",
            840000,
            category="new",
            note="recent chain tip era",
            perf_blocks=120,
        ),
        BlockRangeProfile(
            "protocol-bip34",
            227820,
            category="protocol",
            note="crosses BIP34 activation window",
        ),
        BlockRangeProfile(
            "protocol-segwit",
            481810,
            category="protocol",
            note="crosses SegWit activation window",
        ),
        BlockRangeProfile(
            "protocol-taproot",
            709620,
            category="protocol",
            note="crosses Taproot activation window",
        ),
    ],
    "eth": [
        GENESIS_PROFILE,
        BlockRangeProfile("old", 3000000, category="old", note="early PoW era"),
        BlockRangeProfile(
            "mid",
            11000000,
            category="mid",
            note="pre-merge PoW era",
            perf_blocks=100,
        ),
        BlockRangeProfile(
            "mid-24m",
            24000000,
            category="mid",
            note="recent post-merge era",
            perf_blocks=30,
        ),
        BlockRangeProfile("new", 19000000, category="new", note="post-merge era"),
        BlockRangeProfile(
            "protocol-byzantium",
            4369990,
            category="protocol",
            note="crosses Byzantium activation window",
        ),
        BlockRangeProfile(
            "protocol-london",
            12964990,
            category="protocol",
            note="crosses London activation window",
        ),
        BlockRangeProfile(
            "protocol-merge",
            15537384,
            category="protocol",
            note="crosses Merge activation window",
        ),
        BlockRangeProfile(
            "protocol-shanghai",
            17034860,
            category="protocol",
            note="crosses Shanghai activation window",
        ),
    ],
    "ltc": [
        GENESIS_PROFILE,
        BlockRangeProfile("old", 700000, category="old", note="legacy era"),
        BlockRangeProfile(
            "mid",
            1700000,
            category="mid",
            note="pre-MWEB era",
            perf_blocks=100,
        ),
        BlockRangeProfile("new", 2800000, category="new", note="recent chain tip era"),
        BlockRangeProfile(
            "protocol-segwit",
            1201526,
            category="protocol",
            note="crosses SegWit activation window",
        ),
        BlockRangeProfile(
            "protocol-mweb",
            2265974,
            category="protocol",
            note="crosses MWEB activation window",
        ),
    ],
    "bch": [
        GENESIS_PROFILE,
        BlockRangeProfile("old", 550000, category="old", note="early BCH era"),
        BlockRangeProfile(
            "mid",
            700000,
            category="mid",
            note="mid-life chain era",
            perf_blocks=10,
        ),
        BlockRangeProfile("new", 850000, category="new", note="recent chain tip era"),
        BlockRangeProfile(
            "protocol-schnorr",
            582670,
            category="protocol",
            note="crosses Schnorr upgrade window",
        ),
        BlockRangeProfile(
            "protocol-asert",
            661638,
            category="protocol",
            note="crosses ASERT DAA activation window",
        ),
    ],
    "zec": [
        GENESIS_PROFILE,
        BlockRangeProfile("old", 300000, category="old", note="pre-Sapling era"),
        BlockRangeProfile(
            "mid",
            900000,
            category="mid",
            note="Sapling/Blossom era",
            perf_blocks=100,
        ),
        BlockRangeProfile("new", 2300000, category="new", note="recent chain tip era"),
        BlockRangeProfile(
            "protocol-sapling",
            419190,
            category="protocol",
            note="crosses Sapling activation window",
        ),
        BlockRangeProfile(
            "protocol-canopy",
            1046390,
            category="protocol",
            note="crosses Canopy activation window",
        ),
        BlockRangeProfile(
            "protocol-nu5",
            1687094,
            category="protocol",
            note="crosses NU5 activation window",
        ),
    ],
    "trx": [
        GENESIS_PROFILE,
        BlockRangeProfile("old", 20000000, category="old", note="early mainnet era"),
        BlockRangeProfile(
            "mid",
            45000000,
            category="mid",
            note="mid-life chain era",
            perf_blocks=100,
        ),
        BlockRangeProfile("new", 65000000, category="new", note="recent chain tip era"),
        BlockRangeProfile(
            "protocol-greatvoyage",
            47500000,
            category="protocol",
            note="GreatVoyage upgrade era window",
        ),
    ],
}

ALL_CURRENCIES = list(NETWORK_RANGE_PROFILES.keys())

SCHEMA_TYPE_MAP = {
    "btc": "utxo",
    "ltc": "utxo",
    "bch": "utxo",
    "zec": "utxo",
    "eth": "account",
    "trx": "account_trx",
}

SCHEMA_TABLES_MAP = {
    "utxo": list(UTXO_SCHEMA_RAW.keys()),
    "account": list(ACCOUNT_SCHEMA_RAW.keys()),
    "account_trx": list(ACCOUNT_TRX_SCHEMA_RAW.keys()),
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
    range_id: str = "mid"
    range_category: str = "mid"
    range_note: str = ""
    node_url: str = ""
    secondary_node_references: list[str] = field(default_factory=list)
    gslib_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[4]
    )
    perf_blocks: int = 200

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
    def perf_end_block(self) -> int:
        return self.start_block + self.perf_blocks

    @property
    def tables(self) -> list[str]:
        """Tables to compare — all raw parquet tables for this currency."""
        schema_type = SCHEMA_TYPE_MAP[self.currency]
        return SCHEMA_TABLES_MAP[schema_type]

    @property
    def test_id(self) -> str:
        """Stable pytest id segment."""
        return f"{self.currency}-{self.range_id}"


def _parse_range_categories() -> set[str]:
    raw = os.environ.get("DELTA_RANGE_CATEGORIES", "mid")
    return {token.strip() for token in raw.split(",") if token.strip()}


def _profiles_for_currency(currency: str, categories: set[str]) -> list[BlockRangeProfile]:
    profiles = NETWORK_RANGE_PROFILES.get(currency, [])
    if not profiles:
        return [
            BlockRangeProfile(
                "mid",
                1000000,
                base_blocks=25,
                append_blocks=25,
                category="mid",
                note="fallback profile",
            )
        ]
    selected = [
        p for p in profiles if (p.profile_id == "genesis" or p.category in categories)
    ]
    return selected if selected else [profiles[0]]


def build_delta_configs() -> list[DeltaTestConfig]:
    """Build a DeltaTestConfig per requested currency.

    Reads ``DELTA_CURRENCIES`` (comma-separated, default: all) and
    ``DELTA_RANGE_CATEGORIES`` (default: ``old,mid,new,protocol``),
    resolves node URLs from ``.graphsense.yaml``, and expands each
    currency into multiple block-range scenarios. Currencies without
    a configured node URL are silently skipped.
    """
    ref_version = os.environ.get("DELTA_REF_VERSION", "v25.11.18")
    currencies_str = os.environ.get("DELTA_CURRENCIES", ",".join(ALL_CURRENCIES))
    currencies = [c.strip() for c in currencies_str.split(",") if c.strip()]
    gslib_path = Path(
        os.environ.get("GSLIB_PATH", str(Path(__file__).resolve().parents[4]))
    )
    perf_blocks = int(os.environ.get("DELTA_PERF_BLOCKS", "200"))
    categories = _parse_range_categories()
    ingest_configs = load_ingest_configs()

    configs = []
    for currency in currencies:
        ic = ingest_configs.get(currency)
        if not ic:
            continue
        for profile in _profiles_for_currency(currency, categories):
            configs.append(
                DeltaTestConfig(
                    ref_version=ref_version,
                    currency=currency,
                    start_block=profile.start_block,
                    base_blocks=profile.base_blocks,
                    append_blocks=profile.append_blocks,
                    range_id=profile.profile_id,
                    range_category=profile.category,
                    range_note=profile.note,
                    node_url=ic["node_url"],
                    secondary_node_references=ic.get("secondary_node_references", []),
                    gslib_path=gslib_path,
                    perf_blocks=(
                        profile.perf_blocks
                        if profile.perf_blocks is not None
                        else perf_blocks
                    ),
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
