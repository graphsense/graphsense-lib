"""Configuration for clustering regression tests.

Defines block ranges per UTXO currency and builds test configs from .graphsense.yaml.
Only UTXO currencies are supported (btc, ltc, bch, zec) -- no ETH/TRX.
"""

from dataclasses import dataclass, field
from pathlib import Path

from tests.lib.config import SCHEMA_TYPE_MAP, load_ingest_configs, parse_currencies, resolve_gslib_path


@dataclass(frozen=True)
class ClusteringRange:
    """A single block range for a clustering test."""

    range_id: str
    start_block: int
    end_block: int
    note: str = ""


# Block ranges for clustering tests.
# UTXO chains start from 0 (need full tx_id sequence for address ID assignment).
CLUSTERING_RANGES: dict[str, list[ClusteringRange]] = {
    "btc": [
        ClusteringRange("genesis", 0, 200, "first real multi-input txs around block 170"),
    ],
    "ltc": [
        ClusteringRange("genesis", 0, 500, "first spending tx at block 448"),
    ],
    "bch": [
        ClusteringRange("genesis", 0, 200, "shares BTC history"),
    ],
    "zec": [
        ClusteringRange("genesis", 0, 400, "first spending at 396"),
    ],
}

# Only UTXO currencies -- clustering uses the multi-input heuristic
ALL_CURRENCIES = list(CLUSTERING_RANGES.keys())


@dataclass
class ClusteringConfig:
    currency: str
    range_id: str
    node_url: str
    secondary_node_references: list[str]
    start_block: int
    end_block: int
    schema_type: str
    gslib_path: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[4]
    )
    range_note: str = ""

    @property
    def num_blocks(self) -> int:
        return self.end_block - self.start_block + 1

    @property
    def test_id(self) -> str:
        return f"{self.currency}-{self.range_id}"


def build_clustering_configs() -> list[ClusteringConfig]:
    """Build a ClusteringConfig per configured currency and range."""
    currencies = parse_currencies("CLUSTERING_CURRENCIES", ALL_CURRENCIES)
    gslib_path = resolve_gslib_path()
    ingest_configs = load_ingest_configs()

    configs = []
    for currency in currencies:
        ic = ingest_configs.get(currency)
        if not ic:
            continue
        ranges = CLUSTERING_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                ClusteringConfig(
                    currency=currency,
                    range_id=cr.range_id,
                    node_url=ic["node_url"],
                    secondary_node_references=ic.get(
                        "secondary_node_references", []
                    ),
                    start_block=cr.start_block,
                    end_block=cr.end_block,
                    schema_type=SCHEMA_TYPE_MAP.get(currency, "utxo"),
                    gslib_path=gslib_path,
                    range_note=cr.note,
                )
            )
    return configs
