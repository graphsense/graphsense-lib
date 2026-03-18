"""Configuration for transformation regression tests.

Defines block ranges per currency and builds test configs from .graphsense.yaml.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from tests.deltalake.config import SCHEMA_TYPE_MAP, SCHEMA_TABLES_MAP, load_ingest_configs


@dataclass(frozen=True)
class TransformationRange:
    """A single block range for a transformation test."""

    range_id: str
    start_block: int
    end_block: int
    note: str = ""


# Block ranges for transformation tests.
# UTXO chains start from 0 (need full tx_id sequence).
# Account chains can use mid-range blocks.
TRANSFORMATION_RANGES: dict[str, list[TransformationRange]] = {
    "btc": [
        TransformationRange("genesis", 0, 200, "first real txs at block 170"),
    ],
    "ltc": [
        TransformationRange("genesis", 0, 500, "first spending tx at block 448"),
    ],
    "bch": [
        TransformationRange("genesis", 0, 200, "shares BTC history"),
    ],
    "zec": [
        TransformationRange("genesis", 0, 400, "first spending at 396"),
    ],
    "eth": [
        TransformationRange("mid", 2_000_000, 2_000_010, "early PoW era"),
    ],
    "trx": [
        TransformationRange("mid", 50_000_001, 50_000_010, "mid-chain era"),
    ],
}

ALL_CURRENCIES = list(TRANSFORMATION_RANGES.keys())


@dataclass
class TransformationConfig:
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

    @property
    def tables(self) -> list[str]:
        """Delta Lake tables for this currency."""
        schema_type = SCHEMA_TYPE_MAP[self.currency]
        return SCHEMA_TABLES_MAP[schema_type]


def build_transformation_configs() -> list[TransformationConfig]:
    """Build a TransformationConfig per configured currency and range."""
    currencies_str = os.environ.get(
        "TRANSFORMATION_CURRENCIES", ",".join(ALL_CURRENCIES)
    )
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
        ranges = TRANSFORMATION_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                TransformationConfig(
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
