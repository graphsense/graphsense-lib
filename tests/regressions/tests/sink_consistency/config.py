"""Configuration for sink consistency tests.

Defines small block ranges per currency that exercise the key code paths
without taking too long. Reuses node URLs from .graphsense.yaml.
"""

from dataclasses import dataclass, field
from pathlib import Path

from tests.lib.config import (
    SCHEMA_TYPE_MAP,
    load_ingest_configs,
    parse_currencies,
    resolve_gslib_path,
    tables_for_currency,
)


@dataclass(frozen=True)
class SinkConsistencyRange:
    """A single block range for a consistency test."""

    range_id: str
    start_block: int
    end_block: int
    note: str = ""


# Small block ranges — just enough to verify sink consistency.
# UTXO chains start from 0 (need UTXO resolution context).
# Account chains can use mid-range blocks.
SINK_CONSISTENCY_RANGES: dict[str, list[SinkConsistencyRange]] = {
    "btc": [
        SinkConsistencyRange("genesis", 0, 200, "first real txs at block 170"),
    ],
    "ltc": [
        SinkConsistencyRange("genesis", 0, 500, "first spending tx at block 448"),
    ],
    "bch": [
        SinkConsistencyRange("genesis", 0, 200, "shares BTC history"),
    ],
    "zec": [
        SinkConsistencyRange("genesis", 0, 400, "first spending at 396"),
    ],
    "eth": [
        SinkConsistencyRange("mid", 2_000_000, 2_000_010, "early PoW era"),
    ],
    "trx": [
        SinkConsistencyRange("mid", 50_000_001, 50_000_010, "mid-chain era"),
    ],
}

ALL_CURRENCIES = list(SINK_CONSISTENCY_RANGES.keys())


@dataclass
class SinkConsistencyConfig:
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
        return tables_for_currency(self.currency)


def build_sink_consistency_configs() -> list[SinkConsistencyConfig]:
    """Build a SinkConsistencyConfig per configured currency and range."""
    currencies = parse_currencies("SINK_CONSISTENCY_CURRENCIES", ALL_CURRENCIES)
    gslib_path = resolve_gslib_path()
    ingest_configs = load_ingest_configs()

    configs = []
    for currency in currencies:
        ic = ingest_configs.get(currency)
        if not ic:
            continue
        ranges = SINK_CONSISTENCY_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                SinkConsistencyConfig(
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
