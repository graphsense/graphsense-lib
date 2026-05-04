"""Configuration for sink catch-up regression tests.

Verifies that auto-catch-up of a diverged idempotent sink produces the same
end state as a sync ingest from the start. Catch-up is triggered when a
dual-sink append finds delta ahead of cassandra; cassandra is brought up to
delta's head before the forward run continues.

Account chains only — UTXO catch-up across mid-chain ranges would require
genesis-onwards UTXO context.
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
class SinkCatchupRange:
    """A block range whose middle gives us a divergence point."""

    range_id: str
    start_block: int
    end_block: int  # final block — both phases land here
    mid_block: int  # cassandra ends at this block before catch-up
    note: str = ""


# Small mid-chain ranges — large enough to leave a non-trivial catch-up gap
# but small enough to keep the test fast. mid_block sits a handful of blocks
# below end_block so catch-up has real work to do.
SINK_CATCHUP_RANGES: dict[str, list[SinkCatchupRange]] = {
    "eth": [
        SinkCatchupRange("mid", 2_000_000, 2_000_010, 2_000_005, "early PoW era"),
    ],
    "trx": [
        SinkCatchupRange("mid", 50_000_001, 50_000_010, 50_000_005, "mid-chain era"),
    ],
}

ALL_CURRENCIES = list(SINK_CATCHUP_RANGES.keys())


@dataclass
class SinkCatchupConfig:
    currency: str
    range_id: str
    node_url: str
    secondary_node_references: list[str]
    start_block: int
    end_block: int
    mid_block: int
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
        return tables_for_currency(self.currency)


def build_sink_catchup_configs() -> list[SinkCatchupConfig]:
    """Build a SinkCatchupConfig per configured currency and range."""
    currencies = parse_currencies("SINK_CATCHUP_CURRENCIES", ALL_CURRENCIES)
    gslib_path = resolve_gslib_path()
    ingest_configs = load_ingest_configs()

    configs = []
    for currency in currencies:
        ic = ingest_configs.get(currency)
        if not ic:
            continue
        ranges = SINK_CATCHUP_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                SinkCatchupConfig(
                    currency=currency,
                    range_id=cr.range_id,
                    node_url=ic["node_url"],
                    secondary_node_references=ic.get(
                        "secondary_node_references", []
                    ),
                    start_block=cr.start_block,
                    end_block=cr.end_block,
                    mid_block=cr.mid_block,
                    schema_type=SCHEMA_TYPE_MAP.get(currency, "utxo"),
                    gslib_path=gslib_path,
                    range_note=cr.note,
                )
            )
    return configs
