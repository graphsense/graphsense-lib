"""Configuration for delta-update regression tests.

Defines block ranges per UTXO currency and builds test configs from
.graphsense.yaml. The test ingests a contiguous block range, runs a PySpark
Delta Lake -> Cassandra raw transformation, then runs the UTXO delta updater
twice -- once with the local checkout and once with a reference release of
graphsense-lib (default v2.12.3) -- against separate transformed keyspaces.
The resulting transformed keyspaces are compared and per-version timings are
reported.
"""

from dataclasses import dataclass, field
from pathlib import Path

from tests.lib.config import (
    SCHEMA_TYPE_MAP,
    load_ingest_configs,
    parse_currencies,
    resolve_gslib_path,
)


@dataclass(frozen=True)
class DeltaUpdateRange:
    range_id: str
    start_block: int
    end_block: int
    note: str = ""


# UTXO chains only -- the perf-targeted commit modifies update/utxo/update.py.
# Ranges start at 0 because UTXO delta-update needs the full tx_id sequence.
DELTA_UPDATE_RANGES: dict[str, list[DeltaUpdateRange]] = {
    "btc": [
        DeltaUpdateRange(
            "smoke", 0, 2000,
            note="2k blocks -- fast smoke test, perf numbers are not "
                 "representative because BTC genesis is mostly empty",
        ),
        # Genesis-era blocks are almost empty (block 170 has the first
        # non-coinbase tx, density stays in single-digit tx/block well past
        # block 100k). Going further out gives the perf-targeted code path
        # something to chew on.
        DeltaUpdateRange(
            "dense", 0, 30000,
            note="30k blocks -- mid-genesis era, picks up enough tx volume "
                 "for the batched UTXO update path to dominate fixed overheads",
        ),
    ],
    "ltc": [
        DeltaUpdateRange("early", 0, 2000, note="2k blocks from genesis"),
    ],
    "bch": [
        DeltaUpdateRange("early", 0, 2000, note="shares BTC history"),
    ],
    "zec": [
        DeltaUpdateRange("early", 0, 2000, note="2k blocks from genesis"),
    ],
}

ALL_CURRENCIES = list(DELTA_UPDATE_RANGES.keys())


@dataclass
class DeltaUpdateConfig:
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


def build_delta_update_configs() -> list[DeltaUpdateConfig]:
    currencies = parse_currencies("DELTA_UPDATE_CURRENCIES", ALL_CURRENCIES)
    gslib_path = resolve_gslib_path()
    ingest_configs = load_ingest_configs()

    configs = []
    for currency in currencies:
        ic = ingest_configs.get(currency)
        if not ic:
            continue
        ranges = DELTA_UPDATE_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                DeltaUpdateConfig(
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
