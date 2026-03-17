"""Configuration for continuation (split-ingest) tests.

Defines block ranges per currency split into two halves. Verifies that
ingesting [0, N] in one shot produces the same output as ingesting
[0, M] then [M+1, N] sequentially with the same exporter instance.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from tests.deltalake.config import SCHEMA_TYPE_MAP, SCHEMA_TABLES_MAP, load_ingest_configs


def _range(range_id: str, start: int, note: str = "") -> "ContinuationRange":
    """Helper: always 4 blocks, split 2+2."""
    return ContinuationRange(range_id, start, start + 3, start + 1, note)


@dataclass(frozen=True)
class ContinuationRange:
    """A block range split into two halves for continuation testing."""

    range_id: str
    start_block: int
    end_block: int
    split_block: int  # last block of first ingest; second starts at split_block+1
    note: str = ""


# Always 4 blocks (2+2 split) at spots with real spending activity.
CONTINUATION_RANGES: dict[str, list[ContinuationRange]] = {
    "btc": [
        _range("mid", 600_000, "post-segwit era"),
        _range("new", 840_000, "recent era"),
    ],
    "ltc": [
        _range("mid", 1_700_000, "pre-MWEB era"),
        _range("mweb", 2_266_000, "post-MWEB activation"),
    ],
    "bch": [
        _range("mid", 700_000, "mid-life era"),
        _range("new", 850_000, "recent era"),
    ],
    "zec": [
        _range("sapling", 900_000, "Sapling/Blossom era"),
        _range("nu5", 1_687_104, "NU5 activation — tests finalorchardroot"),
    ],
    "eth": [
        _range("mid", 11_000_000, "pre-merge PoW era"),
        _range("new", 19_000_000, "post-merge era"),
    ],
    "trx": [
        _range("mid", 50_000_001, "mid-chain era"),
    ],
}

ALL_CURRENCIES = list(CONTINUATION_RANGES.keys())


@dataclass
class ContinuationConfig:
    currency: str
    range_id: str
    node_url: str
    secondary_node_references: list[str]
    start_block: int
    end_block: int
    split_block: int
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
        schema_type = SCHEMA_TYPE_MAP[self.currency]
        return SCHEMA_TABLES_MAP[schema_type]


def build_continuation_configs() -> list[ContinuationConfig]:
    """Build a ContinuationConfig per configured currency and range."""
    currencies_str = os.environ.get(
        "CONTINUATION_CURRENCIES", ",".join(ALL_CURRENCIES)
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
        ranges = CONTINUATION_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                ContinuationConfig(
                    currency=currency,
                    range_id=cr.range_id,
                    node_url=ic["node_url"],
                    secondary_node_references=ic.get(
                        "secondary_node_references", []
                    ),
                    start_block=cr.start_block,
                    end_block=cr.end_block,
                    split_block=cr.split_block,
                    schema_type=SCHEMA_TYPE_MAP.get(currency, "utxo"),
                    gslib_path=gslib_path,
                    range_note=cr.note,
                )
            )
    return configs
