"""Configuration for Cassandra ingest regression tests.

Reads node URLs from .graphsense.yaml (same as delta lake tests).
"""

import os
from dataclasses import dataclass
from pathlib import Path

from tests.deltalake.config import SCHEMA_TYPE_MAP, load_ingest_configs


@dataclass(frozen=True)
class CassandraRange:
    """A single block range to test for a currency."""

    range_id: str
    start_block: int
    end_block: int
    note: str = ""


# Block ranges per currency.
# UTXO chains: must start from block 0 because mid-chain ranges require spent
# transaction outputs that aren't available in a fresh database.
# BTC 0-250 covers the first non-coinbase transaction (block 170, Satoshi→Hal
# Finney spending an output from block 9) plus a handful more real transactions
# in blocks 181-182+, giving us basic UTXO address resolution coverage.
# Account chains (ETH/TRX): mid-chain ranges work because there's no UTXO
# dependency.
CASSANDRA_TEST_RANGES: dict[str, list[CassandraRange]] = {
    "btc": [
        CassandraRange("genesis", 0, 250, "first real txs + UTXO resolution"),
    ],
    "eth": [
        CassandraRange("mid", 2_000_000, 2_000_024, "early PoW era"),
    ],
    "ltc": [
        CassandraRange("genesis", 0, 99, "coinbase-only genesis blocks"),
    ],
    "bch": [
        CassandraRange("genesis", 0, 99, "coinbase-only genesis blocks"),
    ],
    "zec": [
        CassandraRange("genesis", 0, 99, "coinbase-only genesis blocks"),
    ],
    "trx": [
        CassandraRange("mid", 50_000_001, 50_000_025, "mid-chain era"),
    ],
}

# Expected minimum row counts per table.
# These are conservative lower bounds — actual values depend on chain activity.
EXPECTED_MIN_ROWS = {
    "btc": {
        "block": 1,
        "transaction": 1,
        "block_transactions": 1,
        "transaction_by_tx_prefix": 1,
        "transaction_spent_in": 1,
        "transaction_spending": 1,
    },
    "eth": {"block": 1, "transaction": 1},
    "ltc": {
        "block": 1,
        "transaction": 1,
        "block_transactions": 1,
        "transaction_by_tx_prefix": 1,
    },
    "bch": {
        "block": 1,
        "transaction": 1,
        "block_transactions": 1,
        "transaction_by_tx_prefix": 1,
    },
    "zec": {
        "block": 1,
        "transaction": 1,
        "block_transactions": 1,
        "transaction_by_tx_prefix": 1,
    },
    "trx": {"block": 1, "transaction": 1},
}

ALL_CASSANDRA_CURRENCIES = list(CASSANDRA_TEST_RANGES.keys())

DEFAULT_REF_VERSION = "v25.11.18"

VANILLA_CASSANDRA_IMAGE = "cassandra:4.1.4"
FAST_CASSANDRA_IMAGE = os.environ.get(
    "CASSANDRA_TEST_IMAGE", "graphsense/cassandra-test:4.1.4"
)


@dataclass
class CassandraTestConfig:
    currency: str
    range_id: str
    node_url: str
    secondary_node_references: list[str]
    start_block: int
    end_block: int
    schema_type: str
    gslib_path: Path
    ref_version: str = DEFAULT_REF_VERSION
    range_note: str = ""

    @property
    def num_blocks(self) -> int:
        return self.end_block - self.start_block + 1

    @property
    def test_id(self) -> str:
        return f"{self.currency}-{self.range_id}"


def build_cassandra_configs() -> list[CassandraTestConfig]:
    """Build a CassandraTestConfig per configured currency and range."""
    ref_version = os.environ.get("CASSANDRA_REF_VERSION", DEFAULT_REF_VERSION)
    currencies_str = os.environ.get(
        "CASSANDRA_CURRENCIES", ",".join(ALL_CASSANDRA_CURRENCIES)
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
        ranges = CASSANDRA_TEST_RANGES.get(currency, [])
        for cr in ranges:
            configs.append(
                CassandraTestConfig(
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
                    ref_version=ref_version,
                    range_note=cr.note,
                )
            )
    return configs
