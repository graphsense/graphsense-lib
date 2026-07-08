"""Tests for RawDb.find_highest_block_with_exchange_rates.

Block timestamps are not strictly monotonic (miner clock skew), so right
after a UTC day boundary a block can map to a date without exchange rates
while a *later* block still maps to the previous day. The frontier search
must lower its result to the last contiguous block with rates instead of
crashing (incident 2026-07-08: block 957103 stamped 00:00:56 UTC, block
957104 stamped before midnight).
"""

from unittest.mock import MagicMock

import pytest

import graphsenselib.db.analytics as analytics
from graphsenselib.db.utxo import RawDbUtxo


def make_db(has_rates, highest_block: int) -> RawDbUtxo:
    db = RawDbUtxo(MagicMock(), MagicMock())
    db.get_highest_block = lambda: highest_block
    db.get_exchange_rates_for_block_batch = lambda batch: [
        {
            "block_id": b,
            "fiat_values": [1.0, 2.0] if has_rates(b) else None,
        }
        for b in batch
    ]
    return db


def test_clean_frontier():
    db = make_db(lambda b: b <= 100, highest_block=150)
    assert db.find_highest_block_with_exchange_rates() == 100


def test_all_blocks_have_rates():
    db = make_db(lambda b: True, highest_block=150)
    assert db.find_highest_block_with_exchange_rates() == 150


def test_no_rates_at_all():
    db = make_db(lambda b: False, highest_block=150)
    assert db.find_highest_block_with_exchange_rates() == -1


def test_day_boundary_clock_skew_lowers_frontier(caplog):
    # Incident scenario: 957103 already maps to the new (rate-less) day,
    # 957104 still maps to the previous day.
    db = make_db(
        lambda b: b <= 957102 or b == 957104,
        highest_block=957128,
    )
    with caplog.at_level("WARNING"):
        assert db.find_highest_block_with_exchange_rates() == 957102
    assert "lowering the exchange-rate frontier" in caplog.text


def test_gap_wider_than_skew_window_raises(monkeypatch):
    # Force the binary searches to land on a frontier whose predecessors
    # lack rates for more than the skew window: not clock skew but a real
    # hole (missing rate day / missing blocks), which must be reported.
    monkeypatch.setattr(analytics, "binary_search", MagicMock(side_effect=[0, 1000]))
    db = make_db(lambda b: b == 999, highest_block=1500)
    with pytest.raises(ValueError, match="timestamp-skew window"):
        db.find_highest_block_with_exchange_rates()
