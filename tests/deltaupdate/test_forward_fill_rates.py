import logging
from datetime import datetime, timedelta, timezone

import pytest

from graphsenselib.deltaupdate.update.abstractupdater import fill_and_store_rates

NOW = datetime.now(timezone.utc)
YESTERDAY = NOW - timedelta(days=1)
FILL = [1.0, 2.0]


class FakeRaw:
    def __init__(self, timestamps, rated):
        self.timestamps = timestamps  # block -> datetime or None
        self.rated = rated  # blocks that have a rate in the raw keyspace

    def get_exchange_rates_for_block_batch(self, batch):
        return [
            {"block_id": b, "fiat_values": FILL if b in self.rated else None}
            for b in batch
        ]

    def find_highest_block_with_exchange_rates(self):
        return max(self.rated)

    def get_block_timestamps_batch(self, blocks):
        return {b: self.timestamps[b] for b in blocks}


class FakeTransformed:
    def __init__(self):
        self.ingested = []

    def ingest(self, table, rows):
        self.ingested.append((table, rows))


class FakeDb:
    def __init__(self, timestamps, rated):
        self.raw = FakeRaw(timestamps, rated)
        self.transformed = FakeTransformed()


def fill_log_records(caplog):
    return [r for r in caplog.records if "forward filled" in r.getMessage()]


@pytest.mark.parametrize(
    "timestamps,level",
    [
        ({1: YESTERDAY, 2: NOW, 3: NOW}, logging.INFO),
        ({1: YESTERDAY - timedelta(days=1), 2: YESTERDAY, 3: NOW}, logging.WARNING),
        ({1: YESTERDAY, 2: NOW, 3: None}, logging.WARNING),
    ],
    ids=["only-today-missing", "earlier-day-missing", "unknown-timestamp"],
)
def test_forward_fill_warns_only_for_missing_past_days(caplog, timestamps, level):
    db = FakeDb(timestamps, rated={1})
    with caplog.at_level(logging.INFO):
        fill_and_store_rates(db, [1, 2, 3], forward_fill_rates=True)

    [record] = fill_log_records(caplog)
    assert record.levelno == level
    [(table, rows)] = db.transformed.ingested
    assert table == "exchange_rates"
    assert all(r["fiat_values"] == FILL for r in rows)


def test_no_fill_no_log(caplog):
    db = FakeDb({1: YESTERDAY, 2: NOW}, rated={1, 2})
    with caplog.at_level(logging.INFO):
        fill_and_store_rates(db, [1, 2], forward_fill_rates=True)
    assert fill_log_records(caplog) == []
