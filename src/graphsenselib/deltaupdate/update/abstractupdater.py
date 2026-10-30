import logging
import os
import socket
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from ...db.analytics import AnalyticsDb
from ...utils.logging import LoggerScope
from .generic import Action

logger = logging.getLogger(__name__)


@contextmanager
def accumulate_phase(timings: Optional[Dict[str, float]], key: str):
    """Accumulate wall-clock time of the with-block into timings[key].

    A None timings dict disables measurement, so call sites don't need
    to branch. Async-driver caveat: sections that only fire futures are
    cheap here; the wait/decode cost lands in whichever later section
    resolves them.
    """
    start = time.time()
    yield
    if timings is not None:
        timings[key] = timings.get(key, 0.0) + (time.time() - start)


def make_run_id() -> str:
    """Identifier for this updater process; used to fence WAL ownership."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


TABLE_NAME_DELTA_HISTORY = "delta_updater_history"
TABLE_NAME_DELTA_STATUS = "delta_updater_status"


def forward_fill_rates_with_fill_value(
    rates, fill_from_block, fill_values
) -> Tuple[List[Dict], bool]:
    return (
        [
            (
                {**x, **{"fiat_values": fill_values}}
                if x["fiat_values"] is None and x["block_id"] > fill_from_block
                else x
            )
            for x in rates
        ],
        any(
            x["fiat_values"] is None and x["block_id"] > fill_from_block for x in rates
        ),
    )


def fill_and_store_rates(db, batch, forward_fill_rates: bool):
    rates = db.raw.get_exchange_rates_for_block_batch(batch)
    if forward_fill_rates:
        hbe, fill_values = get_forward_fill_rate(db, forward_fill_rates)
        filled_blocks = [
            x["block_id"]
            for x in rates
            if x["fiat_values"] is None and x["block_id"] > hbe
        ]
        rates, had_to_fill = forward_fill_rates_with_fill_value(rates, hbe, fill_values)
        if had_to_fill:
            # Ingest fetches rates up to yesterday, so today's blocks are
            # always filled; only a missing earlier day is worth a warning.
            level = (
                logging.INFO
                if only_todays_blocks(db, filled_blocks)
                else logging.WARNING
            )
            logger.log(
                level,
                "Missing exchange rates forward filled with "
                f"last good data from block {hbe} {fill_values}",
            )
    db.transformed.ingest("exchange_rates", rates)


def only_todays_blocks(db, blocks: List[int]) -> bool:
    """True if every block has a timestamp on the current UTC day."""
    today = datetime.now(timezone.utc).date()
    timestamps = db.raw.get_block_timestamps_batch(blocks).values()
    return all(ts is not None and ts.date() == today for ts in timestamps)


def get_forward_fill_rate(db, forward_fill_rates: bool):
    if forward_fill_rates:
        hbe = db.raw.find_highest_block_with_exchange_rates()
        return (
            hbe,
            db.raw.get_exchange_rates_for_block_batch([hbe])[0]["fiat_values"],
        )
    else:
        return (None, None)


class AbstractUpdateStrategy(ABC):
    def __init__(self):
        self._time_last_batch = 0
        self._last_block_processed = None
        self._global_start_time = time.time()
        self._timing_exchange_rates = 0.0
        self._timing_delta_lake = 0.0
        self._timing_cassandra_read = 0.0
        self._timing_transform = 0.0
        self._timing_persist = 0.0
        # Sub-timing for Cassandra reads breakdown
        self._timing_cassandra_check_existence = 0.0
        self._timing_cassandra_read_addresses = 0.0
        self._timing_cassandra_query_relations = 0.0

    @property
    def start_time(self):
        return self._global_start_time

    @property
    def elapsed_seconds_global(self) -> float:
        return time.time() - self._global_start_time

    @property
    def elapsed_seconds_last_batch(self) -> float:
        return self._time_last_batch

    @property
    def last_block_processed(self) -> int:
        return self._last_block_processed

    @property
    def timing_summary(self) -> Dict[str, float]:
        return {
            "exchange_rates": self._timing_exchange_rates,
            "delta_lake": self._timing_delta_lake,
            "cassandra_read": self._timing_cassandra_read,
            "cassandra_read_breakdown": {
                "check_existence": self._timing_cassandra_check_existence,
                "read_addresses": self._timing_cassandra_read_addresses,
                "query_relations": self._timing_cassandra_query_relations,
            },
            "transform": self._timing_transform,
            "persist": self._timing_persist,
        }

    def reset_timing(self):
        self._timing_exchange_rates = 0.0
        self._timing_delta_lake = 0.0
        self._timing_cassandra_read = 0.0
        self._timing_transform = 0.0
        self._timing_persist = 0.0
        # Reset sub-timing
        self._timing_cassandra_check_existence = 0.0
        self._timing_cassandra_read_addresses = 0.0
        self._timing_cassandra_query_relations = 0.0

    @abstractmethod
    def prepare_database(self):
        pass

    @abstractmethod
    def consume_address_id(self):
        pass

    @abstractmethod
    def process_batch(self, batch: Iterable[int]):
        pass

    @abstractmethod
    def persist_updater_progress(self):
        pass


class UpdateStrategy(AbstractUpdateStrategy):
    def __init__(
        self, db: AnalyticsDb, currency: str, forward_fill_rates: bool = False
    ):
        super().__init__()
        self._db = db
        self._currency = currency
        self._batch_start_time = None
        self._batch_first_block = None
        self._nr_new_addresses = 0
        self._nr_new_clusters = 0
        self._nr_new_transactions = 0
        self._highest_address_id = db.transformed.get_highest_address_id() or 0
        self._highest_cluster_id = db.transformed.get_highest_cluster_id() or 1
        self.forward_fill_rates = forward_fill_rates
        self._run_id = make_run_id()
        self._wal = None
        self._wal_enabled = False

    def get_forward_fill_rate(self):
        return

    @property
    def currency(self):
        return self._currency

    @property
    def nr_new_addresses(self):
        return self._nr_new_addresses

    @property
    def highest_address_id(self):
        return self._highest_address_id

    @property
    def highest_cluster_id(self):
        return self._highest_cluster_id

    @property
    def batch_start_time(self):
        return self._batch_start_time

    @property
    def nr_new_clusters(self):
        return self._nr_new_clusters

    def consume_address_id(self):
        self._highest_address_id += 1
        self._nr_new_addresses += 1
        return self._highest_address_id

    def consume_cluster_id(self):
        self._highest_cluster_id += 1
        self._nr_new_clusters += 1
        return self._highest_cluster_id

    @abstractmethod
    def process_batch_impl_hook(self, batch) -> Tuple[Action, Optional[int]]:
        pass

    def import_exchange_rates(self, batch: List[int]):
        fill_and_store_rates(self._db, batch, self.forward_fill_rates)

    @property
    def wal(self):
        """Lazily-constructed redo write-ahead log (DB-backed, shared per
        keyspace). See graphsenselib.deltaupdate.wal."""
        if self._wal is None:
            from graphsenselib import __version__
            from graphsenselib.deltaupdate.wal import DeltaWal

            self._wal = DeltaWal(self._db.transformed, self._run_id, __version__)
            self._wal.ensure_schema()
        return self._wal

    def _stage_wal(self, changes, bookkeeping):
        """Durably record the resolved change set before any of it is applied.

        No-op unless the WAL is enabled. Call immediately before applying; pair
        with ``self.wal.clear()`` after the whole set (data + bookkeeping) is
        acknowledged. Recovery/replay of a staged record happens at run startup
        in ``deltaupdater._recover_pending_writes``.
        """
        if not self._wal_enabled or (not changes and not bookkeeping):
            return
        from graphsenselib import __version__
        from graphsenselib.deltaupdate.wal import WalRecord

        first = self._batch_first_block
        last = self.last_block_processed
        record = WalRecord(
            run_id=self._run_id,
            code_version=__version__,
            block_lo=first if first is not None else (last or 0),
            block_hi=last if last is not None else 0,
            changes=list(changes),
            bookkeeping=list(bookkeeping),
        )
        self.wal.stage(record)

    def process_batch(self, batch: Iterable[int]) -> Action:
        self._batch_start_time = time.time()

        batch_int = list(batch)
        self._batch_first_block = batch_int[0] if batch_int else None
        t_start = time.time()
        with LoggerScope.debug(logger, "Importing exchange rates"):
            self.import_exchange_rates(batch_int)
        self._timing_exchange_rates += time.time() - t_start

        with LoggerScope.debug(logger, "Transform data"):
            action, final_block = self.process_batch_impl_hook(batch_int)

        self._time_last_batch = time.time() - self._batch_start_time

        if action == Action.CONTINUE:
            self._last_block_processed = final_block

        return action
