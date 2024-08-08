import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from ...db.analytics import AnalyticsDb
from ...utils.logging import LoggerScope
from .generic import Action

logger = logging.getLogger(__name__)

TABLE_NAME_DELTA_HISTORY = "delta_updater_history"
TABLE_NAME_DELTA_STATUS = "delta_updater_status"
TABLE_NAME_DIRTY = "dirty_addresses"
TABLE_NAME_NEW = "new_addresses"


def address_to_db_dict(db: AnalyticsDb, address: str):
    adr = db.transformed.to_db_address(address)
    return {"address": adr.db_encoding, "address_prefix": adr.prefix}


def write_new_addresses(db: AnalyticsDb, table_name_new: str, new_addresses: list):
    logger.info("Writing new addresses to cassandra.")
    na = [
        (address_to_db_dict(db, x), addr_id, ts, b_id, tx_hash)
        for x, addr_id, ts, b_id, tx_hash in new_addresses
    ]
    for dic, addr_id, ts, b_id, tx_hash in na:
        dic["address_id"] = addr_id
        dic["timestamp"] = ts
        dic["block_id"] = b_id
        dic["tx_hash"] = tx_hash
    db.transformed.ingest(
        table_name_new, [d for d, _, _, _, _ in na], upsert=True, cl=None
    )


def write_dirty_addresses(
    db: AnalyticsDb, table_name_dirty: str, dirty_addresses: list
):
    logger.info("Writing dirty addresses to cassandra.")
    db.transformed.ingest(
        table_name_dirty, [address_to_db_dict(db, x) for x in dirty_addresses]
    )


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
        rates, had_to_fill = forward_fill_rates_with_fill_value(rates, hbe, fill_values)
        if had_to_fill:
            logger.warning(
                "Missing exchange rates forward filled with "
                f"last good data from block {hbe} {fill_values}"
            )
    db.transformed.ingest("exchange_rates", rates)


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
        self._nr_new_addresses = 0
        self._nr_new_clusters = 0
        self._nr_new_transactions = 0
        self._highest_address_id = db.transformed.get_highest_address_id() or 0
        self._highest_cluster_id = db.transformed.get_highest_cluster_id() or 1
        self.forward_fill_rates = forward_fill_rates

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

    def process_batch(self, batch: Iterable[int]) -> Action:
        self._batch_start_time = time.time()

        batch_int = list(batch)
        with LoggerScope.debug(logger, "Importing exchange rates"):
            self.import_exchange_rates(batch_int)

        with LoggerScope.debug(logger, "Transform data"):
            action, final_block = self.process_batch_impl_hook(batch_int)

        self._time_last_batch = time.time() - self._batch_start_time

        if action == Action.CONTINUE:
            self._last_block_processed = final_block

        return action


class LegacyUpdateStrategy(AbstractUpdateStrategy):
    def __init__(self, db, currency, write_new, write_dirty, forward_fill_rates=False):
        super().__init__()
        self._db = db
        self._write_new = write_new
        self._write_dirty = write_dirty
        self._new_addresses = {}
        self._nr_queried_addresses = 0
        self._nr_new_addresses = 0
        self._highest_address_id = db.transformed.get_highest_address_id() or 0
        self.forward_fill_rates = forward_fill_rates

    def prepare_database(self):
        HISTORY_TABLE_COLUMNS = [
            "last_synced_block bigint",
            "last_synced_block_timestamp timestamp",
            "highest_address_id int",
            "timestamp timestamp",
            "write_new boolean",
            "write_dirty boolean",
            "runtime_seconds int",
        ]
        HISTORY_TABLE_PK = ["last_synced_block"]

        self._db.transformed.ensure_table_exists(
            TABLE_NAME_DELTA_HISTORY,
            HISTORY_TABLE_COLUMNS,
            HISTORY_TABLE_PK,
            truncate=False,
        )

        STATUS_TABLE_COLUMNS = ["keyspace_name text"] + HISTORY_TABLE_COLUMNS
        STATUS_TABLE_PK = ["keyspace_name"]

        self._db.transformed.ensure_table_exists(
            TABLE_NAME_DELTA_STATUS,
            STATUS_TABLE_COLUMNS,
            STATUS_TABLE_PK,
            truncate=False,
        )

    @abstractmethod
    def process_batch_impl_hook(self, batch):
        pass

    def consume_address_id(self):
        self._highest_address_id += 1
        return self._highest_address_id

    def process_batch(self, batch):
        # logging.info(f"Working in batch: [{batch[0]} - {batch[-1]}]")
        start_time = time.time()

        logger.debug("Start - Importing Exchange Rates")
        fill_and_store_rates(self._db, list(batch), self.forward_fill_rates)
        # rates = self._db.raw.get_exchange_rates_for_block_batch(list(batch))
        # self._db.transformed.ingest("exchange_rates", rates)
        logger.debug("End   - Importing Exchange Rates")

        logger.debug("Start - Chain Specific Import")
        self.process_batch_impl_hook(batch)
        logger.debug("End   - Chain Specific Import")

        self._time_last_batch = time.time() - start_time
        self._last_block_processed = batch[-1]

    def persist_updater_progress(self):
        data = {
            "last_synced_block": self.last_block_processed,
            "last_synced_block_timestamp": self._db.raw.get_block_timestamp(
                self.last_block_processed
            ),
            "highest_address_id": self._highest_address_id,
            "timestamp": datetime.now(),
            "write_new": self._write_new,
            "write_dirty": self._write_dirty,
            "runtime_seconds": int(self.elapsed_seconds_last_batch),
        }
        self._db.transformed.ingest(
            TABLE_NAME_DELTA_HISTORY,
            [data],
        )

        data["keyspace_name"] = self._db.transformed.get_keyspace()
        self._db.transformed.ingest(
            TABLE_NAME_DELTA_STATUS,
            [data],
        )
