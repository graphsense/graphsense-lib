"""This file contains all db functions that are specific to the graphsense database.
Functions that have specific implementations based on the currency are stored
in the eth.py and btc.py files. Generic database functions belong in cassandra.py

Attributes:
    DATE_FORMAT (str): Format string of date format used by the database (str)
"""

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache, partial
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd
from cassandra import OperationTimedOut, WriteTimeout
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt

from ..config import keyspace_types
from ..datatypes import DbChangeType
from ..utils import GenericArrayFacade, binary_search, parse_timestamp
from ..utils.account import get_id_group, get_id_group_with_secondary_relations
from .cassandra import (
    CassandraDb,
    build_create_stmt,
    build_delete_stmt,
    build_insert_stmt,
    build_select_stmt,
    build_truncate_stmt,
)

CONCURRENCY = 2000
DATE_FORMAT = "%Y-%m-%d"
FIRST_BLOCK = defaultdict(lambda: 0)
FIRST_BLOCK["trx"] = 1
logger = logging.getLogger(__name__)


@dataclass
class DbChange:
    action: DbChangeType
    table: str
    data: dict

    @classmethod
    def new(Cls, table: str, data: dict):
        return Cls(action=DbChangeType.NEW, table=table, data=data)

    @classmethod
    def update(Cls, table: str, data: dict):
        return Cls(action=DbChangeType.UPDATE, table=table, data=data)

    @classmethod
    def truncate(Cls, table: str):
        return Cls(action=DbChangeType.TRUNCATE, table=table, data=None)

    @classmethod
    def delete(Cls, table: str, data: dict):
        return Cls(action=DbChangeType.DELETE, table=table, data=data)

    def get_cql_statement(self, keyspace):
        if self.action == DbChangeType.UPDATE or self.action == DbChangeType.NEW:
            return build_insert_stmt(
                columns=self.data.keys(), table=self.table, keyspace=keyspace
            )
        elif self.action == DbChangeType.TRUNCATE:
            return build_truncate_stmt(table=self.table, keyspace=keyspace)
        elif self.action == DbChangeType.DELETE:
            return build_delete_stmt(
                table=self.table, key_columns=self.data.keys(), keyspace=keyspace
            )
        else:
            raise Exception(
                f"Don't know how to build statement for action {self.action}."
            )


class KeyspaceConfig:
    def __init__(self, keyspace_name, db_type, address_type, tx_hash_type, currency):
        self._keyspace_name = keyspace_name
        self._db_type = db_type
        self._address_type = address_type
        self._tx_hash_type = tx_hash_type
        self._currency = currency

    @property
    def keyspace_name(self):
        return self._keyspace_name

    @property
    def db_type(self):
        return self._db_type

    @property
    def address_type(self):
        return self._address_type

    @property
    def tx_hash_type(self):
        return self._tx_hash_type


class WithinKeyspace:
    def select_stmt(
        self,
        table: str,
        columns: Sequence[str] = ["*"],
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        per_partition_limit: Optional[int] = None,
    ) -> str:
        return build_select_stmt(
            table,
            columns=columns,
            keyspace=self.get_keyspace(),
            where=where,
            limit=limit,
            per_partition_limit=per_partition_limit,
        )

    def get_keyspace(self):
        return self._keyspace


class DbReaderMixin:
    def select_safe(
        self,
        table: str,
        columns: Sequence[str] = ["*"],
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        per_partition_limit: Optional[int] = None,
        fetch_size=None,
    ):
        return self._db.execute_safe(
            self.select_stmt(
                table=table,
                columns=columns,
                where={k: f"%({k})s" for k, v in where.items()},
                limit=limit,
                per_partition_limit=per_partition_limit,
            ),
            where,
            fetch_size=fetch_size,
        )

    def select(
        self,
        table: str,
        columns: Sequence[str] = ["*"],
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        per_partition_limit: Optional[int] = None,
        fetch_size=None,
    ):
        return self._db.execute(
            self.select_stmt(
                table=table,
                columns=columns,
                where=where,
                limit=limit,
                per_partition_limit=per_partition_limit,
            ),
            fetch_size=fetch_size,
        )

    def select_one(
        self, table: str, columns: Sequence[str] = ["*"], where: Optional[dict] = None
    ):
        return self._at_most_one_result(
            self.select(table=table, columns=columns, where=where, limit=2)
        )

    def select_one_safe(
        self, table: str, columns: Sequence[str] = ["*"], where: Optional[dict] = None
    ):
        return self._at_most_one_result(
            self.select_safe(table=table, columns=columns, where=where, limit=2)
        )

    def get_columns_for_table(self, table: str):
        return self._db.get_columns_for_table(self.get_keyspace(), table)

    def select_async(
        self,
        table: str,
        columns: Sequence[str] = ["*"],
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        per_partition_limit: Optional[int] = None,
        fetch_size=None,
    ):
        return self._db.execute_async(
            self.select_stmt(
                table=table,
                columns=columns,
                where=where,
                limit=limit,
                per_partition_limit=per_partition_limit,
            ),
            fetch_size=fetch_size,
        )

    def select_async_safe(
        self,
        table: str,
        columns: Sequence[str] = ["*"],
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        per_partition_limit: Optional[int] = None,
        fetch_size=None,
    ):
        return self._db.execute_async_safe(
            self.select_stmt(
                table=table,
                columns=columns,
                where={k: f"%({k})s" for k, v in where.items()},
                limit=limit,
                per_partition_limit=per_partition_limit,
            ),
            where,
            fetch_size=fetch_size,
        )

    def _get_hightest_id(
        self, table="block", sanity_check=True, id_col=None
    ) -> Optional[int]:
        """Return last ingested address ID from a table."""

        if id_col is None:
            id_col = f"{table}_id"

        group_id_col = f"{id_col}_group"

        result = self.select(table=table, columns=[group_id_col], per_partition_limit=1)
        groups = [getattr(row, group_id_col) for row in result.current_rows]

        if len(groups) > 0:
            result = self.select(
                table,
                columns=[f"MAX({id_col}) AS max"],
                where={group_id_col: max(groups)},
            )
            highest_id = int(result.current_rows[0].max)

            if sanity_check:
                self._ensure_is_highest_id(table, id_col, highest_id)

            return highest_id
        else:
            return None

    def _get_bucket_divisors_by_table_name(self) -> dict:
        raise Exception("Must be implemented in chain specific child class")

    def _ensure_is_highest_id(
        self, table: str, id_col: str, query_id: Optional[int]
    ) -> bool:
        if query_id is None:
            return None

        groups = self._get_bucket_divisors_by_table_name()

        if table in groups:
            # this case handles tables with group ids.
            group_id_col = f"{id_col}_group"
            bucket_divisor = groups[table]
            highest_plus_one = query_id + 1

            id_group_highest_plus_one = self.get_id_group(
                highest_plus_one, bucket_divisor
            )

            w = {group_id_col: id_group_highest_plus_one, id_col: highest_plus_one}
        else:
            # this case handles tables with no group column and increasing integer ids.
            w = {id_col: query_id + 1}
        result = self.select(table, columns=[id_col], where=w)
        if len(result.current_rows) > 0:
            raise Exception(
                (
                    f"Something went wrong {query_id} "
                    " is not the highest_id"
                    f" {query_id + 1} exist in {table}.{id_col}"
                )
            )

    def _at_most_one_result(self, result):
        if len(result.current_rows) > 1:
            raise Exception("Config tables are supposed to only have exactly one row.")
        return result.current_rows[0] if len(result.current_rows) > 0 else None

    def _get_only_row_from_table(self, table: str = "configuration"):
        return self._at_most_one_result(self.select(table, limit=2))

    def get_id_group(self, id_, bucket_size):
        return get_id_group(id_, bucket_size)


class DbWriterMixin:
    """
    This mixin requires the object to provide a CassandraDB instance at
    self._db and requires the object to provide the WithinKeyspace mixin
    """

    def apply_changes(self, changes: List[DbChange], atomic=True, nr_retries=10):
        statements = [
            (chng.get_cql_statement(keyspace=self.get_keyspace()), chng)
            for chng in changes
        ]
        unique_statements = {stmt for stmt, _ in statements}

        prepared_statements = {
            stmt: self._db.get_prepared_statement(stmt) for stmt in unique_statements
        }

        change_stmts = [
            prepared_statements[
                chng.get_cql_statement(keyspace=self.get_keyspace())
            ].bind(chng.data)
            for chng in changes
        ]

        attempts_made = 0
        for attempt in Retrying(
            retry=retry_if_exception_type((WriteTimeout, OperationTimedOut)),
            reraise=True,
            stop=stop_after_attempt(nr_retries),
        ):
            # see https://tenacity.readthedocs.io/en/latest/#retrying-code-block
            with attempt:
                attempts_made += 1
                if attempts_made > 1:
                    logger.warning(
                        "Applying changes ran into a write timeout. "
                        f"Retrying {(nr_retries - attempts_made) + 1} more times."
                    )
                if atomic:
                    self._db.execute_statements_atomic(change_stmts)
                else:
                    self._db.execute_statements(change_stmts)

    def ensure_table_exists(
        self,
        table_name: str,
        columns: Sequence[str],
        pk_columns: Sequence[str],
        truncate: bool = False,
    ):
        self._db.execute(
            build_create_stmt(
                columns,
                pk_columns,
                table_name,
                fail_if_exists=False,
                keyspace=self.get_keyspace(),
            )
        )
        if truncate:
            self._db.execute(
                build_truncate_stmt(table_name, keyspace=self.get_keyspace())
            )

    def ingest(
        self,
        table: str,
        items: Iterable,
        upsert=True,
        cl=None,
        concurrency: int = 100,
        auto_none_to_unset=False,
    ):
        self._db.ingest(
            table,
            self.get_keyspace(),
            items,
            upsert=upsert,
            cl=cl,
            concurrency=concurrency,
            auto_none_to_unset=auto_none_to_unset,
        )


def get_last_notnone(result, start, end):
    for i in range(end, start, -1):
        if result[i] is not None:
            return i
    return -1


class RawDb(ABC, WithinKeyspace, DbReaderMixin, DbWriterMixin):
    def __init__(self, keyspace_config: KeyspaceConfig, db: CassandraDb):
        self._keyspace_config = keyspace_config
        self._keyspace = keyspace_config.keyspace_name
        self._db = db

    def _get_bucket_divisors_by_table_name(self) -> dict:
        return {
            "block": self.get_block_bucket_size(),
            "transaction": self.get_tx_bucket_size(),
        }

    def exists(self) -> bool:
        return self._db.has_keyspace(self._keyspace)

    def keyspace_name(self) -> str:
        return self._keyspace

    def get_summary_statistics(self) -> Optional[object]:
        if not self.exists():
            return None
        if self._db.has_table(self._keyspace, "summary_statistics"):
            return self._get_only_row_from_table("summary_statistics")
        else:
            return None

    def find_block_nr_for_date(self, date: datetime) -> int:
        hb = self.get_highest_block()
        start = 0

        def get_item(date, index):
            daq = self.get_block_timestamp(index)
            return 0 if daq <= date else 1

        get_item_date = partial(get_item, date)

        r = binary_search(GenericArrayFacade(get_item_date), 1, lo=start, hi=hb)
        # r = get_last_notnone(GenericArrayFacade(get_item_date), start, hb)

        if r == -1:
            # minus one could mean two things, either
            # no missing exchangesrates are found within the lookback range
            # or that all have exchange rates, so we recheck if the
            # second case applies
            er = self.get_block_timestamp(hb)
            if er is not None:
                r = hb
        else:
            r = r - 1

        return r

    def find_highest_block_with_exchange_rates(
        self, lookback_in_blocks=86400, validate=True
    ) -> int:
        """Summary
            Searches for the highest block with available exchange_rates in
            the raw keyspace this is the maximum block the importer works on.

        Args:
            lookback_in_blocks (int, optional): how many blocks to look in the past.
                                                Default 86400 so approx.
                                                13 days on ethereum pace
        Returns:
            int: Last block that exchangerates are available
        """
        hb = self.get_highest_block()

        start = max(hb - lookback_in_blocks, 0)

        def has_er_value(result, i=0):
            return result[i]["fiat_values"] is not None

        def get_item(index):
            batch = self.get_exchange_rates_for_block_batch([index])
            return 0 if has_er_value(batch) else 1

        def get_item_first_rates(index):
            batch = self.get_exchange_rates_for_block_batch([index])
            return 1 if has_er_value(batch) else 0

        # find first block with exchange rates
        first_rates = max(
            binary_search(GenericArrayFacade(get_item_first_rates), 1, lo=start, hi=hb),
            0,
        )
        r = binary_search(GenericArrayFacade(get_item), 1, lo=first_rates, hi=hb)

        if r == -1:
            # minus one could mean two things, either
            # no missing exchangesrates are found within the lookback range
            # or that all have exchange rates, so we recheck if the
            # second case applies
            er = self.get_exchange_rates_for_block_batch([hb])
            if len(er) > 0 and has_er_value(er):
                r = hb
        else:
            r = r - 1

        if validate and r != -1:
            # ers = self.get_exchange_rates_for_block_batch([r - 1, r, r + 1])
            ers = self.get_exchange_rates_for_block_batch([r - 1, r])
            assert has_er_value(ers, i=0) and has_er_value(ers, i=1)
            # if has_er_value(ers, i=2):
            #     logger.warning(f"Found exchange-rate for "
            # "not yet imported block {r+1}")

        return r

    @lru_cache(maxsize=1)
    def get_configuration(self) -> Optional[object]:
        return self._get_only_row_from_table("configuration")

    def is_configuration_populated(self) -> bool:
        return self._get_only_row_from_table("configuration") is not None

    def get_exchange_rates(self, table=None) -> Iterable:
        r = self.select("exchange_rates" if table is None else table)
        return list(r)

    def get_last_exchange_rate_date(self, table=None) -> Optional[datetime]:
        ra = self.get_exchange_rates(table=table)
        return (
            max([datetime.fromisoformat(r.date) for r in ra]) if len(ra) > 0 else None
        )

    def get_exchange_rates_by_date(self, date) -> Iterable:
        date_str = date.strftime(DATE_FORMAT)
        r = self.select("exchange_rates", where={"date": date_str})
        return self._at_most_one_result(r)

    def get_block_bucket_size(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.block_bucket_size) if config is not None else None

    def get_tx_bucket_size(self) -> Optional[int]:
        config = self.get_configuration()
        return (
            int(config.tx_bucket_size)
            if config is not None and "tx_bucket_size" in dir(config)
            else None
        )

    def get_tx_prefix_length(self) -> Optional[int]:
        config = self.get_configuration()
        return (
            int(config.tx_prefix_length)
            if config is not None and "tx_prefix_length" in dir(config)
            else None
        )

    def get_highest_block(self, sanity_check=True) -> Optional[int]:
        """Return last ingested block ID from block table."""
        if self.exists():
            return self._get_hightest_id(table="block", sanity_check=sanity_check)
        else:
            return None

    @abstractmethod
    def get_transaction_ids_in_block(self, block: int) -> Iterable:
        raise Exception("Must be implemented in chain specific child class")

    @abstractmethod
    def get_addresses_in_block(self, block: int) -> Iterable:
        raise Exception("Must be implemented in chain specific child class")

    @abstractmethod
    def get_transactions_in_block(self, block: int) -> Iterable:
        raise Exception("Must be implemented in chain specific child class")

    def get_block_timestamps_batch(self, blocks: list[int]):
        bucket_size = self.get_block_bucket_size()
        stmt = self.select_stmt(
            "block",
            columns=["timestamp"],
            where={"block_id_group": "?", "block_id": "?"},
            limit=1,
        )
        parameters = [(b, [self.get_id_group(b, bucket_size), b]) for b in blocks]
        results = self._db.execute_batch(stmt, parameters)
        return {
            a: (
                (parse_timestamp(row.current_rows[0].timestamp))
                if len(row.current_rows) > 0
                else None
            )
            for (a, row) in results
        }

    def get_block_timestamp(self, block: int) -> datetime:
        btc = self.get_block_timestamps_batch([block])
        return btc[block]

    def get_exchange_rates_batch(self, dates: list[datetime]):
        stmt = self.select_stmt("exchange_rates", where={"date": "?"}, limit=1)
        ds = list({date.strftime(DATE_FORMAT) for date in dates if date is not None})
        results = self._db.execute_batch(stmt, [(d, [d]) for d in ds])

        # TODO maybe refactor with _at_most_one?
        def get_result_item(r):
            r = r.current_rows
            return r[0] if len(r) > 0 else None

        return {a: get_result_item(row) for (a, row) in results}

    def get_exchange_rates_for_block_batch(self, batch: list[int]):
        block_to_ts = self.get_block_timestamps_batch(batch)
        exchange_rate_to_date = self.get_exchange_rates_batch(block_to_ts.values())

        def get_values_list(er):
            return (
                [er.fiat_values.get(x) for x in list(er.fiat_values.keys())]
                if er is not None
                else None
            )

        ers = [
            (
                b,
                (
                    exchange_rate_to_date[block_to_ts[b].strftime(DATE_FORMAT)]
                    if block_to_ts[b] is not None
                    else None
                ),
            )
            for b in batch
        ]
        return [{"block_id": b, "fiat_values": get_values_list(er)} for b, er in ers]


class TransformedDb(ABC, WithinKeyspace, DbReaderMixin, DbWriterMixin):
    def __init__(self, keyspace_config: KeyspaceConfig, db: CassandraDb):
        self._keyspace_config = keyspace_config
        self._keyspace = keyspace_config.keyspace_name
        self._db = db
        self._db_config = None

    def _get_bucket_divisors_by_table_name(self) -> dict:
        bucket_size = self.get_address_id_bucket_size()
        return {
            "address": bucket_size,
            "cluster": self.get_cluster_id_bucket_size(),
            "transaction_ids_by_transaction_id_group": bucket_size,
        }

    def exists(self) -> bool:
        return self._db.has_keyspace(self._keyspace)

    def keyspace_name(self) -> str:
        return self._keyspace

    def get_summary_statistics(self) -> Optional[object]:
        if not self.exists():
            return None
        return self._get_only_row_from_table("summary_statistics")

    def get_exchange_rates_by_block(self, block) -> Iterable:
        return self.select_one("exchange_rates", where={"block_id": block})

    def get_address_id_bucket_size(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.bucket_size) if config is not None else None

    def get_block_id_bucket_size(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.bucket_size) if config is not None else None

    def get_address_transactions_id_bucket_size(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.block_bucket_size_address_txs) if config is not None else None

    def get_addressrelations_ids_nbuckets(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.addressrelations_ids_nbuckets) if config is not None else None

    def get_cluster_id_bucket_size(self) -> Optional[int]:
        return self.get_address_id_bucket_size()

    def get_configuration(self) -> Optional[object]:
        if self._db_config is None:
            self._db_config = self._get_only_row_from_table("configuration")
        return self._db_config

    def is_configuration_populated(self) -> bool:
        return self._get_only_row_from_table("configuration") is not None

    def get_highest_address_id(self, sanity_check=True) -> Optional[int]:
        """Return last ingested address ID from address table."""
        du = self.get_last_delta_updater_state()
        ha = self._get_hightest_id(table="address", sanity_check=sanity_check)
        return max(ha or 0, du.highest_address_id) if du is not None else ha

    def get_highest_transaction_id(self):
        return None

    @abstractmethod
    def get_highest_cluster_id(self, sanity_check=True) -> Optional[int]:
        raise Exception("Must be implemented in chain specific child class")

    def get_highest_block(self) -> Optional[int]:
        stats = self.get_summary_statistics()
        if stats is None:
            return None

        # minus one when starting to count at 0
        height_minus_noblocks = FIRST_BLOCK[self._keyspace_config._currency] - 1
        height = height_minus_noblocks + int(stats.no_blocks)

        return height

    def is_first_delta_update_run(self) -> bool:
        stats = self.get_summary_statistics()
        return stats is not None and int(stats.no_blocks) == int(
            stats.no_blocks_transform
        )

    def get_highest_block_fulltransform(self) -> Optional[int]:
        stats = self.get_summary_statistics()
        height_minus_noblocks = FIRST_BLOCK[self._keyspace_config._currency] - 1
        if stats is None:
            return None

        if hasattr(stats, "no_blocks_transform"):
            return height_minus_noblocks + int(stats.no_blocks_transform)
        else:
            return height_minus_noblocks + int(stats.no_blocks)

    def get_highest_exchange_rate_block(self, sanity_check=True) -> Optional[int]:
        res = self.select("exchange_rates", columns=["block_id"], per_partition_limit=1)
        m = max([x.block_id for x in res])
        if sanity_check:
            self._ensure_is_highest_id("exchange_rates", "block_id", m)
        return m

    def get_highest_block_delta_updater(self, sanity_check=True) -> Optional[int]:
        hb = self.get_highest_block()
        du = self.get_last_delta_updater_state()
        if du is not None and sanity_check:
            self._ensure_is_highest_id(
                "delta_updater_history", "last_synced_block", du.last_synced_block
            )

        return int(du.last_synced_block) if du is not None else hb

    def get_delta_updater_history(self) -> Iterable:
        if self._db.has_table(self._keyspace, "delta_updater_history"):
            return list(self.select("delta_updater_history", fetch_size=100))
        else:
            return []

    def get_last_delta_updater_state(self):
        if self._db.has_table(self._keyspace, "delta_updater_status"):
            return self._get_only_row_from_table("delta_updater_status")
        else:
            return None

    def get_address_prefix_length(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.address_prefix_length) if config is not None else None

    def to_db_address(self, address):
        Address = self._keyspace_config.address_type
        return Address(address, self.get_configuration())

    def to_db_tx_hash(self, tx_hash):
        TxHash = self._keyspace_config.tx_hash_type
        return TxHash(tx_hash, self.get_configuration())

    @lru_cache(maxsize=1_000_000)
    def knows_address(self, address: Union[str, bytearray]) -> bool:
        """Checks if address is in transformed keyspace.
        uses lru cache of 1_000_000 items. This should speedup lookups.
        Conservative estimate is that a full cache is about 130 MB

        Args:
            address (Union[str, bytearray]): Address as either bytearray or
                                             hex string.

        Returns:
            bool: True if present
        """
        adr = self.to_db_address(address)
        w = {"address_prefix": f"'{adr.prefix}'", "address": adr.db_encoding_query}
        rows = self.select(
            "address_ids_by_address_prefix", columns=["address"], where=w, limit=1
        )
        return len(rows.current_rows) > 0

    def known_addresses_batch(
        self, address: list[str], inlcude_new_adresses_table=True
    ):
        stmt = self.select_stmt(
            "address_ids_by_address_prefix",
            columns=["address"],
            where={"address_prefix": "?", "address": "?"},
            limit=1,
        )
        adrs = [(a, self.to_db_address(a)) for a in address]
        parameters = [(a, [adr.prefix, adr.db_encoding]) for a, adr in adrs]

        results = self._db.execute_batch_async(stmt, parameters)
        if inlcude_new_adresses_table:
            stmt = self.select_stmt(
                "new_addresses",
                columns=["address"],
                where={"address_prefix": "?", "address": "?"},
                limit=1,
            )
            results_new = self._db.execute_batch_async(stmt, parameters)
            r1 = {
                a: (len(row.current_rows) > 0)
                for (a, row) in self._db.await_batch(results)
            }
            r2 = {
                a: (len(row.current_rows) > 0)
                for (a, row) in self._db.await_batch(results_new)
            }

            return {a: (r or r2[a]) for (a, r) in r1.items()}

        else:
            return {
                a: (len(row.current_rows) > 0)
                for (a, row) in self._db.await_batch(results)
            }

    def get_token_configuration(self):
        stmt = self.select_stmt("token_configuration", limit=100)
        res = self._db.execute(stmt)
        df = pd.DataFrame(res)
        df["token_address"] = df["token_address"].apply(lambda x: "0x" + x.hex())
        return df

    def get_address_id_async_batch(self, addresses: List[str]):
        stmt = self.select_stmt(
            "address_ids_by_address_prefix",
            columns=["*"],
            where=dict.fromkeys(["address_prefix", "address"], "?"),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind({"address_prefix": f"{adr.prefix}", "address": adr.db_encoding})
            for adr in [self.to_db_address(address) for address in addresses]
        ]

        return zip(
            addresses,
            self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY),
        )

    def get_address_id_async(self, address: str):
        adr = self.to_db_address(address)
        w = {"address_prefix": f"'{adr.prefix}'", "address": adr.db_encoding_query}
        return self.select_async(
            "address_ids_by_address_prefix",
            columns=["*"],
            where=w,
            limit=1,
        )

    def get_address_async_batch(self, address_ids: List[int]):
        bs = self.get_address_id_bucket_size()
        stmt = self.select_stmt(
            "address",
            columns=["*"],
            where=dict.fromkeys(["address_id_group", "address_id"], "?"),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "address_id_group": self.get_id_group(addr_id, bs),
                    "address_id": addr_id,
                }
            )
            for addr_id in address_ids
        ]

        return zip(
            address_ids,
            self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY),
        )

    def get_address_async(self, address_id: int):
        bucket = self.get_id_group(address_id, self.get_address_id_bucket_size())
        w = {"address_id_group": bucket, "address_id": f"{address_id}"}
        return self.select_async(
            "address",
            columns=["*"],
            where=w,
            limit=1,
        )

    def get_address_incoming_relations_async_batch(
        self, rel_ids: List[Tuple[int, int]]
    ):
        stmt = self.select_stmt(
            "address_incoming_relations",
            columns=["*"],
            where=dict.fromkeys(
                ["dst_address_id_group", "dst_address_id", "src_address_id"], "?"
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "dst_address_id_group": self.get_id_group(
                        dst_address, self.get_address_id_bucket_size()
                    ),
                    "dst_address_id": dst_address,
                    "src_address_id": src_address,
                }
            )
            for dst_address, src_address in rel_ids
        ]

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_address_inrelations_async_batch_account(
        self, rel_ids: List[Tuple[int, int]]
    ):
        stmt = self.select_stmt(
            "address_incoming_relations",
            columns=["*"],
            where=dict.fromkeys(
                [
                    "dst_address_id_group",
                    "dst_address_id_secondary_group",
                    "dst_address_id",
                    "src_address_id",
                ],
                "?",
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bucketsize = self.get_address_id_bucket_size()
        relations_nbuckets = self.get_addressrelations_ids_nbuckets()

        bstmts = []
        for dst_address, src_address in rel_ids:
            address_group, secondary_group = get_id_group_with_secondary_relations(
                dst_address, src_address, bucketsize, relations_nbuckets
            )
            bstmts.append(
                prep.bind(
                    {
                        "dst_address_id_group": address_group,
                        "dst_address_id_secondary_group": secondary_group,
                        "dst_address_id": dst_address,
                        "src_address_id": src_address,
                    }
                )
            )

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_address_incoming_relations_async(
        self, address_id: int, src_address_id: Optional[int]
    ):
        w = {
            "dst_address_id_group": self.get_id_group(
                address_id, self.get_address_id_bucket_size()
            ),
            "dst_address_id": address_id,
        }
        if src_address_id is not None:
            w["src_address_id"] = src_address_id
        return self.select_async(
            "address_incoming_relations",
            columns=["*"],
            where=w,
            limit=1,
        )

    def get_max_secondary_ids_async(
        self, address_id_groups: List[int], tablename: str, id_group_col: str
    ):
        stmt = self.select_stmt(
            tablename,  # address_transactions_secondary_ids
            columns=["*"],
            where=dict.fromkeys(
                [
                    id_group_col,
                ],
                "?",
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind({id_group_col: address_id_group})
            for address_id_group in address_id_groups
        ]

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_address_outgoing_relations_async_batch(
        self, rel_ids: List[Tuple[int, int]]
    ):
        stmt = self.select_stmt(
            "address_outgoing_relations",
            columns=["*"],
            where=dict.fromkeys(
                ["src_address_id_group", "src_address_id", "dst_address_id"], "?"
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "src_address_id_group": self.get_id_group(
                        src_address, self.get_address_id_bucket_size()
                    ),
                    "src_address_id": src_address,
                    "dst_address_id": dst_address,
                }
            )
            for src_address, dst_address in rel_ids
        ]

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_address_outrelations_async_batch_account(
        self, rel_ids: List[Tuple[int, int]]
    ):
        stmt = self.select_stmt(
            "address_outgoing_relations",
            columns=["*"],
            where=dict.fromkeys(
                [
                    "src_address_id_group",
                    "src_address_id_secondary_group",
                    "src_address_id",
                    "dst_address_id",
                ],
                "?",
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)
        bucketsize = self.get_address_id_bucket_size()
        relations_nbuckets = self.get_addressrelations_ids_nbuckets()

        bstmts = []
        for src_address, dst_address in rel_ids:
            address_group, secondary_group = get_id_group_with_secondary_relations(
                src_address, dst_address, bucketsize, relations_nbuckets
            )
            bstmts.append(
                prep.bind(
                    {
                        "src_address_id_group": address_group,
                        "src_address_id_secondary_group": secondary_group,
                        "src_address_id": src_address,
                        "dst_address_id": dst_address,
                    }
                )
            )

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_balance_async_batch_account(self, address_ids: List[id]):
        stmt = self.select_stmt(
            "balance",
            columns=["*"],
            where=dict.fromkeys(["address_id_group", "address_id"], "?"),
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "address_id_group": self.get_id_group(
                        address_id, self.get_address_id_bucket_size()
                    ),
                    "address_id": address_id,
                }
            )
            for address_id in address_ids
        ]

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_address_outgoing_relations_async(
        self, address_id: int, dst_address_id: Optional[int]
    ):
        w = {
            "src_address_id_group": self.get_id_group(
                address_id, self.get_address_id_bucket_size()
            ),
            "src_address_id": address_id,
        }
        if dst_address_id is not None:
            w["dst_address_id"] = dst_address_id
        return self.select_async(
            "address_outgoing_relations",
            columns=["*"],
            where=w,
            limit=1,
        )

    def get_cluster_async_batch(self, cluster_ids: List[int]):
        bs = self.get_cluster_id_bucket_size()
        stmt = self.select_stmt(
            "cluster",
            columns=["*"],
            where=dict.fromkeys(["cluster_id_group", "cluster_id"], "?"),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "cluster_id_group": self.get_id_group(clstr_id, bs),
                    "cluster_id": clstr_id,
                }
            )
            for clstr_id in cluster_ids
        ]

        return zip(
            cluster_ids,
            self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY),
        )

    def get_cluster_async(self, cluster_id: int):
        bucket = self.get_id_group(cluster_id, self.get_cluster_id_bucket_size())
        w = {"cluster_id_group": bucket, "cluster_id": f"{cluster_id}"}
        return self.select_async(
            "cluster",
            columns=["*"],
            where=w,
            limit=1,
        )

    def get_cluster_incoming_relations_async_batch(
        self, rel_ids: List[Tuple[int, int]]
    ):
        stmt = self.select_stmt(
            "cluster_incoming_relations",
            columns=["*"],
            where=dict.fromkeys(
                ["dst_cluster_id_group", "dst_cluster_id", "src_cluster_id"], "?"
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "dst_cluster_id_group": self.get_id_group(
                        dst_address, self.get_address_id_bucket_size()
                    ),
                    "dst_cluster_id": dst_address,
                    "src_cluster_id": src_address,
                }
            )
            for dst_address, src_address in rel_ids
        ]

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_cluster_incoming_relations_async(
        self, cluster_id: int, src_cluster_id: Optional[int]
    ):
        w = {
            "dst_cluster_id_group": self.get_id_group(
                cluster_id, self.get_cluster_id_bucket_size()
            ),
            "dst_cluster_id": cluster_id,
        }
        if src_cluster_id is not None:
            w["src_cluster_id"] = src_cluster_id
        return self.select_async(
            "cluster_incoming_relations",
            columns=["*"],
            where=w,
            limit=1,
        )

    def get_cluster_outgoing_relations_async_batch(
        self, rel_ids: List[Tuple[int, int]]
    ):
        stmt = self.select_stmt(
            "cluster_outgoing_relations",
            columns=["*"],
            where=dict.fromkeys(
                ["src_cluster_id_group", "src_cluster_id", "dst_cluster_id"], "?"
            ),
            limit=1,
        )
        prep = self._db.get_prepared_statement(stmt)

        bstmts = [
            prep.bind(
                {
                    "src_cluster_id_group": self.get_id_group(
                        src_address, self.get_address_id_bucket_size()
                    ),
                    "src_cluster_id": src_address,
                    "dst_cluster_id": dst_address,
                }
            )
            for src_address, dst_address in rel_ids
        ]

        return self._db.execute_statements_async(bstmts, concurrency=CONCURRENCY)

    def get_cluster_outgoing_relations_async(
        self, cluster_id: int, dst_cluster_id: Optional[int]
    ):
        w = {
            "src_cluster_id_group": self.get_id_group(
                cluster_id, self.get_cluster_id_bucket_size()
            ),
            "src_cluster_id": cluster_id,
        }
        if dst_cluster_id is not None:
            w["dst_cluster_id"] = dst_cluster_id
        return self.select_async(
            "cluster_outgoing_relations",
            columns=["*"],
            where=w,
            limit=1,
        )

    def has_delta_updater_v1_tables(self) -> bool:
        return (
            self._db.has_table(self._keyspace, "delta_updater_state")
            or self._db.has_table(self._keyspace, "dirty_addresses")
            or self._db.has_table(self._keyspace, "new_addresses")
        )


class AnalyticsDb:
    """Unified analytics DB interface"""

    def __init__(
        self, raw: KeyspaceConfig, transformed: KeyspaceConfig, db: CassandraDb
    ):
        self._raw_config = raw
        self._transformed_config = transformed
        self._db = db
        self._raw = raw.db_type(raw, db)
        self._transformed = transformed.db_type(transformed, db)

    def __repr__(self):
        return (
            f"Raw: {self._raw_config.keyspace_name}, "
            f"Transformed: {self._transformed_config.keyspace_name}, "
            f"DB: {self._db}"
        )

    def db(self) -> CassandraDb:
        return self._db

    def open(self):
        self._db.connect()

    def close(self):
        self._db.close()

    def __enter__(self):
        self.open()
        return self

    def clone(self) -> "AnalyticsDb":
        db_cloned = self._db.clone()
        return AnalyticsDb(self._raw_config, self._transformed_config, db_cloned)

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def by_ks_type(self, keyspace_type):
        if keyspace_type == "transformed":
            return self.transformed
        elif keyspace_type == "raw":
            return self.raw
        else:
            raise Exception(f"Unknown keyspace type choose from {keyspace_types}")

    @property
    def raw(self) -> RawDb:
        return self._raw

    @property
    def transformed(self) -> TransformedDb:
        return self._transformed
