"""This file contains all db functions that are specific to the graphsense database.
Functions that have specific implementations based on the currency are stored
in the eth.py and btc.py files. Generic database functions belong in cassandra.py

Attributes:
    DATE_FORMAT (str): Format string of date format used by the database (str)
"""
from abc import ABC, abstractmethod
from datetime import datetime
from functools import lru_cache
from typing import Iterable, Optional, Sequence, Union

from ..utils import GenericArrayFacade, binary_search
from .cassandra import (
    CassandraDb,
    build_create_stmt,
    build_select_stmt,
    build_truncate_stmt,
)

DATE_FORMAT = "%Y-%m-%d"


class KeyspaceConfig:
    def __init__(self, keyspace_name, db_type, address_type):
        self._keyspace_name = keyspace_name
        self._db_type = db_type
        self._address_type = address_type

    @property
    def keyspace_name(self):
        return self._keyspace_name

    @property
    def db_type(self):
        return self._db_type

    @property
    def address_type(self):
        return self._address_type


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

    def _get_hightest_id(self, table="block", sanity_check=True) -> Optional[int]:
        """Return last ingested address ID from a table."""
        group_id_col = f"{table}_id_group"
        id_col = f"{table}_id"

        result = self.select(table=table, columns=[group_id_col], per_partition_limit=1)
        groups = [getattr(row, group_id_col) for row in result.current_rows]

        if any(groups):
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

    def _ensure_is_highest_id(self, table: str, id_col: str, id: Optional[int]) -> bool:
        if id is None:
            return None
        groups = self._get_bucket_divisors_by_table_name()
        if table in groups:
            # this case handles tables with group ids.
            group_id_col = f"{table}_id_group"
            bucket_divisor = groups[table]
            w = {group_id_col: (id + 1) // bucket_divisor, id_col: id + 1}
        else:
            # this case handles tables with no group column and increasing integer ids.
            w = {id_col: id + 1}
        result = self.select(table, columns=[id_col], where=w)
        if len(result.current_rows) > 0:
            raise Exception(
                (
                    f"Something went wrong {id} "
                    " is not the highest_id"
                    f" {id+1} exist in {table}.{id_col}"
                )
            )

    def _at_most_one_result(self, result):
        if len(result.current_rows) > 1:
            raise Exception("Config tables are supposed to only have exactly one row.")
        return result.current_rows[0] if len(result.current_rows) > 0 else None

    def _get_only_row_from_table(self, table: str = "configuration"):
        return self._at_most_one_result(self.select(table, limit=2))


class DbWriterMixin:

    """
    This mixin requires the object to provide a CassandraDB instance at
    self._db and requires the object to provide the KeyspaceScope layout
    """

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
        self, table: str, items: Iterable, upsert=True, cl=None, concurrency: int = 100
    ):
        self._db.ingest(
            table,
            self.get_keyspace(),
            items,
            upsert=upsert,
            cl=cl,
            concurrency=concurrency,
        )


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

    def find_highest_block_with_exchange_rates(self, lookback_in_blocks=86400) -> int:
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
        start = hb - lookback_in_blocks

        def has_er_value(result):
            return result[0]["fiat_values"] is not None

        def get_item(index):
            batch = self.get_exchange_rates_for_block_batch([index])
            return 0 if has_er_value(batch) else 1

        r = binary_search(GenericArrayFacade(get_item), 1, lo=start, hi=hb)

        if r == -1:
            # minus one could mean two things, either
            # no missing exchangesrates are found within the lookback range
            # or that all have exchange rates, so we recheck if the
            # second case applies
            er = self.get_exchange_rates_for_block_batch([hb])
            if len(er) > 0 and has_er_value(er):
                r = hb

        return r - 1

    @lru_cache(maxsize=1)
    def get_configuration(self) -> Optional[object]:
        return self._get_only_row_from_table("configuration")

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

    def get_highest_block(self, sanity_check=True) -> Optional[int]:
        """Return last ingested block ID from block table."""
        return self._get_hightest_id(table="block", sanity_check=sanity_check)

    @abstractmethod
    def get_transaction_ids_in_block(self, block: int) -> Iterable:
        raise Exception("Must be implemented in chain specific child class")

    @abstractmethod
    def get_addresses_in_block(self, block: int) -> Iterable:
        raise Exception("Must be implemented in chain specific child class")

    def get_block_timestamps_batch(self, blocks: list[int]):
        bucket_size = self.get_block_bucket_size()
        stmt = self.select_stmt(
            "block",
            columns=["timestamp"],
            where={"block_id_group": "?", "block_id": "?"},
            limit=1,
        )
        parameters = [(b, [b // bucket_size, b]) for b in blocks]
        results = self._db.execute_batch(stmt, parameters)
        return {
            a: (datetime.fromtimestamp(row.current_rows[0].timestamp))
            for (a, row) in results
        }

    def get_block_timestamp(self, block: int):
        btc = self.get_block_timestamps_batch([block])
        return btc[block]

    def get_exchange_rates_batch(self, dates: list[datetime]):
        stmt = self.select_stmt("exchange_rates", where={"date": "?"}, limit=1)
        ds = list({date.strftime(DATE_FORMAT) for date in dates})
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
            (b, exchange_rate_to_date[block_to_ts[b].strftime(DATE_FORMAT)])
            for b in batch
        ]
        return [{"block_id": b, "fiat_values": get_values_list(er)} for b, er in ers]


class TransformedDb(ABC, WithinKeyspace, DbReaderMixin, DbWriterMixin):
    def __init__(self, keyspace_config: KeyspaceConfig, db: CassandraDb):
        self._keyspace_config = keyspace_config
        self._keyspace = keyspace_config.keyspace_name
        self._db = db

    def _get_bucket_divisors_by_table_name(self) -> dict:
        return {"address": self.get_address_id_bucket_size()}

    @lru_cache(maxsize=1)
    def get_summary_statistics(self) -> Optional[object]:
        return self._get_only_row_from_table("summary_statistics")

    def get_exchange_rates_by_block(self, block) -> Iterable:
        r = self.select("exchange_rates", where={"block_id": block}, limit=2)
        return self._at_most_one_result(r)

    def get_address_id_bucket_size(self) -> Optional[int]:
        config = self.get_configuration()
        return int(config.bucket_size) if config is not None else None

    @lru_cache(maxsize=1)
    def get_configuration(self) -> Optional[object]:
        return self._get_only_row_from_table("configuration")

    def get_highest_address_id(self, sanity_check=True) -> Optional[int]:
        """Return last ingested address ID from block table."""
        du = self.get_last_delta_updater_state()
        ha = self._get_hightest_id(table="address", sanity_check=sanity_check)
        return max(ha, du.highest_address_id) if du is not None else ha

    def get_highest_block(self) -> Optional[int]:
        stats = self.get_summary_statistics()
        return int(stats.no_blocks) - 1 if stats is not None else None

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
        return Address(address, self.get_address_prefix_length())

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


class AnalyticsDb:

    """Unified analytics DB interface for etl jobs"""

    def __init__(
        self, raw: KeyspaceConfig, transformed: KeyspaceConfig, db: CassandraDb
    ):
        self._raw_keyspace = raw.keyspace_name
        self._transformed_keyspace = transformed.keyspace_name
        self._db = db
        self._raw = raw.db_type(raw, db)
        self._transformed = transformed.db_type(transformed, db)

    def db(self) -> CassandraDb:
        return self._db

    def open(self):
        self._db.connect()

    def close(self):
        self._db.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    @property
    def raw(self) -> RawDb:
        return self._raw

    @property
    def transformed(self) -> TransformedDb:
        return self._transformed
