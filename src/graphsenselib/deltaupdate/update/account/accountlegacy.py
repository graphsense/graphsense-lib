import logging

from graphsenselib.deltaupdate.update.abstractupdater import (
    TABLE_NAME_DIRTY,
    TABLE_NAME_NEW,
    LegacyUpdateStrategy,
    write_dirty_addresses,
    write_new_addresses,
)

logger = logging.getLogger(__name__)


class UpdateStrategyAccountLegacy(LegacyUpdateStrategy):
    def __init__(self, db, currency, write_new, write_dirty, forward_fill_rates=False):
        super().__init__(
            db, currency, write_new, write_dirty, forward_fill_rates=forward_fill_rates
        )

    def prepare_database(self):
        super().prepare_database()
        ADDRESS_TABLE_COLUMNS = ["address_prefix text", "address blob"]
        ADDRESS_TABLE_PK = ["address_prefix", "address"]

        if self._write_new:
            self._db.transformed.ensure_table_exists(
                TABLE_NAME_NEW,
                ADDRESS_TABLE_COLUMNS
                + ["address_id int", "block_id int", "timestamp int", "tx_hash blob"],
                ADDRESS_TABLE_PK,
                truncate=False,
            )

        if self._write_dirty:
            self._db.transformed.ensure_table_exists(
                TABLE_NAME_DIRTY,
                ADDRESS_TABLE_COLUMNS,
                ADDRESS_TABLE_PK,
                truncate=False,
            )

    def process_batch_impl_hook(self, batch):
        addresses = {}
        bts = self._db.raw.get_block_timestamps_batch(batch)

        for block in batch:
            for row in self._db.raw.get_addresses_in_block(block):
                if row.from_address is not None and row.from_address not in addresses:
                    # From address can be None for coinbase
                    addresses[row.from_address] = row

                if row.to_address is not None and row.to_address not in addresses:
                    # To addresses can be None for e.g. deployments
                    # addresses.add(row.to_address)
                    addresses[row.to_address] = row

        if self._write_dirty:
            write_dirty_addresses(self._db, TABLE_NAME_DIRTY, set(addresses.keys()))

        self._nr_queryied_addresses_batch = len(addresses)
        self._nr_queried_addresses += self._nr_queryied_addresses_batch

        if self._write_new:
            logger.info(
                f"Checking existence for {self._nr_queryied_addresses_batch} addresses"
            )

            ret = self._db.transformed.known_addresses_batch(
                addresses.keys(), inlcude_new_adresses_table=True
            )

            for addr, exists in ret.items():
                # this is very slow but good for crosschecking
                if not exists:
                    self._new_addresses[addr] = addresses[addr]
                    self._nr_new_addresses += 1

            if self._nr_queried_addresses > 0:
                logger.info(
                    f"Found {self._nr_new_addresses} new addresses so "
                    f"far {(self._nr_new_addresses / self._nr_queried_addresses):.3f} "
                    "are new."
                )
            # logging.info(self._db.transformed.knows_address.cache_info())

            # At the end of the batch write data to the db.
            write_new_addresses(
                self._db,
                TABLE_NAME_NEW,
                [
                    (
                        addr,
                        self.consume_address_id(),
                        int(bts[row.block_id].timestamp()),
                        row.block_id,
                        row.tx_hash,
                    )
                    for addr, row in self._new_addresses.items()
                ],
            )
            # Resetting here means less memory footprint for this job
            # But more writes to Cassandra.
            self._new_addresses = {}
