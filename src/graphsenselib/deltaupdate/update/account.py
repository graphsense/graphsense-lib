# flake8: noqa
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Set, Tuple, Union

from cassandra.cluster import Cluster
from cassandra.cqlengine.columns import Integer
from cassandra.cqlengine.usertype import UserType
from diskcache import Cache

from graphsenselib.utils.cache import TableBasedCache

from ...config.config import DeltaUpdaterConfig
from ...datatypes import EntityType, FlowDirection
from ...db import AnalyticsDb, DbChange
from ...rates import convert_to_fiat
from ...schema.schema import GraphsenseSchemas
from ...utils import DataObject as MutableNamedTuple
from ...utils import group_by, no_nones
from ...utils.account import (
    get_slim_tx_from_trace,
    get_unique_addresses_from_traces,
    get_unique_ordered_addresses,
)
from ...utils.adapters import (
    AccountLogAdapter,
    AccountTransactionAdapter,
    EthTraceAdapter,
    TrxTraceAdapter,
)
from ...utils.errorhandling import CrashRecoverer
from ...utils.logging import LoggerScope
from .abstractupdater import TABLE_NAME_DELTA_HISTORY, UpdateStrategy
from .generic import (
    ApplicationStrategy,
    DbDeltaAccount,
    DeltaValue,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
    Tx,
    get_id_group,
    groupby_property,
)
from .utxo import apply_changes

logger = logging.getLogger(__name__)


class TxReference(UserType):
    trace_index = Integer(required=False)
    log_index = Integer(required=False)


COINBASE_PSEUDO_ADDRESS = None  # todo is this true
PSEUDO_ADDRESS_AND_IDS = {COINBASE_PSEUDO_ADDRESS: 0}

DEFAULT_SUMMARY_STATISTICS = MutableNamedTuple(
    **{
        "id": 0,
        "timestamp": 0,
        "timestamp_transform": 0,
        "no_blocks": 0,
        "no_blocks_transform": 0,
        "no_transactions": 0,
        "no_addresses": 0,
        "no_address_relations": 0,
        "no_clusters": 0,
        "no_cluster_relations": 0,
    }
)


logger = logging.getLogger(__name__)


def get_bookkeeping_changes(
    base_statistics: MutableNamedTuple,
    current_statistics: NamedTuple,
    last_block_processed: int,
    nr_new_address_relations: int,
    nr_new_addresses: int,
    nr_new_tx: int,
    highest_address_id: int,
    runtime_seconds: int,
    bts: Dict[int, datetime],
    patch_mode: bool,
) -> List[DbChange]:
    """Creates changes for the bookkeeping tables like summary statistics after
    other data has been updated.

    Args:
        base_statistics (MutableNamedTuple): statistics db row, all the other
        parameters are note data is updated in this process
        current_statistics (NamedTuple): Current value of db statistics for comparison
        last_block_processed (int): Last block processed
        nr_new_address_relations (int): Delta new addresses relations in changeset
        nr_new_addresses (int): Delta new addresses in changeset
        nr_new_cluster_relations (int): Delta new cluster relations in changeset
        nr_new_clusters (int): Delta new clusters in changeset
        nr_new_tx (int): Delta new txs in changeset
        highest_address_id (int): current highest address_id
        runtime_seconds (int): runtime to create the last changes in seconds
        bts (Dict[int, datetime]): mapping from block to its timestamp
        delta values
    """
    changes = []
    with LoggerScope.debug(logger, "Creating summary_statistics updates") as lg:
        lb_date = bts[last_block_processed]
        stats = base_statistics
        no_blocks = last_block_processed - 1

        """ Update local stats """
        if not patch_mode:
            """when in patch mode (end block set by user)"""
            stats.no_blocks = no_blocks  # todo dont get this yet
        stats.timestamp = int(lb_date.timestamp())
        stats.no_address_relations += nr_new_address_relations
        stats.no_addresses += nr_new_addresses
        stats.no_transactions += nr_new_tx

        statistics = stats.as_dict()

        if current_statistics is not None and current_statistics.no_blocks != no_blocks:
            if not patch_mode:
                assert current_statistics.no_blocks < no_blocks
            # changes.append(
            #     DbChange.delete(
            #         table="summary_statistics",
            #         data={"no_blocks": current_statistics.no_blocks},
            #     )
            # )
        changes.append(DbChange.new(table="summary_statistics", data=statistics))

        lg.debug(f"Statistics: {statistics}")

        data_history = {
            "last_synced_block": last_block_processed,
            "last_synced_block_timestamp": lb_date,
            "highest_address_id": highest_address_id,
            "timestamp": datetime.now(),
            "write_new": False,
            "write_dirty": False,
            "runtime_seconds": runtime_seconds,
        }
        changes.append(DbChange.new(table=TABLE_NAME_DELTA_HISTORY, data=data_history))

        lg.debug(f"History: {data_history}")
    return changes


class UpdateStrategyAccount(UpdateStrategy):
    def __init__(
        self,
        db,
        du_config: DeltaUpdaterConfig,
        pedantic: bool,
        application_strategy: ApplicationStrategy = ApplicationStrategy.TX,
        patch_mode: bool = False,
        forward_fill_rates: bool = False,
    ):
        super().__init__(db, du_config.currency, forward_fill_rates=forward_fill_rates)
        self.du_config = du_config
        crash_file = (
            "/tmp/account_deltaupdate_"
            f"{self._db.raw.get_keyspace()}_{self._db.transformed.get_keyspace()}"
            "_crashreport.err"
        )
        stats_value = self._db.transformed.get_summary_statistics()
        """ Make statistics row mutable"""
        self._statistics = (
            MutableNamedTuple(**stats_value._asdict())
            if stats_value is not None
            else DEFAULT_SUMMARY_STATISTICS
        )
        # get ingest config
        self._pedantic = pedantic
        self._patch_mode = patch_mode
        self.changes = None
        self.application_strategy = application_strategy
        logger.info(f"Updater running in {application_strategy} mode.")
        self.crash_recoverer = CrashRecoverer(crash_file)

    def persist_updater_progress(self):
        if self.changes is not None:
            atomic = ApplicationStrategy.TX == self.application_strategy
            apply_changes(
                self._db, self.changes, self._pedantic, try_atomic_writes=atomic
            )
            self.changes = None
        self._time_last_batch = time.time() - self._batch_start_time

    def prepare_database(self):
        with LoggerScope.debug(logger, "Preparing database"):
            if self._db.transformed.has_delta_updater_v1_tables():
                raise Exception(
                    "Tables of the delta-updater v1 detected. "
                    "please delete new_addresses, dirty_address, "
                    "detla_updater_state and delta-updater_history "
                    "before using delta updater v2."
                )
            GraphsenseSchemas().ensure_table_exists_by_name(
                self._db.transformed,
                TABLE_NAME_DELTA_HISTORY,
                truncate=False,
            )

    def process_batch_impl_hook(self, batch):
        global trace_adapter
        rates = {}
        transactions = []
        traces = []
        logs = []
        bts = {}
        """
            Read transaction and exchange rates data
        """
        with LoggerScope.debug(logger, "Checking recovery state.") as lg:
            if self.crash_recoverer.is_in_recovery_mode():
                """
                If we are in recovery mode we start with a block earlier to catch up
                the delta otherwise would start with whats in the db +1
                In case of an error in between blocks this would mean skipping to
                the next block
                """
                mb = max(0, min(batch) - 1)
                lg.warning(
                    "Delta update is in crash recovery mode. Crash hint is "
                    f"{self.crash_recoverer.get_recovery_hint()} in "
                    f"{self.crash_recoverer.get_recovery_hint_filename()} "
                    f" restarting at block {mb}."
                )
                batch = [mb] + batch

        with LoggerScope.debug(logger, "Reading transaction and rates data") as log:
            missing_rates_in_block = False
            cache = TableBasedCache(Cache(self.du_config.fs_cache.directory))
            for block in batch:
                transactions.extend(cache.get(("transaction", block), []))
                traces.extend(cache.get(("trace", block), []))
                logs.extend(cache.get(("log", block), []))
                fiat_values = self._db.transformed.get_exchange_rates_by_block(
                    block
                ).fiat_values
                if fiat_values is None:
                    # raise Exception(
                    #     "No exchange rate for block {block}. Abort processing."
                    # )
                    missing_rates_in_block = True
                    fiat_values = [0, 0]
                rates[block] = fiat_values
                bts[block] = self._db.raw.get_block_timestamp(block)

            if missing_rates_in_block:
                log.warning("Block Range has missing exchange rates. Using Zero.")

        if self.application_strategy == ApplicationStrategy.BATCH:
            if self.crash_recoverer.is_in_recovery_mode():
                raise Exception("Batch mode is not allowed in recovery mode.")

            if self.currency == "trx":
                trace_adapter = TrxTraceAdapter()
            elif self.currency == "eth":
                trace_adapter = EthTraceAdapter()

            transaction_adapter = AccountTransactionAdapter()
            log_adapter = AccountLogAdapter()
            traces = trace_adapter.dicts_to_dataclasses(traces)  # todo slow
            traces = trace_adapter.rename_fields_in_list(traces)  # todo slow
            traces = trace_adapter.process_fields_in_list(traces)

            transactions = transaction_adapter.dicts_to_dataclasses(
                transactions
            )  # todo slow
            logs = log_adapter.dicts_to_dataclasses(logs)  # todo slow

            changes = []

            (tx_changes, nr_new_addresses, nr_new_address_relations) = self.get_changes(
                transactions, traces, logs, rates
            )  # noqa: E501

            changes.extend(tx_changes)
            print("made changes for blocks", batch)

            last_block_processed = batch[-1]
            nr_new_tx = len(traces)  # todo not true, get from get_changes function
            runtime_seconds = int(time.time() - self.batch_start_time)
            """
            bookkeeping_changes = get_bookkeeping_changes(
                    self._statistics,
                    self._db.transformed.get_summary_statistics(),
                    last_block_processed,
                    nr_new_address_relations,
                    nr_new_addresses,
                    nr_new_tx,
                    self.highest_address_id,
                    runtime_seconds,
                    bts,
                    patch_mode=self._patch_mode,
                )

            changes.extend(bookkeeping_changes)
            """
            # Store changes to be written
            # They are applied at the end of the batch in
            # persist_updater_progress
            self.changes = changes

        else:
            raise ValueError(
                f"Unknown application strategy {self.application_strategy}"
            )

    def get_changes(
        self, transactions: List, traces: List, logs: List, rates: Dict[int, List]
    ) -> List[DbChange]:
        # index transaction hashes
        tx_hashes = [tx.tx_hash for tx in transactions]

        if self.currency == "eth":
            hash_to_id = {
                tx.tx_hash: self.consume_transaction_id() for tx in transactions
            }
        elif self.currency == "trx":
            # 64 bit integer, first 32 is defined by block and the other by transaction index
            consume_transaction_id_trx = (
                lambda block_id, transaction_index: (block_id << 32) + transaction_index
            )
            hash_to_id = {
                tx.tx_hash: consume_transaction_id_trx(
                    tx.block_id, tx.transaction_index
                )
                for tx in transactions
            }

        if self.currency == "eth":
            coin_decimals = 18
        elif self.currency == "trx":
            coin_decimals = 6

        from graphsenselib.deltaupdate.update.tokens import TokenTransfer

        from .generic import RawEntityTxAccount
        from .tokens import ERC20Decoder

        hash_to_tx = {tx_hash: tx for tx_hash, tx in zip(tx_hashes, transactions)}

        id_bucket_size = self._db.transformed.get_address_id_bucket_size()
        block_bucket_size = self._db.transformed.get_block_id_bucket_size()

        txdeltas = []

        for tx_hash in tx_hashes:
            tx_id = hash_to_id[tx_hash]
            tx_index = hash_to_tx[tx_hash].transaction_index
            block_id = hash_to_tx[tx_hash].block_id
            txdeltas.append(
                Tx(block_id=block_id, tx_id=tx_id, tx_hash=tx_hash, tx_index=tx_index)
            )

        tdb = self._db.transformed
        traces = [
            trace for trace in traces if trace.tx_hash is not None
        ]  # todo ignores reward tx
        # make sure they go through
        traces = [trace for trace in traces if trace.status == 1]

        def ignore_coinbase(hashes):
            return [hash for hash in hashes if hash not in PSEUDO_ADDRESS_AND_IDS]

        tokendecoder = ERC20Decoder(self.currency)

        with LoggerScope.debug(logger, "Prepare transaction/transfer data") as lg:
            lg.debug(f"Decode logs to token transfers.")
            token_transfers = no_nones(
                [tokendecoder.log_to_transfer(log) for log in logs]
            )

            # transaction > traces > token_transfers within a transaction
            address_containing_objects = transactions + traces + token_transfers
            address_containing_objects_sorted = sorted(
                address_containing_objects,
                key=lambda row: (row.block_id, hash_to_id[row.tx_hash]),
            )
            ordered_receiver_addresses = ignore_coinbase(
                get_unique_ordered_addresses(
                    address_containing_objects_sorted, mode="sender"
                )
            )
            ordered_sender_addresses = ignore_coinbase(
                get_unique_ordered_addresses(
                    address_containing_objects_sorted, mode="receiver"
                )
            )
            addresses = no_nones(
                get_unique_ordered_addresses(
                    address_containing_objects_sorted, mode="both"
                )
            )

        len_addr = len(addresses)

        with LoggerScope.debug(
            logger, f"Checking existence for {len_addr} addresses"
        ) as _:
            addr_ids = {  # noqa: C416
                adr: address_id
                for adr, address_id in tdb.get_address_id_async_batch(list(addresses))
            }

        with LoggerScope.debug(logger, "Reading addresses to be updated") as lg:
            existing_addr_ids = no_nones(
                [address_id.result_or_exc.one() for adr, address_id in addr_ids.items()]
            )
            addresses_resolved = dict(
                tdb.get_address_async_batch(
                    [adr.address_id for adr in existing_addr_ids]
                )
            )

            def get_resolved_address(addr_id_exc):
                addr_id = addr_id_exc.result_or_exc.one()
                return (
                    (None, None)
                    if addr_id is None
                    else (
                        addr_id.address_id,
                        addresses_resolved[
                            addr_id.address_id
                        ].result_or_exc.one(),  # noqa
                    )
                )

            addresses_to_id__rows = {
                adr: get_resolved_address(address_id)
                for adr, address_id in addr_ids.items()
            }

            del addresses_resolved

        def get_next_address_ids_with_aliases(address: str):
            return (
                self.consume_address_id()
                # if address not in PSEUDO_ADDRESS_AND_IDS # todo ignores coinbase
                # else PSEUDO_ADDRESS_AND_IDS[address]
            )

        # bytes_to_row_address = {
        #    address: addresses_to_rows[address][1] for address in addresses_to_rows
        # }
        bytes_to_row_address = {
            address: row[1] for address, row in addresses_to_id__rows.items()
        }
        with LoggerScope.debug(
            logger, "Assigning new address ids for new addresses"
        ) as lg:
            for out_addr in ordered_receiver_addresses:
                addr_id, address = addresses_to_id__rows[out_addr]
                if addr_id is None:
                    new_addr_id = get_next_address_ids_with_aliases(address)
                    addresses_to_id__rows[out_addr] = (new_addr_id, None)
                elif address is None:
                    lg.warning(
                        f"TODO WIP: The following is because the addresses "
                        f"are in the prefix table but not in the address table. "
                        f"Address {out_addr} has address "
                        f"id {addr_id} but no address entry"
                    )

            del ordered_receiver_addresses

            not_yet_seen_sender_addresses = {
                k
                for k, (addr_id2, _) in addresses_to_id__rows.items()
                if addr_id2 is None
            }

            if len(not_yet_seen_sender_addresses) > 0:
                for out_addr in ordered_sender_addresses:
                    if out_addr in not_yet_seen_sender_addresses:
                        new_addr_id = get_next_address_ids_with_aliases(out_addr)
                        lg.debug(
                            "Encountered new sender address - "
                            f"{out_addr.hex()}. Creating it with id {new_addr_id}."
                        )
                        addresses_to_id__rows[out_addr] = (new_addr_id, None)

                del ordered_sender_addresses

            del not_yet_seen_sender_addresses

            assert (
                len(
                    [
                        1
                        for k, (addr_id2, _) in addresses_to_id__rows.items()
                        if addr_id2 is None
                    ]
                )
                == 0
            )

        def get_tx_prefix(tx_hash):
            tx_hash = tdb.to_db_tx_hash(tx_hash)
            return (tx_hash.db_encoding, tx_hash.prefix)

        def get_address_prefix(address_str):
            address = tdb.to_db_address(address_str)
            return (address.db_encoding, address.prefix)

        address_hash_to_id_with_new = {
            address: id_row[0] for address, id_row in addresses_to_id__rows.items()
        }
        # TODO could filter here for only new ones (not in hash_to_id), to make it easier for cassandra
        # but i think it wasnt done btc so there could be a reason
        changes, _ = self.prepare_txs_for_ingest_account(
            txdeltas,
            id_bucket_size,
            block_bucket_size,
            get_address_prefix,
            get_tx_prefix,
            address_hash_to_id_with_new,
        )

        currency = self.currency.upper()

        def get_prices(
            value, decimals, block_rates, usd_equivalent, coin_equivalent
        ) -> List[int]:
            dollar_rate = block_rates[0]
            euro_rate = block_rates[1]
            dollar_to_euro = dollar_rate / euro_rate

            if usd_equivalent == 1:
                dollar_value = value / 10**decimals
            elif coin_equivalent == 1:
                dollar_value = value / 10**decimals * dollar_to_euro
            else:
                raise Exception(
                    "Unknown price type. only native coin and dollar equivalent supported atm"
                )

            euro_value = dollar_value * dollar_to_euro
            return [dollar_value, euro_value]

        def get_prices_coin(value, block_rates):
            return get_prices(value, coin_decimals, block_rates, 0, 1)

        def prepare_txs_for_ingest(
            delta: List[RawEntityTxAccount],
            id_bucket_size: int,
        ) -> List[DbChange]:
            """
            Creating new address transaction
            """
            changes = []
            for atx in delta:
                ident = atx.identifier
                for tokenname in atx.token_values.keys():
                    chng = DbChange.new(
                        table=f"address_transactions",
                        data={
                            "address_id_group": get_id_group(ident, id_bucket_size),
                            "address_id_secondary_group": atx.tx_id % 128,
                            "address_id": ident,
                            "currency": tokenname,
                            "transaction_id": atx.tx_id,
                            "is_outgoing": atx.is_outgoing,
                            "tx_reference": atx.tx_reference,
                        },
                    )
                    changes.append(
                        chng
                    )  # todo not sure how RawEntityTxAccount are bundled and therefore how to create the changes

                chng = DbChange.new(
                    table=f"address_transactions",
                    data={
                        "address_id_group": get_id_group(ident, id_bucket_size),
                        "address_id_secondary_group": atx.tx_id % 128,
                        "address_id": ident,
                        "currency": currency,
                        "transaction_id": atx.tx_id,
                        "is_outgoing": atx.is_outgoing,
                        "tx_reference": atx.tx_reference,
                    },
                )

                changes.append(chng)
            return changes

        def get_entitytx_from_trace(trace, is_outgoing) -> RawEntityTxAccount:
            tx_id = hash_to_id[trace.tx_hash]
            address_hash = trace.from_address if is_outgoing else trace.to_address
            address_id = address_hash_to_id_with_new[address_hash]

            tx_reference = {
                "trace_index": trace.trace_index,
                "log_index": None,
            }
            tx_reference = TxReference(**tx_reference)

            reta = RawEntityTxAccount(
                identifier=address_id,
                is_outgoing=is_outgoing,
                tx_id=tx_id,
                tx_reference=tx_reference,
                value=trace.value,
                token_values={},  # we dont support TRC10 right now
            )
            return reta

        def get_entitytx_from_tokentransfer(
            tokentransfer: TokenTransfer, is_outgoing
        ) -> RawEntityTxAccount:
            tx_id = hash_to_id[tokentransfer.tx_hash]

            address_hash = (
                tokentransfer.from_address if is_outgoing else tokentransfer.to_address
            )
            address_id = address_hash_to_id_with_new[address_hash]

            dv = DeltaValue(
                tokentransfer.value,
                get_prices(
                    tokentransfer.value,
                    tokentransfer.decimals,
                    rates[tokentransfer.block_id],
                    tokentransfer.usd_equivalent,
                    tokentransfer.coin_equivalent,
                ),
            )

            token_values = {tokentransfer.asset: dv}

            tx_reference = {
                "trace_index": None,
                "log_index": tokentransfer.log_index,
            }
            tx_reference = TxReference(**tx_reference)

            reta = RawEntityTxAccount(
                identifier=address_id,
                is_outgoing=is_outgoing,
                tx_id=tx_id,
                tx_reference=tx_reference,
                value=tokentransfer.value,
                token_values=token_values,
            )
            return reta

        def get_entitytx_from_transaction(tx, is_outgoing) -> RawEntityTxAccount:
            tx_id = hash_to_id[tx.tx_hash]
            address_hash = tx.from_address if is_outgoing else tx.to_address

            address_id = address_hash_to_id_with_new[address_hash]

            tx_reference = {
                "trace_index": None,
                "log_index": None,
            }
            tx_reference = TxReference(**tx_reference)

            reta = RawEntityTxAccount(
                identifier=address_id,
                is_outgoing=is_outgoing,
                tx_id=tx_id,
                tx_reference=tx_reference,
                value=tx.value,
                token_values={},
            )
            return reta

        def get_entitydelta_from_trace(trace, is_outgoing):
            identifier = trace.from_address if is_outgoing else trace.to_address
            total_received_value = 0 if is_outgoing else trace.value
            total_spent_value = trace.value if is_outgoing else 0
            total_received = DeltaValue(
                total_received_value,
                get_prices_coin(trace.value, rates[trace.block_id]),
            )
            total_spent = DeltaValue(
                total_spent_value, get_prices_coin(trace.value, rates[trace.block_id])
            )
            total_tokens_received = (
                {}
            )  # for now we dont support TRC10, so an empty dict is fine
            total_tokens_spent = (
                {}
            )  # for now we dont support TRC10, so an empty dict is fine
            first_tx_id = hash_to_id[trace.tx_hash]
            last_tx_id = hash_to_id[trace.tx_hash]
            no_incoming_txs = int(not is_outgoing)  # todo should we count if its zero?
            no_outgoing_txs = int(is_outgoing)  # todo should we count if its zero?
            no_incoming_txs_zero_value = 0 if is_outgoing else int(trace.value == 0)
            no_outgoing_txs_zero_value = int(trace.value == 0) if is_outgoing else 0

            eda = EntityDeltaAccount(
                identifier=identifier,
                total_received=total_received,
                total_spent=total_spent,
                total_tokens_received=total_tokens_received,
                total_tokens_spent=total_tokens_spent,
                first_tx_id=first_tx_id,
                last_tx_id=last_tx_id,
                no_incoming_txs=no_incoming_txs,
                no_outgoing_txs=no_outgoing_txs,
                no_incoming_txs_zero_value=no_incoming_txs_zero_value,
                no_outgoing_txs_zero_value=no_outgoing_txs_zero_value,
            )
            return eda

        def get_entitydelta_from_tokentransfer(tokentransfer, is_outgoing):
            identifier = (
                tokentransfer.from_address if is_outgoing else tokentransfer.to_address
            )
            total_received_value = 0 if is_outgoing else tokentransfer.value
            total_spent_value = tokentransfer.value if is_outgoing else 0

            fiat_values = get_prices(
                tokentransfer.value,
                tokentransfer.decimals,
                rates[tokentransfer.block_id],
                tokentransfer.usd_equivalent,
                tokentransfer.coin_equivalent,
            )

            received_fiat = [0, 0] if is_outgoing else fiat_values
            spent_fiat = fiat_values if is_outgoing else [0, 0]

            total_received = DeltaValue(total_received_value, received_fiat)
            total_spent = DeltaValue(total_spent_value, spent_fiat)
            total_tokens_received = {tokentransfer.asset: total_received}
            total_tokens_spent = {tokentransfer.asset: total_spent}
            first_tx_id = hash_to_id[tokentransfer.tx_hash]
            last_tx_id = hash_to_id[tokentransfer.tx_hash]
            no_incoming_txs = int(not is_outgoing)
            no_outgoing_txs = int(is_outgoing)
            no_incoming_txs_zero_value = 0
            no_outgoing_txs_zero_value = 0

            eda = EntityDeltaAccount(
                identifier=identifier,
                total_received=total_received,
                total_spent=total_spent,
                total_tokens_received=total_tokens_received,
                total_tokens_spent=total_tokens_spent,
                first_tx_id=first_tx_id,
                last_tx_id=last_tx_id,
                no_incoming_txs=no_incoming_txs,
                no_outgoing_txs=no_outgoing_txs,
                no_incoming_txs_zero_value=no_incoming_txs_zero_value,
                no_outgoing_txs_zero_value=no_outgoing_txs_zero_value,
            )
            return eda

        def get_entitydelta_from_transaction(tx, is_outgoing):
            identifier = tx.from_address if is_outgoing else tx.to_address
            total_received_value = 0 if is_outgoing else tx.value
            total_spent_value = tx.value if is_outgoing else 0
            total_received = DeltaValue(
                total_received_value, get_prices_coin(tx.value, rates[tx.block_id])
            )
            total_spent = DeltaValue(
                total_spent_value, get_prices_coin(tx.value, rates[tx.block_id])
            )
            total_tokens_received = {}
            total_tokens_spent = {}
            first_tx_id = hash_to_id[tx.tx_hash]
            last_tx_id = hash_to_id[tx.tx_hash]
            no_incoming_txs = int(not is_outgoing)
            no_outgoing_txs = int(is_outgoing)
            no_incoming_txs_zero_value = 0 if is_outgoing else int(tx.value == 0)
            no_outgoing_txs_zero_value = int(tx.value == 0) if is_outgoing else 0

            eda = EntityDeltaAccount(
                identifier=identifier,
                total_received=total_received,
                total_spent=total_spent,
                total_tokens_received=total_tokens_received,
                total_tokens_spent=total_tokens_spent,
                first_tx_id=first_tx_id,
                last_tx_id=last_tx_id,
                no_incoming_txs=no_incoming_txs,
                no_outgoing_txs=no_outgoing_txs,
                no_incoming_txs_zero_value=no_incoming_txs_zero_value,
                no_outgoing_txs_zero_value=no_outgoing_txs_zero_value,
            )
            return eda

        def relationdelta_from_trace(trace):
            iadr, oadr = trace.from_address, trace.to_address
            value = DeltaValue(
                trace.value, get_prices_coin(trace.value, rates[trace.block_id])
            )
            token_values = {}  # for now we dont support TRC10, so an empty dict is fine
            no_transactions = 1
            return RelationDeltaAccount(
                src_identifier=iadr,
                dst_identifier=oadr,
                no_transactions=no_transactions,
                value=value,
                token_values=token_values,
            )

        def relationdelta_from_transaction(tx):
            iadr, oadr = tx.from_address, tx.to_address

            value = DeltaValue(tx.value, get_prices_coin(tx.value, rates[tx.block_id]))
            token_values = {}
            no_transactions = 1
            return RelationDeltaAccount(
                src_identifier=iadr,
                dst_identifier=oadr,
                no_transactions=no_transactions,
                value=value,
                token_values=token_values,
            )

        def relationdelta_from_tokentransfer(tokentransfer: TokenTransfer):
            iadr, oadr = tokentransfer.from_address, tokentransfer.to_address
            dollar_rate = rates[tokentransfer.block_id][0]
            euro_rate = rates[tokentransfer.block_id][1]
            dollar_to_euro = dollar_rate / euro_rate
            value = tokentransfer.value
            if tokentransfer.usd_equivalent == 1:
                dollar_value = value / tokentransfer.decimals
            elif tokentransfer.coin_equivalent == 1:
                dollar_value = value / tokentransfer.decimals * dollar_to_euro
            else:
                raise Exception(
                    "Unknown price type. only native coin and dollar equivalent supported atm"
                )

            euro_value = dollar_value * dollar_to_euro

            value = DeltaValue(value, [dollar_value, euro_value])

            token_values = {tokentransfer.asset: value}
            no_transactions = 1
            return RelationDeltaAccount(
                src_identifier=iadr,
                dst_identifier=oadr,
                no_transactions=no_transactions,
                value=DeltaValue(0, [0, 0]),
                token_values=token_values,
            )

        entity_transactions = []

        # entity transactions from traces
        entity_transactions.extend(
            [get_entitytx_from_trace(trace, True) for trace in traces]
        )
        entity_transactions.extend(
            [get_entitytx_from_trace(trace, False) for trace in traces]
        )

        # entity transactions from token transfers
        entity_transactions.extend(
            [get_entitytx_from_tokentransfer(tt, True) for tt in token_transfers]
        )
        entity_transactions.extend(
            [get_entitytx_from_tokentransfer(tt, False) for tt in token_transfers]
        )

        # entity deltas from traces
        entity_deltas = []
        entity_deltas.extend(
            [get_entitydelta_from_trace(trace, True) for trace in traces]
        )
        entity_deltas.extend(
            [get_entitydelta_from_trace(trace, False) for trace in traces]
        )

        # entity deltas from token transfers
        entity_deltas.extend(
            [get_entitydelta_from_tokentransfer(tt, True) for tt in token_transfers]
        )
        entity_deltas.extend(
            [get_entitydelta_from_tokentransfer(tt, False) for tt in token_transfers]
        )

        # relation deltas from traces
        relation_updates = [relationdelta_from_trace(trace) for trace in traces]

        # relation deltas from token transfers
        relation_updates.extend(
            [relationdelta_from_tokentransfer(tt) for tt in token_transfers]
        )

        # in eth we disregard the eth values because they are already in the traces
        # in tron only traces that are not the initial transaction have values, so we still need to add
        # the value from the transaction
        if self.currency == "trx":
            transactions_nonone = [
                tx
                for tx in transactions
                if tx.from_address is not None and tx.to_address is not None
            ]
            # todo no coinbase txs

            entity_transactions.extend(
                [get_entitytx_from_transaction(tx, True) for tx in transactions_nonone]
            )
            entity_transactions.extend(
                [get_entitytx_from_transaction(tx, False) for tx in transactions_nonone]
            )

            entity_deltas.extend(
                [
                    get_entitydelta_from_transaction(tx, True)
                    for tx in transactions_nonone
                ]
            )
            entity_deltas.extend(
                [
                    get_entitydelta_from_transaction(tx, False)
                    for tx in transactions_nonone
                ]
            )

            relation_updates.extend(
                [relationdelta_from_transaction(tx) for tx in transactions_nonone]
            )

        bucket_size = id_bucket_size
        dbdelta = DbDeltaAccount(
            entity_deltas,
            entity_transactions,
            relation_updates,
        )
        dbdelta = dbdelta.compress()

        rel_to_query = [
            (
                addresses_to_id__rows[update.src_identifier][0],
                addresses_to_id__rows[update.dst_identifier][0],
            )
            for update in dbdelta.relation_updates
        ]
        addr_outrelations_q = tdb.get_address_outgoing_relations_async_batch_account(
            rel_to_query
        )
        addr_outrelations = {
            (update.src_identifier, update.dst_identifier): qr
            for update, qr in zip(dbdelta.relation_updates, addr_outrelations_q)
        }

        rel_to_query = [
            (
                addresses_to_id__rows[update.dst_identifier][0],
                addresses_to_id__rows[update.src_identifier][0],
            )
            for update in dbdelta.relation_updates
        ]
        addr_inrelations_q = tdb.get_address_incoming_relations_async_batch_account(
            rel_to_query
        )

        addr_inrelations = {
            (update.src_identifier, update.dst_identifier): qr
            for update, qr in zip(dbdelta.relation_updates, addr_inrelations_q)
        }
        rel_to_query_from_to = [(100, 14743)]
        rel_to_query_to_from = [(14743, 100)]
        list(
            tdb.get_address_outgoing_relations_async_batch_account(rel_to_query_from_to)
        )[0].result_or_exc.one()
        list(
            tdb.get_address_incoming_relations_async_batch_account(rel_to_query_to_from)
        )[0].result_or_exc.one()

        lg.debug(f"Prepare data.")
        """
        Creating new transactions
        """
        changes += prepare_txs_for_ingest(dbdelta.new_entity_txs, id_bucket_size)

        """ Merging relations deltas """
        (
            changes_relations,
            new_rels_in,
            new_rels_out,
            nr_new_rel_total,
        ) = self.prepare_relations_for_ingest(
            dbdelta.relation_updates,
            address_hash_to_id_with_new,
            addr_inrelations,
            addr_outrelations,
            bucket_size,
        )
        nr_new_rels = sum(new_rels_in.values()) + sum(new_rels_out.values())
        assert nr_new_rels == nr_new_rel_total

        new_relations_in = new_rels_in
        new_relations_out = new_rels_out
        changes.extend(changes_relations)

        """ Merging entity deltas """
        entity_changes, nr_new_entities = self.prepare_entities_for_ingest(
            dbdelta.entity_updates,
            address_hash_to_id_with_new,
            bytes_to_row_address,
            new_relations_in,
            new_relations_out,
            bucket_size,
            get_address_prefix,
        )
        changes.extend(entity_changes)
        nr_new_entities_created = nr_new_entities
        del nr_new_entities

        return (
            changes,
            nr_new_entities_created,
            nr_new_rels,
        )

        """


        return changes

        (
            delta_changes,
            nr_new_addresses,
            nr_new_clusters,
            nr_new_address_relations,
            nr_new_cluster_relations,
        ) = get_trace_deltas(
            db,
            traces,
            rates,
            get_next_address_id,
            get_next_transaction_id,
        )


        def to_fiat_currency(value_column, fiat_value_column, data):
            for row in data:
                if value_column in row and fiat_value_column in row:
                    row[fiat_value_column] = (row[value_column] * row[fiat_value_column]) / 1e18
            return data

        def compute_encoded_transactions(traces, transactions_ids, address_ids, exchange_rates):
            # Filter traces where status is 1 and rename 'txHash' to 'transaction'
            filtered_traces = [
                dict(trace, transaction=trace['txHash']) for trace in traces if trace['status'] == 1
            ]

            # Join operations (left join)
            def left_join(primary, secondary, key):
                joined = []
                for p_item in primary:
                    match = next((s_item for s_item in secondary if s_item[key] == p_item[key]), None)
                    joined.append({**p_item, **(match if match else {})})
                return joined

            # Performing joins
            traces_transactions = left_join(filtered_traces, transactions_ids, 'transaction')
            traces_transactions_from = left_join(traces_transactions, [
                {**item, 'fromAddress': item['address'], 'fromAddressId': item['addressId']} for item in address_ids],
                                                 'fromAddress')
            final_join = left_join(traces_transactions_from,
                                   [{**item, 'toAddress': item['address'], 'toAddressId': item['addressId']} for item in
                                    address_ids], 'toAddress')

            # Dropping unnecessary columns
            for item in final_join:
                for key in ['blockIdGroup', 'status', 'callType', 'toAddress', 'fromAddress', 'receiptGasUsed',
                            'transaction', 'traceId']:
                    item.pop(key, None)

                # Renaming columns
                item['srcAddressId'] = item.pop('fromAddressId', None)
                item['dstAddressId'] = item.pop('toAddressId', None)

            # Join with exchange rates and apply currency conversion
            final_data = left_join(final_join, exchange_rates, 'blockId')
            encoded_transactions = to_fiat_currency('value', 'fiatValues', final_data)

            return encoded_transactions

        return changes
        """

    def prepare_txs_for_ingest_account(
        self,
        delta: List[Tx],
        id_bucket_size: int,
        block_bucket_size: int,
        get_address_prefix: Callable[[bytes], Tuple[str, str]],
        get_transaction_prefix: Callable[[bytes], Tuple[str, str]],
        address_hash_to_id_with_new: Dict[bytes, int],
    ) -> Tuple[List[DbChange], int]:
        changes = []

        for update in delta:
            transaction_id = update.tx_id
            transaction_id_group = self._db.transformed.get_id_group(
                transaction_id, id_bucket_size
            )
            transaction = update.tx_hash

            transaction_prefix = get_transaction_prefix(transaction)[1]

            data = {
                "transaction_id_group": transaction_id_group,
                "transaction_id": transaction_id,
                "transaction": transaction,
            }

            chng = DbChange.new(
                table="transaction_ids_by_transaction_id_group",
                data=data,
            )
            changes.append(chng)

            data = {
                "transaction_prefix": transaction_prefix,
                "transaction": transaction,
                "transaction_id": transaction_id,
            }

            chng = DbChange.new(
                table="transaction_ids_by_transaction_prefix",
                data=data,
            )
            changes.append(chng)

            # get transaction ids

            changes.append(chng)
        if self.currency == "eth":
            # insert blocks in block_transactions
            grouped = groupby_property(delta, "block_id", sort_by="tx_id")
            changes.extend(
                [
                    DbChange.new(
                        table="block_transactions",
                        data={
                            "block_id_group": get_id_group(block_id, block_bucket_size),
                            "block_id": block_id,
                            "txs": [tx.tx_id for tx in txs],
                        },
                    )
                    for block_id, txs in grouped.items()
                ]
            )
        elif self.currency == "trx":
            # keep it flat
            changes.extend(
                [
                    DbChange.new(
                        table="block_transactions",
                        data={
                            "block_id_group": get_id_group(
                                tx.block_id, block_bucket_size
                            ),
                            "block_id": tx.block_id,
                            "tx_id": tx.tx_id,
                        },
                    )
                    for tx in delta
                ]
            )

        # comment out because its done later in entity perparation
        # for hash_ in address_hash_to_id_with_new:
        #    chng = DbChange.new(
        #        table="address_ids_by_address_prefix",
        #        data={
        #            "address_prefix": get_address_prefix(hash_)[1],
        #            "address": hash_,
        #            "address_id": address_hash_to_id_with_new[hash_],
        #        },
        #    )
        #    changes.append(chng)

        nr_new_txs = len(delta)

        return changes, nr_new_txs

    def prepare_relations_for_ingest(
        self,
        delta: List[RelationDeltaAccount],
        hash_to_id: Dict[str, bytes],
        inrelations: dict,
        outrelations: dict,
        id_bucket_size: int,
    ) -> Tuple[List[DbChange], dict, dict, int]:
        new_relations_in = defaultdict(int)
        new_relations_out = defaultdict(int)
        nr_new_rel = 0

        changes = []
        """ Merging relations deltas """
        for relations_update in delta:
            outr = outrelations[
                (relations_update.src_identifier, relations_update.dst_identifier)
            ].result_or_exc.one()
            inr = inrelations[
                (relations_update.src_identifier, relations_update.dst_identifier)
            ].result_or_exc.one()
            assert (outr is None) == (inr is None)

            id_src = hash_to_id[relations_update.src_identifier]
            id_dst = hash_to_id[relations_update.dst_identifier]
            src_group = get_id_group(id_src, id_bucket_size)
            dst_group = get_id_group(id_dst, id_bucket_size)

            if outr is None:
                """new address/cluster relation to insert"""
                new_relations_out[relations_update.src_identifier] += 1
                new_relations_in[relations_update.dst_identifier] += 1

                chng_in = DbChange.new(
                    table=f"address_incoming_relations",
                    data={
                        "dst_address_id_group": dst_group,
                        "dst_address_id_secondary_group": id_dst % 100,
                        "dst_address_id": id_dst,
                        "src_address_id": id_src,
                        "no_transactions": relations_update.no_transactions,
                        "value": {},
                        "token_values": relations_update.token_values,
                    },
                )
                chng_out = DbChange.new(
                    table=f"address_outgoing_relations",
                    data={
                        "src_address_id_group": src_group,
                        "src_address_id_secondary_group": id_src % 100,
                        "src_address_id": id_src,
                        "dst_address_id": id_dst,
                        "no_transactions": relations_update.no_transactions,
                        "value": relations_update.value,
                    },
                )
                nr_new_rel += 2
            else:
                """update existing adddress relation"""
                nv = DeltaValue.from_db(outr.value).merge(relations_update.value)

                nv_token = (
                    outr.token_values
                )  # todo adding the dicts together, maybe a dedicated datastrucure would be better
                nv_token = nv_token if nv_token is not None else {}
                new_token = relations_update.token_values
                keys = set(nv_token.keys()).union(new_token.keys())
                for key in keys:
                    if key in nv_token and key in relations_update.token_values:
                        nv_token[key] = DeltaValue.from_db(nv_token[key]).merge(
                            relations_update.token_values[key]
                        )
                    elif key in nv_token:
                        pass
                    elif key in relations_update.token_values:
                        nv_token[key] = relations_update.token_values[key]

                chng_in = DbChange.update(
                    table=f"address_incoming_relations",
                    data={
                        "dst_address_id_group": dst_group,
                        "dst_address_id_secondary_group": id_dst % 100,
                        "dst_address_id": id_dst,
                        "src_address_id": id_src,
                        "no_transactions": outr.no_transactions
                        + relations_update.no_transactions,
                        "value": nv,
                        "token_values": nv_token,
                    },
                )

                chng_out = DbChange.update(
                    table=f"address_outgoing_relations",
                    data={
                        "src_address_id_group": src_group,
                        "src_address_id_secondary_group": id_src % 100,
                        "src_address_id": id_src,
                        "dst_address_id": id_dst,
                        "no_transactions": outr.no_transactions,
                        "value": nv,
                        "token_values": nv_token,
                    },
                )

            changes.append(chng_in)
            changes.append(chng_out)

        return changes, new_relations_in, new_relations_out, nr_new_rel

    def prepare_entities_for_ingest(
        self,
        delta: List[EntityDeltaAccount],
        resolve_identifier: Dict[str, int],
        bytes_to_row_address: Dict[str, Any],
        new_rel_in: dict,
        new_rel_out: dict,
        id_bucket_size: int,
        get_address_prefix: Callable[[str], Tuple[str, str]],
    ) -> Tuple[List[DbChange], int]:
        changes = []
        nr_new_entities = 0
        for update in delta:
            int_ident, entity = (
                resolve_identifier[update.identifier],
                bytes_to_row_address[update.identifier],
            )

            if int_ident == 19280:
                print("----\n", int_ident, "-\n", entity, "-\n", update)
            group = get_id_group(int_ident, id_bucket_size)
            if entity is not None:
                """old Address"""

                assert getattr(entity, f"address_id") == int_ident

                # recast so we can calculate without handling None all the time
                new_value = EntityDeltaAccount.from_db(entity).merge(update)
                # bytes to hex
                bytes_ = new_value.identifier
                bytes_.hex()
                assert new_value.first_tx_id <= new_value.last_tx_id

                # Nr. of addresses (no_addresses) is currently not updated for clusters
                # Since no merges happen there should not be a difference
                generic_data = {
                    "no_incoming_txs": new_value.no_incoming_txs,
                    "no_outgoing_txs": new_value.no_outgoing_txs,
                    "no_incoming_txs_zero_value": new_value.no_incoming_txs_zero_value,
                    "no_outgoing_txs_zero_value": new_value.no_outgoing_txs_zero_value,
                    "first_tx_id": new_value.first_tx_id,
                    "last_tx_id": new_value.last_tx_id,
                    "total_received": new_value.total_received,
                    "total_spent": new_value.total_spent,
                    "total_tokens_received": new_value.total_tokens_received,
                    "total_tokens_spent": new_value.total_tokens_spent,
                    "in_degree": entity.in_degree + new_rel_in[update.identifier],
                    "out_degree": entity.out_degree + new_rel_out[update.identifier],
                    "in_degree_zero_value": entity.in_degree_zero_value
                    + new_value.no_incoming_txs_zero_value,
                    "out_degree_zero_value": entity.out_degree_zero_value
                    + new_value.no_outgoing_txs_zero_value,
                    "address_id": int_ident,
                    "address_id_group": group,
                }

                chng = DbChange.update(
                    table=f"address",
                    data=generic_data,
                )

                changes.append(chng)
            else:
                """new address"""
                assert update.first_tx_id <= update.last_tx_id
                nr_new_entities += 1

                data = {
                    "no_incoming_txs": update.no_incoming_txs,
                    "no_outgoing_txs": update.no_outgoing_txs,
                    "no_incoming_txs_zero_value": update.no_incoming_txs_zero_value,
                    "no_outgoing_txs_zero_value": update.no_outgoing_txs_zero_value,
                    "first_tx_id": update.first_tx_id,
                    "last_tx_id": update.last_tx_id,
                    "total_received": update.total_received,
                    "total_spent": update.total_spent,
                    "total_tokens_received": update.total_tokens_received,
                    "total_tokens_spent": update.total_tokens_spent,
                    f"address_id": int_ident,
                    f"address_id_group": group,
                    "in_degree": new_rel_in[update.identifier],
                    "out_degree": new_rel_out[update.identifier],
                    "in_degree_zero_value": update.no_incoming_txs_zero_value,
                    "out_degree_zero_value": update.no_outgoing_txs_zero_value,
                    "is_contract": False,  # todo
                }
                data["address"] = update.identifier
                chng = DbChange.new(table=f"address", data=data)
                changes.append(chng)
                address, address_prefix = get_address_prefix(update.identifier)

                changes.append(
                    DbChange.new(
                        table="address_ids_by_address_prefix",
                        data={
                            "address": address,
                            "address_id": int_ident,
                            "address_prefix": address_prefix,
                        },
                    )
                )
        return changes, nr_new_entities
