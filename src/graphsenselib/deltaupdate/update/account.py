import logging
import time
from datetime import datetime
from typing import Dict, List, NamedTuple, Tuple

import pandas as pd
from diskcache import Cache

from graphsenselib.deltaupdate.update.tokens import ERC20Decoder
from graphsenselib.utils.cache import TableBasedCache

from ...config.config import DeltaUpdaterConfig
from ...db import DbChange
from ...schema.schema import GraphsenseSchemas
from ...utils import DataObject as MutableNamedTuple
from ...utils import no_nones
from ...utils.account import (
    get_id_group_with_secondary_addresstransactions,
    get_id_group_with_secondary_relations,
)
from ...utils.adapters import (
    AccountBlockAdapter,
    AccountLogAdapter,
    AccountTransactionAdapter,
    EthTraceAdapter,
    TrxTraceAdapter,
    TrxTransactionAdapter,
)
from ...utils.errorhandling import CrashRecoverer
from ...utils.logging import LoggerScope
from .abstractupdater import TABLE_NAME_DELTA_HISTORY, UpdateStrategy
from .deltahelpers import (
    balance_updates_tokens,
    balance_updates_traces_txs,
    get_entitydelta_from_tokentransfer,
    get_entitydelta_from_trace,
    get_entitydelta_from_transaction,
    get_entitytx_from_tokentransfer,
    get_entitytx_from_trace,
    get_entitytx_from_transaction,
    get_sorted_unique_addresses,
    relationdelta_from_tokentransfer,
    relationdelta_from_trace,
    relationdelta_from_transaction,
)
from .deltatochanges import (
    prepare_balances_for_ingest,
    prepare_entities_for_ingest,
    prepare_entity_txs_for_ingest,
    prepare_relations_for_ingest,
    prepare_txs_for_ingest,
)
from .generic import ApplicationStrategy, DeltaScalar, Tx
from .modelsaccount import (
    BalanceDelta,
    DbDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)
from .utxo import apply_changes

logger = logging.getLogger(__name__)


def only_call_traces(traces: List) -> List:
    return [trace for trace in traces if trace.call_type == "call"]


COINBASE_PSEUDO_ADDRESS = None  # todo is this true
PSEUDO_ADDRESS_AND_IDS = {COINBASE_PSEUDO_ADDRESS: -1}

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
    new_blocks: int,
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
        stats.no_blocks = current_statistics.no_blocks + new_blocks
        stats.timestamp = int(lb_date.timestamp())
        stats.no_address_relations += nr_new_address_relations
        stats.no_addresses += nr_new_addresses
        stats.no_transactions += nr_new_tx

        statistics = stats.as_dict()

        if current_statistics.no_blocks != stats.no_blocks:
            if not patch_mode:
                assert current_statistics.no_blocks < stats.no_blocks

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

    def get_block_data(self, cache, block):
        txs = cache.get(("transaction", block), [])
        traces = cache.get(("trace", block), [])
        logs = cache.get(("log", block), [])
        blocks = cache.get(("block", block), [])
        return txs, traces, logs, blocks

    def get_fee_data(self, cache, txs):
        return cache.get(("fee", txs), [{"fee": None}])[0]["fee"]

    def process_batch_impl_hook(self, batch):
        rates = {}
        transactions = []
        traces = []
        logs = []
        blocks = []
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
                txs_new, traces_new, logs_new, blocks_new = self.get_block_data(
                    cache, block
                )
                transactions.extend(txs_new)
                traces.extend(traces_new)
                logs.extend(logs_new)
                blocks.extend(blocks_new)

                fiat_values = self._db.transformed.get_exchange_rates_by_block(
                    block
                ).fiat_values
                if fiat_values is None:
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
                transaction_adapter = TrxTransactionAdapter()
                for tx in transactions:
                    tx["fee"] = self.get_fee_data(cache, tx["tx_hash"])

            elif self.currency == "eth":
                trace_adapter = EthTraceAdapter()
                transaction_adapter = AccountTransactionAdapter()

            # convert dictionaries to dataclasses and unify naming
            log_adapter = AccountLogAdapter()
            block_adapter = AccountBlockAdapter()
            traces = trace_adapter.dicts_to_renamed_dataclasses(traces)
            traces = trace_adapter.process_fields_in_list(traces)
            transactions = transaction_adapter.dicts_to_dataclasses(transactions)
            logs = log_adapter.dicts_to_dataclasses(logs)
            blocks = block_adapter.dicts_to_dataclasses(blocks)

            changes = []
            (tx_changes, nr_new_addresses, nr_new_address_relations) = self.get_changes(
                transactions, traces, logs, blocks, rates
            )

            changes.extend(tx_changes)
            last_block_processed = batch[-1]

            if self.currency == "trx":
                nr_new_tx = len([tx for tx in transactions if tx.receipt_status == 1])
            else:
                nr_new_tx = len(transactions)

            runtime_seconds = int(time.time() - self.batch_start_time)

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
                len(batch),
                patch_mode=self._patch_mode,
            )

            changes.extend(bookkeeping_changes)

            # Store changes to be written
            # They are applied at the end of the batch in
            # persist_updater_progress
            self.changes = changes

        else:
            raise ValueError(
                f"Unknown application strategy {self.application_strategy}"
            )

    def get_changes(
        self,
        transactions: List,
        traces: List,
        logs: List,
        blocks: List,
        rates: Dict[int, List],
    ) -> Tuple[List[DbChange], int, int]:
        currency = self.currency.upper()
        id_bucket_size = self._db.transformed.get_address_id_bucket_size()
        block_bucket_size = self._db.transformed.get_block_id_bucket_size()
        tdb = self._db.transformed

        def get_next_address_ids_with_aliases(address: str):
            return (
                self.consume_address_id()
                if address not in PSEUDO_ADDRESS_AND_IDS
                else PSEUDO_ADDRESS_AND_IDS[address]
            )

        def get_tx_prefix(tx_hash):
            tx_hash = tdb.to_db_tx_hash(tx_hash)
            return (tx_hash.db_encoding, tx_hash.prefix)

        def get_address_prefix(address_str):
            address = tdb.to_db_address(address_str)
            return (address.db_encoding, address.prefix)

        if currency == "TRX":
            transactions = [tx for tx in transactions if tx.to_address is not None]
            transactions = [tx for tx in transactions if tx.receipt_status == 1]

            consume_transaction_id_trx = (
                lambda block_id, transaction_index: (block_id << 32) + transaction_index
            )
            hash_to_id = {
                tx.tx_hash: consume_transaction_id_trx(
                    tx.block_id, tx.transaction_index
                )
                for tx in transactions
            }

        elif currency == "ETH":
            hash_to_id = {
                tx.tx_hash: self.consume_transaction_id() for tx in transactions
            }
        else:
            raise ValueError(f"Unknown currency {currency}")

        tx_hashes = [tx.tx_hash for tx in transactions]
        reward_traces = [t for t in traces if t.tx_hash is None]
        # traces without reward traces:
        traces = [trace for trace in traces if trace.tx_hash is not None]
        # calculate successful traces:
        traces_s = [trace for trace in traces if trace.status == 1]

        hash_to_tx = dict(zip(tx_hashes, transactions))

        txdeltas = []

        for tx_hash in tx_hashes:
            tx_id = hash_to_id[tx_hash]
            tx_index = hash_to_tx[tx_hash].transaction_index
            block_id = hash_to_tx[tx_hash].block_id
            failed = hash_to_tx[tx_hash].receipt_status == 0
            txdeltas.append(
                Tx(
                    block_id=block_id,
                    tx_id=tx_id,
                    tx_hash=tx_hash,
                    tx_index=tx_index,
                    failed=failed,
                )
            )

        with LoggerScope.debug(logger, "Decode logs to token transfers") as lg:
            tokendecoder = ERC20Decoder(self.currency)
            token_transfers = no_nones(
                [tokendecoder.log_to_transfer(log) for log in logs]
            )

        with LoggerScope.debug(
            logger, "Compute unique addresses in correct order"
        ) as lg:
            if currency == "TRX":
                addresses = get_sorted_unique_addresses(
                    traces_s, reward_traces, token_transfers, transactions
                )
            elif currency == "ETH":
                addresses = get_sorted_unique_addresses(
                    traces_s, reward_traces, token_transfers, []
                )
            else:
                raise ValueError(f"Unknown currency {currency}")
            len_addr = len(addresses)

        with LoggerScope.debug(
            logger, f"Checking existence for {len_addr} addresses"
        ) as _:
            addr_ids = dict(tdb.get_address_id_async_batch(list(addresses)))

        with LoggerScope.debug(logger, "Reading addresses to be updated") as lg:
            existing_addr_ids = no_nones(
                [address_id.result_or_exc.one() for adr, address_id in addr_ids.items()]
            )

            global addresses_resolved
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

        bytes_to_row_address = {
            address: row[1] for address, row in addresses_to_id__rows.items()
        }
        for addr in addresses:
            addr_id, address = addresses_to_id__rows[addr]
            if addr_id is None:
                new_addr_id = get_next_address_ids_with_aliases(addr)
                addresses_to_id__rows[addr] = (new_addr_id, None)

        address_hash_to_id = {
            address: id_row[0] for address, id_row in addresses_to_id__rows.items()
        }

        changes = prepare_txs_for_ingest(
            txdeltas,
            id_bucket_size,
            block_bucket_size,
            get_tx_prefix,
        )

        entity_transactions = []

        if currency == "TRX":
            traces_s_filtered = only_call_traces(traces_s)  # successful and call
        elif currency == "ETH":
            traces_s_filtered = traces_s
        else:
            raise ValueError(f"Unknown currency {currency}")

        # entity transactions from traces
        entity_transactions.extend(
            [
                get_entitytx_from_trace(trace, True, hash_to_id, address_hash_to_id)
                for trace in traces_s_filtered
            ]
        )
        entity_transactions.extend(
            [
                get_entitytx_from_trace(trace, False, hash_to_id, address_hash_to_id)
                for trace in traces_s_filtered
            ]
        )

        # entity transactions from token transfers
        entity_transactions.extend(
            [
                get_entitytx_from_tokentransfer(
                    tt, True, rates, hash_to_id, address_hash_to_id
                )
                for tt in token_transfers
            ]
        )
        entity_transactions.extend(
            [
                get_entitytx_from_tokentransfer(
                    tt, False, rates, hash_to_id, address_hash_to_id
                )
                for tt in token_transfers
            ]
        )

        # entity deltas from traces
        entity_deltas = []
        entity_deltas.extend(
            [
                get_entitydelta_from_trace(trace, True, rates, hash_to_id, currency)
                for trace in traces_s_filtered
            ]
        )
        entity_deltas.extend(
            [
                get_entitydelta_from_trace(trace, False, rates, hash_to_id, currency)
                for trace in traces_s_filtered + reward_traces
            ]
        )

        # entity deltas from token transfers
        entity_deltas.extend(
            [
                get_entitydelta_from_tokentransfer(tt, True, rates, hash_to_id)
                for tt in token_transfers
            ]
        )
        entity_deltas.extend(
            [
                get_entitydelta_from_tokentransfer(tt, False, rates, hash_to_id)
                for tt in token_transfers
            ]
        )

        # relation deltas from traces
        relation_updates_trace = [
            relationdelta_from_trace(trace, rates, currency)
            for trace in traces_s_filtered
        ]

        # relation deltas from token transfers
        relation_updates_tokens = [
            relationdelta_from_tokentransfer(tt, rates) for tt in token_transfers
        ]

        relation_updates = relation_updates_trace + relation_updates_tokens

        # in eth we disregard the eth values because they are already in the traces
        # in tron only traces that are not the initial transaction have values,
        # so we still need to add the value from the transaction
        if currency == "TRX":
            entity_transactions.extend(
                [
                    get_entitytx_from_transaction(
                        tx, True, hash_to_id, address_hash_to_id
                    )
                    for tx in transactions
                    if tx.from_address is not None
                ]
            )
            entity_transactions.extend(
                [
                    get_entitytx_from_transaction(
                        tx, False, hash_to_id, address_hash_to_id
                    )
                    for tx in transactions
                    if tx.to_address is not None
                ]
            )

            entity_deltas_txs = []
            entity_deltas_txs.extend(
                [
                    get_entitydelta_from_transaction(
                        tx, True, rates, hash_to_id, currency
                    )
                    for tx in transactions
                ]
            )
            entity_deltas_txs.extend(
                [
                    get_entitydelta_from_transaction(
                        tx, False, rates, hash_to_id, currency
                    )
                    for tx in transactions
                ]
            )
            entity_deltas.extend(no_nones(entity_deltas_txs))

            relation_updates_tx = [
                relationdelta_from_transaction(tx, rates, hash_to_id)
                for tx in transactions
            ]

            relation_updates_tx = [
                x for x in relation_updates_tx if x.src_identifier is not None
            ]

            relation_updates.extend(relation_updates_tx)

        else:
            relation_updates_tx = []

        credits_debits_tokens_eth = []
        credits_debits_tokens_eth += balance_updates_traces_txs(
            relation_updates_trace + relation_updates_tx, address_hash_to_id, currency
        )
        credits_debits_tokens_eth += balance_updates_tokens(
            relation_updates_tokens, address_hash_to_id
        )

        miner_rewards = [
            BalanceDelta(
                address_hash_to_id[t.to_address], {currency: DeltaScalar(t.value)}
            )
            for t in reward_traces
        ]

        if currency == "TRX":
            txFeeDebits = []
            burntFees = []
            txFeeCredits = [
                BalanceDelta(
                    address_hash_to_id[tx.from_address],
                    {currency: DeltaScalar(-tx.fee)},
                )
                for tx in transactions
                if tx.from_address in address_hash_to_id
            ]
        elif currency == "ETH":
            block_to_miner_id = {
                block.block_id: address_hash_to_id[block.miner] for block in blocks
            }
            txFeeDebits = [
                BalanceDelta(
                    block_to_miner_id[tx.block_id],
                    {currency: DeltaScalar(tx.receipt_gas_used * tx.gas_price)},
                )
                for tx in transactions
            ]

            burntFees = [
                BalanceDelta(
                    block_to_miner_id[b.block_id],
                    {currency: DeltaScalar(-b.base_fee_per_gas * b.gas_used)},
                )
                for b in blocks
            ]
            txFeeCredits = [
                BalanceDelta(
                    address_hash_to_id[tx.from_address],
                    {currency: DeltaScalar(-tx.receipt_gas_used * tx.gas_price)},
                )
                for tx in transactions
                if tx.from_address in address_hash_to_id
            ]

        balance_updates = (
            credits_debits_tokens_eth
            + txFeeDebits
            + txFeeCredits
            + burntFees
            + miner_rewards
        )

        bucket_size = id_bucket_size

        dbdelta = DbDeltaAccount(
            entity_deltas, entity_transactions, relation_updates, balance_updates
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
        address_ids = [addresses_to_id__rows[address][0] for address in addresses]
        addr_balances_q = (
            tdb.get_balance_async_batch_account(  # todo could probably query less
                address_ids
            )
        )

        addr_balances = {
            addr_id: BalanceDelta.from_db(addr_id, qr.result_or_exc.all())
            for addr_id, qr in zip(address_ids, addr_balances_q)
        }

        lg.debug("Prepare data.")
        changes += self.prepare_and_query_max_secondary_id(
            dbdelta.relation_updates,
            dbdelta.new_entity_txs,
            id_bucket_size,
            address_hash_to_id,
        )

        """
        Creating new transactions
        """
        changes += prepare_entity_txs_for_ingest(
            dbdelta.new_entity_txs, id_bucket_size, currency
        )

        """ balances"""
        changes += prepare_balances_for_ingest(
            dbdelta.balance_updates, id_bucket_size, addr_balances
        )

        """ Merging relations deltas """
        (
            changes_relations,
            new_rels_in,
            new_rels_out,
        ) = prepare_relations_for_ingest(
            dbdelta.relation_updates,
            address_hash_to_id,
            addr_inrelations,
            addr_outrelations,
            bucket_size,
        )

        assert sum(new_rels_in.values()) == sum(new_rels_out.values())
        nr_new_rels = sum(new_rels_in.values())

        changes.extend(changes_relations)

        """ Merging entity deltas """
        entity_changes, nr_new_entities = prepare_entities_for_ingest(
            dbdelta.entity_updates,
            address_hash_to_id,
            bytes_to_row_address,
            new_rels_in,
            new_rels_out,
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

    def prepare_and_query_max_secondary_id(
        self,
        relation_updates: List[RelationDeltaAccount],
        new_entity_txs: List[RawEntityTxAccount],
        id_bucket_size: int,
        address_hash_to_id: Dict[bytes, int],
    ):
        def max_secondary_dict_from_db(df, id_group_col, grp_col):
            # query max secondary ids from database
            unique_address_id_groups = list(df[grp_col])
            max_secondary_atx = self._db.transformed.get_max_secondary_ids_async(
                unique_address_id_groups, tablename, id_group_col
            )
            max_secondary_atx = [qr.result_or_exc.one() for qr in max_secondary_atx]
            # use placeholder -1 if there is nothing in the database yet.
            # Will be consumed by max
            # and not 0, or otherwise the logic will say it shouldnt update
            max_secondary_atx = [
                res[1] if res is not None else -1 for res in max_secondary_atx
            ]
            max_secondary_atx = dict(zip(unique_address_id_groups, max_secondary_atx))
            return max_secondary_atx

        def get_max_secondary_changes(data, tablename, grp_col, sec_col):
            max_col = "max_secondary_id"
            df = pd.DataFrame(data, columns=[grp_col, sec_col])
            df = df.groupby(grp_col).max()
            df = df.reset_index()
            df = df.rename(columns={sec_col: max_col})
            max_secondary_atx = max_secondary_dict_from_db(df, grp_col, grp_col)
            df[max_col + "old"] = df[grp_col].map(max_secondary_atx)
            df[max_col] = df[[max_col, max_col + "old"]].max(axis=1)
            # convert to Db changes

            changes = [
                DbChange.update(
                    table=tablename,
                    data={
                        grp_col: row[grp_col],
                        "max_secondary_id": row[max_col],
                    },
                )
                for _, row in df.iterrows()
                if row[max_col] != row[max_col + "old"]
            ]
            return changes

        """ secondary group id for address transactions and address in/out relations"""
        tablename = "address_transactions_secondary_ids"
        grp_col, sec_col = "address_id_group", "address_id_secondary_group"
        secondary_group_data = [
            get_id_group_with_secondary_addresstransactions(
                tx.identifier, id_bucket_size, tx.block_id
            )
            for tx in new_entity_txs
        ]
        changes_secondary_atx = get_max_secondary_changes(
            secondary_group_data, tablename, grp_col, sec_col
        )

        tablename = "address_outgoing_relations_secondary_ids"
        grp_col, sec_col = "src_address_id_group", "src_address_id_secondary_group"

        secondary_group_data = [
            get_id_group_with_secondary_relations(
                address_hash_to_id[tx.src_identifier],
                address_hash_to_id[tx.dst_identifier],
                id_bucket_size,
            )
            for tx in relation_updates
        ]
        changes_secondary_aor = get_max_secondary_changes(
            secondary_group_data, tablename, grp_col, sec_col
        )

        tablename = "address_incoming_relations_secondary_ids"
        grp_col, sec_col = "dst_address_id_group", "dst_address_id_secondary_group"

        secondary_group_data = [
            get_id_group_with_secondary_relations(
                address_hash_to_id[tx.dst_identifier],
                address_hash_to_id[tx.src_identifier],
                id_bucket_size,
            )
            for tx in relation_updates
        ]

        changes_secondary_air = get_max_secondary_changes(
            secondary_group_data, tablename, grp_col, sec_col
        )

        changes = []
        changes += changes_secondary_atx
        changes += changes_secondary_aor
        changes += changes_secondary_air
        return changes
