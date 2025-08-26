import logging
import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from graphsenselib.config.config import DeltaUpdaterConfig
from graphsenselib.db import DbChange
from graphsenselib.deltaupdate.update.abstractupdater import (
    TABLE_NAME_DELTA_HISTORY,
    UpdateStrategy,
)
from graphsenselib.deltaupdate.update.account.createchanges import (
    get_bookkeeping_changes,
    prepare_balances_for_ingest,
    prepare_entities_for_ingest,
    prepare_entity_txs_for_ingest,
    prepare_relations_for_ingest,
    prepare_txs_for_ingest,
)
from graphsenselib.deltaupdate.update.account.createdeltas import (
    get_balance_deltas,
    get_entity_transaction_updates_trace_token,
    get_entity_transactions_updates_tx,
    get_entity_updates_trace_token,
    get_entity_updates_tx,
    get_sorted_unique_addresses,
    is_contract_transaction,
    only_call_traces,
    relationdelta_from_tokentransfer,
    relationdelta_from_trace,
    relationdelta_from_transaction,
)
from graphsenselib.deltaupdate.update.account.modelsdelta import (
    BalanceDelta,
    DbDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)
from graphsenselib.deltaupdate.update.account.modelsraw import (
    AccountBlockAdapter,
    AccountLogAdapter,
    AccountTransactionAdapter,
    Block,
    EthTraceAdapter,
    Log,
    Trace,
    Transaction,
    TrxTraceAdapter,
    TrxTransactionAdapter,
)
from graphsenselib.deltaupdate.update.account.tokens import ERC20Decoder
from graphsenselib.deltaupdate.update.generic import Action, ApplicationStrategy, Tx
from graphsenselib.deltaupdate.update.utxo.update import apply_changes
from graphsenselib.schema.schema import GraphsenseSchemas
from graphsenselib.utils import DataObject as MutableNamedTuple
from graphsenselib.utils import no_nones
from graphsenselib.utils.account import (
    get_id_group_with_secondary_addresstransactions,
    get_id_group_with_secondary_relations,
)
from graphsenselib.utils.DeltaTableConnector import DeltaTableConnector
from graphsenselib.utils.errorhandling import CrashRecoverer
from graphsenselib.utils.logging import LoggerScope

logger = logging.getLogger(__name__)
inout_logger = logging.getLogger("inout_logger")


COINBASE_PSEUDO_ADDRESS = None
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

    def consume_transaction_id_composite(self, block_id, transaction_index):
        return (block_id << 32) + transaction_index

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

    def get_block_data(self, dt_connector: DeltaTableConnector, block_ids: List[int]):
        time_start = time.time()
        blocks = dt_connector.get(("block", block_ids), pd.DataFrame())
        logger.debug(f"Got {len(blocks)} blocks in {time.time() - time_start} seconds.")
        time_start = time.time()
        txs = dt_connector.get(("transaction", block_ids), pd.DataFrame())
        logger.debug(
            f"Got {len(txs)} transactions in {time.time() - time_start} seconds."
        )
        time_start = time.time()
        traces = dt_connector.get(("trace", block_ids), pd.DataFrame())
        logger.debug(f"Got {len(traces)} traces in {time.time() - time_start} seconds.")
        time_start = time.time()
        logs = dt_connector.get(("log", block_ids), pd.DataFrame())
        logger.debug(f"Got {len(logs)} logs in {time.time() - time_start} seconds.")
        return txs, traces, logs, blocks

    def get_fee_data(self, dt_connector: DeltaTableConnector, block_ids: List[int]):
        time_start = time.time()
        fees = dt_connector.get(("fee", block_ids), pd.DataFrame())
        logger.debug(f"Got {len(fees)} traces in {time.time() - time_start} seconds.")
        return fees

    def process_batch_impl_hook(self, batch: List[int]) -> Tuple[Action, Optional[int]]:
        rates = {}
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
            tableconnector = DeltaTableConnector(
                self.du_config.delta_sink.directory, self.du_config.s3_credentials
            )

            transactions, traces, logs, blocks = self.get_block_data(
                tableconnector, batch
            )

            block_ids_got = set(blocks["block_id"].unique())
            block_ids_expected = set(batch)
            if block_ids_got != block_ids_expected:
                missing_blocks = block_ids_expected - block_ids_got
                log.error(
                    f"Blocks {missing_blocks} are not present in the delta lake."
                    f"Please ingest more blocks into the delta lake before running the"
                    f"Delta updater."
                )
                return Action.DATA_TO_PROCESS_NOT_FOUND, None

            log.debug(f"Getting fiat values for blocks {batch}")
            for block in batch:
                fiat_values = self._db.transformed.get_exchange_rates_by_block(
                    block
                ).fiat_values
                if fiat_values is None:
                    missing_rates_in_block = True
                    fiat_values = [0, 0]
                rates[block] = fiat_values
                bts[block] = self._db.raw.get_block_timestamp(block)
            log.debug(f"Getting fiat values for blocks {batch} done")

            final_block = max(batch)

            if missing_rates_in_block:
                log.warning("Block Range has missing exchange rates. Using Zero.")

        if self.application_strategy == ApplicationStrategy.BATCH:
            if self.crash_recoverer.is_in_recovery_mode():
                raise Exception("Batch mode is not allowed in recovery mode.")

            if self.currency == "trx":
                trace_adapter = TrxTraceAdapter()
                transaction_adapter = TrxTransactionAdapter()

                fees = self.get_fee_data(tableconnector, batch)
                # merge fees into transactions
                # cast tx_hash of both to bytes so it can be hashed
                assert fees.empty == transactions.empty
                # to be 100% clean would have to add empty fee fields in the case where
                # there are transactions but no fees

                if not transactions.empty:
                    transactions["tx_hash"] = transactions["tx_hash"].apply(
                        lambda x: bytes(x)
                    )
                    fees["tx_hash"] = fees["tx_hash"].apply(lambda x: bytes(x))
                    # assuming no fees -> no transactions
                    transactions = pd.merge(
                        transactions,
                        fees,
                        on="tx_hash",
                        how="left",
                        suffixes=("", "_y"),
                    )

            elif self.currency == "eth":
                trace_adapter = EthTraceAdapter()
                transaction_adapter = AccountTransactionAdapter()

            # preprocess
            if not logs.empty:
                logs["topics"] = logs["topics"].apply(
                    lambda x: list(x) if x is not None else []
                )
            # replace np.nan with None
            transactions.replace({pd.NA: None}, inplace=True)
            traces.replace({pd.NA: None}, inplace=True)

            # i havent found out why in some cases pd.NA doesnt work
            blocks[["base_fee_per_gas"]] = blocks[["base_fee_per_gas"]].replace(
                {np.nan: None}
            )
            logger.debug("Converting to dataclasses")
            # convert dictionaries to dataclasses and unify naming
            log_adapter = AccountLogAdapter()
            block_adapter = AccountBlockAdapter()
            traces = trace_adapter.df_to_renamed_dataclasses(traces)
            traces = trace_adapter.process_fields_in_list(traces)
            transactions = transaction_adapter.df_to_dataclasses(transactions)

            logs = log_adapter.df_to_dataclasses(logs)
            blocks = block_adapter.df_to_dataclasses(blocks)
            blocks = block_adapter.process_fields_in_list(blocks)
            logger.debug("Converting to dataclasses done")

            changes = []

            (tx_changes, nr_new_addresses, nr_new_address_relations) = self.get_changes(
                transactions, traces, logs, blocks, rates
            )

            changes.extend(tx_changes)

            if self.currency == "trx":
                nr_new_tx = len([tx for tx in transactions if tx.receipt_status == 1])
            else:
                nr_new_tx = len(transactions)

            runtime_seconds = int(time.time() - self.batch_start_time)

            logger.debug("Getting bookkeeping changes")
            bookkeeping_changes = get_bookkeeping_changes(
                self._statistics,
                self._db.transformed.get_summary_statistics(),
                final_block,
                nr_new_address_relations,
                nr_new_addresses,
                nr_new_tx,
                self.highest_address_id,
                runtime_seconds,
                bts,
                len(blocks),
                patch_mode=self._patch_mode,
            )
            logger.debug("Bookkeeping changes done")

            changes.extend(bookkeeping_changes)

            # Store changes to be written
            # They are applied at the end of the batch in
            # persist_updater_progress
            self.changes = changes
            return Action.CONTINUE, final_block

        else:
            raise ValueError(
                f"Unknown application strategy {self.application_strategy}"
            )

    def get_changes(
        self,
        transactions: List[Transaction],
        traces: List[Trace],
        logs: List[Log],
        blocks: List[Block],
        rates: Dict[int, List],
    ) -> Tuple[List[DbChange], int, int]:
        currency = self.currency.upper()
        id_bucket_size = self._db.transformed.get_address_id_bucket_size()
        block_bucket_size = self._db.transformed.get_block_id_bucket_size()
        block_bucket_size_address_txs = (
            self._db.transformed.get_address_transactions_id_bucket_size()
        )
        relations_nbuckets = self._db.transformed.get_addressrelations_ids_nbuckets()
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

        hash_to_id = {
            tx.tx_hash: self.consume_transaction_id_composite(
                tx.block_id, tx.transaction_index
            )
            for tx in transactions
        }

        if currency == "TRX":
            transactions = [
                tx
                for tx in transactions
                if (tx.to_address is not None)
                or is_contract_transaction(tx, currency=currency)
            ]
            # set the target address to the contract address if
            # it is a contract creation to match ethereum
            for tx in transactions:
                if is_contract_transaction(tx, currency=currency):
                    tx.to_address = tx.receipt_contract_address

            transactions = [tx for tx in transactions if tx.receipt_status == 1]

            # todo fix in spark
            # until then, we need to uncomment the following when testing vs spark
            # remove transactions with transactionType=30, contract creations to
            # remain consistent with spark (but wrong)
            # transactions = [
            #    x
            #    for x in transactions
            #    if not is_contract_transaction(x, currency=currency)
            # ]

        elif currency == "ETH":
            pass
        else:
            raise ValueError(f"Unknown currency {currency}")

        tx_hashes = [tx.tx_hash for tx in transactions]
        reward_traces = [t for t in traces if t.tx_hash is None]
        # traces without reward traces:
        traces = [trace for trace in traces if trace.tx_hash is not None]
        # calculate successful traces:
        traces_s = [trace for trace in traces if trace.status == 1]

        hash_to_tx = dict(zip(tx_hashes, transactions))

        with LoggerScope.debug(logger, "Decode logs to token transfers"):
            supported_tokens = self._db.transformed.get_token_configuration()
            tokendecoder = ERC20Decoder(currency, supported_tokens)
            token_transfers = no_nones(
                [tokendecoder.log_to_transfer(log) for log in logs]
            )

        with LoggerScope.debug(logger, "Compute unique addresses in correct order"):
            if currency == "TRX":
                transactions_for_addresses = transactions

            elif currency == "ETH":
                transactions_for_addresses = []
            else:
                raise ValueError(f"Unknown currency {currency}")

            addresses = get_sorted_unique_addresses(
                traces_s,
                reward_traces,
                token_transfers,
                transactions_for_addresses,
                blocks,
            )
            len_addr = len(addresses)

        with LoggerScope.debug(
            logger, f"Checking existence for {len_addr} addresses"
        ) as _:
            addr_ids = dict(tdb.get_address_id_async_batch(list(addresses)))

        with LoggerScope.debug(logger, "Reading addresses to be updated"):
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
                        addresses_resolved[addr_id.address_id].result_or_exc.one(),  # noqa
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

        with LoggerScope.debug(logger, "Get transactions to insert into the database"):
            txs_to_insert = []

            for tx_hash in tx_hashes:
                tx_id = hash_to_id[tx_hash]
                tx_index = hash_to_tx[tx_hash].transaction_index
                block_id = hash_to_tx[tx_hash].block_id
                failed = hash_to_tx[tx_hash].receipt_status == 0
                txs_to_insert.append(
                    Tx(
                        block_id=block_id,
                        tx_id=tx_id,
                        tx_hash=tx_hash,
                        tx_index=tx_index,
                        failed=failed,
                    )
                )

        with LoggerScope.debug(
            logger, "Get entity transaction updates - traces and tokens"
        ):
            entity_transactions = []
            entity_deltas = []

            if currency == "TRX":
                traces_s_filtered = only_call_traces(traces_s)  # successful and call
            elif currency == "ETH":
                traces_s_filtered = traces_s
            else:
                raise ValueError(f"Unknown currency {currency}")

            entity_transactions += get_entity_transaction_updates_trace_token(
                traces_s_filtered,
                token_transfers,
                hash_to_id,
                address_hash_to_id,
                rates,
            )

        with LoggerScope.debug(logger, "Get entity updates - traces and tokens"):
            entity_deltas += get_entity_updates_trace_token(
                traces_s_filtered,
                token_transfers,
                reward_traces,
                hash_to_id,
                currency,
                rates,
            )

        with LoggerScope.debug(logger, "Get relation updates - traces and tokens"):
            relation_updates_trace = [
                relationdelta_from_trace(trace, rates, currency)
                for trace in traces_s_filtered
            ]
            relation_updates_tokens = [
                relationdelta_from_tokentransfer(tt, rates) for tt in token_transfers
            ]
            relation_updates = relation_updates_trace + relation_updates_tokens

        with LoggerScope.debug(
            logger,
            "Get entity and entity transaction updates from tranasactions (only tron)",
        ):
            # in eth we disregard the eth values because they are already in the traces
            # in tron only traces that are not the initial transaction have values,
            # so we still need to add the value from the transaction
            if currency == "TRX":
                entity_transactions_tx = get_entity_transactions_updates_tx(
                    transactions, hash_to_id, address_hash_to_id
                )

                entity_deltas_tx = get_entity_updates_tx(
                    transactions,
                    hash_to_id,
                    currency,
                    rates,
                )

                entity_deltas += entity_deltas_tx

                relation_updates_tx = [
                    relationdelta_from_transaction(tx, rates, currency)
                    for tx in transactions
                    if tx.from_address is not None
                ]

                entity_transactions += entity_transactions_tx
                relation_updates += relation_updates_tx

            elif currency == "ETH":
                relation_updates_tx = []
            else:
                raise ValueError(f"Unknown currency {currency}")

        with LoggerScope.debug(logger, "Get balance updates"):
            """Get balance updates"""
            balance_updates = get_balance_deltas(
                relation_updates_trace,
                relation_updates_tx,
                relation_updates_tokens,
                reward_traces,
                transactions,
                blocks,
                address_hash_to_id,
                currency,
            )

        with LoggerScope.debug(logger, "Create dbdelta and compress"):
            """Combine all updates except the pure inserts into a delta object"""
            dbdelta = DbDeltaAccount(
                entity_deltas, entity_transactions, relation_updates, balance_updates
            )
            """ Group and merge deltas before merge with db deltas """
            dbdelta = dbdelta.compress()

        with LoggerScope.debug(logger, "Query data from database"):
            # Query outrelations
            rel_to_query = [
                (
                    addresses_to_id__rows[update.src_identifier][0],
                    addresses_to_id__rows[update.dst_identifier][0],
                )
                for update in dbdelta.relation_updates
            ]
            addr_outrelations_q = tdb.get_address_outrelations_async_batch_account(
                rel_to_query
            )
            addr_outrelations = {
                (update.src_identifier, update.dst_identifier): qr
                for update, qr in zip(dbdelta.relation_updates, addr_outrelations_q)
            }

            # Query inrelations
            rel_to_query = [
                (
                    addresses_to_id__rows[update.dst_identifier][0],
                    addresses_to_id__rows[update.src_identifier][0],
                )
                for update in dbdelta.relation_updates
            ]
            addr_inrelations_q = tdb.get_address_inrelations_async_batch_account(
                rel_to_query
            )
            addr_inrelations = {
                (update.src_identifier, update.dst_identifier): qr
                for update, qr in zip(dbdelta.relation_updates, addr_inrelations_q)
            }

            # Query balances of addresses
            address_ids = [addresses_to_id__rows[address][0] for address in addresses]
            addr_balances_q = (
                tdb.get_balance_async_batch_account(  # could probably query less
                    address_ids
                )
            )
            addr_balances = {
                addr_id: BalanceDelta.from_db(addr_id, qr.result_or_exc.all())
                for addr_id, qr in zip(address_ids, addr_balances_q)
            }

        with LoggerScope.debug(logger, "Prepare changes"):
            changes = []

            """ Inserts of new transactions """
            changes += prepare_txs_for_ingest(
                txs_to_insert,
                id_bucket_size,
                block_bucket_size,
                get_tx_prefix,
            )

            """ Merging max secondary ID """
            changes += self.prepare_and_query_max_secondary_id(
                dbdelta.relation_updates,
                dbdelta.new_entity_txs,
                id_bucket_size,
                address_hash_to_id,
            )

            """ Merging entity transactions """
            changes += prepare_entity_txs_for_ingest(
                dbdelta.new_entity_txs,
                id_bucket_size,
                currency,
                block_bucket_size_address_txs,
            )

            """ Merging balances"""
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
                id_bucket_size,
                relations_nbuckets,
            )
            changes += changes_relations

            """ Merging entity deltas """
            entity_changes, nr_new_entities = prepare_entities_for_ingest(
                dbdelta.entity_updates,
                address_hash_to_id,
                bytes_to_row_address,
                new_rels_in,
                new_rels_out,
                id_bucket_size,
                get_address_prefix,
            )
            changes += entity_changes

            assert sum(new_rels_in.values()) == sum(new_rels_out.values())
            nr_new_rels = sum(new_rels_in.values())
            nr_new_entities_created = nr_new_entities

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
        relations_nbuckets = self._db.transformed.get_addressrelations_ids_nbuckets()

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

        block_bucket_size_address_txs = (
            self._db.transformed.get_address_transactions_id_bucket_size()
        )
        """ secondary group id for address transactions and address in/out relations"""
        tablename = "address_transactions_secondary_ids"
        grp_col, sec_col = "address_id_group", "address_id_secondary_group"
        secondary_group_data = [
            get_id_group_with_secondary_addresstransactions(
                tx.identifier,
                id_bucket_size,
                tx.block_id,
                block_bucket_size_address_txs,
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
                relations_nbuckets,
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
                relations_nbuckets,
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
