# flake8: noqa
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Set, Tuple

from ...datatypes import EntityType, FlowDirection
from ...db import AnalyticsDb, DbChange
from ...rates import convert_to_fiat
from ...schema.schema import GraphsenseSchemas
from ...utils import DataObject as MutableNamedTuple
from ...utils import group_by, no_nones
from ...utils.account import (
    get_slim_tx_from_trace,
    get_unique_addresses_from_traces,
    get_unique_ordered_receiver_addresses_from_traces,
    get_unique_ordered_sender_addresses_from_traces,
)
from ...utils.adapters import EthTraceAdapter, TrxTraceAdapter
from ...utils.errorhandling import CrashRecoverer
from ...utils.logging import LoggerScope
from .abstractupdater import TABLE_NAME_DELTA_HISTORY, UpdateStrategy
from .generic import (
    ApplicationStrategy,
    DbDelta,
    DeltaValue,
    EntityDelta,
    RawEntityTx,
    RelationDelta,
    TraceDelta,
    TxDelta,
    get_id_group,
    groupby_property,
    prepare_entities_for_ingest,
    prepare_relations_for_ingest,
)
from .utxo import apply_changes

logger = logging.getLogger(__name__)


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


def txdeltas_from_account_transaction(
    transaction: dict,
    tx_ids: dict,
    rates: List[int],
    bucket_size: int,
    prefix_length: int,
) -> List[TxDelta]:
    """Create a DbDelta instance from a transaction

    Args:
        transaction (dict): Row object to build the delta from
        tx_ids (dict): Mapping from tx_hash to tx_id
        rates (List[int]): convertion rates to use.
        bucket_size (int): bucket size to use for tx_id_group
        prefix_length (int): prefix length to use for tx_prefix

    Returns:
        DbDelta: delta to apply to the db.
    """
    raise NotImplementedError


def dbdelta_from_account_trace(trace: dict, rates: List[int]) -> DbDelta:
    """Create a DbDelta instance from a trace

    Args:
        trace (dict): Row object to build the delta from
        rates (List[int]): convertion rates to use.

    Returns:
        DbDelta: delta to apply to the db.
    """

    slim_tx = get_slim_tx_from_trace(trace)

    sending_addr = [x.address for x in slim_tx if x.direction == FlowDirection.OUT][0]
    receiving_addr = [x.address for x in slim_tx if x.direction == FlowDirection.IN][0]

    entity_updates = []
    new_entity_transactions = []
    relations_updates = []

    # create deltas for sending and receiving address
    entity_updates.append(
        TraceDelta(
            tx_hash=trace.tx_hash,
            from_address=sending_addr,
            to_address=receiving_addr,
            asset="native",
            value=trace.value,
        )
    )

    entity_updates.append(
        TraceDelta(
            identifier=receiving_addr,
            total_spent=DeltaValue(value=0, fiat_values=convert_to_fiat(0, rates)),
            total_received=DeltaValue(
                value=trace.value, fiat_values=convert_to_fiat(trace.value, rates)
            ),
            first_tx_id=trace.tx_id,
            last_tx_id=trace.tx_id,
            no_incoming_txs=1,  # traces are also tracked in transactions
            no_outgoing_txs=0,
        )
    )


def dbdelta_from_account_trace_old(
    trace: dict, rates: List[int]
) -> DbDelta:  # todo remove when finished
    """Create a DbDelta instance from a trace

    Args:
        trace (dict): Row object to build the delta from
        rates (List[int]): convertion rates to use.

    Returns:
        DbDelta: delta to apply to the db.
    """

    slim_tx = get_slim_tx_from_trace(trace)

    sending_addr = [x.address for x in slim_tx if x.direction == FlowDirection.OUT][0]
    receiving_addr = [x.address for x in slim_tx if x.direction == FlowDirection.IN][0]

    entity_updates = []
    new_entity_transactions = []
    relations_updates = []

    # create deltas for sending and receiving address
    entity_updates.append(
        EntityDelta(
            identifier=sending_addr,
            total_spent=DeltaValue(
                value=trace.value, fiat_values=convert_to_fiat(trace.value, rates)
            ),
            total_received=DeltaValue(value=0, fiat_values=convert_to_fiat(0, rates)),
            first_tx_id=trace.tx_id,
            last_tx_id=trace.tx_id,
            no_incoming_txs=0,
            no_outgoing_txs=1,  # traces are also tracked in transactions
        )
    )

    entity_updates.append(
        EntityDelta(
            identifier=receiving_addr,
            total_spent=DeltaValue(value=0, fiat_values=convert_to_fiat(0, rates)),
            total_received=DeltaValue(
                value=trace.value, fiat_values=convert_to_fiat(trace.value, rates)
            ),
            first_tx_id=trace.tx_id,
            last_tx_id=trace.tx_id,
            no_incoming_txs=1,  # traces are also tracked in transactions
            no_outgoing_txs=0,
        )
    )

    # create new entity transactions for sending and receiving address
    new_entity_transactions.append(
        RawEntityTx(
            identifier=sending_addr,
            is_outgoing=True,
            value=trace.value,
            tx_id=trace.tx_id,
        )
    )

    new_entity_transactions.append(
        RawEntityTx(
            identifier=receiving_addr,
            is_outgoing=True,
            value=trace.value,
            tx_id=trace.tx_id,
        )
    )

    # create a delta for the relation between sending and receiving address
    relations_updates.append(
        RelationDelta(
            src_identifier=sending_addr,
            dst_identifier=receiving_addr,
            estimated_value=DeltaValue(
                value=trace.value, fiat_values=convert_to_fiat(trace.value, rates)
            ),
            no_transactions=1,
        )
    )

    return DbDelta(
        entity_updates=entity_updates,
        new_entity_txs=new_entity_transactions,
        relation_updates=relations_updates,
    )


def get_trace_deltas(
    db: AnalyticsDb,
    traces: List,
    rates: Dict[int, List],
    get_next_address_id: Callable[[], int],
    get_next_cluster_id: Callable[[], int],
) -> Tuple[List[DbChange], int, int, int, int]:
    """Main function to transform a list of traces from the raw
    keyspace to changes to the transformed db.

    Args:
        db (AnalyticsDb): database instance
        txs (List): list of transaction rows (raw db schema)
        rates (Dict[int, List]): Mapping from block to exchange rates
        (transformed db schema)
        get_next_address_id (Callable[[], int]): Function to fetch next new address_id
        get_next_cluster_id (Callable[[], int]): Function to fetch next new cluster_id
    """
    # tdb = db.transformed

    """
        Add pseudo inputs for coinbase txs
    """
    # new_traces = []
    # for trace in traces:
    #    if trace.reward_type == "block":
    #        new_traces.append(
    #            trace._replace(from_address = COINBASE_PSEUDO_ADDRESS)
    #        )
    #    else:
    #        # non-coinbase tx, nothing to do
    #        new_traces.append(trace)
    # traces = new_traces
    # del new_traces
    traces = [trace for trace in traces if trace.from_address is not None]

    """
        Build dict of unique addresses in the batch.
    """
    addresses = get_unique_addresses_from_traces(traces)

    len_addr = len(addresses)

    """
        Start loading the address_ids for the addresses async
    """
    with LoggerScope.debug(logger, f"Checking existence for {len_addr} addresses") as _:
        addr_ids = {  # noqa: C416
            adr: address_id
            for adr, address_id in db.transformed.get_address_id_async_batch(
                list(addresses)
            )
        }

        del addresses
    print(addr_ids)  # todo this makes flake8 happy
    """
        Sort transactions by block and tx id.
        Database should already return them sorted.
        This is just for caution
    """
    # todo skip for now. not sure if needed
    # with LoggerScope.debug(logger, "Prepare transaction data") as lg:
    #    traces = sorted(traces, key=lambda row: (row.block_id, row.tx_id))
    #    lg.debug(f"Working on batch of {len(traces)} transactions.")
    #    ordered_output_addresses = (
    #        get_unique_ordered_output_addresses_from_transactions(traces)
    #    )
    #    ordered_input_addresses = get_unique_ordered_input_addresses_from_transactions(
    #        traces
    #    )

    """
        Compute the changeset for each trace of the batch and convert
        all currency values with the corresponding rates.
    """
    with LoggerScope.debug(logger, "Creating address changeset") as _:
        per_trace_changes = []

        for trace in traces:
            per_trace_changes.append(
                dbdelta_from_account_trace(trace, rates[trace.block_id])
            )

        del rates
        del traces

        """
            Aggregate and compress the changeset to minimize database writes
        """
        address_delta = DbDelta.merge(per_trace_changes)
        print(address_delta)  # todo this makes flake8 happy
        del per_trace_changes


def todo_bypassflake8_continue_here(
    ordered_output_addresses,
    ordered_input_addresses,
    addr_ids,
    tdb,
    get_next_address_id,
    get_next_cluster_id,
    address_delta,
):
    """
    Read address data to merge for address updates
    """
    with LoggerScope.debug(logger, "Reading addresses to be updated") as lg:
        existing_addr_ids = no_nones(
            [address_id.result_or_exc.one() for adr, address_id in addr_ids.items()]
        )
        addresses_resolved = {  # noqa: C416
            addr_id: address
            for addr_id, address in tdb.get_address_async_batch(
                [adr.address_id for adr in existing_addr_ids]
            )
        }
        del existing_addr_ids

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

        addresses = {
            adr: get_resolved_address(address_id)
            for adr, address_id in addr_ids.items()
        }

        del addresses_resolved

    def get_next_address_ids_with_aliases(address: str):
        return (
            get_next_address_id()
            if address not in PSEUDO_ADDRESS_AND_IDS
            else PSEUDO_ADDRESS_AND_IDS[address]
        )

    with LoggerScope.debug(
        logger, "Assigning new address ids and cluster ids for new addresses"
    ) as lg:
        new_cluster_ids = {}

        for out_addr in ordered_output_addresses:
            addr_id, address = addresses[out_addr]
            if addr_id is None:
                new_addr_id = get_next_address_ids_with_aliases(address)
                addresses[out_addr] = (new_addr_id, None)
                new_cluster_ids[new_addr_id] = get_next_cluster_id()
            elif address is None:
                """
                I have found cases where address_prefix table is written
                But not the address table, for those cases we set a new
                cluster id and create the address in turn
                """
                lg.warning(
                    f"Address {out_addr} has address "
                    f"id {addr_id} but no address entry"
                )
                new_cluster_ids[addr_id] = get_next_cluster_id()

        del ordered_output_addresses

        not_yet_seen_input_addresses = {
            k for k, (addr_id2, _) in addresses.items() if addr_id2 is None
        }

        if len(not_yet_seen_input_addresses) > 0:
            for out_addr in ordered_input_addresses:
                if out_addr in not_yet_seen_input_addresses:
                    new_addr_id = get_next_address_ids_with_aliases(out_addr)
                    lg.debug(
                        "Encountered new input address - "
                        f"{out_addr:64}. Creating it with id {new_addr_id}."
                    )
                    addresses[out_addr] = (new_addr_id, None)
                    new_cluster_ids[new_addr_id] = get_next_cluster_id()

            del ordered_input_addresses

        del not_yet_seen_input_addresses

        assert (
            len([1 for k, (addr_id2, _) in addresses.items() if addr_id2 is None]) == 0
        )

    """
        Reading relations to be updated.
    """
    with LoggerScope.debug(logger, "Reading address relations to be updated") as _:
        rel_to_query = [
            (addresses[update.src_identifier][0], addresses[update.dst_identifier][0])
            for update in address_delta.relation_updates
        ]
        addr_outrelations_q = tdb.get_address_outgoing_relations_async_batch(
            rel_to_query
        )
        addr_outrelations = {
            (update.src_identifier, update.dst_identifier): qr
            for update, qr in zip(address_delta.relation_updates, addr_outrelations_q)
        }

        rel_to_query = [
            (addresses[update.dst_identifier][0], addresses[update.src_identifier][0])
            for update in address_delta.relation_updates
        ]
        addr_inrelations_q = tdb.get_address_incoming_relations_async_batch(
            rel_to_query
        )
        addr_inrelations = {
            (update.src_identifier, update.dst_identifier): qr
            for update, qr in zip(address_delta.relation_updates, addr_inrelations_q)
        }

        del rel_to_query, addr_inrelations_q, addr_outrelations_q

    with LoggerScope.debug(logger, "Reading clusters for addresses") as _:
        clusters_resolved = {  # noqa: C416
            cluster_id: cluster
            for cluster_id, cluster in tdb.get_cluster_async_batch(
                [
                    address.cluster_id
                    for adr, (addr_id, address) in addresses.items()
                    if address is not None
                ]
            )
        }

        def get_resolved_cluster(address_tuple):
            aidr, address = address_tuple
            if address is not None:
                assert address.cluster_id not in new_cluster_ids.values()
                assert aidr == address.address_id
                return (
                    aidr,
                    address,
                    address.cluster_id,
                    clusters_resolved[address.cluster_id].result_or_exc.one(),  # noqa
                )
            else:
                return (aidr, None, new_cluster_ids[aidr], None)

        """Assigning new address ids and cluster ids for new addresses"""

        addresses_with_cluster = {
            adr: get_resolved_cluster(address_tuple)
            for adr, address_tuple in addresses.items()
        }
        del clusters_resolved, addresses

    with LoggerScope.debug(logger, "Creating local lookup tables") as lg:
        cluster_to_addr_id = group_by(
            [
                (cluster_id, address_id)
                for _, (
                    address_id,
                    _,
                    cluster_id,
                    _,
                ) in addresses_with_cluster.items()
            ],
            lambda x: x[0],
        )

        cluster_from_cluster_id = group_by(
            [
                (cluster_id, cluster)
                for _, (_, _, cluster_id, cluster) in addresses_with_cluster.items()
            ],
            lambda x: x[0],
        )

        def address_to_cluster_id(addr: str) -> int:
            return addresses_with_cluster[addr][2]

        def address_to_address_id(addr: str) -> int:
            return addresses_with_cluster[addr][0]

        def address_to_address_obj(addr: str) -> int:
            return addresses_with_cluster[addr][1]

        def address_to_cluster(addr: str) -> int:
            return addresses_with_cluster[addr][3]

        def cluster_id_to_address_id(cluster_id: int) -> Set[int]:
            s = {addr_id for _, addr_id in cluster_to_addr_id[cluster_id]}
            if len(s) != 1:
                raise Exception(f"Found more than one address id for cluster_id {s}")
            return list(s)[0]

        def cluster_id_to_cluster(cluster_id: int) -> Any:
            clusters = [
                x for (_, x) in cluster_from_cluster_id[cluster_id] if x is not None
            ]
            assert len({x.cluster_id for x in clusters}) <= 1
            return clusters[0] if len(clusters) > 0 else None

        def get_address_prefix(address_str):
            address = tdb.to_db_address(address_str)
            return (address.db_encoding, address.prefix)

    with LoggerScope.debug(logger, "Creating cluster changeset") as lg:
        cluster_delta = address_delta.to_cluster_delta(address_to_cluster_id)

    with LoggerScope.debug(logger, "Reading cluster relations to be updated") as lg:
        rel_to_query = [
            (update.src_identifier, update.dst_identifier)
            for update in cluster_delta.relation_updates
        ]
        clstr_outrelations_q = tdb.get_cluster_outgoing_relations_async_batch(
            rel_to_query
        )
        clstr_outrelations = {
            (update.src_identifier, update.dst_identifier): qr
            for update, qr in zip(cluster_delta.relation_updates, clstr_outrelations_q)
        }

        rel_to_query = [
            (update.dst_identifier, update.src_identifier)
            for update in cluster_delta.relation_updates
        ]
        clstr_inrelations_q = tdb.get_cluster_incoming_relations_async_batch(
            rel_to_query
        )
        clstr_inrelations = {
            (update.src_identifier, update.dst_identifier): qr
            for update, qr in zip(cluster_delta.relation_updates, clstr_inrelations_q)
        }

        del rel_to_query, clstr_inrelations_q, clstr_outrelations_q

    """
        Merge Db Entries with deltas
    """

    with LoggerScope.debug(logger, "Preparing data to be written.") as lg:
        changes = []

        ingest_configs = {
            EntityType.ADDRESS: {
                "bucket_size": tdb.get_address_id_bucket_size(),
                "delta": address_delta,
                "id_transformation": address_to_address_id,
                "get_entity": address_to_address_obj,
                "incoming_relations_db": addr_inrelations,
                "outgoing_relations_db": addr_outrelations,
            }
        }

        new_relations_in = {}
        new_relations_out = {}
        nr_new_relations = {}
        nr_new_entities_created = {}
        for mode, config in ingest_configs.items():
            lg.debug(f"Prepare {mode} data.")
            """ Merging new transactions """
            changes.extend(
                self.prepare_txs_for_ingest_account(
                    config["delta"].new_entity_txs,
                    config["id_transformation"],
                    config["bucket_size"],
                    mode=mode,
                )
            )

            """ Merging relations deltas """
            (
                changes_relations,
                new_rels_in,
                new_rels_out,
                nr_new_rel_total,
            ) = prepare_relations_for_ingest(
                config["delta"].relation_updates,
                config["id_transformation"],
                config["incoming_relations_db"],
                config["outgoing_relations_db"],
                config["bucket_size"],
                mode=mode,
            )
            nr_new_rels = sum(new_rels_in.values()) + sum(new_rels_out.values())
            assert nr_new_rels == nr_new_rel_total
            nr_new_relations[mode] = nr_new_rels

            new_relations_in[mode] = new_rels_in
            new_relations_out[mode] = new_rels_out
            changes.extend(changes_relations)

            """ Merging entity deltas """
            entity_changes, nr_new_entities = prepare_entities_for_ingest(
                config["delta"].entity_updates,
                config["id_transformation"],
                config["get_entity"],
                address_to_cluster_id,
                cluster_id_to_address_id,
                new_relations_in[mode],
                new_relations_out[mode],
                config["bucket_size"],
                get_address_prefix,
                mode=mode,
            )
            changes.extend(entity_changes)
            nr_new_entities_created[mode] = nr_new_entities
            del nr_new_entities

    return (
        changes,
        nr_new_entities_created[EntityType.ADDRESS],
        nr_new_entities_created[EntityType.CLUSTER],
        nr_new_relations[EntityType.ADDRESS],
        nr_new_relations[EntityType.CLUSTER],
    )


def get_bookkeeping_changes(
    base_statistics: MutableNamedTuple,
    current_statistics: NamedTuple,
    last_block_processed: int,
    nr_new_address_relations: int,
    nr_new_addresses: int,
    nr_new_cluster_relations: int,
    nr_new_clusters: int,
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
            stats.no_blocks = no_blocks
        stats.timestamp = int(lb_date.timestamp())
        stats.no_address_relations += nr_new_address_relations
        stats.no_addresses += nr_new_addresses
        stats.no_cluster_relations += nr_new_cluster_relations
        stats.no_clusters += nr_new_clusters
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
        currency: str,
        pedantic: bool,
        application_strategy: ApplicationStrategy = ApplicationStrategy.TX,
        patch_mode: bool = False,
        forward_fill_rates: bool = False,
    ):
        super().__init__(db, currency, forward_fill_rates=forward_fill_rates)
        crash_file = (
            "/tmp/utxo_deltaupdate_"
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
            for block in batch:
                # todo, next line requires an index
                # todo, if necessary CREATE INDEX blockindex ON eth_raw_dev.transaction (block_id);
                transactions.extend(self._db.raw.get_transactions_in_block(block))
                traces.extend(self._db.raw.get_traces_in_block(block))
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

            traces = trace_adapter.cassandra_rows_to_dataclass(
                traces
            )  # todo change if we get data directly from ingest
            traces = trace_adapter.rename_fields_in_list(traces)

            changes = []
            tx_changes = self.get_transaction_changes(
                transactions, traces, rates
            )  # noqa: E501

            changes.extend(tx_changes)
            self.changes = changes

            # todo continue here
            """
            changes += tx_changes


            print(tx_changes)  # todo this makes flake8 happy

            (
                delta_changes,
                nr_new_addresses,
                nr_new_clusters,
                nr_new_address_relations,
                nr_new_cluster_relations,
            ) = get_trace_changes(
                self._db,
                traces,
                rates,
                self.consume_address_id,
                self.consume_cluster_id,
            )

            last_block_processed = batch[-1]
            nr_new_tx = len(traces)
            changes.extend(delta_changes)
            runtime_seconds = int(time.time() - self.batch_start_time)

            changes.extend(
                get_bookkeeping_changes(
                    self._statistics,
                    self._db.transformed.get_summary_statistics(),
                    last_block_processed,
                    nr_new_address_relations,
                    nr_new_addresses,
                    nr_new_cluster_relations,
                    nr_new_clusters,
                    nr_new_tx,
                    self.highest_address_id,
                    runtime_seconds,
                    bts,
                    patch_mode=self._patch_mode,
                )
            )

            # Store changes to be written
            # They are applied at the end of the batch in
            # persist_updater_progress
            self.changes = changes
            """

        else:
            raise ValueError(
                f"Unknown application strategy {self.application_strategy}"
            )

    def get_transaction_changes(
        self, transactions: List, traces: List, rates: Dict[int, List]
    ) -> List[DbChange]:
        # index transaction hashes
        tx_hashes = [tx.tx_hash for tx in transactions]
        # assign ids
        hash_to_id = {tx_hash: self.consume_transaction_id() for tx_hash in tx_hashes}
        hash_to_tx = {tx_hash: tx for tx_hash, tx in zip(tx_hashes, transactions)}

        id_bucket_size = self._db.transformed.get_address_id_bucket_size()
        block_bucket_size = self._db.transformed.get_block_id_bucket_size()

        txdeltas = []

        for tx_hash in tx_hashes:
            tx_id = hash_to_id[tx_hash]
            tx_index = hash_to_tx[tx_hash].transaction_index
            block_id = hash_to_tx[tx_hash].block_id
            txdeltas.append(
                TxDelta(
                    block_id=block_id, tx_id=tx_id, tx_hash=tx_hash, tx_index=tx_index
                )
            )

        tdb = self._db.transformed
        traces = [
            trace for trace in traces if trace.tx_hash is not None
        ]  # todo ignores reward tx
        # make sure they go through
        traces = [trace for trace in traces if trace.status == 1]

        def ignore_coinbase(hashes):
            return [hash for hash in hashes if hash not in PSEUDO_ADDRESS_AND_IDS]

        with LoggerScope.debug(logger, "Prepare transaction data") as lg:
            traces = sorted(
                traces, key=lambda row: (row.block_id, hash_to_id[row.tx_hash])
            )
            lg.debug(f"Working on batch of {len(traces)} transactions.")
            ordered_receiver_addresses = ignore_coinbase(
                get_unique_ordered_receiver_addresses_from_traces(traces)
            )
            ordered_sender_addresses = ignore_coinbase(
                get_unique_ordered_sender_addresses_from_traces(traces)
            )

        addresses = list(set(ordered_receiver_addresses + ordered_sender_addresses))
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
            addresses_resolved = {  # noqa: C416
                addr_id: address
                for addr_id, address in tdb.get_address_async_batch(
                    [adr.address_id for adr in existing_addr_ids]
                )
            }
            del existing_addr_ids

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

            addresses = {
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

        with LoggerScope.debug(
            logger, "Assigning new address ids for new addresses"
        ) as lg:
            for out_addr in ordered_receiver_addresses:
                addr_id, address = addresses[out_addr]
                if addr_id is None:
                    new_addr_id = get_next_address_ids_with_aliases(address)
                    addresses[out_addr] = (new_addr_id, None)
                elif address is None:
                    lg.warning(
                        f"TODO WIP: The following is because the addresses "
                        f"are in the prefix table but not in the address table. "
                        f"Address {out_addr} has address "
                        f"id {addr_id} but no address entry"
                    )

            del ordered_receiver_addresses

            not_yet_seen_sender_addresses = {
                k for k, (addr_id2, _) in addresses.items() if addr_id2 is None
            }

            if len(not_yet_seen_sender_addresses) > 0:
                for out_addr in ordered_sender_addresses:
                    if out_addr in not_yet_seen_sender_addresses:
                        new_addr_id = get_next_address_ids_with_aliases(out_addr)
                        lg.debug(
                            "Encountered new sender address - "
                            f"{out_addr.hex()}. Creating it with id {new_addr_id}."
                        )
                        addresses[out_addr] = (new_addr_id, None)

                del ordered_sender_addresses

            del not_yet_seen_sender_addresses

            assert (
                len([1 for k, (addr_id2, _) in addresses.items() if addr_id2 is None])
                == 0
            )

        def get_tx_prefix(tx_hash):
            tx_hash = tdb.to_db_tx_hash(tx_hash)
            return (tx_hash.db_encoding, tx_hash.prefix)

        def get_address_prefix(address_str):
            address = tdb.to_db_address(address_str)
            return (address.db_encoding, address.prefix)

        address_hash_to_id_with_new = {adr: addresses[adr][0] for adr in addresses}
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

        # hash to id mapping
        # hash_to_id = {tx_hash: self.consume_transaction_id() for tx_hash in tx_hashes}

        # get address_transactions
        # first create RawEntityTx
        # then create DbChange
        from typing import Union

        from .generic import RawEntityTxAccount

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

                chng = DbChange.new(
                    table=f"address_transactions",
                    data={
                        "address_id_group": get_id_group(ident, id_bucket_size),
                        "address_id_secondary_group": 0,  # todo, verify
                        "address_id": ident,
                        "currency": atx.currency,
                        "transaction_id": atx.tx_id,
                        "is_outgoing": atx.is_outgoing,
                        "tx_reference": atx.tx_reference,
                    },
                )

                changes.append(chng)
            return changes

        def get_entitytx_from_trace(trace, is_outgoing):
            tx_id = hash_to_id[trace.tx_hash]
            address_hash = trace.from_address if is_outgoing else trace.to_address
            address_id = address_hash_to_id_with_new[address_hash]
            currency = self.currency.upper()
            from cassandra.cluster import Cluster
            from cassandra.cqlengine.columns import Integer
            from cassandra.cqlengine.usertype import UserType

            # Step 1: Define the UserType in Python
            class TxReference(UserType):
                trace_index = Integer(required=False)
                log_index = Integer(required=False)

            tx_reference = {
                "trace_index": trace.trace_index,
                "log_index": None,
            }  # todo tokens (log_index)
            tx_reference = TxReference(**tx_reference)

            reta = RawEntityTxAccount(
                identifier=address_id,
                is_outgoing=is_outgoing,
                currency=currency,
                tx_id=tx_id,
                tx_reference=tx_reference,
            )
            return reta

        new_entity_transactions = []
        new_entity_transactions.extend(
            [get_entitytx_from_trace(trace, True) for trace in traces]
        )
        new_entity_transactions.extend(
            [get_entitytx_from_trace(trace, False) for trace in traces]
        )
        changes += prepare_txs_for_ingest(new_entity_transactions, id_bucket_size)

        return changes

        """
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
        delta: List[TxDelta],
        id_bucket_size: int,
        block_bucket_size: int,
        get_address_prefix: Callable[[bytes], Tuple[str, str]],
        get_transaction_prefix: Callable[[bytes], Tuple[str, str]],
        address_hash_to_id_with_new: Dict[str, bytes],
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

        for hash_ in address_hash_to_id_with_new:
            chng = DbChange.new(
                table="address_ids_by_address_prefix",
                data={
                    "address_prefix": get_address_prefix(hash_)[1],
                    "address": hash_,
                    "address_id": address_hash_to_id_with_new[hash_],
                },
            )
            changes.append(chng)

        nr_new_txs = len(delta)
        return changes, nr_new_txs
