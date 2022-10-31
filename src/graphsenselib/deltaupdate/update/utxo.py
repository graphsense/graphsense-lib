import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Set, Tuple

from ...datatypes import DbChangeType, EntityType
from ...db import AnalyticsDb, DbChange
from ...rates import convert_to_fiat
from ...utils import group_by
from ...utils.errorhandling import CrashRecoverer
from ...utils.logging import LoggerScope
from ...utils.utxo import (
    get_regflow,
    get_total_input_sum,
    get_unique_addresses_from_transaction,
    get_unique_addresses_from_transactions,
    get_unique_ordered_input_addresses_from_transactions,
    get_unique_ordered_output_addresses_from_transactions,
    regularize_inoutputs,
)
from .abstractupdater import TABLE_NAME_DELTA_HISTORY, UpdateStrategy
from .generic import (
    ApplicationStrategy,
    DbDelta,
    DeltaValue,
    EntityDelta,
    RawEntityTx,
    RelationDelta,
    prepare_entities_for_ingest,
    prepare_relations_for_ingest,
    prepare_txs_for_ingest,
)

logger = logging.getLogger(__name__)


def dbdelta_from_utxo_transaction(tx: dict, rates: List[int]) -> DbDelta:
    """Create a DbDelta instance form a transaction

    Args:
        tx (dict): transaction to build the delta from
        rates (List[int]): convertion rates to use.

    Returns:
        DbDelta: delta to apply to the db.
    """
    tx_adrs = get_unique_addresses_from_transaction(tx)
    reg_in = regularize_inoutputs(tx.inputs)
    reg_out = regularize_inoutputs(tx.outputs)
    reginput_sum = sum([v for _, v in reg_in.items()])
    flows = {adr: get_regflow(reg_in, reg_out, adr) for adr in tx_adrs}
    input_flows_sum = sum([f for adr, f in flows.items() if adr in reg_in])
    """
        reginput_sum == -input_flows_sum,
        unless input address is used as output in same tx
    """
    reduced_input_sum = get_total_input_sum(tx.inputs) - (
        reginput_sum + input_flows_sum
    )

    entity_updates = []
    new_entity_transactions = []
    relations_updates = []

    for adr, value in reg_in.items():
        entity_updates.append(
            EntityDelta(
                identifier=adr,
                total_spent=DeltaValue(
                    value=value, fiat_values=convert_to_fiat(value, rates)
                ),
                total_received=DeltaValue(
                    value=0, fiat_values=convert_to_fiat(0, rates)
                ),
                first_tx_id=tx.tx_id,
                last_tx_id=tx.tx_id,
                no_incoming_txs=0,
                no_outgoing_txs=1,
            )
        )

    for adr, value in reg_out.items():
        entity_updates.append(
            EntityDelta(
                identifier=adr,
                total_spent=DeltaValue(value=0, fiat_values=convert_to_fiat(0, rates)),
                total_received=DeltaValue(
                    value=value, fiat_values=convert_to_fiat(value, rates)
                ),
                first_tx_id=tx.tx_id,
                last_tx_id=tx.tx_id,
                no_incoming_txs=1,
                no_outgoing_txs=0,
            )
        )

    for adr in tx_adrs:
        flow = flows[adr]
        new_entity_transactions.append(
            RawEntityTx(
                identifier=adr, is_outgoing=(flow < 0), value=flow, tx_id=tx.tx_id
            )
        )

    for iadr, _ in reg_in.items():
        for oadr, _ in reg_out.items():
            if iadr == oadr:
                continue

            iflow = flows[iadr]
            oflow = flows[oadr]
            v = abs(round((iflow / reduced_input_sum) * oflow))
            relations_updates.append(
                RelationDelta(
                    src_identifier=iadr,
                    dst_identifier=oadr,
                    estimated_value=DeltaValue(
                        value=v, fiat_values=convert_to_fiat(v, rates)
                    ),
                    no_transactions=1,
                )
            )

    return DbDelta(
        entity_updates=entity_updates,
        new_entity_txs=new_entity_transactions,
        relation_updates=relations_updates,
    )


def get_transaction_changes(
    db: AnalyticsDb,
    txs: List,
    rates: Dict[int, List],
    get_next_address_id: Callable[[], int],
    get_next_cluster_id: Callable[[], int],
) -> Tuple[List[DbChange], int, int, int]:
    """Main function to transform a list of transactions from the raw
    keyspace to changes to the transformed db.

    Args:
        db (AnalyticsDb): database instance
        txs (List): list of transaction rows (raw db schema)
        rates (Dict[int, List]): Mapping from block to exchange rates
        (transformed db schema)
        get_next_address_id (Callable[[], int]): Function to fetch next new address_id
        get_next_cluster_id (Callable[[], int]): Function to fetch next new cluster_id
    """
    lgr = LoggerScope.get_indent_logger(logger)
    tdb = db.transformed
    """
        Build dict of unique addresses in the batch.
    """
    addresses = {}
    addresses = get_unique_addresses_from_transactions(txs)

    len_addr = len(addresses)
    # self.set_nr_queried_addresses_batch(len_addr)

    """
            Start loading the address_ids for the addresses async
    """
    lgr.info("Checking existence for " f"{len_addr} addresses")

    addr_ids_futures = {
        adr: db.transformed.get_address_id_async(adr) for adr in addresses
    }
    del addresses

    """
        Sort transactions by block and tx id.
        Database should already return them sorted.
        This is just for caution
    """
    with LoggerScope(logger, "Prepare transaction data") as lg:
        txs = sorted(txs, key=lambda row: (row.block_id, row.tx_id))
        ordered_output_addresses = (
            get_unique_ordered_output_addresses_from_transactions(txs)
        )
        ordered_input_addresses = get_unique_ordered_input_addresses_from_transactions(
            txs
        )

    """
        Compute the changeset for each tx of the batch and convert
        all currency values with the corresponding rates.
    """
    with LoggerScope(logger, "Creating address changeset") as lg:
        per_tx_changes = []

        for tx in txs:
            per_tx_changes.append(dbdelta_from_utxo_transaction(tx, rates[tx.block_id]))

        del rates
        del txs

        """
            Aggregate and compress the changeset to minimize database writes
        """
        address_delta = DbDelta.merge(per_tx_changes)
        del per_tx_changes

    """
        Read address data to merge for address updates
    """
    lgr.info("Start reading addresses to be updated")

    def get_address(addr_id_future):
        aidr = addr_id_future.result().one()
        if aidr is not None:
            return (
                aidr.address_id,
                tdb.get_address_async(aidr.address_id),
            )
        else:
            return (None, None)

    addresses = {adr: get_address(future) for adr, future in addr_ids_futures.items()}

    with LoggerScope(
        logger, "Assigning new address ids and cluster ids for new addresses"
    ) as lg:
        new_cluster_ids = {}

        for out_addr in ordered_output_addresses:
            addr_id, address = addresses[out_addr]
            if addr_id is None:
                new_addr_id = get_next_address_id()
                addresses[out_addr] = (new_addr_id, None)
                new_cluster_ids[new_addr_id] = get_next_cluster_id()

        del ordered_output_addresses

        not_yet_seen_input_addresses = {
            k for k, (addr_id2, _) in addresses.items() if addr_id2 is None
        }
        if len(not_yet_seen_input_addresses) > 0:
            for out_addr in ordered_input_addresses:
                if out_addr in not_yet_seen_input_addresses:
                    new_addr_id = get_next_address_id()
                    lg.warning(
                        "Encountered an input address that is not yet known: "
                        f"{out_addr}. Creating it with id {new_addr_id}."
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
    lgr.info("Start reading address relations to be updated")
    addr_outrelations = {
        (
            update.src_identifier,
            update.dst_identifier,
        ): tdb.get_address_outgoing_relations_async(
            addresses[update.src_identifier][0], addresses[update.dst_identifier][0]
        )
        for update in address_delta.relation_updates
    }

    addr_inrelations = {
        (
            update.src_identifier,
            update.dst_identifier,
        ): tdb.get_address_incoming_relations_async(
            addresses[update.dst_identifier][0], addresses[update.src_identifier][0]
        )
        for update in address_delta.relation_updates
    }

    lgr.info("Start reading clusters for addresses")

    def get_clusters(address_tuple):
        aidr, address_future = address_tuple
        if address_future is not None:
            address = address_future.result().one()
            assert address.cluster_id not in new_cluster_ids
            assert aidr == address.address_id
            return (
                aidr,
                address,
                address.cluster_id,
                tdb.get_cluster_async(address.cluster_id),
            )
        else:
            return (aidr, None, new_cluster_ids[aidr], None)

    """Assigning new address ids and cluster ids for new addresses"""

    addresses_with_cluster = {
        adr: get_clusters(address_tuple) for adr, address_tuple in addresses.items()
    }

    with LoggerScope(logger, "Creating local lookup tables") as lg:

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
            return addresses_with_cluster[addr][3].result().one()

        def cluster_id_to_address_id(cluster_id: int) -> Set[int]:
            s = {addr_id for _, addr_id in cluster_to_addr_id[cluster_id]}
            if len(s) != 1:
                raise Exception(f"Found more than one address id for cluster_id {s}")
            return list(s)[0]

        def cluster_id_to_cluster(cluster_id: int) -> Any:
            clusters = [
                x.result().one()
                for (_, x) in cluster_from_cluster_id[cluster_id]
                if x is not None
            ]
            assert len({x.cluster_id for x in clusters}) <= 1
            return clusters[0] if len(clusters) > 0 else None

        def get_address_prefix(address_str):
            address = tdb.to_db_address(address_str)
            return (address.db_encoding, address.prefix)

        del addresses

    with LoggerScope(logger, "Creating cluster changeset") as lg:
        cluster_delta = address_delta.to_cluster_delta(address_to_cluster_id)

    lgr.info("Start reading cluster relations to be updated")
    clstr_outrelations = {
        (
            update.src_identifier,
            update.dst_identifier,
        ): tdb.get_cluster_outgoing_relations_async(
            update.src_identifier,
            update.dst_identifier,
        )
        for update in cluster_delta.relation_updates
    }

    clstr_inrelations = {
        (
            update.src_identifier,
            update.dst_identifier,
        ): tdb.get_cluster_incoming_relations_async(
            update.dst_identifier,
            update.src_identifier,
        )
        for update in cluster_delta.relation_updates
    }

    """
        Merge Db Entries with deltas
    """

    with LoggerScope(logger, "Preparing data to be written.") as lg:
        changes = []

        ingest_configs = {
            EntityType.ADDRESS: {
                "bucket_size": tdb.get_address_id_bucket_size(),
                "delta": address_delta,
                "id_transformation": address_to_address_id,
                "get_entity": address_to_address_obj,
                "incoming_relations_db": addr_inrelations,
                "outgoing_relations_db": addr_outrelations,
            },
            EntityType.CLUSTER: {
                "bucket_size": tdb.get_cluster_id_bucket_size(),
                "delta": cluster_delta,
                "id_transformation": (lambda x: x),
                "get_entity": cluster_id_to_cluster,
                "incoming_relations_db": clstr_inrelations,
                "outgoing_relations_db": clstr_outrelations,
            },
        }

        new_relations_in = {}
        new_relations_out = {}
        nr_new_relations = {}
        for mode, config in ingest_configs.items():
            lg.info(f"Prepare {mode} data.")
            """
            Creating new address/cluster transaction
            """
            changes.extend(
                prepare_txs_for_ingest(
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

            """ Merging address deltas """
            changes.extend(
                prepare_entities_for_ingest(
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
            )

    return (
        changes,
        len_addr,
        nr_new_relations[EntityType.ADDRESS],
        nr_new_relations[EntityType.CLUSTER],
    )


def get_bookkeeping_changes(
    base_statistics,
    current_statistics,
    last_block_processed: int,
    nr_new_address_relations: int,
    nr_new_addresses: int,
    nr_new_cluster_relations: int,
    nr_new_clusters: int,
    nr_new_tx: int,
    highest_address_id: int,
    runtime_seconds: int,
    bts: Dict[int, datetime],
) -> List[DbChange]:
    """Creates changes for the bookkeeping tables like summary statistics after
    other data has been updated.

    Args:
        base_statistics (Row): statistics db row, all the other parameters are
        delta values
        last_block_processed (int): Last block processed
        nr_new_address_relations (int): Delta new addresses relations in changeset
        nr_new_addresses (int): Delta new addresses in changeset
        nr_new_cluster_relations (int): Delta new cluster relations in changeset
        nr_new_clusters (int): Delta new clusters in changeset
        nr_new_tx (int): Delta new txs in changeset
        highest_address_id (int): current highest address_id
        runtime_seconds (int): runtime to create the last changes in seconds
        bts (Dict[int, datetime]): mapping from block to its timestamp
    """
    changes = []
    with LoggerScope(logger, "Creating summary_statistics updates") as lg:
        lb_date = bts[last_block_processed]
        stats = base_statistics
        no_blocks = last_block_processed - 1
        statistics = {
            "no_blocks": no_blocks,
            "timestamp": int(lb_date.timestamp()),
            "no_address_relations": stats.no_address_relations
            + nr_new_address_relations,
            "no_addresses": stats.no_addresses + nr_new_addresses,
            "no_cluster_relations": stats.no_cluster_relations
            + nr_new_cluster_relations,
            "no_clusters": stats.no_clusters + nr_new_clusters,
            "no_transactions": stats.no_transactions + nr_new_tx,
        }
        if current_statistics is not None and current_statistics.no_blocks != no_blocks:
            assert current_statistics.no_blocks < no_blocks
            changes.append(
                DbChange.delete(
                    table="summary_statistics",
                    data={"no_blocks": current_statistics.no_blocks},
                )
            )
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


def validate_changes(db: AnalyticsDb, changes: List[DbChange]):
    """Validate a set of changes for correctness and consistency

    Args:
        db (AnalyticsDb): database instance
        changes (List[DbChange]): list of changes to be validated

    Raises:
        AssertionExecption: In case the data is in-correct.
    """
    with LoggerScope(logger, "Validating changes (pedantic mode)") as _:
        tdb = db.transformed
        addresses_seen = {}
        cluster_seen = {}
        cluster_new = {}
        addresses_new = {}
        seen_summary_delete = False
        current_summary_stats = db.transformed.get_summary_statistics()
        for change in changes:

            if change.action == DbChangeType.NEW and change.table == "cluster":
                # only one update per batch
                assert change.data["cluster_id"] not in cluster_seen
                cluster_seen[change.data["cluster_id"]] = True
                cluster_new[change.data["cluster_id"]] = True

                assert (
                    len(list(tdb.get_cluster_async(change.data["cluster_id"]).result()))
                    == 0
                )

            elif change.action == DbChangeType.NEW and change.table == "address":
                # only one update per batch
                assert change.data["address_id"] not in addresses_seen
                addresses_seen[change.data["address_id"]] = True
                addresses_new[change.data["address_id"]] = True

                assert "cluster_id" in change.data and change.data["cluster_id"] > 0
                assert (
                    len(list(tdb.get_address_async(change.data["address_id"]).result()))
                    == 0
                )

            elif change.action == DbChangeType.UPDATE and change.table == "cluster":
                # only one update per batch
                assert change.data["cluster_id"] not in cluster_seen
                cluster_seen[change.data["cluster_id"]] = True

                assert "address_id" not in change.data
                assert (
                    len(list(tdb.get_cluster_async(change.data["cluster_id"]).result()))
                    == 1
                )

            elif change.action == DbChangeType.UPDATE and change.table == "address":
                # only one update per batch
                assert change.data["address_id"] not in addresses_seen
                addresses_seen[change.data["address_id"]] = True

                assert "cluster_id" not in change.data

                ad = tdb.get_address_async(change.data["address_id"]).result().one()
                assert ad is not None
                assert tdb.get_address_id_async(ad.address).result().one() is not None

            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "address_incoming_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_address_incoming_relations_async(
                                change.data["dst_address_id"],
                                change.data["src_address_id"],
                            ).result()
                        )
                    )
                    == 1
                )
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "address_outgoing_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_address_outgoing_relations_async(
                                change.data["src_address_id"],
                                change.data["dst_address_id"],
                            ).result()
                        )
                    )
                    == 1
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_incoming_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_address_incoming_relations_async(
                                change.data["dst_address_id"],
                                change.data["src_address_id"],
                            ).result()
                        )
                    )
                    == 0
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_outgoing_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_address_outgoing_relations_async(
                                change.data["src_address_id"],
                                change.data["dst_address_id"],
                            ).result()
                        )
                    )
                    == 0
                )
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "cluster_incoming_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_cluster_incoming_relations_async(
                                change.data["dst_cluster_id"],
                                change.data["src_cluster_id"],
                            ).result()
                        )
                    )
                    == 1
                )
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "cluster_outgoing_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_cluster_outgoing_relations_async(
                                change.data["src_cluster_id"],
                                change.data["dst_cluster_id"],
                            ).result()
                        )
                    )
                    == 1
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_incoming_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_cluster_incoming_relations_async(
                                change.data["dst_cluster_id"],
                                change.data["src_cluster_id"],
                            ).result()
                        )
                    )
                    == 0
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_outgoing_relations"
            ):
                assert (
                    len(
                        list(
                            tdb.get_cluster_outgoing_relations_async(
                                change.data["src_cluster_id"],
                                change.data["dst_cluster_id"],
                            ).result()
                        )
                    )
                    == 0
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_addresses"
            ):
                assert (
                    change.data["cluster_id"] in cluster_seen
                    and change.data["cluster_id"] in cluster_new
                )
                assert (
                    change.data["address_id"] in addresses_seen
                    and change.data["address_id"] in addresses_new
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_ids_by_address_prefix"
            ):
                assert (
                    tdb.get_address_id_async(change.data["address"]).result().one()
                    is None
                )
                assert (
                    tdb.get_address_async(change.data["address_id"]).result().one()
                    is None
                )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_transactions"
            ):
                pass
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_transactions"
            ):
                pass
            elif (
                change.action == DbChangeType.NEW
                and change.table == "summary_statistics"
            ):
                if current_summary_stats.no_blocks < change.data["no_blocks"]:
                    assert seen_summary_delete

                assert current_summary_stats.no_blocks <= change.data["no_blocks"]
                assert (
                    current_summary_stats.no_address_relations
                    <= change.data["no_address_relations"]
                )
                assert current_summary_stats.no_addresses <= change.data["no_addresses"]
                assert (
                    current_summary_stats.no_cluster_relations
                    <= change.data["no_cluster_relations"]
                )
                assert current_summary_stats.no_clusters <= change.data["no_clusters"]
                assert (
                    current_summary_stats.no_transactions
                    < change.data["no_transactions"]
                )
                assert current_summary_stats.timestamp <= change.data["timestamp"]
            elif (
                change.action == DbChangeType.NEW
                and change.table == "delta_updater_history"
            ):
                pass
            elif change.action == DbChangeType.DELETE:
                seen_summary_delete = True
                assert change.table == "summary_statistics"
            else:
                raise Exception(f"Have not found validation rule for {change}.")


def apply_changes(db: AnalyticsDb, changes: List[DbChange], pedantic: bool):
    """Apply a list of db-changes to the database. Changes are applied
    atomically and in order.

    Args:
        db (AnalyticsDb): Database instance
        changes (List[DbChange]): List of changes

    Returns:
        None: Nothing

    Raises:
        e: reraises exceptions, a common instance for exceptions is when batches
        are too large. Cassandra has a hard limit for batch sizes
    """
    # Validate changes
    if pedantic:
        validate_changes(db, changes)

    with LoggerScope(logger, "Summarize updates") as lg:
        if len(changes) == 0:
            lg.debug("Nothing to apply")
            return
        lg.info(f"{len(changes)} updates to apply. Change Summary:")
        summary = group_by(changes, lambda x: (str(x.action), x.table))

        for (a, t), x in summary.items():
            logger.info(f"{len(x):6} {a:7} on {t:20}")

    with LoggerScope(logger, "Applying changes") as _:
        try:
            # Apply the changes atomic and in-order
            db.transformed.apply_changes(changes, atomic=True)
        except Exception as e:
            logger.error(
                f"Failed to apply {len(changes)} changes. Nothing was written."
            )
            raise e


class UpdateStrategyUtxo(UpdateStrategy):
    def __init__(
        self,
        db,
        currency: str,
        pedantic: bool,
        application_strategy: ApplicationStrategy = ApplicationStrategy.TX,
    ):
        super().__init__(db, currency)
        crash_file = (
            "/tmp/utxo_deltaupdate_"
            f"{self._db.raw.get_keyspace()}_{self._db.transformed.get_keyspace()}"
            "_crashreport.err"
        )
        self._statistics = self._db.transformed.get_summary_statistics()
        self._pedantic = pedantic
        self.changes = None
        self.application_strategy = application_strategy
        self.crash_recoverer = CrashRecoverer(crash_file)

    def persist_updater_progress(self):
        if self.changes is not None:
            apply_changes(self._db, self.changes, self._pedantic)
            self.changes = None
        self._time_last_batch = time.time() - self._batch_start_time

    def prepare_database(self):
        with LoggerScope(logger, "Preparing database"):
            if self._db.transformed.has_delta_updater_v1_tables():
                raise Exception(
                    "Tables of the delta-updater v1 detected. "
                    "please delete new_addresses, dirty_address, "
                    "detla_updater_state and delta-updater_history "
                    "before using delta updater v2."
                )

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

    def process_batch_impl_hook(self, batch):
        rates = {}
        txs = []
        bts = {}
        """
            Read transaction and exchange rates data
        """
        with LoggerScope(logger, "Checking recovery state.") as _:
            if self.crash_recoverer.is_in_recovery_mode():
                logger.warning(
                    "Delta update is in crash recovery mode. Crash hint is "
                    f"{self.crash_recoverer.get_recovery_hint()} in "
                    f"{self.crash_recoverer.get_recovery_hint_filename()}"
                )

        with LoggerScope(logger, "Reading transaction and rates data") as _:
            for block in batch:
                txs.extend(self._db.raw.get_transactions_in_block(block))
                rates[block] = self._db.transformed.get_exchange_rates_by_block(
                    block
                ).fiat_values
                bts[block] = self._db.raw.get_block_timestamp(block)

        if self.application_strategy == ApplicationStrategy.BATCH:
            if self.crash_recoverer.is_in_recovery_mode():
                raise Exception("Batch mode is not allowed in recovery mode.")
            changes = []
            (
                delta_changes,
                _,
                nr_new_address_relations,
                nr_new_cluster_relations,
            ) = get_transaction_changes(
                self._db,
                txs,
                rates,
                lambda: self.consume_address_id(),
                lambda: self.consume_cluster_id(),
            )

            last_block_processed = batch[-1]
            nr_new_tx = len(txs)
            changes.extend(delta_changes)
            runtime_seconds = int(time.time() - self.batch_start_time)

            changes.extend(
                get_bookkeeping_changes(
                    self._statistics,
                    self._db.transformed.get_summary_statistics(),
                    last_block_processed,
                    nr_new_address_relations,
                    self._nr_new_addresses,
                    nr_new_cluster_relations,
                    self._nr_new_clusters,
                    nr_new_tx,
                    self.highest_address_id,
                    runtime_seconds,
                    bts,
                )
            )

            # Store changes to be written
            # They are applied at the end of the batch in
            # persist_upater_progress
            self.changes = changes
        elif self.application_strategy == ApplicationStrategy.TX:
            nr_new_address_relations, nr_new_cluster_relations, nr_new_tx = (0, 0, 0)
            last_tx = None
            last_recovery_hint = None
            if self.crash_recoverer.is_in_recovery_mode():
                last_recovery_hint = self.crash_recoverer.get_recovery_hint()
                current_tx = last_recovery_hint["current_tx_id"]
                logger.warning(f"Recovering skipping to tx_id {current_tx}")
                txs = [tx for tx in txs if tx.tx_id >= current_tx]
                self.crash_recoverer.leave_recovery_mode()
            for tx in txs:
                try:
                    crash_last_succ_tx_id = None
                    crash_last_succ_tx_block_id = None
                    if last_tx is not None:
                        crash_last_succ_tx_id = last_tx.tx_id
                        crash_last_succ_tx_block_id = last_tx.block_id
                    elif last_recovery_hint is not None:
                        """
                        If recovery hint is available and no last tx take over
                        values from there.
                        """
                        crash_last_succ_tx_id = last_recovery_hint[
                            "last_successful_tx_id"
                        ]
                        crash_last_succ_tx_block_id = last_recovery_hint[
                            "last_successful_tx_block_id"
                        ]

                    crash_hint = {
                        "current_block_id": tx.block_id,
                        "current_tx_id": tx.tx_id,
                        "last_successful_tx_id": crash_last_succ_tx_id,
                        "last_successful_tx_block_id": crash_last_succ_tx_block_id,
                    }
                    last_recovery_hint = None
                    with LoggerScope(
                        logger, f"Working on tx_id {tx.tx_id} at block {tx.block_id}"
                    ):
                        with self.crash_recoverer.enter_critical_section(crash_hint):
                            (
                                delta_changes_tx,
                                _,
                                nr_new_address_relations_tx,
                                nr_new_cluster_relations_tx,
                            ) = get_transaction_changes(
                                self._db,
                                [tx],
                                rates,
                                lambda: self.consume_address_id(),
                                lambda: self.consume_cluster_id(),
                            )
                            last_block_processed = tx.block_id
                            nr_new_tx += 1
                            nr_new_address_relations += nr_new_address_relations_tx
                            nr_new_cluster_relations += nr_new_cluster_relations_tx
                            runtime_seconds = int(time.time() - self.batch_start_time)
                            bookkeepin_changes = get_bookkeeping_changes(
                                self._statistics,
                                self._db.transformed.get_summary_statistics(),
                                last_block_processed,
                                nr_new_address_relations,
                                self._nr_new_addresses,
                                nr_new_cluster_relations,
                                self._nr_new_clusters,
                                nr_new_tx,
                                self.highest_address_id,
                                runtime_seconds,
                                bts,
                            )
                            apply_changes(
                                self._db,
                                delta_changes_tx + bookkeepin_changes,
                                self._pedantic,
                            )
                except Exception as e:
                    if last_tx is None:
                        block = tx.block_id - 1
                        last_applied_msg = f"the last tx of block {block}"
                    else:
                        last_applied_msg = (
                            f" {last_tx.tx_id} ({last_tx.tx_hash}) "
                            "in block {last_tx.block_id}"
                        )
                    assert self.crash_recoverer.is_in_recovery_mode()
                    logger.error(
                        "Entering recovery mode. Recovery hint written "
                        f"at {self.crash_recoverer.get_recovery_hint_filename()}"
                    )
                    logger.error(f"Failed to apply tx {tx.tx_id} ({tx.tx_hash.hex()}).")
                    logger.error(f"Last applied tx is {last_applied_msg}.")
                    raise e
                finally:
                    last_tx = tx
        else:
            raise ValueError(
                f"Unknown application strategy {self.application_strategy}"
            )
