import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Tuple

from cassandra import InvalidRequest

from graphsenselib.config import is_fresh_clustering_enabled
from graphsenselib.datatypes import DbChangeType, EntityType
from graphsenselib.db import AnalyticsDb, DbChange
from graphsenselib.db.analytics import ApplyChangesResult
from graphsenselib.deltaupdate.update.abstractupdater import (
    TABLE_NAME_DELTA_HISTORY,
    UpdateStrategy,
)
from graphsenselib.deltaupdate.update.generic import (
    Action,
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
from graphsenselib.rates import convert_to_fiat
from graphsenselib.utils import DataObject as MutableNamedTuple
from graphsenselib.utils import group_by, no_nones
from graphsenselib.monitoring.notifications import send_msg_to_topic
from graphsenselib.utils.errorhandling import CrashRecoverer
from graphsenselib.utils.logging import LoggerScope
from graphsenselib.utils.utxo import (
    get_regflow,
    get_total_input_sum,
    get_unique_addresses_from_transaction,
    get_unique_addresses_from_transactions,
    get_unique_ordered_input_addresses_from_transactions,
    get_unique_ordered_output_addresses_from_transactions,
    regularize_inoutputs,
)

logger = logging.getLogger(__name__)

_GS_CLUSTERING_AVAILABLE = None


def _check_gs_clustering():
    global _GS_CLUSTERING_AVAILABLE
    if _GS_CLUSTERING_AVAILABLE is None:
        import importlib

        _GS_CLUSTERING_AVAILABLE = importlib.util.find_spec("gs_clustering") is not None
    return _GS_CLUSTERING_AVAILABLE


LARGE_MERGE_WARNING_THRESHOLD = 100_000


class ClusteringChanges(NamedTuple):
    """Planned writes for one incremental fresh-clustering step.

    address_assignments: (address_id, cluster_id) to (over)write in
        ``fresh_address_cluster`` and insert into ``fresh_cluster_addresses``.
    stats_upserts: (cluster_id, size, min_address_id) for ``fresh_cluster_stats``
        of new / joined / surviving clusters.
    member_deletes: (cluster_id, address_id) reverse-index rows to delete from
        ``fresh_cluster_addresses`` for the absorbed side of a merge.
    stats_deletes: cluster_ids whose ``fresh_cluster_stats`` row to delete.
    """

    address_assignments: List[Tuple[int, int]]
    stats_upserts: List[Tuple[int, int, int]]
    member_deletes: List[Tuple[int, int]]
    stats_deletes: List[int]

    @property
    def is_empty(self) -> bool:
        return not (
            self.address_assignments
            or self.stats_upserts
            or self.member_deletes
            or self.stats_deletes
        )


def _components_via_rust(
    new_tx_inputs: List[List[int]], addr_to_cluster: Dict[int, int]
) -> List[List[int]]:
    """Group the touched addresses into connected components with the Rust
    union-find (``gs_clustering``).

    Two kinds of unions are fed in:

    * each new multi-input transaction (its inputs share a cluster), and
    * one synthetic union per existing cluster over its touched members, so all
      components that reference the same existing cluster collapse into one.
      Without it the same cluster could be "joined to" in one component and
      "absorbed" in another, producing inconsistent cluster_ids.

    Returns components as lists of original address_ids.
    """
    from gs_clustering import Clustering

    all_ids = sorted({a for tx in new_tx_inputs for a in tx})
    to_dense = {orig: i for i, orig in enumerate(all_ids)}

    dense_txs = [[to_dense[a] for a in tx] for tx in new_tx_inputs]

    touched_by_cluster: Dict[int, List[int]] = defaultdict(list)
    for addr, cid in addr_to_cluster.items():
        touched_by_cluster[cid].append(addr)
    for addrs in touched_by_cluster.values():
        if len(addrs) >= 2:
            dense_txs.append([to_dense[a] for a in addrs])

    dense_txs = [tx for tx in dense_txs if len(tx) >= 2]

    c = Clustering(max_address_id=len(all_ids) - 1)
    c.process_transactions(dense_txs)
    mapping = c.get_mapping()

    components: Dict[int, List[int]] = defaultdict(list)
    for dense_a, dense_root in zip(
        mapping.column("address_id").to_pylist(),
        mapping.column("cluster_id").to_pylist(),
    ):
        components[dense_root].append(all_ids[dense_a])
    return list(components.values())


def _plan_clustering_changes(
    components: List[List[int]],
    addr_to_cluster: Dict[int, int],
    stats: Dict[int, Tuple[int, int]],
    get_members: Callable[[List[int]], List[Tuple[int, int]]],
) -> ClusteringChanges:
    """Classify each component as new / join / merge and plan the writes.

    Pure function (no DB or Rust). ``stats`` maps cluster_id -> (size,
    min_address_id) for every existing cluster referenced by ``components``;
    ``get_members(cluster_ids)`` returns (cluster_id, address_id) rows and is
    called only for the absorbed (non-survivor) side of a merge.
    """
    address_assignments: List[Tuple[int, int]] = []
    stats_upserts: List[Tuple[int, int, int]] = []
    member_deletes: List[Tuple[int, int]] = []
    stats_deletes: List[int] = []

    for members in components:
        existing = {addr_to_cluster[a] for a in members if a in addr_to_cluster}
        new_addrs = [a for a in members if a not in addr_to_cluster]

        if not existing:
            # NEW cluster — skip singletons (the one-off does not persist
            # size-1 clusters); cluster_id = min address (canonical).
            if len(members) < 2:
                continue
            cid = min(members)
            for a in members:
                address_assignments.append((a, cid))
            stats_upserts.append((cid, len(members), cid))

        elif len(existing) == 1:
            # JOIN — only the new addresses change; existing members keep their id.
            cid = next(iter(existing))
            if not new_addrs:
                continue  # no-op (idempotent re-run)
            for a in new_addrs:
                address_assignments.append((a, cid))
            old_size, old_min = stats[cid]
            stats_upserts.append(
                (cid, old_size + len(new_addrs), min(old_min, min(new_addrs)))
            )

        else:
            # MERGE — keep the largest cluster's id (tie: smallest cluster_id) so
            # the survivor (e.g. an exchange) is never rewritten; read and
            # re-point only the smaller absorbed side(s).
            survivor = max(existing, key=lambda c: (stats[c][0], -c))
            absorbed = sorted(existing - {survivor})
            new_size, new_min = stats[survivor]
            for cid, address_id in get_members(absorbed):
                address_assignments.append((address_id, survivor))
                member_deletes.append((cid, address_id))
            for ac in absorbed:
                ac_size, ac_min = stats[ac]
                if ac_size > LARGE_MERGE_WARNING_THRESHOLD:
                    logger.warning(
                        f"Merging large cluster {ac} ({ac_size:,} members) into "
                        f"survivor {survivor}; reading its full membership."
                    )
                new_size += ac_size
                new_min = min(new_min, ac_min)
                stats_deletes.append(ac)
            for a in new_addrs:
                address_assignments.append((a, survivor))
            if new_addrs:
                new_size += len(new_addrs)
                new_min = min(new_min, min(new_addrs))
            stats_upserts.append((survivor, new_size, new_min))

    return ClusteringChanges(
        address_assignments, stats_upserts, member_deletes, stats_deletes
    )


def run_incremental_clustering(
    db: AnalyticsDb, new_tx_inputs: List[List[int]]
) -> ClusteringChanges:
    """Plan fresh-clustering writes for a batch of new multi-input txs.

    Reads only ``address -> cluster`` for the touched addresses and
    ``fresh_cluster_stats`` for the clusters they belong to; the full membership
    (``fresh_cluster_addresses``) is read only for the absorbed side of an actual
    merge.  See :func:`_plan_clustering_changes`.
    """
    tdb = db.transformed

    new_address_ids = {a for tx in new_tx_inputs for a in tx}
    if not new_address_ids:
        return ClusteringChanges([], [], [], [])

    addr_to_cluster = dict(tdb.get_fresh_clusters_for_addresses(list(new_address_ids)))

    components = _components_via_rust(new_tx_inputs, addr_to_cluster)

    existing_cluster_ids = set(addr_to_cluster.values())
    stats = tdb.get_fresh_cluster_stats(existing_cluster_ids)
    missing = existing_cluster_ids - stats.keys()
    if missing:
        raise RuntimeError(
            f"fresh_cluster_stats missing {len(missing)} cluster rows "
            f"(e.g. {sorted(missing)[:5]}); run the fresh_cluster_stats backfill "
            "before incremental clustering."
        )

    return _plan_clustering_changes(
        components, addr_to_cluster, stats, tdb.get_fresh_cluster_members
    )


def _clustering_changes_to_db(cc: ClusteringChanges) -> List[DbChange]:
    """Translate planned :class:`ClusteringChanges` into DbChange writes."""
    changes: List[DbChange] = []
    for address_id, cluster_id in cc.address_assignments:
        changes.append(
            DbChange.new(
                table="fresh_address_cluster",
                data={"address_id": address_id, "cluster_id": cluster_id},
            )
        )
        changes.append(
            DbChange.new(
                table="fresh_cluster_addresses",
                data={"cluster_id": cluster_id, "address_id": address_id},
            )
        )
    for cluster_id, size, min_address_id in cc.stats_upserts:
        changes.append(
            DbChange.new(
                table="fresh_cluster_stats",
                data={
                    "cluster_id": cluster_id,
                    "size": size,
                    "min_address_id": min_address_id,
                },
            )
        )
    for cluster_id, address_id in cc.member_deletes:
        changes.append(
            DbChange.delete(
                table="fresh_cluster_addresses",
                data={"cluster_id": cluster_id, "address_id": address_id},
            )
        )
    for cluster_id in cc.stats_deletes:
        changes.append(
            DbChange.delete(
                table="fresh_cluster_stats",
                data={"cluster_id": cluster_id},
            )
        )
    return changes


COINBASE_PSEUDO_ADDRESS = "coinbase"
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


def get_table_abbrev(table_name: str) -> str:
    return "".join([f"{x[:1]}" for x in table_name.split("_")])


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
    input_flows_sum = sum([f for adr, f in flows.items() if adr in reg_in and f <= 0])
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
            if reduced_input_sum == 0:
                # This can happen for txs with zero value and
                # zero fees. eg. in btc
                # c1e0db6368a43f5589352ed44aa1ff9af33410e4a9fd9be0f6ac42d9e4117151
                v = 0
            else:
                v = abs(round((iflow / reduced_input_sum) * oflow))
            assert v <= max(abs(iflow), abs(oflow))
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
) -> Tuple[List[DbChange], int, int, int, int]:
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
    tdb = db.transformed

    """
        Add pseudo inputs for coinbase txs
    """
    new_txs = []

    for tx in txs:
        if tx.coinbase:
            assert tx.inputs is None
            outputsum = (
                sum([o.value for o in tx.outputs]) if tx.outputs is not None else 0
            )
            new_txs.append(
                tx._replace(
                    inputs=[
                        MutableNamedTuple(
                            **{
                                "address": [COINBASE_PSEUDO_ADDRESS],
                                "value": outputsum,
                                "address_type": None,
                            }
                        )
                    ]
                )
            )
        else:
            # non-coinbase tx, nothing to do
            new_txs.append(tx)

    txs = new_txs
    del new_txs

    """
        Build dict of unique addresses in the batch.
    """
    addresses = get_unique_addresses_from_transactions(txs)

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

    """
        Sort transactions by block and tx id.
        Database should already return them sorted.
        This is just for caution
    """
    with LoggerScope.debug(logger, "Prepare transaction data") as lg:
        txs = sorted(txs, key=lambda row: (row.block_id, row.tx_id))
        lg.debug(f"Working on batch of {len(txs)} transactions.")
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
    with LoggerScope.debug(logger, "Creating address changeset") as lg:
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
                    f"Address {out_addr} has address id {addr_id} but no address entry"
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
        # Only query incoming relations: outgoing carries identical
        # (no_transactions, estimated_value) data for the same edge.
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

        del rel_to_query, addr_inrelations_q

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
        # Only query incoming relations: outgoing carries identical data.
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

        del rel_to_query, clstr_inrelations_q

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
            },
            EntityType.CLUSTER: {
                "bucket_size": tdb.get_cluster_id_bucket_size(),
                "delta": cluster_delta,
                "id_transformation": (lambda x: x),
                "get_entity": cluster_id_to_cluster,
                "incoming_relations_db": clstr_inrelations,
            },
        }

        new_relations_in = {}
        new_relations_out = {}
        nr_new_relations = {}
        nr_new_entities_created = {}
        for mode, config in ingest_configs.items():
            lg.debug(f"Prepare {mode} data.")
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
        no_blocks = last_block_processed + 1

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


def validate_changes(db: AnalyticsDb, changes: List[DbChange]):
    """Validate a set of changes for correctness and consistency

    Args:
        db (AnalyticsDb): database instance
        changes (List[DbChange]): list of changes to be validated

    Raises:
        AssertionExecption: In case the data is in-correct.
    """
    with LoggerScope.debug(logger, "Validating changes (pedantic mode)") as _:
        tdb = db.transformed
        addresses_seen = {}
        cluster_seen = {}
        cluster_new = {}
        addresses_new = {}
        current_summary_stats = (
            db.transformed.get_summary_statistics() or DEFAULT_SUMMARY_STATISTICS
        )
        for change in changes:
            if change.action == DbChangeType.NEW and change.table == "cluster":
                # only one update per batch
                if change.data["cluster_id"] in cluster_seen:
                    raise ValueError(f"Only one update per cluster allowed: {change}")
                cluster_seen[change.data["cluster_id"]] = True
                cluster_new[change.data["cluster_id"]] = True

                if not (
                    len(list(tdb.get_cluster_async(change.data["cluster_id"]).result()))
                    == 0
                ):
                    raise ValueError(f"New cluster id is already in db: {change}")

            elif change.action == DbChangeType.NEW and change.table == "address":
                # only one update per batch
                if change.data["address_id"] in addresses_seen:
                    raise ValueError(f"Only one update per address allowed: {change}")
                addresses_seen[change.data["address_id"]] = True
                addresses_new[change.data["address_id"]] = True

                if not ("cluster_id" in change.data and change.data["cluster_id"] > 0):
                    raise ValueError(f"No cluster id in new address: {change}")
                if not (
                    len(list(tdb.get_address_async(change.data["address_id"]).result()))
                    == 0
                ):
                    raise ValueError(f"New address_id already in database: {change}")

            elif change.action == DbChangeType.UPDATE and change.table == "cluster":
                # only one update per batch
                if change.data["cluster_id"] in cluster_seen:
                    raise ValueError(f"Only one update per cluster allowed: {change}")
                cluster_seen[change.data["cluster_id"]] = True

                if "address_id" in change.data:
                    raise ValueError(
                        "Found address id in cluster update. "
                        f"Confused address with cluster? {change}"
                    )

                clusters = list(
                    tdb.get_cluster_async(change.data["cluster_id"]).result()
                )
                if not (len(clusters) == 1):
                    raise ValueError(
                        f"Could not find cluster to update in db! {change}"
                    )

                cluster = clusters[0]

                if cluster.cluster_id != change.data["cluster_id"]:
                    raise ValueError(f"Cluster_id do not match {cluster} {change}")

                if cluster.no_incoming_txs > change.data["no_incoming_txs"]:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.no_outgoing_txs > change.data["no_outgoing_txs"]:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.first_tx_id != change.data["first_tx_id"]:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.last_tx_id >= change.data["last_tx_id"]:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.total_received.value > change.data["total_received"].value:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.total_spent.value > change.data["total_spent"].value:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.in_degree > change.data["in_degree"]:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                if cluster.out_degree > change.data["out_degree"]:
                    raise ValueError(
                        f"Cluster changes are inconsistent {cluster} {change}"
                    )

                del cluster

            elif change.action == DbChangeType.UPDATE and change.table == "address":
                # only one update per batch
                if change.data["address_id"] in addresses_seen:
                    raise ValueError(f"Only one update per address allowed. {change}")
                addresses_seen[change.data["address_id"]] = True

                if "cluster_id" in change.data:
                    raise ValueError(
                        "Found cluster id in address update. "
                        f"Confused address with cluster? {change}"
                    )

                ad = tdb.get_address_async(change.data["address_id"]).result().one()
                if ad is None:
                    raise ValueError(f"Did not find address_id to update! {change}")

                if ad.no_incoming_txs > change.data["no_incoming_txs"]:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.no_outgoing_txs > change.data["no_outgoing_txs"]:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.first_tx_id != change.data["first_tx_id"]:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.last_tx_id >= change.data["last_tx_id"]:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.total_received.value > change.data["total_received"].value:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.total_spent.value > change.data["total_spent"].value:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.in_degree > change.data["in_degree"]:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                if ad.out_degree > change.data["out_degree"]:
                    raise ValueError(f"Address changes are inconsistent {ad} {change}")

                adid = tdb.get_address_id_async(ad.address).result().one()
                if adid is None:
                    raise ValueError(f"Did not find address to update! {change}")

                del ad, adid
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "address_incoming_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_address_incoming_relations_async(
                                change.data["dst_address_id"],
                                change.data["src_address_id"],
                            ).result()
                        )
                    )
                    == 1
                ):
                    raise ValueError(
                        f"Updated incoming address relation does not exist: {change}"
                    )
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "address_outgoing_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_address_outgoing_relations_async(
                                change.data["src_address_id"],
                                change.data["dst_address_id"],
                            ).result()
                        )
                    )
                    == 1
                ):
                    raise ValueError(
                        f"Updated outgoing address relation does not exist: {change}"
                    )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_incoming_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_address_incoming_relations_async(
                                change.data["dst_address_id"],
                                change.data["src_address_id"],
                            ).result()
                        )
                    )
                    == 0
                ):
                    raise ValueError(
                        f"New incoming address relation already exists: {change}"
                    )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_outgoing_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_address_outgoing_relations_async(
                                change.data["src_address_id"],
                                change.data["dst_address_id"],
                            ).result()
                        )
                    )
                    == 0
                ):
                    raise ValueError(
                        f"New outgoing address relation already exists: {change}"
                    )
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "cluster_incoming_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_cluster_incoming_relations_async(
                                change.data["dst_cluster_id"],
                                change.data["src_cluster_id"],
                            ).result()
                        )
                    )
                    == 1
                ):
                    raise ValueError(
                        f"Updated incoming cluster relation does not exist: {change}"
                    )
            elif (
                change.action == DbChangeType.UPDATE
                and change.table == "cluster_outgoing_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_cluster_outgoing_relations_async(
                                change.data["src_cluster_id"],
                                change.data["dst_cluster_id"],
                            ).result()
                        )
                    )
                    == 1
                ):
                    raise ValueError(
                        f"Updated outgoing cluster relation does not exist: {change}"
                    )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_incoming_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_cluster_incoming_relations_async(
                                change.data["dst_cluster_id"],
                                change.data["src_cluster_id"],
                            ).result()
                        )
                    )
                    == 0
                ):
                    raise ValueError(
                        f"New incoming cluster relation already exists: {change}"
                    )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_outgoing_relations"
            ):
                if not (
                    len(
                        list(
                            tdb.get_cluster_outgoing_relations_async(
                                change.data["src_cluster_id"],
                                change.data["dst_cluster_id"],
                            ).result()
                        )
                    )
                    == 0
                ):
                    raise ValueError(
                        f"New outgoing cluster relation already exists: {change}"
                    )
            elif (
                change.action == DbChangeType.NEW
                and change.table == "cluster_addresses"
            ):
                if not (
                    change.data["cluster_id"] in cluster_seen
                    and change.data["cluster_id"] in cluster_new
                ):
                    raise ValueError(
                        f"Have not seen change creating cluster id: {change}"
                    )
                if not (
                    change.data["address_id"] in addresses_seen
                    and change.data["address_id"] in addresses_new
                ):
                    raise ValueError(f"Have not seen change of address id: {change}")
            elif (
                change.action == DbChangeType.NEW
                and change.table == "address_ids_by_address_prefix"
            ):
                if (
                    tdb.get_address_id_async(change.data["address"]).result().one()
                    is not None
                ):
                    raise ValueError(f"New address already in db: {change}")
                if (
                    tdb.get_address_async(change.data["address_id"]).result().one()
                    is not None
                ):
                    raise ValueError(f"New address_id already in db: {change}")
            elif (
                change.action == DbChangeType.NEW
                and change.table == "summary_statistics"
            ):
                if not (current_summary_stats.no_blocks <= change.data["no_blocks"]):
                    raise ValueError(
                        "Violation: no_blocks db "
                        f"{current_summary_stats.no_blocks} <= "
                        f"{change.data['no_blocks']}"
                    )
                if not (
                    current_summary_stats.no_address_relations
                    <= change.data["no_address_relations"]
                ):
                    raise ValueError(
                        "Violation: no_address_relations db "
                        f"{current_summary_stats.no_address_relations} <= "
                        f"{change.data['no_address_relations']}"
                    )
                if not (
                    current_summary_stats.no_addresses <= change.data["no_addresses"]
                ):
                    raise ValueError(
                        "Violation: no_addresses db "
                        f"{current_summary_stats.no_addresses} <= "
                        f"{change.data['no_addresses']}"
                    )
                if not (
                    current_summary_stats.no_cluster_relations
                    <= change.data["no_cluster_relations"]
                ):
                    raise ValueError(
                        "Violation: no_cluster_relations db "
                        f"{current_summary_stats.no_cluster_relations} <= "
                        f"{change.data['no_cluster_relations']}"
                    )
                if not (
                    current_summary_stats.no_clusters <= change.data["no_clusters"]
                ):
                    raise ValueError(
                        "Violation: no_clusters db "
                        f"{current_summary_stats.no_clusters} <= "
                        f"{change.data['no_clusters']}"
                    )
                if not (
                    current_summary_stats.no_transactions
                    <= change.data["no_transactions"]
                ):
                    raise ValueError(
                        "Violation: no_transactions db "
                        f"{current_summary_stats.no_transactions} <= "
                        f"{change.data['no_transactions']}"
                    )
                # if not (current_summary_stats.timestamp <= change.data["timestamp"]):
                #     raise ValueError(
                #         "Violation: timestamp db "
                #         f"{current_summary_stats.timestamp} <= "
                #         f"{change.data['timestamp']}"
                #     )
            elif change.action == DbChangeType.DELETE:
                raise ValueError(f"Deletes are not allowed: {change}")
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
                and change.table == "delta_updater_history"
            ):
                pass
            else:
                raise Exception(f"Have not found validation rule for {change}.")


def _notify_db_retries(result: ApplyChangesResult, num_changes: int) -> None:
    """Send retry warning notification using the monitoring topic."""
    msg = (
        f"{result.warning_text}; threshold={result.warning_threshold}; "
        f"changes={num_changes}"
    )
    try:
        send_msg_to_topic("info", f":warning: GraphSense DB alert: {msg}")
    except Exception as e:
        logger.warning(f"Failed to send retry notification: {e}")


def apply_changes(
    db: AnalyticsDb,
    changes: List[DbChange],
    pedantic: bool,
    try_atomic_writes: bool,
):
    """Apply a list of db-changes to the database. Changes are applied
    atomically and in order.

    Args:
        db (AnalyticsDb): Database instance
        changes (List[DbChange]): List of changes
        pedantic (bool): Validate changes before applying
        try_atomic_writes (bool): Attempt atomic batch writes first
    Returns:
        None: Nothing

    Raises:
        e: reraises exceptions, a common instance for exceptions is when batches
        are too large. Cassandra has a hard limit for batch sizes
    """

    # Validate changes
    atomic = True
    if pedantic:
        validate_changes(db, changes)

    with LoggerScope.debug(logger, "Summarize updates") as lg:
        if len(changes) == 0:
            lg.debug("Nothing to apply")
            return
        lg.debug(f"{len(changes)} updates to apply. Change Summary:")

        chng_summary = {
            t: group_by(c, lambda y: str(y.action))
            for t, c in group_by(changes, lambda x: x.table).items()
        }
        t_actions = [
            (
                t,
                "; ".join(
                    [
                        f"{a[0].replace('n', '+').replace('d', '-')}{len(chngs)}"
                        for a, chngs in actions.items()
                    ]
                ),
            )
            for t, actions in chng_summary.items()
        ]

        short_summary = "; ".join(
            [f"{get_table_abbrev(t)}: {action_str}" for t, action_str in t_actions]
        )
        logger.info(short_summary)

    with LoggerScope.debug(logger, "Applying changes") as _:
        try:
            if try_atomic_writes:
                # try to apply the changes atomic and in-order
                try:
                    result = db.transformed.apply_changes(changes, atomic=True)
                except InvalidRequest as e:
                    atomic = False
                    msg = getattr(e, "message", repr(e)).lower()
                    if "batch too large" in msg:
                        logger.warning(
                            "Batch to large: Retrying to apply changes "
                            "without atomic write."
                        )
                        result = db.transformed.apply_changes(changes, atomic=False)
                    else:
                        raise e
            else:
                atomic = False
                result = db.transformed.apply_changes(changes, atomic=False)

        except Exception as e:
            atomicity_msg = (
                " Nothing was written (atomic writes)."
                if atomic
                else " Atomicity not guarantied."
            )
            logger.error(f"Failed to apply {len(changes)} changes.{atomicity_msg}")
            raise e

        if result.warning_text is not None:
            _notify_db_retries(result, len(changes))


class UpdateStrategyUtxo(UpdateStrategy):
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

    def run_fresh_clustering(self, start_block: int, end_block: int):
        """Run incremental clustering for the full block range [start_block, end_block].

        Reads multi-input tx inputs for the new block range via
        :func:`iter_multi_input_tx_inputs` (concurrent point/range reads of
        ``block_transactions``, ``transaction``, and
        ``address_ids_by_address_prefix``), then calls
        ``run_incremental_clustering`` which reads only the affected clusters
        from ``fresh_address_cluster`` / ``fresh_cluster_addresses``.

        Cost is ``O(new_blocks + new_txs + new_addresses)`` instead of
        ``O(total_chain)``.  Called once after all batches have been processed.
        """
        if not is_fresh_clustering_enabled():
            logger.info(
                "Fresh clustering disabled "
                "(GRAPHSENSE_FRESH_CLUSTERING_ENABLED is not true), skipping"
            )
            return

        if not _check_gs_clustering():
            logger.warning("gs_clustering not available, skipping fresh clustering")
            return

        from graphsenselib.transformation.clustering import iter_multi_input_tx_inputs

        with LoggerScope.debug(
            logger, f"Fresh clustering for blocks {start_block}-{end_block}"
        ) as lg:
            total_txs = 0
            total_assignments = 0
            total_writes = 0
            for chunk in iter_multi_input_tx_inputs(self._db, start_block, end_block):
                if not chunk:
                    continue
                total_txs += len(chunk)
                cc = run_incremental_clustering(self._db, chunk)
                if cc.is_empty:
                    continue
                changes = _clustering_changes_to_db(cc)
                # Apply per chunk so a merge in an earlier chunk is visible to
                # later chunks (single writer under the keyspace lock), and so
                # driver memory stays bounded to one block-chunk of activity.
                apply_changes(
                    self._db, changes, self._pedantic, try_atomic_writes=False
                )
                total_assignments += len(cc.address_assignments)
                total_writes += len(changes)

            if total_txs == 0:
                lg.debug(
                    f"No resolvable multi-input transactions in blocks "
                    f"{start_block}-{end_block}"
                )
                return
            lg.debug(
                f"Fresh clustering: {total_txs} multi-input txs over blocks "
                f"{start_block}-{end_block} -> {total_assignments} address "
                f"assignments, {total_writes} db changes written"
            )

    def process_batch_impl_hook(self, batch) -> Tuple[Action, Optional[int]]:
        rates = {}
        txs = []
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
                txs.extend(self._db.raw.get_transactions_in_block(block))
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
            changes = []
            (
                delta_changes,
                nr_new_addresses,
                nr_new_clusters,
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
                    if last_recovery_hint is not None:
                        logger.info(
                            f"Resuming processing at tx_id {tx.tx_id} at "
                            f"block {tx.block_id}"
                        )
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
                        block_last_error = last_recovery_hint["current_block_id"]
                        assert tx.block_id == block_last_error

                    crash_hint = {
                        "current_block_id": tx.block_id,
                        "current_tx_id": tx.tx_id,
                        "last_successful_tx_id": crash_last_succ_tx_id,
                        "last_successful_tx_block_id": crash_last_succ_tx_block_id,
                    }
                    last_recovery_hint = None
                    with LoggerScope.debug(
                        logger, f"Working on tx_id {tx.tx_id} at block {tx.block_id}"
                    ):
                        with self.crash_recoverer.enter_critical_section(crash_hint):
                            (
                                delta_changes_tx,
                                nr_new_addresses,
                                nr_new_clusters,
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
                                nr_new_addresses,
                                nr_new_cluster_relations,
                                nr_new_clusters,
                                nr_new_tx,
                                self.highest_address_id,
                                runtime_seconds,
                                bts,
                                patch_mode=self._patch_mode,
                            )
                            apply_changes(
                                self._db,
                                delta_changes_tx + bookkeepin_changes,
                                self._pedantic,
                                try_atomic_writes=True,
                            )
                except Exception as e:
                    assert self.crash_recoverer.is_in_recovery_mode()
                    logger.error(
                        "Entering recovery mode. Recovery hint written "
                        f"at {self.crash_recoverer.get_recovery_hint_filename()}"
                    )
                    logger.error(f"Failed to apply tx {tx.tx_id} ({tx.tx_hash.hex()}).")
                    raise e
                finally:
                    last_tx = tx
        else:
            raise ValueError(
                f"Unknown application strategy {self.application_strategy}"
            )

        return Action.CONTINUE, batch[-1]
