import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Tuple

from graphsenselib.db import DbChange
from graphsenselib.deltaupdate.update.abstractupdater import TABLE_NAME_DELTA_HISTORY
from graphsenselib.deltaupdate.update.account.modelsdelta import (
    BalanceDelta,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)
from graphsenselib.deltaupdate.update.generic import DeltaValue, Tx
from graphsenselib.utils import DataObject as MutableNamedTuple

# from graphsenselib.utils import truncateI32
from graphsenselib.utils.account import (
    get_id_group,
    get_id_group_with_secondary_addresstransactions,
    get_id_group_with_secondary_relations,
)
from graphsenselib.utils.logging import LoggerScope

logger = logging.getLogger(__name__)


def prepare_txs_for_ingest(
    delta: List[Tx],
    id_bucket_size: int,
    block_bucket_size: int,
    get_transaction_prefix: Callable[[bytes], Tuple[str, str]],
) -> List[DbChange]:
    changes = []

    for update in delta:
        transaction_id = update.tx_id
        transaction_id_group = get_id_group(transaction_id, id_bucket_size)
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

    changes.extend(
        [
            DbChange.new(
                table="block_transactions",
                data={
                    "block_id_group": get_id_group(tx.block_id, block_bucket_size),
                    "block_id": tx.block_id,
                    "tx_id": tx.tx_id,
                },
            )
            for tx in delta
            if not tx.failed
        ]
    )

    return changes


def prepare_balances_for_ingest(
    delta: List[BalanceDelta], id_bucket_size: int, addr_balances: dict
) -> List[DbChange]:
    changes = []
    for balance in delta:
        addr_id = balance.identifier
        addr_group = get_id_group(addr_id, id_bucket_size)
        balance_update = balance.left_join(
            addr_balances.get(addr_id, BalanceDelta(addr_id, {}))
        )

        for assetname, dv in balance_update.asset_balances.items():
            chng = DbChange.update(
                table="balance",
                data={
                    "address_id_group": addr_group,
                    "address_id": addr_id,
                    "currency": assetname,
                    "balance": dv.value,
                },
            )

            changes.append(chng)

    return changes


def prepare_relations_for_ingest(
    delta: List[RelationDeltaAccount],
    hash_to_id: Dict[str, bytes],
    inrelations: dict,
    outrelations: dict,
    id_bucket_size: int,
    relations_nbuckets: int,
) -> Tuple[List[DbChange], dict, dict]:
    new_relations_in = defaultdict(int)
    new_relations_out = defaultdict(int)

    changes = []

    """ Merging relations deltas """
    for relations_update in delta:
        outr = outrelations[
            (relations_update.src_identifier, relations_update.dst_identifier)
        ].result_or_exc.one()
        inr = inrelations[
            (relations_update.src_identifier, relations_update.dst_identifier)
        ].result_or_exc.one()

        # assert (outr is None) == (inr is None)

        id_src = hash_to_id[relations_update.src_identifier]
        id_dst = hash_to_id[relations_update.dst_identifier]

        src_group, src_secondary = get_id_group_with_secondary_relations(
            id_src, id_dst, id_bucket_size, relations_nbuckets
        )
        dst_group, dst_secondary = get_id_group_with_secondary_relations(
            id_dst, id_src, id_bucket_size, relations_nbuckets
        )

        # checking and logging inconsistencies
        if (outr is None) == (inr is None):
            pass
        else:
            debug_msg = "inoutcheck: inconsistency "
            debug_msg += f"src: {bytes.hex(relations_update.src_identifier)} "
            debug_msg += f"dst: {bytes.hex(relations_update.dst_identifier)} "
            debug_msg += f"inr: {inr} "
            debug_msg += f"outr: {outr} "
            logger.warning(debug_msg)

        # missing record patching
        if (outr is None) and (inr is not None):
            outr = MutableNamedTuple(**inr._asdict())

            outr.src_address_id_group = src_group
            outr.src_address_id_secondary_group = src_secondary

        if (inr is None) and (outr is not None):
            inr = MutableNamedTuple(**outr._asdict())

            inr.dst_address_id_group = dst_group
            inr.dst_address_id_secondary_group = dst_secondary

        if outr is None:
            """new address relation to insert"""
            new_relations_out[relations_update.src_identifier] += 1
            new_relations_in[relations_update.dst_identifier] += 1

            chng_in = DbChange.new(
                table="address_incoming_relations",
                data={
                    "dst_address_id_group": dst_group,
                    "dst_address_id_secondary_group": dst_secondary,
                    "dst_address_id": id_dst,
                    "src_address_id": id_src,
                    "no_transactions": relations_update.no_transactions,
                    "value": relations_update.value,
                    "token_values": relations_update.token_values,
                },
            )
            chng_out = DbChange.new(
                table="address_outgoing_relations",
                data={
                    "src_address_id_group": src_group,
                    "src_address_id_secondary_group": src_secondary,
                    "src_address_id": id_src,
                    "dst_address_id": id_dst,
                    "no_transactions": relations_update.no_transactions,
                    "value": relations_update.value,
                    "token_values": relations_update.token_values,
                },
            )

        else:
            """update existing adddress relation"""
            nv = DeltaValue.from_db(outr.value).merge(relations_update.value)

            nv_token = outr.token_values
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

            # assert outr.no_transactions == inr.no_transactions

            chng_in = DbChange.update(
                table="address_incoming_relations",
                data={
                    "dst_address_id_group": dst_group,
                    "dst_address_id_secondary_group": dst_secondary,
                    "dst_address_id": id_dst,
                    "src_address_id": id_src,
                    "no_transactions": outr.no_transactions
                    + relations_update.no_transactions,
                    # outr and and inr should be the same
                    "value": nv,
                    "token_values": nv_token,
                },
            )

            chng_out = DbChange.update(
                table="address_outgoing_relations",
                data={
                    "src_address_id_group": src_group,
                    "src_address_id_secondary_group": src_secondary,
                    "src_address_id": id_src,
                    "dst_address_id": id_dst,
                    "no_transactions": outr.no_transactions
                    + relations_update.no_transactions,
                    "value": nv,
                    "token_values": nv_token,
                },
            )

        changes.append(chng_in)
        changes.append(chng_out)

    return changes, new_relations_in, new_relations_out


def prepare_entities_for_ingest(
    delta: List[EntityDeltaAccount],
    resolve_identifier: Dict[str, int],
    bytes_to_row_address: Dict[str, Any],
    new_rel_in: dict,
    new_rel_out: dict,
    id_bucket_size: int,
    get_address_prefix: Callable[[str], Tuple[str, str]],
) -> Tuple[List[DbChange], int]:
    changes = []
    int_signed_32_max = 2147483647
    nr_new_entities = 0
    for update in delta:
        int_ident, entity = (
            resolve_identifier[update.identifier],
            bytes_to_row_address[update.identifier],
        )

        group = get_id_group(int_ident, id_bucket_size)

        if entity is not None:
            """old Address"""
            assert getattr(entity, "address_id") == int_ident

            # recast so we can calculate without handling None all the time
            new_value = EntityDeltaAccount.from_db(entity).merge(update)
            # bytes to hex
            bytes_ = new_value.identifier
            bytes_.hex()
            assert new_value.first_tx_id <= new_value.last_tx_id

            # Nr. of addresses (no_addresses) is currently not updated for clusters
            # Since no merges happen there should not be a difference

            generic_data = {
                "no_incoming_txs": min(new_value.no_incoming_txs, int_signed_32_max),
                "no_outgoing_txs": min(new_value.no_outgoing_txs, int_signed_32_max),
                "no_incoming_txs_zero_value": min(
                    new_value.no_incoming_txs_zero_value, int_signed_32_max
                ),
                "no_outgoing_txs_zero_value": min(
                    new_value.no_outgoing_txs_zero_value, int_signed_32_max
                ),
                "first_tx_id": new_value.first_tx_id,
                "last_tx_id": new_value.last_tx_id,
                "total_received": new_value.total_received,
                "total_spent": new_value.total_spent,
                "total_tokens_received": new_value.total_tokens_received,
                "total_tokens_spent": new_value.total_tokens_spent,
                "in_degree": entity.in_degree + new_rel_in[update.identifier],
                "out_degree": entity.out_degree + new_rel_out[update.identifier],
                "in_degree_zero_value": entity.in_degree_zero_value,  # too broad
                "out_degree_zero_value": entity.out_degree_zero_value,  # too broad
                "address_id": int_ident,
                "address_id_group": group,
                "is_contract": new_value.is_contract,
            }

            chng = DbChange.update(
                table="address",
                data=generic_data,
            )

            changes.append(chng)
        else:
            """new address"""
            assert update.first_tx_id <= update.last_tx_id
            nr_new_entities += 1

            data = {
                "no_incoming_txs": min(update.no_incoming_txs, int_signed_32_max),
                "no_outgoing_txs": min(update.no_outgoing_txs, int_signed_32_max),
                "no_incoming_txs_zero_value": min(
                    update.no_incoming_txs_zero_value, int_signed_32_max
                ),
                "no_outgoing_txs_zero_value": min(
                    update.no_outgoing_txs_zero_value, int_signed_32_max
                ),
                "first_tx_id": update.first_tx_id,
                "last_tx_id": update.last_tx_id,
                "total_received": update.total_received,
                "total_spent": update.total_spent,
                "total_tokens_received": update.total_tokens_received,
                "total_tokens_spent": update.total_tokens_spent,
                "address_id": int_ident,
                "address_id_group": group,
                "in_degree": new_rel_in[update.identifier],
                "out_degree": new_rel_out[update.identifier],
                "in_degree_zero_value": 0,
                # update.no_incoming_txs_zero_value, # too broad
                "out_degree_zero_value": 0,
                # update.no_outgoing_txs_zero_value, #  too broad
                "is_contract": update.is_contract,
            }
            data["address"] = update.identifier
            chng = DbChange.new(table="address", data=data)
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


def prepare_entity_txs_for_ingest(
    delta: List[RawEntityTxAccount],
    id_bucket_size: int,
    currency: str,
    block_bucket_size_address_txs: int,
) -> List[DbChange]:
    """
    Creating new address transaction
    """
    changes = []
    for atx in delta:
        ident = atx.identifier
        is_token_transfer = len(atx.token_values.keys()) > 0
        for tokenname in atx.token_values.keys():
            (
                address_id_group,
                address_id_secondary_group,
            ) = get_id_group_with_secondary_addresstransactions(
                ident, id_bucket_size, atx.block_id, block_bucket_size_address_txs
            )
            chng = DbChange.new(
                table="address_transactions",
                data={
                    "address_id_group": address_id_group,
                    "address_id_secondary_group": address_id_secondary_group,
                    "address_id": ident,
                    "currency": tokenname,
                    "transaction_id": atx.tx_id,
                    "is_outgoing": atx.is_outgoing,
                    "tx_reference": atx.tx_reference,
                },
            )
            changes.append(chng)

        if not is_token_transfer:
            (
                address_id_group,
                address_id_secondary_group,
            ) = get_id_group_with_secondary_addresstransactions(
                ident, id_bucket_size, atx.block_id, block_bucket_size_address_txs
            )
            chng = DbChange.new(
                table="address_transactions",
                data={
                    "address_id_group": address_id_group,
                    "address_id_secondary_group": address_id_secondary_group,
                    "address_id": ident,
                    "currency": currency,
                    "transaction_id": atx.tx_id,
                    "is_outgoing": atx.is_outgoing,
                    "tx_reference": atx.tx_reference,
                },
            )

            changes.append(chng)
    return changes


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
