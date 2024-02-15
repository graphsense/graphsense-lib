import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Tuple

from ...db import DbChange
from ...utils.account import (
    get_id_group,
    get_id_group_with_secondary_addresstransactions,
    get_id_group_with_secondary_relations,
)
from .generic import DeltaValue, Tx
from .modelsaccount import (
    BalanceDelta,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)

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
        assert (outr is None) == (inr is None)

        id_src = hash_to_id[relations_update.src_identifier]
        id_dst = hash_to_id[relations_update.dst_identifier]

        src_group, src_secondary = get_id_group_with_secondary_relations(
            id_src, id_dst, id_bucket_size
        )
        dst_group, dst_secondary = get_id_group_with_secondary_relations(
            id_dst, id_src, id_bucket_size
        )

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
            # todo adding the dicts together,
            #  maybe a dedicated datastrucure would be better
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

            assert outr.no_transactions == inr.no_transactions

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
                "in_degree_zero_value": entity.in_degree_zero_value,  # todo too broad
                "out_degree_zero_value": entity.out_degree_zero_value,  # todo too broad
                "address_id": int_ident,
                "address_id_group": group,
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
                "address_id": int_ident,
                "address_id_group": group,
                "in_degree": new_rel_in[update.identifier],
                "out_degree": new_rel_out[update.identifier],
                "in_degree_zero_value": 0,
                # update.no_incoming_txs_zero_value, # todo too broad
                "out_degree_zero_value": 0,
                # update.no_outgoing_txs_zero_value, # todo too broad
                "is_contract": False,  # todo
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
    delta: List[RawEntityTxAccount], id_bucket_size: int, currency: str
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
                ident, id_bucket_size, atx.block_id
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
                ident, id_bucket_size, atx.block_id
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
