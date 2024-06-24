from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from functools import reduce
from typing import Any, Callable, List, Tuple, Union

from ...datatypes import EntityType
from ...db import DbChange
from ...utils import group_by, groupby_property
from ...utils.account import get_id_group


class ApplicationStrategy(Enum):
    TX = "transaction"
    BATCH = "batch"

    def __str__(self):
        return str(self.value)


class DeltaUpdate(ABC):
    @abstractmethod
    def merge(self, other_delta):
        pass


@dataclass
class DeltaScalar(DeltaUpdate):
    value: int

    def merge(self, other):
        return DeltaScalar(self.value + other.value)


@dataclass
class DeltaValue(DeltaUpdate):
    value: int
    fiat_values: List[int]

    @classmethod
    def from_db(Cls, db_row):
        return Cls(value=db_row.value, fiat_values=list(db_row.fiat_values))

    def merge(self, other):
        if other is None:
            return self
        assert self.fiat_values is not None and other.fiat_values is not None
        assert len(self.fiat_values) == len(other.fiat_values)
        return DeltaValue(
            value=self.value + other.value,
            fiat_values=[sum(i) for i in zip(self.fiat_values, other.fiat_values)],
        )


def merge_asset_dicts(d1, d2):  # probably better to wrap in class and define __add__
    d = {}
    for k in set(d1.keys()) | set(d2.keys()):
        d[k] = d1.get(k, DeltaValue(0, [0, 0])).merge(d2.get(k, DeltaValue(0, [0, 0])))
    return d


@dataclass
class Tx:
    block_id: int
    tx_id: int
    tx_hash: bytes
    tx_index: int
    failed: bool


@dataclass
class EntityDelta(DeltaUpdate):
    """The identifier is either an address or cluster identifier"""

    identifier: Union[str, int]
    total_received: DeltaValue
    total_spent: DeltaValue
    first_tx_id: int
    last_tx_id: int
    no_incoming_txs: int
    no_outgoing_txs: int

    @classmethod
    def from_db(Cls, db_row, mode: EntityType):
        if mode == EntityType.CLUSTER:
            identifier = db_row.cluster_id
        elif mode == EntityType.ADDRESS:
            identifier = db_row.address
        return Cls(
            identifier=identifier,
            total_received=DeltaValue.from_db(db_row.total_received),
            total_spent=DeltaValue.from_db(db_row.total_spent),
            first_tx_id=db_row.first_tx_id,
            last_tx_id=db_row.last_tx_id,
            no_incoming_txs=db_row.no_incoming_txs,
            no_outgoing_txs=db_row.no_outgoing_txs,
        )

    def merge(self, other_delta):
        assert self.identifier == other_delta.identifier
        return EntityDelta(
            identifier=self.identifier,
            total_received=self.total_received.merge(other_delta.total_received),
            total_spent=self.total_spent.merge(other_delta.total_spent),
            first_tx_id=min(self.first_tx_id, other_delta.first_tx_id),
            last_tx_id=max(self.last_tx_id, other_delta.last_tx_id),
            no_incoming_txs=self.no_incoming_txs + other_delta.no_incoming_txs,
            no_outgoing_txs=self.no_outgoing_txs + other_delta.no_outgoing_txs,
        )


def minusone_respecting_function(x, y, f):
    """
    -1 is a placeholder for first and last tx id in reward traces
    which dont have a tx_id
    """
    if x == -1 and y == -1:
        return -1
    if x == -1:
        return y
    if y == -1:
        return x
    return f(x, y)


@dataclass
class RawEntityTx:
    identifier: Union[str, int]
    is_outgoing: bool
    value: int
    tx_id: int


@dataclass
class RelationDelta(DeltaUpdate):
    src_identifier: Union[str, int]
    dst_identifier: Union[str, int]
    no_transactions: int
    estimated_value: DeltaValue

    @classmethod
    def from_db(Cls, db_row, mode="address"):
        return Cls(
            src_identifier=getattr(db_row, f"src_{mode}"),
            dst_identifier=getattr(db_row, f"dst_{mode}"),
            no_transactions=db_row.no_transactions,
            estimated_value=DeltaValue.from_db(db_row.estimated_value),
        )

    def merge(self, other_delta):
        assert self.src_identifier == other_delta.src_identifier
        assert self.dst_identifier == other_delta.dst_identifier
        return RelationDelta(
            src_identifier=self.src_identifier,
            dst_identifier=self.dst_identifier,
            estimated_value=self.estimated_value.merge(other_delta.estimated_value),
            no_transactions=self.no_transactions + other_delta.no_transactions,
        )


@dataclass
class DbDelta:
    entity_updates: List[EntityDelta]
    new_entity_txs: List[RawEntityTx]
    relation_updates: List[RelationDelta]

    def concat(self, other):
        return DbDelta(
            entity_updates=self.entity_updates + other.entity_updates,
            new_entity_txs=self.new_entity_txs + other.new_entity_txs,
            relation_updates=self.relation_updates + other.relation_updates,
        )

    @staticmethod
    def merge(change_sets: List[DbDelta]) -> "DbDelta":
        return reduce(lambda x, y: x.concat(y), change_sets).compress()

    def to_cluster_delta(self, address_to_cluster_id: Callable[[str], int]):
        eu = deepcopy(self.entity_updates)
        etxs = deepcopy(self.new_entity_txs)
        rel = deepcopy(self.relation_updates)
        for update in eu:
            update.identifier = address_to_cluster_id(update.identifier)

        for update in etxs:
            update.identifier = address_to_cluster_id(update.identifier)

        for update in rel:
            update.src_identifier = address_to_cluster_id(update.src_identifier)
            update.dst_identifier = address_to_cluster_id(update.dst_identifier)

        return DbDelta(
            entity_updates=eu, new_entity_txs=etxs, relation_updates=rel
        ).compress()

    def compress(self):
        grouped = groupby_property(
            self.entity_updates, "identifier", sort_by="first_tx_id"
        )
        entity_updates_merged = {
            k: reduce(lambda x, y: x.merge(y), v) for k, v in grouped.items()
        }
        assert len(entity_updates_merged.keys()) == len(
            set(entity_updates_merged.keys())
        )

        grouped = group_by(
            self.relation_updates, lambda x: (x.src_identifier, x.dst_identifier)
        )
        relations_updates_merged = {
            (src, dst): reduce(lambda x, y: x.merge(y), v)
            for (src, dst), v in grouped.items()
        }

        return DbDelta(
            entity_updates=sorted(
                entity_updates_merged.values(),
                key=lambda x: (x.first_tx_id, x.last_tx_id),
            ),
            new_entity_txs=self.new_entity_txs,
            relation_updates=list(relations_updates_merged.values()),
        )


def prepare_txs_for_ingest(
    delta: List[RawEntityTx],
    resolve_identifier: Callable[[Union[str, int]], int],
    id_bucket_size: int,
    mode: EntityType,
) -> List[DbChange]:
    """
    Creating new address transaction
    """
    changes = []
    for atx in delta:
        ident = resolve_identifier(atx.identifier)

        chng = DbChange.new(
            table=f"{mode}_transactions",
            data={
                f"{mode}_id_group": get_id_group(ident, id_bucket_size),
                f"{mode}_id": ident,
                "tx_id": atx.tx_id,
                "is_outgoing": atx.is_outgoing,
                "value": atx.value,
            },
        )

        changes.append(chng)
    return changes


def prepare_entities_for_ingest(
    delta: List[EntityDelta],
    resolve_identifier: Callable[[Union[str, int]], int],
    resolve_entity: Callable[[Union[str, int]], Any],
    address_to_cluster_id: Callable[[str], int],
    cluster_id_to_address_id: Callable[[int], int],
    new_rel_in: dict,
    new_rel_out: dict,
    id_bucket_size: int,
    get_address_prefix: Callable[[str], Tuple[str, str]],
    mode: EntityType,
) -> Tuple[List[DbChange], int]:
    changes = []
    nr_new_entities = 0
    for update in delta:
        int_ident, entity = (
            resolve_identifier(update.identifier),
            resolve_entity(update.identifier),
        )
        group = get_id_group(int_ident, id_bucket_size)
        if entity is not None:
            """old Address/cluster"""

            assert getattr(entity, f"{mode}_id") == int_ident

            new_value = EntityDelta.from_db(entity, mode=mode).merge(update)

            assert new_value.first_tx_id <= new_value.last_tx_id

            # Nr. of addresses (no_addresses) is currently not updated for clusters
            # Since no merges happen there should not be a difference
            generic_data = {
                "no_incoming_txs": new_value.no_incoming_txs,
                "no_outgoing_txs": new_value.no_outgoing_txs,
                "first_tx_id": new_value.first_tx_id,
                "last_tx_id": new_value.last_tx_id,
                "total_received": new_value.total_received,
                "total_spent": new_value.total_spent,
                "in_degree": entity.in_degree + new_rel_in[update.identifier],
                "out_degree": entity.out_degree + new_rel_out[update.identifier],
                f"{mode}_id": int_ident,
                f"{mode}_id_group": group,
            }

            chng = DbChange.update(
                table=f"{mode}",
                data=generic_data,
            )

            changes.append(chng)
        else:
            """new address/cluster"""
            assert update.first_tx_id <= update.last_tx_id
            nr_new_entities += 1

            data = {
                "no_incoming_txs": update.no_incoming_txs,
                "no_outgoing_txs": update.no_outgoing_txs,
                "first_tx_id": update.first_tx_id,
                "last_tx_id": update.last_tx_id,
                "total_received": update.total_received,
                "total_spent": update.total_spent,
                f"{mode}_id": int_ident,
                f"{mode}_id_group": group,
                "in_degree": new_rel_in[update.identifier],
                "out_degree": new_rel_out[update.identifier],
            }

            if mode == EntityType.ADDRESS:
                data["address"] = update.identifier
                data["cluster_id"] = address_to_cluster_id(update.identifier)
            elif mode == EntityType.CLUSTER:
                data["no_addresses"] = 1

            chng = DbChange.new(table=f"{mode}", data=data)

            changes.append(chng)

            if mode == EntityType.ADDRESS:
                """Clusters don't have a prefix table"""
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
            elif mode == EntityType.CLUSTER:
                """Add a entry to cluster addresses"""
                changes.append(
                    DbChange.new(
                        table="cluster_addresses",
                        data={
                            "address_id": cluster_id_to_address_id(int_ident),
                            "cluster_id": int_ident,
                            "cluster_id_group": group,
                        },
                    )
                )

    return changes, nr_new_entities


def prepare_relations_for_ingest(
    delta: List[RelationDelta],
    resolve_identifier: Callable[[Union[str, int]], int],
    inrelations: dict,
    outrelations: dict,
    id_bucket_size: int,
    mode: EntityType,
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

        id_src = resolve_identifier(relations_update.src_identifier)
        id_dst = resolve_identifier(relations_update.dst_identifier)
        src_group = get_id_group(id_src, id_bucket_size)
        dst_group = get_id_group(id_dst, id_bucket_size)

        if outr is None:
            """new address/cluster relation to insert"""
            new_relations_out[relations_update.src_identifier] += 1
            new_relations_in[relations_update.dst_identifier] += 1

            chng_in = DbChange.new(
                table=f"{mode}_incoming_relations",
                data={
                    f"dst_{mode}_id_group": dst_group,
                    f"dst_{mode}_id": id_dst,
                    f"src_{mode}_id": id_src,
                    "no_transactions": relations_update.no_transactions,
                    "estimated_value": relations_update.estimated_value,
                },
            )
            chng_out = DbChange.new(
                table=f"{mode}_outgoing_relations",
                data={
                    f"src_{mode}_id_group": src_group,
                    f"src_{mode}_id": id_src,
                    f"dst_{mode}_id": id_dst,
                    "no_transactions": relations_update.no_transactions,
                    "estimated_value": relations_update.estimated_value,
                },
            )
            nr_new_rel += 2
        else:
            """update existing adddress relation"""
            nv = DeltaValue.from_db(outr.estimated_value).merge(
                relations_update.estimated_value
            )

            chng_in = DbChange.update(
                table=f"{mode}_incoming_relations",
                data={
                    f"dst_{mode}_id_group": dst_group,
                    f"dst_{mode}_id": id_dst,
                    f"src_{mode}_id": id_src,
                    "no_transactions": outr.no_transactions
                    + relations_update.no_transactions,
                    "estimated_value": nv,
                },
            )

            chng_out = DbChange.update(
                table=f"{mode}_outgoing_relations",
                data={
                    f"src_{mode}_id_group": src_group,
                    f"src_{mode}_id": id_src,
                    f"dst_{mode}_id": id_dst,
                    "no_transactions": outr.no_transactions,
                    "estimated_value": nv,
                },
            )

        changes.append(chng_in)
        changes.append(chng_out)

    return changes, new_relations_in, new_relations_out, nr_new_rel


class Action(Enum):
    CONTINUE = 1
    DATA_TO_PROCESS_NOT_FOUND = 2
