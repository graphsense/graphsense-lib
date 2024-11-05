from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import List

from cassandra.cqlengine.usertype import UserType

from graphsenselib.deltaupdate.update.generic import (
    DeltaScalar,
    DeltaUpdate,
    DeltaValue,
    merge_asset_dicts,
    minusone_respecting_function,
)
from graphsenselib.utils import group_by, groupby_property


@dataclass
class EntityDeltaAccount(DeltaUpdate):
    identifier: str
    total_received: DeltaValue
    total_spent: DeltaValue
    total_tokens_received: dict[str, DeltaValue]
    total_tokens_spent: dict[str, DeltaValue]
    first_tx_id: int
    last_tx_id: int
    no_incoming_txs: int
    no_outgoing_txs: int
    no_incoming_txs_zero_value: int
    no_outgoing_txs_zero_value: int
    is_contract: bool

    @classmethod
    def from_db(Cls, db_row):
        identifier = db_row.address

        # empty mapping is None in cassandra
        # python cassandra driver saves {} as None, so we dont
        # need to worry about empty dicts not
        # fitting into cassandra later
        if db_row.total_tokens_spent is None:
            total_tokens_spent = {}
        else:
            total_tokens_spent = {
                k: DeltaValue.from_db(v) for k, v in db_row.total_tokens_spent.items()
            }

        if db_row.total_tokens_received is None:
            total_tokens_received = {}
        else:
            total_tokens_received = {
                k: DeltaValue.from_db(v)
                for k, v in db_row.total_tokens_received.items()
            }

        return Cls(
            identifier=identifier,
            total_received=DeltaValue.from_db(db_row.total_received),
            total_spent=DeltaValue.from_db(db_row.total_spent),
            total_tokens_received=total_tokens_received,
            total_tokens_spent=total_tokens_spent,
            first_tx_id=db_row.first_tx_id,
            last_tx_id=db_row.last_tx_id,
            no_incoming_txs=db_row.no_incoming_txs,
            no_outgoing_txs=db_row.no_outgoing_txs,
            no_incoming_txs_zero_value=db_row.no_incoming_txs_zero_value,
            no_outgoing_txs_zero_value=db_row.no_outgoing_txs_zero_value,
            is_contract=db_row.is_contract,
        )

    def merge(self, other_delta):
        assert self.identifier == other_delta.identifier

        # self and other total_tokens_received
        # may not have the same keys, fix the following:
        total_tokens_received = merge_asset_dicts(
            self.total_tokens_received, other_delta.total_tokens_received
        )

        total_tokens_spent = merge_asset_dicts(
            self.total_tokens_spent, other_delta.total_tokens_spent
        )

        return EntityDeltaAccount(
            identifier=self.identifier,
            total_received=self.total_received.merge(other_delta.total_received),
            total_spent=self.total_spent.merge(other_delta.total_spent),
            total_tokens_received=total_tokens_received,
            total_tokens_spent=total_tokens_spent,
            first_tx_id=minusone_respecting_function(
                self.first_tx_id, other_delta.first_tx_id, min
            ),
            last_tx_id=minusone_respecting_function(
                self.last_tx_id, other_delta.last_tx_id, max
            ),
            no_incoming_txs=self.no_incoming_txs + other_delta.no_incoming_txs,
            no_outgoing_txs=self.no_outgoing_txs + other_delta.no_outgoing_txs,
            no_incoming_txs_zero_value=self.no_incoming_txs_zero_value
            + other_delta.no_incoming_txs_zero_value,
            no_outgoing_txs_zero_value=self.no_outgoing_txs_zero_value
            + other_delta.no_outgoing_txs_zero_value,
            is_contract=self.is_contract or other_delta.is_contract,
        )


@dataclass
class RawEntityTxAccount:
    identifier: str
    is_outgoing: bool
    tx_id: int
    tx_reference: UserType
    block_id: int
    value: int
    token_values: dict[str, int]


@dataclass
class RelationDeltaAccount(DeltaUpdate):
    src_identifier: bytes
    dst_identifier: bytes
    no_transactions: int
    value: DeltaValue
    token_values: dict[str, DeltaValue]
    type: str  # noqa

    @classmethod
    def from_db(Cls, db_row):
        return Cls(
            src_identifier=getattr(db_row, "src_address"),
            dst_identifier=getattr(db_row, "dst_address"),
            no_transactions=db_row.no_transactions,
            value=DeltaValue.from_db(db_row.value),
            token_values={
                k: DeltaValue.from_db(v) for k, v in db_row.token_values.items()
            },
            type="from_db",
        )

    def merge(self, other_delta):
        assert self.src_identifier == other_delta.src_identifier
        assert self.dst_identifier == other_delta.dst_identifier

        token_values = merge_asset_dicts(self.token_values, other_delta.token_values)

        return RelationDeltaAccount(
            src_identifier=self.src_identifier,
            dst_identifier=self.dst_identifier,
            value=self.value.merge(other_delta.value),
            token_values=token_values,
            no_transactions=self.no_transactions + other_delta.no_transactions,
            type="merged",
        )


@dataclass
class BalanceDelta(DeltaUpdate):
    identifier: int
    asset_balances: dict[str, DeltaScalar]

    @classmethod
    def from_db(Cls, identifier, db_row_list):
        if len(db_row_list) == 0:
            return Cls(
                identifier=identifier,
                asset_balances={},
            )

        asset_balances = {x.currency: DeltaScalar(x.balance) for x in db_row_list}
        return Cls(
            identifier=identifier,
            asset_balances=asset_balances,
        )

    def merge(self, other_delta):
        assert self.identifier == other_delta.identifier

        asset_balances = {
            k: self.asset_balances.get(k, DeltaScalar(0)).merge(
                other_delta.asset_balances.get(k, DeltaScalar(0))
            )
            for k in set(self.asset_balances.keys())
            | set(other_delta.asset_balances.keys())
        }
        return BalanceDelta(
            identifier=self.identifier,
            asset_balances=asset_balances,
        )

    def left_join(self, other_delta):
        assert self.identifier == other_delta.identifier
        asset_balances = {
            k: self.asset_balances.get(k, DeltaScalar(0)).merge(
                other_delta.asset_balances.get(k, DeltaScalar(0))
            )
            for k in self.asset_balances.keys()
        }
        return BalanceDelta(
            identifier=self.identifier,
            asset_balances=asset_balances,
        )


@dataclass
class DbDeltaAccount:
    entity_updates: List[EntityDeltaAccount]
    new_entity_txs: List[RawEntityTxAccount]
    relation_updates: List[RelationDeltaAccount]
    balance_updates: List[BalanceDelta]

    def concat(self, other):
        return DbDeltaAccount(
            entity_updates=self.entity_updates + other.entity_updates,
            new_entity_txs=self.new_entity_txs + other.new_entity_txs,
            relation_updates=self.relation_updates + other.relation_updates,
            balance_updates=self.balance_updates + other.balance_updates,
        )

    @staticmethod
    def merge(change_sets: List[DbDeltaAccount]) -> "DbDeltaAccount":
        return reduce(lambda x, y: x.concat(y), change_sets).compress()

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

        grouped = group_by(self.balance_updates, lambda x: x.identifier)
        balance_updates_merged = {
            k: reduce(lambda x, y: x.merge(y), v) for k, v in grouped.items()
        }

        return DbDeltaAccount(
            entity_updates=sorted(
                entity_updates_merged.values(),
                key=lambda x: (x.first_tx_id, x.last_tx_id),
            ),
            new_entity_txs=self.new_entity_txs,
            relation_updates=list(relations_updates_merged.values()),
            balance_updates=list(balance_updates_merged.values()),
        )
