# flake8: noqa

import logging
import unittest
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Tuple
from unittest.mock import MagicMock, patch

import graphsenselib
from graphsenselib.db import DbChange
from graphsenselib.deltaupdate.update.abstractupdater import TABLE_NAME_DELTA_HISTORY
from graphsenselib.deltaupdate.update.account.createchanges import (
    INT32_MAX,
    prepare_balances_for_ingest,
    prepare_entities_for_ingest,
    prepare_entity_txs_for_ingest,
    prepare_relations_for_ingest,
    prepare_txs_for_ingest,
)
from graphsenselib.deltaupdate.update.account.modelsdelta import (
    BalanceDelta,
    EntityDeltaAccount,
    RawEntityTxAccount,
    RelationDeltaAccount,
)
from graphsenselib.deltaupdate.update.account.update import resolve_tx_count_cap
from graphsenselib.deltaupdate.update.generic import DeltaScalar, DeltaValue, Tx
from graphsenselib.utils import DataObject as MutableNamedTuple
from graphsenselib.utils.account import (
    get_id_group,
    get_id_group_with_secondary_addresstransactions,
    get_id_group_with_secondary_relations,
)
from graphsenselib.utils.logging import LoggerScope


class TestPrepareTxsForIngest(unittest.TestCase):
    def setUp(self):
        self.mock_delta = [
            Tx(tx_id=1, tx_hash=b"0x1234567", block_id=123, failed=False, tx_index=0),
            Tx(
                tx_id=2, tx_hash=b"0x1222234", block_id=234, failed=True, tx_index=1
            ),  # This should be skipped in block_transactions
            # Add more mock Tx objects as needed
        ]
        self.id_bucket_size = 10
        self.block_bucket_size = 5
        self.mock_get_transaction_prefix = MagicMock(return_value=("prefix", "1"))

    def test_prepare_txs_for_ingest(self):
        # Mock the external function behaviors
        self.mock_get_transaction_prefix.return_value = ("prefix", "1")

        changes = prepare_txs_for_ingest(
            self.mock_delta,
            self.id_bucket_size,
            self.block_bucket_size,
            self.mock_get_transaction_prefix,
        )
        # print(changes)

        n_changes_transaction_ids_by_transaction_id_group = len(
            [x for x in changes if x.table == "transaction_ids_by_transaction_id_group"]
        )
        n_changes_block_transactions = len(
            [x for x in changes if x.table == "block_transactions"]
        )

        self.assertEqual(n_changes_transaction_ids_by_transaction_id_group, 2)
        self.assertEqual(n_changes_block_transactions, 1)


class TestPrepareBalancesForIngest(unittest.TestCase):
    def setUp(self):
        # Mock BalanceDelta objects and addr_balances
        self.mock_delta = [
            BalanceDelta(identifier=123, asset_balances={"USDT": DeltaScalar(1)}),
            BalanceDelta(
                identifier=234,
                asset_balances={"USDT": DeltaScalar(2), "ETH": DeltaScalar(1)},
            ),
            # BalanceDelta(identifier=123, asset_balances={'USDT': DeltaScalar(2)}), cant be twice because it is compressed before
            BalanceDelta(identifier=456, asset_balances={"ETH": DeltaScalar(2)}),
        ]
        self.addr_balances = {
            234: BalanceDelta(identifier=234, asset_balances={"USDT": DeltaScalar(2)}),
            123: BalanceDelta(
                identifier=123,
                asset_balances={"USDT": DeltaScalar(2), "ETH": DeltaScalar(1)},
            ),
        }

        self.addr_balances_ref = {
            234: {"USDT": 4, "ETH": 1},
            123: {"USDT": 3},
            456: {"ETH": 2},
        }

        self.id_bucket_size = 100

    def test_prepare_balances_for_ingest(self):
        changes = prepare_balances_for_ingest(
            self.mock_delta, self.id_bucket_size, self.addr_balances
        )

        # print(changes)
        # Check the total number of DbChange instances created
        self.assertEqual(
            len(changes), 4, "Incorrect number of DbChange instances created"
        )

        # transform DbChange to dict and compare with expected
        change_dict = defaultdict(dict)

        for change in changes:
            data = change.data
            change_dict[data["address_id"]][data["currency"]] = data["balance"]

        for k, v in self.addr_balances_ref.items():
            for currency, balance in v.items():
                self.assertEqual(change_dict[k][currency], balance)


class TestPrepareEntityTxsForIngest(unittest.TestCase):
    def setUp(self):
        # Setup your mock data here
        self.mock_delta = [
            RawEntityTxAccount(
                identifier=222,
                tx_id=123,
                block_id=100,
                is_outgoing=True,
                tx_reference="ref1",  # should be UserType but its fine
                token_values={"tokenA": 100, "tokenB": 200},
                value=0,  #  A RawEntityTxAccount can only have either value or token_values the way the functions are written. Could write a unit test for that too
            ),
            RawEntityTxAccount(
                identifier=111,
                tx_id=234,
                block_id=200,
                is_outgoing=False,
                tx_reference="ref2",
                token_values={},  # This should test the non-token transfer scenario
                value=1,
            ),
        ]
        self.id_bucket_size = 10
        self.currency = "currency1"
        self.block_bucket_size_address_txs = 5

        self.expected_changes_count = 3

    def test_prepare_entity_txs_for_ingest(self):
        changes = prepare_entity_txs_for_ingest(
            self.mock_delta,
            self.id_bucket_size,
            self.currency,
            self.block_bucket_size_address_txs,
        )

        # print(changes)

        self.assertEqual(len(changes), self.expected_changes_count)


class TestPrepareRelationsForIngest(unittest.TestCase):
    """inrelations carries plain rows (or None), not driver result wrappers,
    so relation rows can come from worker processes."""

    def setUp(self):
        from graphsenselib.deltaupdate.update.generic import DeltaValue

        self.delta = [
            RelationDeltaAccount(
                src_identifier=b"src",
                dst_identifier=b"dst",
                no_transactions=2,
                value=DeltaValue(10, [1, 2]),
                token_values={"USDT": DeltaValue(5, [1, 1])},
                type="test",
            )
        ]
        self.hash_to_id = {b"src": 1, b"dst": 2}
        self.id_bucket_size = 10
        self.relations_nbuckets = 10

    def test_merges_existing_relation_given_plain_row(self):
        from graphsenselib.db.parallel import PlainRow
        from graphsenselib.deltaupdate.update.generic import DeltaValue

        inrelations = {
            (b"src", b"dst"): PlainRow(
                {
                    "no_transactions": 3,
                    "value": PlainRow({"value": 100, "fiat_values": [10, 20]}),
                    "token_values": None,
                }
            )
        }
        changes, new_in, new_out = prepare_relations_for_ingest(
            self.delta,
            self.hash_to_id,
            inrelations,
            set(),
            self.id_bucket_size,
            self.relations_nbuckets,
        )
        in_chg = [c for c in changes if c.table == "address_incoming_relations"][0]
        self.assertEqual(in_chg.data["no_transactions"], 5)
        self.assertEqual(in_chg.data["value"], DeltaValue(110, [11, 22]))

    def test_inserts_new_relation_given_none_row(self):
        from graphsenselib.deltaupdate.update.generic import DeltaValue

        inrelations = {(b"src", b"dst"): None}
        changes, new_in, new_out = prepare_relations_for_ingest(
            self.delta,
            self.hash_to_id,
            inrelations,
            set(),
            self.id_bucket_size,
            self.relations_nbuckets,
        )
        in_chg = [c for c in changes if c.table == "address_incoming_relations"][0]
        self.assertEqual(in_chg.data["no_transactions"], 2)
        self.assertEqual(in_chg.data["value"], DeltaValue(10, [1, 2]))
        self.assertEqual(new_in[b"dst"], 1)
        self.assertEqual(new_out[b"src"], 1)


# TRON USDT's incoming tx count, the value that motivated the bigint widening.
TRON_USDT_INCOMING = 3703869446


class _FakeColumns:
    def __init__(self, rows):
        self._current_rows = rows


class _FakeTransformedDb:
    def __init__(self, rows):
        self._rows = rows

    def get_columns_for_table(self, table):
        assert table == "address"
        return _FakeColumns(self._rows)


class _RaisingTransformedDb:
    def get_columns_for_table(self, table):
        raise RuntimeError("no session")


def _column(name, type_):
    return MutableNamedTuple(column_name=name, type=type_)


class TestResolveTxCountCap(unittest.TestCase):
    def test_bigint_column_lifts_the_cap(self):
        db = _FakeTransformedDb([_column("no_incoming_txs", "bigint")])
        self.assertIsNone(resolve_tx_count_cap(db))

    def test_int_column_keeps_the_cap(self):
        db = _FakeTransformedDb([_column("no_incoming_txs", "int")])
        self.assertEqual(resolve_tx_count_cap(db), INT32_MAX)

    def test_missing_column_falls_back_to_the_cap(self):
        db = _FakeTransformedDb([_column("address_id", "int")])
        self.assertEqual(resolve_tx_count_cap(db), INT32_MAX)

    def test_probe_failure_falls_back_to_the_cap(self):
        self.assertEqual(resolve_tx_count_cap(_RaisingTransformedDb()), INT32_MAX)


class TestEntityTxCountCapping(unittest.TestCase):
    """A capped write into a bigint keyspace returns less than it read."""

    def _delta(self, no_incoming_txs):
        return EntityDeltaAccount(
            identifier=b"addr",
            total_received=DeltaValue(10, [1, 2]),
            total_spent=DeltaValue(5, [1, 2]),
            total_tokens_received={},
            total_tokens_spent={},
            first_tx_id=1,
            last_tx_id=2,
            no_incoming_txs=no_incoming_txs,
            no_outgoing_txs=0,
            no_incoming_txs_zero_value=0,
            no_outgoing_txs_zero_value=0,
            is_contract=False,
        )

    def _stored_row(self, no_incoming_txs):
        return MutableNamedTuple(
            address=b"addr",
            address_id=1,
            total_received=MutableNamedTuple(value=10, fiat_values=[1, 2]),
            total_spent=MutableNamedTuple(value=5, fiat_values=[1, 2]),
            total_tokens_received=None,
            total_tokens_spent=None,
            first_tx_id=1,
            last_tx_id=2,
            no_incoming_txs=no_incoming_txs,
            no_outgoing_txs=0,
            no_incoming_txs_zero_value=0,
            no_outgoing_txs_zero_value=0,
            is_contract=False,
            in_degree=0,
            out_degree=0,
            in_degree_zero_value=0,
            out_degree_zero_value=0,
        )

    def _address_change(self, delta, stored_row, **kwargs):
        changes, _ = prepare_entities_for_ingest(
            [delta],
            {b"addr": 1},
            {b"addr": stored_row},
            defaultdict(int),
            defaultdict(int),
            10,
            lambda a: ("ADDR", "AD"),
            **kwargs,
        )
        return [c for c in changes if c.table == "address"][0]

    def test_update_of_a_bigint_keyspace_keeps_the_true_total(self):
        stored = self._stored_row(TRON_USDT_INCOMING)
        chng = self._address_change(self._delta(1), stored, count_cap=None)
        self.assertEqual(chng.data["no_incoming_txs"], TRON_USDT_INCOMING + 1)

    def test_update_of_an_int_keyspace_still_saturates(self):
        stored = self._stored_row(TRON_USDT_INCOMING)
        chng = self._address_change(self._delta(1), stored)
        self.assertEqual(chng.data["no_incoming_txs"], INT32_MAX)
        # The regression the cap causes on a widened keyspace, and the reason
        # it must not be the default there: it writes back less than it read.
        self.assertLess(chng.data["no_incoming_txs"], stored.no_incoming_txs)

    def test_new_address_respects_the_cap_setting(self):
        capped = self._address_change(self._delta(TRON_USDT_INCOMING), None)
        self.assertEqual(capped.data["no_incoming_txs"], INT32_MAX)

        uncapped = self._address_change(
            self._delta(TRON_USDT_INCOMING), None, count_cap=None
        )
        self.assertEqual(uncapped.data["no_incoming_txs"], TRON_USDT_INCOMING)
