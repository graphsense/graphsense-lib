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
from graphsenselib.deltaupdate.update.generic import DeltaScalar, Tx
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
