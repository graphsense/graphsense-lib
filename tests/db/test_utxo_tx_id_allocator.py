from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from graphsenselib.db.utxo import RawDbUtxo
from graphsenselib.errors.errors import DBInconsistencyException


def _make_db(block_transactions_row, highest_block):
    """Build a RawDbUtxo stub whose select_one_safe / get_highest_block
    return the values supplied here. The block_transactions row, when
    present, must expose .txs as an iterable of objects with .tx_id."""
    db = RawDbUtxo.__new__(RawDbUtxo)
    db.select_one_safe = MagicMock(return_value=block_transactions_row)
    db.get_highest_block = MagicMock(return_value=highest_block)
    db.get_block_bucket_size = MagicMock(return_value=100)
    db.get_id_group = lambda i, bs: i // bs
    db.get_keyspace = MagicMock(return_value="zec_raw_test")
    return db


def test_genesis_returns_minus_one_without_db_lookup():
    db = _make_db(block_transactions_row=None, highest_block=12345)
    assert db.get_latest_tx_id_before_block(0) == -1
    db.select_one_safe.assert_not_called()
    db.get_highest_block.assert_not_called()


def test_chain_order_returns_max_tx_id_from_preceding_block():
    row = SimpleNamespace(txs=[SimpleNamespace(tx_id=42), SimpleNamespace(tx_id=99)])
    db = _make_db(block_transactions_row=row, highest_block=10)
    assert db.get_latest_tx_id_before_block(11) == 99


def test_fresh_keyspace_returns_minus_one():
    db = _make_db(block_transactions_row=None, highest_block=None)
    assert db.get_latest_tx_id_before_block(5) == -1


def test_partial_keyspace_raises_instead_of_silently_restarting_at_zero():
    db = _make_db(block_transactions_row=None, highest_block=3_331_605)
    with pytest.raises(DBInconsistencyException) as exc_info:
        db.get_latest_tx_id_before_block(3_325_000)
    msg = str(exc_info.value)
    assert "block_transactions[3324999]" in msg
    assert "zec_raw_test" in msg
    assert "3,331,605" in msg
    assert "Refusing to start tx_id allocation at 0" in msg
