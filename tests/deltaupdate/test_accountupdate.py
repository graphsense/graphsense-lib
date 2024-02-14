from graphsenselib.deltaupdate.update.generic import DbDelta
from graphsenselib.deltaupdate.update.utxo import dbdelta_from_utxo_transaction
from graphsenselib.utils import group_by, groupby_property
from graphsenselib.utils.utxo import get_unique_addresses_from_transaction

from .test_data import get_arel, get_atxs, get_exchange_rates_per_block, get_txs


def test_transaction_changeset_address_txs():
    rates = get_exchange_rates_per_block()
    expected_atxs = get_atxs()

    expected = groupby_property(expected_atxs, "tx_id", sort_by="identifier")
    atxs = []
    for tx in get_txs():
        cngset = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])
        atxs.extend(list(cngset.new_entity_txs))
    res = groupby_property(atxs, "tx_id", sort_by="identifier")

    assert res == expected


def test_merge_address_deltas():
    rates = get_exchange_rates_per_block()
    changes = DbDelta.merge(
        [dbdelta_from_utxo_transaction(tx, rates[tx.block_id]) for tx in get_txs()]
    )

    expected = get_arel()

    expected_grouped = group_by(expected, key=lambda x: (x.src_address, x.dst_address))

    changes_grouped = group_by(
        changes.relation_updates, key=lambda x: (x.src_identifier, x.dst_identifier)
    )

    assert len(expected) == len(changes.relation_updates)

    for k, v in changes_grouped.items():
        ev = expected_grouped[k]
        assert len(v) == 1
        assert len(ev) == 1
        v = v[0]
        ev = ev[0]
        assert ev.estimated_value == v.estimated_value


def test_address_updates_order():
    rates = get_exchange_rates_per_block()
    changes = DbDelta.merge(
        [dbdelta_from_utxo_transaction(tx, rates[tx.block_id]) for tx in get_txs()]
    )
    last_tx_id = (-1, -1)
    for cng in changes.entity_updates:
        assert last_tx_id <= (cng.first_tx_id, cng.last_tx_id)
        last_tx_id = (cng.first_tx_id, cng.last_tx_id)


def test_address_addresses_unique():
    rates = get_exchange_rates_per_block()
    changes = DbDelta.merge(
        [dbdelta_from_utxo_transaction(tx, rates[tx.block_id]) for tx in get_txs()]
    )
    addr = [chng.identifier for chng in changes.entity_updates]
    assert len(addr) == len(set(addr))


def test_unique_addresses_in_tx():
    assert get_unique_addresses_from_transaction(get_txs()[5]) == {
        "3Fkx2TFdcHoab4xGgSjhAVh5YBPvbBWjNL",
        "1FAkhqm95YnV5Mi7Q5j2Wb8CkbK7Z9zpyB",
    }
