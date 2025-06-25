# flake8: noqa: E501
from graphsenselib.deltaupdate.update.generic import DbDelta
from graphsenselib.deltaupdate.update.utxo.update import dbdelta_from_utxo_transaction
from graphsenselib.utils import dict_to_dataobject, group_by, groupby_property
from graphsenselib.utils.utxo import get_unique_addresses_from_transaction

from .test_data import (
    get_arel,
    get_atxs,
    get_exchange_rates_per_block,
    get_flow_test_tx,
    get_txs,
    preprocess_inputs,
)

test_tx = preprocess_inputs(
    """
{"txIdGroup": 40316, "txId": 1007912450, "txHash": "b78b611912b05183f6d6a445d1b6d2df79bd1568cfb294ead29489dab77636d0", "coinbase": false, "coinjoin": false, "blockId": 844317, "inputs": [{"address": ["bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d"], "value": 331979681, "addressType": 9}, {"address": ["bc1qgdnam3uw0ehkwhp2zx2s0f6lva8xf8l2cedrddl7sltkzguallusm3ztwh"], "value": 11848561, "addressType": 9}, {"address": ["3R2YUX89nBDCz1u81McM1sGeLCVyzEbJna"], "value": 23464540, "addressType": 5}, {"address": ["3GTGXJD2MbmbJosCFBK5PzbdK6z5Cwr5TT"], "value": 33000000, "addressType": 5}], "outputs": [{"address": ["1CyDN8jDATWmJm5jDhJUUddex9D2nDRfcF"], "value": 141747, "addressType": 3}, {"address": ["bc1ph9e8t4pszlcea4r4xdugsdwy3q2jke32azfqu4zsg3zj96ke32wswf7d25"], "value": 5194073, "addressType": 11}, {"address": ["12Do7TSs2qtdriDJDUJWbNgcE9akfcDd2i"], "value": 8942202, "addressType": 3}, {"address": ["bc1qc72u3jg442kn8xxvwgk5j6wmh74hw7l5rhh0tl"], "value": 402100, "addressType": 8}, {"address": ["bc1q3s38x4atceu9rt4505pkm43lhrnfgmqnjp5ggs"], "value": 13330000, "addressType": 8}, {"address": ["1N5QJyqRrBP5gKxT8CT4H3M6x79Rci6zPd"], "value": 6191600, "addressType": 3}, {"address": ["bc1pmhj6fqg00kq5zmpmztyu2ac987mzgkam39z4vwq2suka0uh7un5que5qfq"], "value": 536377, "addressType": 11}, {"address": ["bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d"], "value": 365507683, "addressType": 9}], "timestamp": 1716222382, "totalInput": 400292782, "totalOutput": 400245782}
"""
)


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


def test_transaction_changeset_address_txs2():
    rates = {844317: [500, 498.18]}
    tx = dict_to_dataobject(test_tx[0])
    cngset = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])

    # both are incoming since 8l2d receives more than it spends in total
    # this might be counter intuitive.
    target = [
        x
        for x in cngset.new_entity_txs
        if x.identifier
        == "bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d"
        and not x.is_outgoing
    ]
    target2 = [
        x
        for x in cngset.new_entity_txs
        if x.identifier
        == "bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d"
        and not x.is_outgoing
    ]
    assert len(target) == 1
    assert len(target2) == 1


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


def test_reg_io_outlier():
    tx = get_flow_test_tx()[0]
    rates = get_exchange_rates_per_block()
    res = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])
    assert len(res.new_entity_txs) == 3


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
