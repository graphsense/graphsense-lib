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


# Netting semantics: flows are netted per (tx, address) before deriving stats
# and relations, mirroring graphsense-spark (computeAddressTransactions +
# splitTransactions). An address on both sides of a tx counts only on the side
# of its net flow and can only be a relation endpoint on that side.

# btc 18fc98305665ece27f44912dca54a75a91c270e838b5972e80646c27eccc2224
# (block 926059) reduced to its shape: `both` is an input AND an output with a
# net outflow of 349, so it is a net sender despite appearing in the outputs.
both_sides_tx = preprocess_inputs(
    """
{"txId": 7, "txHash": "18fc9830", "coinbase": false, "coinjoin": false, "blockId": 926059, "inputs": [{"address": ["other_input"], "value": 2300}, {"address": ["both"], "value": 2300}], "outputs": [{"address": ["pure_output"], "value": 3902}, {"address": ["both"], "value": 1951}], "timestamp": 1764626417, "totalInput": 4600, "totalOutput": 5853}
"""
)


def test_net_sender_is_no_relation_dst():
    rates = {926059: [500, 498.18]}
    tx = dict_to_dataobject(both_sides_tx[0])
    cngset = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])

    pairs = {(r.src_identifier, r.dst_identifier) for r in cngset.relation_updates}
    assert pairs == {
        ("other_input", "pure_output"),
        ("both", "pure_output"),
    }


def test_net_sender_stats_count_net_flow_once():
    rates = {926059: [500, 498.18]}
    tx = dict_to_dataobject(both_sides_tx[0])
    cngset = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])

    deltas = [e for e in cngset.entity_updates if e.identifier == "both"]
    merged = deltas[0]
    for d in deltas[1:]:
        merged = merged.merge(d)
    assert merged.no_outgoing_txs == 1
    assert merged.no_incoming_txs == 0
    assert merged.total_spent.value == 349
    assert merged.total_received.value == 0


def test_net_receiver_is_no_relation_src():
    # test_tx: 8l2d spends 331979681 but receives 365507683 in the same tx,
    # so it is a net receiver and must not be the src of any relation.
    rates = {844317: [500, 498.18]}
    tx = dict_to_dataobject(test_tx[0])
    cngset = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])

    both = "bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d"
    srcs = {r.src_identifier for r in cngset.relation_updates}
    dsts = {r.dst_identifier for r in cngset.relation_updates}
    assert both not in srcs
    assert both in dsts
    # 3 net senders x 8 net receivers
    assert len(cngset.relation_updates) == 24

    deltas = [e for e in cngset.entity_updates if e.identifier == both]
    merged = deltas[0]
    for d in deltas[1:]:
        merged = merged.merge(d)
    assert merged.no_incoming_txs == 1
    assert merged.no_outgoing_txs == 0
    assert merged.total_received.value == 365507683 - 331979681
    assert merged.total_spent.value == 0


zero_flow_tx = preprocess_inputs(
    """
{"txId": 8, "txHash": "aa00", "coinbase": false, "coinjoin": false, "blockId": 926059, "inputs": [{"address": ["payer"], "value": 1000}, {"address": ["balanced"], "value": 500}], "outputs": [{"address": ["balanced"], "value": 500}, {"address": ["receiver"], "value": 900}], "timestamp": 1764626417, "totalInput": 1500, "totalOutput": 1400}
"""
)


def test_zero_net_flow_counts_on_neither_side():
    rates = {926059: [500, 498.18]}
    tx = dict_to_dataobject(zero_flow_tx[0])
    cngset = dbdelta_from_utxo_transaction(tx, rates[tx.block_id])

    pairs = {(r.src_identifier, r.dst_identifier) for r in cngset.relation_updates}
    assert pairs == {("payer", "receiver")}

    deltas = [e for e in cngset.entity_updates if e.identifier == "balanced"]
    merged = deltas[0]
    for d in deltas[1:]:
        merged = merged.merge(d)
    assert merged.no_incoming_txs == 0
    assert merged.no_outgoing_txs == 0
    assert merged.total_received.value == 0
    assert merged.total_spent.value == 0
    # the address row still gets created and first/last tx tracked
    assert merged.first_tx_id == tx.tx_id
    assert merged.last_tx_id == tx.tx_id

    rows = [x for x in cngset.new_entity_txs if x.identifier == "balanced"]
    assert len(rows) == 1
    assert rows[0].value == 0
    assert not rows[0].is_outgoing
