"""Net-flow filter used by list_links for UTXO currencies.

A link is a transaction of the directed relation id -> neighbor, and UTXO
relations connect NET senders to NET receivers (the transform nets flows per
(tx, address) — see computeAddressTransactions + splitTransactions in
graphsense-spark). utxo_net_flows reproduces that netting from a tx's raw io
lists so list_links only returns txs the relation actually contains.

Regression context: btc tx 18fc9830… has address A and address N both as
inputs while N is also an output with a net outflow. Raw io membership called
that a link A -> N although no relation edge exists, so /links disagreed with
the neighbor list and the relations pre-check (which correctly found no edge)
returned an empty result.
"""

from types import SimpleNamespace

from graphsenselib.db.asynchronous.cassandra import utxo_net_flows


def io(value, *addresses):
    return SimpleNamespace(address=list(addresses) or None, value=value)


def test_pure_sides_net_to_their_raw_values():
    flows = utxo_net_flows([io(2300, "a")], [io(2000, "b"), io(200, "c")])
    assert flows == {"a": -2300, "b": 2000, "c": 200}


def test_both_sides_address_nets_to_one_side():
    # the 18fc9830 shape: n is input (2300) and output (1951) -> net sender
    flows = utxo_net_flows(
        [io(2300, "a"), io(2300, "n")],
        [io(3902, "f"), io(1951, "n")],
    )
    assert flows["n"] == -349
    assert flows["a"] == -2300
    assert flows["f"] == 3902


def test_balanced_address_nets_to_zero():
    flows = utxo_net_flows([io(500, "x")], [io(500, "x"), io(100, "y")])
    assert flows["x"] == 0
    assert flows["y"] == 100


def test_multisig_ios_are_ignored():
    # multi-address ios carry no single-address flow, like the transform's
    # regularized inputs/outputs
    flows = utxo_net_flows(
        [io(700, "m1", "m2"), io(300, "a")],
        [io(900, "b")],
    )
    assert flows == {"a": -300, "b": 900}


def test_coinbase_inputs_none():
    flows = utxo_net_flows(None, [io(2500000, "miner")])
    assert flows == {"miner": 2500000}


def test_repeated_ios_sum_per_address():
    flows = utxo_net_flows(
        [io(100, "a"), io(150, "a")],
        [io(60, "b"), io(40, "b")],
    )
    assert flows == {"a": -250, "b": 100}
