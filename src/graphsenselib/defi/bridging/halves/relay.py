"""Relay (relay.link) half bridges (defi/bridging/delegated.py): the deposits.

A deposit is an event of Relay's Depository, counted only from that contract:

    RelayNativeDeposit(address from, uint256 amount, bytes32 id)
    RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id)

nothing indexed. Neither names the chain it is filled on, so the destination
is unknown until the external backend pairs it (through Relay's own record of
the order). A refunded deposit, and one filled on its own chain, stays this
half: the backend answers no pair for it.

Legs, as the external backend adapter reads them (services/bridges/relay.py),
so its pairs name these legs: the one Transfer of the event's token and amount
into the Depository among the logs since the deposit before it, else the tx (a
native deposit always). A Transfer leg's fromAddress is its sender, set by
half_bridges (delegated._paid_by): a router often deposits for the user. A
bare-tx leg's fromAddress is the depositor the event states (`from`), never
the tx's sender, which is a bundler when the deposit comes through an
ERC-4337 EntryPoint (11 of 80 native deposits the adapter sampled).

Not read: a fill. It logs nothing of Relay's (the order's id is the end of its
calldata), and a refund looks the same, so an eth fill shows no bridge here;
seen from its deposit on another chain the pair names it. Not read either: a
raw `ERC20.transfer` to the Depository, which logs no Depository event.

Contract and topic0s: Relay's /chains answer (protocol.v2.depository) and the
adapter's recorded receipts (tests/data/bridges/relay/), 2026-11-22.

A deposit's transfer id is its `id`, Relay's orderId (0x hex)."""

from typing import List

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _leg,
    _transfers,
    _words,
)
from graphsenselib.defi.bridging.models import Bridge

PROTOCOL = "relay"
NATIVE = "native"

_NATIVE_DEPOSIT = bytes.fromhex(
    "8032066556caf3967d8fec4ad22a2d9e1e9576556b2903a0fcd5b1fd201e3477"
)
_ERC20_DEPOSIT = bytes.fromhex(
    "49fed1d0b752ce30eee63c7a81133f3363b532fec5d4d7dd1ccfd005de4555e1"
)
# network -> its Depository; a network not listed has no Relay half bridge
_DEPOSITORIES = {"eth": bytes.fromhex("4cd00e387622c35bddb9b4c962c136462338bc31")}


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's Relay deposits on network, in log order."""
    depository = _DEPOSITORIES.get(network)
    bridges = []
    since = 0  # the position after the last deposit
    for position, log in enumerate(logs):
        topics = log.get("topics")
        if not topics or bytes(log["address"]) != depository:
            continue
        topic0 = bytes(topics[0])
        if topic0 == _NATIVE_DEPOSIT:
            depositor, amount, order = _words(log["data"], 3)
            asset, payment = NATIVE, _leg(log, [])
        elif topic0 == _ERC20_DEPOSIT:
            depositor, token, amount, order = _words(log["data"], 4)
            token = token.to_bytes(32, "big")[-20:]
            asset = _address(token)
            payment = _leg(
                log,
                _transfers(
                    logs[since:position],
                    token=token,
                    receiver=depository,
                    value=amount,
                ),
            )
        else:
            continue
        since = position + 1
        bridges.append(
            Bridge(
                # the event's depositor; half_bridges puts a Transfer leg's
                # sender in its place
                fromAddress=_address(depositor.to_bytes(32, "big")[-20:]),
                fromAsset=asset,
                fromAmount=amount,
                fromNetwork=network,
                fromPayment=payment,
                toAddress=UNKNOWN,
                toAsset=UNKNOWN,
                toAmount=0,
                toNetwork=UNKNOWN,  # a deposit does not say where it is filled
                toPayment=PENDING,
                protocol=PROTOCOL,
                transferId="0x" + order.to_bytes(32, "big").hex(),
            )
        )
    return bridges
