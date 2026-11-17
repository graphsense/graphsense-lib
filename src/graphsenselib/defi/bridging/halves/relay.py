"""Relay (relay.link) half bridges (defi/bridging/delegated.py): the deposits.

A deposit is a Depository event, counted only from Relay's Depository:

    RelayNativeDeposit(address from, uint256 amount, bytes32 id)
    RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id)

no member indexed. Neither names the chain it is filled on, so the
destination is unknown until the external backend pairs it (by Relay's own
API). A fill is not read: it emits no Relay event (its id is in calldata
only), so eth fills are not delegated. Not read either: a raw
`ERC20.transfer(Depository, ...)` deposit, which emits no Depository event.

Contract and topic0s: Relay's /chains answer (protocol.v2.depository), and
the adapter's recorded receipts (iknaio-external-backend-adapter,
tests/data/bridges/relay/), 2026-11-16.

A deposit's transfer id is its `id`, Relay's orderId (0x hex)."""

import logging
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

logger = logging.getLogger(__name__)

PROTOCOL = "relay"

DEPOSITORY = bytes.fromhex("4cd00e387622c35bddb9b4c962c136462338bc31")
NATIVE_DEPOSIT = bytes.fromhex(
    "8032066556caf3967d8fec4ad22a2d9e1e9576556b2903a0fcd5b1fd201e3477"
)
ERC20_DEPOSIT = bytes.fromhex(
    "49fed1d0b752ce30eee63c7a81133f3363b532fec5d4d7dd1ccfd005de4555e1"
)

# network -> its Depository; a network not listed has no Relay half bridge
_DEPOSITORIES = {"eth": DEPOSITORY}


def is_event(network: str, log: Log) -> bool:
    """Whether log is a Relay deposit this reader reads on network."""
    depository, topics = _DEPOSITORIES.get(network), log.get("topics")
    return (
        depository is not None
        and bool(topics)
        and bytes(log["address"]) == depository
        and bytes(topics[0]) in (NATIVE_DEPOSIT, ERC20_DEPOSIT)
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's Relay deposits on network, in log order."""
    depository = _DEPOSITORIES.get(network)
    if depository is None:
        return []
    bridges = []
    for position, log in enumerate(logs):
        topics = log.get("topics")
        if not topics or bytes(log["address"]) != depository:
            continue
        topic0 = bytes(topics[0])
        try:
            if topic0 == NATIVE_DEPOSIT:
                depositor, amount, order = _words(log["data"], 3)
                asset, payment = "native", _leg(log, [])
            elif topic0 == ERC20_DEPOSIT:
                depositor, token, amount, order = _words(log["data"], 4)
                token_bytes = token.to_bytes(32, "big")[-20:]
                asset = _address(token_bytes)
                # from anyone: a router funds a deposit, not the depositor
                funding = _transfers(
                    logs[:position],
                    token=token_bytes,
                    receiver=depository,
                    value=amount,
                )
                payment = _leg(log, funding)
            else:
                continue
        except (IndexError, ValueError) as exc:
            logger.warning(
                "Relay event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
            continue
        bridges.append(
            Bridge(
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
