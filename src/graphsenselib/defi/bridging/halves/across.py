"""Across half bridges (defi/bridging/delegated.py): V3 SpokePool deposits
and fills.

A deposit (FundsDeposited, or V3FundsDeposited before the bytes32 upgrade)
sends: its leg is the Transfer of inputAmount of inputToken into the SpokePool
among the logs since the SpokePool's previous event, or, with none, the tx
(native: the SpokePool wrapped msg.value). A fill (FilledRelay, or
FilledV3Relay) receives: the SpokePool emits it and then pays
updatedOutputAmount to updatedRecipient (a speed-up changes both), so its leg
is that Transfer among the logs up to the SpokePool's next event, or the tx
(native: the SpokePool unwrapped it). The other side holds what the event says
about it. The external backend adapter pins its legs the same way
(services/bridges/across.py), so its pairs name these legs.

Every one of the four events indexes the depositId second (topic 2): the
half's transfer id, in decimal, unique per ORIGIN chain only.

Both eras share one word layout; the old fill carries ``bytes message``, so its
relayExecutionInfo sits at the offset in word 11. The same SpokePool emits
both eras.
"""

import logging
from typing import List, Optional

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

PROTOCOL = "across"


def _hex(s: str) -> bytes:
    return bytes.fromhex(s.removeprefix("0x"))


# FundsDeposited(bytes32,bytes32,uint256,uint256,uint256,uint256,uint32,uint32,
# uint32,bytes32,bytes32,bytes32,bytes)
_FUNDS_DEPOSITED = _hex(
    "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
)
# V3FundsDeposited(address,address,uint256,uint256,uint256,uint32,uint32,uint32,
# uint32,address,address,address,bytes)
_V3_FUNDS_DEPOSITED = _hex(
    "0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f"
)
# FilledRelay(bytes32,bytes32,uint256,uint256,uint256,uint256,uint256,uint32,
# uint32,bytes32,bytes32,bytes32,bytes32,bytes32,(bytes32,bytes32,uint256,uint8))
_FILLED_RELAY = _hex(
    "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"
)
# FilledV3Relay(address,address,uint256,uint256,uint256,uint256,uint32,uint32,
# uint32,address,address,address,address,bytes,(address,bytes,uint256,uint8))
_FILLED_V3_RELAY = _hex(
    "0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7"
)
_DEPOSITS = (_FUNDS_DEPOSITED, _V3_FUNDS_DEPOSITED)
_EVENTS = (*_DEPOSITS, _FILLED_RELAY, _FILLED_V3_RELAY)

# WETH9 Deposit(address indexed dst, uint256) / Withdrawal(address indexed src, uint256)
_WRAP = _hex("0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c")
_UNWRAP = _hex("0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65")

# The SpokePool of each network (as in the adapter's table): eth, base, pol, bnb
# and opt seen on recorded Across txs, arb on its SpokePool's logs.
_SPOKE_POOLS = {
    "eth": _hex("0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5"),
    "opt": _hex("0x6f26bf09b1c792e3228e5467807a900a503c0281"),
    "bnb": _hex("0x4e8e101924ede233c13e2d8622dc8aed2872d505"),
    "pol": _hex("0x9295ee1d8c5b022be115a2ad3c30c72e34e7f096"),
    "base": _hex("0x09aea4b2242abc8bb4bb78d537a67a245a7bec64"),
    "arb": _hex("0xe35e9842fceaca96570b734083f4a58e8f7c5f2a"),
}
# Across names chains by their EVM chain id
_ACROSS_NETWORKS = {
    1: "eth",
    10: "opt",
    56: "bnb",
    137: "pol",
    8453: "base",
    42161: "arb",
}


def _evm(word: int) -> Optional[bytes]:
    """A 32-byte word as a 20-byte EVM address, or None for a non-EVM id."""
    raw = word.to_bytes(32, "big")
    return raw[12:] if not any(raw[:12]) else None


def _declared(word: int, network: Optional[str]) -> str:
    """An address the event names on the other chain, if that chain is one we
    name; the zero address (an old deposit's "any output token") is none."""
    address = _evm(word) if network else None
    return _address(address) if address and any(address) else UNKNOWN


def _deposit_id(log: Log) -> str:
    return str(int.from_bytes(bytes(log["topics"][2]), "big"))


def _is_event(log: Log, spoke: bytes) -> bool:
    topics = log.get("topics")
    return (
        bool(topics) and bytes(log["address"]) == spoke and bytes(topics[0]) in _EVENTS
    )


def is_event(network: str, log: Log) -> bool:
    """Whether log is an Across deposit or fill this reader reads on network."""
    spoke = _SPOKE_POOLS.get(network)
    return spoke is not None and _is_event(log, spoke)


def _wrapped(
    logs: List[Log], topic0: bytes, token: bytes, party: bytes, value: int
) -> bool:
    return any(
        bytes(log["address"]) == token
        and len(log.get("topics") or ()) == 2
        and bytes(log["topics"][0]) == topic0
        and bytes(log["topics"][1])[-20:] == party
        and int.from_bytes(bytes(log["data"]), "big") == value
        for log in logs
    )


def _deposit(network: str, log: Log, before: List[Log], spoke: bytes) -> Bridge:
    destination = _ACROSS_NETWORKS.get(int.from_bytes(bytes(log["topics"][1]), "big"))
    depositor = int.from_bytes(bytes(log["topics"][3]), "big")
    (in_token, out_token, in_amount, out_amount, _, _, _, recipient) = _words(
        log["data"], 8
    )
    token = in_token.to_bytes(32, "big")[12:]
    funding = _transfers(before, token=token, receiver=spoke, value=in_amount)
    native = not funding and _wrapped(before, _WRAP, token, spoke, in_amount)
    # whoever paid into the SpokePool; the depositor for a bare-tx leg
    payer = bytes(funding[0]["topics"][1])[-20:] if len(funding) == 1 else None
    payer = payer or _evm(depositor)
    return Bridge(
        fromAddress=_address(payer) if payer else UNKNOWN,
        fromAsset="native" if native else _address(token),
        fromAmount=in_amount,
        fromNetwork=network,
        fromPayment=_leg(log, funding),
        toAddress=_declared(recipient, destination),
        toAsset=_declared(out_token, destination),
        toAmount=out_amount,
        toNetwork=destination or UNKNOWN,
        toPayment=PENDING,
        protocol=PROTOCOL,
        transferId=_deposit_id(log),
    )


def _fill(network: str, log: Log, after: List[Log], spoke: bytes) -> Bridge:
    origin = _ACROSS_NETWORKS.get(int.from_bytes(bytes(log["topics"][1]), "big"))
    words = _words(log["data"], 15)
    in_token, out_token, in_amount, depositor = words[0], words[1], words[2], words[8]
    if bytes(log["topics"][0]) == _FILLED_V3_RELAY:
        # relayExecutionInfo is dynamic: word 11 is its offset
        info = words[11] // 32
        words = _words(log["data"], info + 3)
    else:
        info = 11
    recipient, amount = words[info], words[info + 2]
    token = out_token.to_bytes(32, "big")[12:]
    payee = _evm(recipient)
    payout = (
        _transfers(after, token=token, receiver=payee, value=amount) if payee else []
    )
    native = not payout and _wrapped(after, _UNWRAP, token, spoke, amount)
    return Bridge(
        fromAddress=_declared(depositor, origin),
        fromAsset=_declared(in_token, origin),
        fromAmount=in_amount,
        fromNetwork=origin or UNKNOWN,
        fromPayment=UNKNOWN,
        toAddress=_address(payee) if payee else UNKNOWN,
        toAsset="native" if native else _address(token),
        toAmount=amount,
        toNetwork=network,
        toPayment=_leg(log, payout),
        protocol=PROTOCOL,
        transferId=_deposit_id(log),
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's Across half bridges on network, in log order. An event counts
    only from that network's SpokePool."""
    spoke = _SPOKE_POOLS.get(network)
    if spoke is None:
        return []
    positions = [i for i, log in enumerate(logs) if _is_event(log, spoke)]
    bridges = []
    for n, position in enumerate(positions):
        log = logs[position]
        since = positions[n - 1] + 1 if n else 0
        until = positions[n + 1] if n + 1 < len(positions) else len(logs)
        try:
            if bytes(log["topics"][0]) in _DEPOSITS:
                bridges.append(_deposit(network, log, logs[since:position], spoke))
            else:
                bridges.append(_fill(network, log, logs[position + 1 : until], spoke))
        except (IndexError, ValueError) as exc:
            logger.warning(
                "Across event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
    return bridges
