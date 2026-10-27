"""Bridges graphsense-lib recognizes but does not pair.

graphsense-lib does not decode these bridges or pair a transfer with its leg
on the other chain. It only recognizes a tx that emitted one of their events,
in the logs the conversions endpoint loads anyway, and reports each such event
as an UnresolvedBridge. An external backend that pairs these bridges may add
the pairing (web/middleware/external_backends.py, rule 6); without one the
report is not used. A tx without such an event costs nothing: the check reads
no more than those logs.

An event counts only from its protocol's own contract on that network, by
topic0 AND emitter: any contract can emit an event with the same signature.

Circle CCTP: each version's TokenMessenger emits DepositForBurn on the burning
chain and MintAndWithdraw on the minting chain.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

CCTP = "cctp"

# Circle's eth TokenMessengers
_CCTP_V1_TOKEN_MESSENGER_ETH = "0xbd3fa81b58ba92a82136038b25adec7066af3155"
_CCTP_V2_TOKEN_MESSENGER_ETH = "0x28b5a0e9c621a5badaa536219b3a228c8168cf5d"

# DepositForBurn(uint64,address,uint256,address,bytes32,uint32,bytes32,bytes32)
_CCTP_V1_DEPOSIT_FOR_BURN = (
    "0x2fa9ca894982930190727e75500a97d8dc500233a5065e0f3126c48fbe0343c0"
)
# DepositForBurn(address,uint256,address,bytes32,uint32,bytes32,bytes32,uint256,uint32,bytes)
_CCTP_V2_DEPOSIT_FOR_BURN = (
    "0x0c8c1cbdc5190613ebd485511d4e2812cfa45eecb79d845893331fedad5130a5"
)
# MintAndWithdraw(address,uint256,address)
_CCTP_V1_MINT_AND_WITHDRAW = (
    "0x1b2a7ff080b8cb6ff436ce0372e399692bbfb6d4ae5766fd8d58a7b8cc6142e6"
)
# MintAndWithdraw(address,uint256,address,uint256)
_CCTP_V2_MINT_AND_WITHDRAW = (
    "0x50c55e915134d457debfa58eb6f4342956f8b0616d51a89a3659360178e1ab63"
)


def _event(topic0: str, emitter: str) -> Tuple[bytes, bytes]:
    """An event's key, as the log table stores topic0 and emitter."""
    return bytes.fromhex(topic0[2:]), bytes.fromhex(emitter[2:])


# network -> (topic0, emitter) -> bridge. Another bridge that an external
# backend pairs is one more row here.
UNRESOLVED_BRIDGE_EVENTS: Dict[str, Dict[Tuple[bytes, bytes], str]] = {
    "eth": {
        _event(_CCTP_V1_DEPOSIT_FOR_BURN, _CCTP_V1_TOKEN_MESSENGER_ETH): CCTP,
        _event(_CCTP_V1_MINT_AND_WITHDRAW, _CCTP_V1_TOKEN_MESSENGER_ETH): CCTP,
        _event(_CCTP_V2_DEPOSIT_FOR_BURN, _CCTP_V2_TOKEN_MESSENGER_ETH): CCTP,
        _event(_CCTP_V2_MINT_AND_WITHDRAW, _CCTP_V2_TOKEN_MESSENGER_ETH): CCTP,
    },
}


@dataclass(frozen=True)
class UnresolvedBridge:
    """A bridge event graphsense-lib cannot pair with the transfer's leg on
    the other chain; an external backend may."""

    bridge: str
    network: str
    # 0x-prefixed, lower case
    tx_hash: str
    log_index: int


def unresolved_bridges(
    network: str, raw_logs: Iterable[Dict[str, Any]]
) -> Tuple[UnresolvedBridge, ...]:
    """The events among a tx's raw logs (rows of the log table) that are a
    bridge graphsense-lib cannot pair."""
    events = UNRESOLVED_BRIDGE_EVENTS.get(network)
    if not events:
        return ()
    found = []
    for log in raw_logs:
        topics = log.get("topics")
        if not topics:
            continue
        bridge = events.get((bytes(topics[0]), bytes(log["address"])))
        if bridge is not None:
            tx_hash = "0x" + bytes(log["tx_hash"]).hex()
            found.append(UnresolvedBridge(bridge, network, tx_hash, log["log_index"]))
    return tuple(found)
