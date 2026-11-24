"""Wormhole Portal Token Bridge half bridges (defi/bridging/delegated.py).

    LogMessagePublished(address indexed sender, uint64 sequence, uint32 nonce,
                        bytes payload, uint8 consistencyLevel)        on Core
    TransferRedeemed(uint16 indexed emitterChainId, bytes32 indexed emitterAddress,
                     uint64 indexed sequence)                    on Token Bridge

Core is a generic messaging layer and any contract can emit either signature. A
send counts only as the network's Core logging a message whose sender is the
network's Token Bridge, with a payload of type 1 (transfer) or 3 (transfer with
payload): type(1) amount(32) tokenAddress(32) tokenChain(2) to(32) toChain(2).
A redeem counts only as the network's Token Bridge logging TransferRedeemed.

Legs, pinned as the external backend adapter pins them (services/bridges/
wormhole.py), so its pairs name these legs. The payload amount has 8 decimals
and the bridge moves only the de-dusted amount: what enters it is amount *
10**k. A send of the network's own token names it in the payload; a wrapped
token is named by the bridge's burn of what it was sent. The leg is the one
Transfer of that token and value into the bridge. wrapAndTransferETH has none:
the bridge wraps msg.value itself (a Deposit to the bridge by the payload's
token, or the token minting to it), the leg is the tx. A send's fromAddress is
set by half_bridges (delegated._paid_by): whoever paid its leg.

A redeem pays right after it logs TransferRedeemed: the first Transfer from 0x0
(a wrapped token minted) or from the bridge (a locked token released) is the
leg, whatever its recipient does next. completeTransferAndUnwrapETH logs a
Withdrawal by the bridge (or the wrapped token's burn from it) and sends the
coin in an internal call: the leg is the tx, and no log names the recipient. A
redeem names no sender.

A half's transfer id is the VAA id Wormholescan finds the transfer by,
`<emitterChain>/<emitterAddress, 64 hex>/<sequence>`: a send's emitter is its
own network's Token Bridge, a redeem's the three indexed topics. Chain ids are
Wormhole's own (sdk/vaa/structs.go), not EVM chain ids; a chain not named here
(Solana is 1) is "unknown"."""

from typing import Dict, List, Optional, Tuple

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _leg,
    _transfers,
)
from graphsenselib.defi.bridging.models import Bridge

PROTOCOL = "wormhole"
NATIVE = "native"

_LOG_MESSAGE_PUBLISHED = bytes.fromhex(
    "6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2"
)
_TRANSFER_REDEEMED = bytes.fromhex(
    "caf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169"
)
# WETH's Deposit(address indexed dst, uint wad) / Withdrawal(address indexed src, uint wad)
_DEPOSIT = bytes.fromhex(
    "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
)
_WITHDRAWAL = bytes.fromhex(
    "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
)
_ZERO_ADDRESS = bytes(20)
_TRANSFER_TYPES = (1, 3)

# network -> (Wormhole chain id, Core, Token Bridge), as the adapter's
# DEPLOYMENTS; both contracts are on the recorded eth receipts
_CONTRACTS: Dict[str, Tuple[int, bytes, bytes]] = {
    "eth": (
        2,
        bytes.fromhex("98f3c9e6e3face36baad05fe09d375ef1464288b"),
        bytes.fromhex("3ee18b2214aff97000d974cf647e7c347e8fa585"),
    ),
}
# the networks the conversions name, by Wormhole chain id
_CHAIN_NETWORKS = {
    2: "eth",
    4: "bnb",
    5: "pol",
    6: "avax",
    23: "arb",
    24: "opt",
    30: "base",
}


def _topics(log: Log) -> List[bytes]:
    return [bytes(topic) for topic in log.get("topics") or ()]


def _value(log: Log) -> int:
    return int.from_bytes(bytes(log["data"]), "big")


def _scaled(value: int, amount: int) -> bool:
    """Whether value is the 8-decimal amount in a token of 8 or more decimals."""
    factor, rest = divmod(value, amount)
    return rest == 0 and factor == 10 ** (len(str(factor)) - 1)


def _vaa_id(emitter_chain: int, emitter: bytes, sequence: int) -> str:
    return f"{emitter_chain}/{emitter.hex()}/{sequence}"


def _send(
    network: str, log: Log, before: List[Log], chain_id: int, bridge: bytes
) -> Optional[Bridge]:
    """The half of a Token Bridge message; before: the logs since the last one."""
    data = bytes(log["data"])
    at = int.from_bytes(data[64:96], "big")
    payload = data[at + 32 :]  # the fields read here end at byte 101
    if payload[0] not in _TRANSFER_TYPES:
        return None  # a token attestation
    amount = int.from_bytes(payload[1:33], "big")
    token, to = payload[45:65], payload[79:99]
    home = int.from_bytes(payload[65:67], "big") == chain_id
    into = [
        transfer
        for transfer in _transfers(before, receiver=bridge)
        if _scaled(_value(transfer), amount)
    ]
    if home:
        fitting = [t for t in into if bytes(t["address"]) == token]
    else:
        burnt = {
            (bytes(burn["address"]), _value(burn))
            for burn in _transfers(before, sender=bridge, receiver=_ZERO_ADDRESS)
        }
        fitting = [t for t in into if (bytes(t["address"]), _value(t)) in burnt]
    wrapped = [
        _value(deposit)
        for deposit in before
        if _topics(deposit)[:1] == [_DEPOSIT]
        and bytes(deposit["address"]) == token
        and _topics(deposit)[1][-20:] == bridge
    ]
    asset, paid, payment = UNKNOWN, 0, UNKNOWN
    minted = [t for t in fitting if _topics(t)[1][-20:] == _ZERO_ADDRESS]
    if len(fitting) == 1 and not minted:
        asset, paid = _address(bytes(fitting[0]["address"])), _value(fitting[0])
        payment = _leg(log, fitting)
    elif home and len(fitting) + len(wrapped) == 1:
        # the bridge wrapped msg.value: a Deposit, or the token minting to it
        asset, paid = NATIVE, wrapped[0] if wrapped else _value(fitting[0])
        payment = _leg(log, [])
    destination = _CHAIN_NETWORKS.get(int.from_bytes(payload[99:101], "big"))
    return Bridge(
        fromAddress=UNKNOWN,  # set by half_bridges: whoever paid the leg
        fromAsset=asset,
        fromAmount=paid,
        fromNetwork=network,
        fromPayment=payment,
        toAddress=_address(to) if destination else UNKNOWN,
        # the destination's own token, in its decimals: known when redeemed
        toAsset=UNKNOWN,
        toAmount=0,
        toNetwork=destination or UNKNOWN,
        toPayment=PENDING,
        protocol=PROTOCOL,
        transferId=_vaa_id(
            chain_id, bytes(12) + bridge, int.from_bytes(data[:32], "big")
        ),
    )


def _redeem(network: str, log: Log, after: List[Log], bridge: bytes) -> Bridge:
    """The half of a redeem; after: the tx's logs that follow it."""
    chain, emitter, sequence = _topics(log)[1:]
    emitter_chain = int.from_bytes(chain, "big")
    recipient, asset, amount, payment = UNKNOWN, UNKNOWN, 0, _leg(log, [])
    for paid in after:  # the bridge's first payment
        topics = _topics(paid)
        if topics[:1] == [_WITHDRAWAL] and topics[1][-20:] == bridge:
            asset, amount = NATIVE, _value(paid)
            break
        if not _transfers([paid]) or topics[1][-20:] not in (_ZERO_ADDRESS, bridge):
            continue
        if topics[2][-20:] == _ZERO_ADDRESS:  # the wrapped coin's burn: an unwrap
            asset, amount = NATIVE, _value(paid)
        else:
            recipient, asset = (
                _address(topics[2][-20:]),
                _address(bytes(paid["address"])),
            )
            amount, payment = _value(paid), _leg(log, [paid])
        break
    return Bridge(
        fromAddress=UNKNOWN,
        fromAsset=UNKNOWN,
        fromAmount=0,
        fromNetwork=_CHAIN_NETWORKS.get(emitter_chain, UNKNOWN),
        fromPayment=UNKNOWN,
        toAddress=recipient,
        toAsset=asset,
        toAmount=amount,
        toNetwork=network,
        toPayment=payment,
        protocol=PROTOCOL,
        transferId=_vaa_id(emitter_chain, emitter, int.from_bytes(sequence, "big")),
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's Wormhole half bridges on network, in log order."""
    if network not in _CONTRACTS:
        return []
    chain_id, core, bridge = _CONTRACTS[network]
    halves = []
    since = 0  # the position after the last Token Bridge message
    for position, log in enumerate(logs):
        topics, emitter = _topics(log), bytes(log["address"])
        if (
            topics[:1] == [_LOG_MESSAGE_PUBLISHED]
            and emitter == core
            and topics[1] == bytes(12) + bridge
        ):
            half = _send(network, log, logs[since:position], chain_id, bridge)
            since = position + 1
            if half is not None:
                halves.append(half)
        elif topics[:1] == [_TRANSFER_REDEEMED] and emitter == bridge:
            halves.append(_redeem(network, log, logs[position + 1 :], bridge))
    return halves
