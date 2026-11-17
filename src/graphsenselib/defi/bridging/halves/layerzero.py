"""LayerZero V2 OFT half bridges, Stargate V2's pools included
(defi/bridging/delegated.py).

    OFTSent(bytes32 indexed guid, uint32 dstEid, address indexed fromAddress,
            uint256 amountSentLD, uint256 amountReceivedLD)
    OFTReceived(bytes32 indexed guid, uint32 srcEid, address indexed toAddress,
            uint256 amountReceivedLD)

Any contract can emit an OFT event. One counts only when the network's
EndpointV2 emitted, in the same tx, the packet carrying its guid: a PacketSent
whose payload header names it (send), or a PacketDelivered whose guid,
recomputed as the endpoint does, is the OFTReceived's (receive; PacketDelivered
has no guid field). The packet's OApp must be the event's emitter, or the
network's Stargate TokenMessaging when the emitter is a Stargate pool.

A Stargate bus ride is an OFTSent with guid 0 and no packet of its own (the bus
leaves later); it counts only from a Stargate pool.

A send's leg is the Transfer that funded it (into the emitter, or burnt from the
sender), a receive's the Transfer that paid the recipient; a Stargate native
pool moves the chain's coin, so its leg is the tx. The other side holds the
network the event names; the rest is pending (send) or unknown (receive).

Contracts as the external backend adapter's services/bridges/layerzero.py, which
pairs the halves: the eth endpoint from recorded receipts, the Stargate pools
and TokenMessaging from Stargate's mainnet contract list."""

import logging
from typing import Dict, FrozenSet, List, Optional, Tuple

from eth_hash.auto import keccak

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


def _hex(s: str) -> bytes:
    return bytes.fromhex(s.removeprefix("0x"))


# OFTSent(bytes32,uint32,address,uint256,uint256)
_OFT_SENT = _hex("0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a")
# OFTReceived(bytes32,uint32,address,uint256)
_OFT_RECEIVED = _hex(
    "0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c"
)
# PacketSent(bytes,bytes,address)
_PACKET_SENT = _hex(
    "0x1ab700d4ced0c005b164c0f789fd09fcbb0156d4c2041b8a3bfbcd961cd1567f"
)
# PacketDelivered((uint32,bytes32,uint64),address)
_PACKET_DELIVERED = _hex(
    "0x3cd5e48f9730b129dc7550f0fcea9c767b7be37837cd10e55eb35f734f4bca04"
)

_ZERO_GUID = bytes(32)
_ZERO_ADDRESS = bytes(20)
# PacketSent payload header: version(1) nonce(8) srcEid(4) sender(32) dstEid(4)
# receiver(32) guid(32)
_HEADER_BYTES = 113

# LayerZero endpoint ids of the networks the conversions name
_LZ_NETWORKS = {
    30101: "eth",
    30102: "bnb",
    30106: "avax",
    30109: "pol",
    30110: "arb",
    30111: "opt",
    30145: "gnosis",
    30184: "base",
}
_NETWORK_EIDS = {network: eid for eid, network in _LZ_NETWORKS.items()}

_ENDPOINT_V2 = {"eth": _hex("0x1a44076050125825900e736c501f859c50fe728c")}
_STARGATE_NATIVE_POOLS: Dict[str, FrozenSet[bytes]] = {
    "eth": frozenset({_hex("0x77b2043768d28e9c9ab44e1abfc95944bce57931")}),
}
_STARGATE_POOLS: Dict[str, FrozenSet[bytes]] = {
    "eth": frozenset(
        {
            _hex("0x77b2043768d28e9c9ab44e1abfc95944bce57931"),  # ETH (native)
            _hex("0xc026395860db2d07ee33e05fe50ed7bd583189c7"),  # USDC
            _hex("0x933597a323eb81cae705c5bc29985172fd5a3973"),  # USDT
            _hex("0x268ca24daeff1fac2ed883c598200ccbb79e931d"),  # mETH
        }
    ),
}
_STARGATE_TOKEN_MESSAGING = {"eth": _hex("0x6d6620efa72948c5f68a3c8646d58c00d3f4a980")}

# guid -> (sender OApp, receiver OApp)
_Packets = Dict[bytes, Tuple[bytes, bytes]]


def _sent_packet(data: bytes) -> Optional[Tuple[bytes, bytes, bytes]]:
    """(guid, sender, receiver) from a PacketSent's payload header."""
    if len(data) < 64:
        return None
    offset = int.from_bytes(data[0:32], "big")
    length = int.from_bytes(data[offset : offset + 32], "big")
    payload = data[offset + 32 : offset + 32 + length]
    if length < _HEADER_BYTES or len(payload) < _HEADER_BYTES:
        return None
    return payload[81:113], payload[25:45], payload[61:81]


def _delivered_packet(
    data: bytes, network: str
) -> Optional[Tuple[bytes, bytes, bytes]]:
    """(guid, sender, receiver) of a PacketDelivered, the guid recomputed as
    keccak(nonce ‖ srcEid ‖ sender ‖ dstEid ‖ receiver)."""
    dst_eid = _NETWORK_EIDS.get(network)
    if dst_eid is None or len(data) < 128:
        return None
    src_eid, _, nonce, _ = _words(data, 4)
    if src_eid >= 1 << 32 or nonce >= 1 << 64:
        return None
    sender, receiver = data[32:64], data[96:128]
    guid = keccak(
        nonce.to_bytes(8, "big")
        + src_eid.to_bytes(4, "big")
        + sender
        + dst_eid.to_bytes(4, "big")
        + receiver
    )
    return guid, sender[-20:], receiver[-20:]


def _packets(network: str, logs: List[Log]) -> Tuple[_Packets, _Packets]:
    """The endpoint's sent and delivered packets among a tx's logs."""
    endpoint = _ENDPOINT_V2.get(network)
    sent: _Packets = {}
    delivered: _Packets = {}
    for log in logs:
        topics = log.get("topics") or ()
        if not topics or endpoint is None or bytes(log["address"]) != endpoint:
            continue
        topic0, data = bytes(topics[0]), bytes(log["data"])
        if topic0 == _PACKET_SENT:
            packet, into = _sent_packet(data), sent
        elif topic0 == _PACKET_DELIVERED:
            packet, into = _delivered_packet(data, network), delivered
        else:
            continue
        if packet is not None:
            guid, sender, receiver = packet
            into[guid] = (sender, receiver)
    return sent, delivered


def _is_stargate(emitter: bytes, network: str) -> bool:
    return emitter in _STARGATE_POOLS.get(network, frozenset())


def _oapp_emits(oapp: bytes, emitter: bytes, network: str) -> bool:
    if oapp == emitter:
        return True
    return _is_stargate(emitter, network) and oapp == _STARGATE_TOKEN_MESSAGING.get(
        network
    )


def _asset(transfers: List[Log]) -> Optional[str]:
    tokens = {bytes(log["address"]) for log in transfers}
    return _address(tokens.pop()) if len(tokens) == 1 else None


def _sent(
    network: str, log: Log, before: List[Log], sent: _Packets
) -> Optional[Bridge]:
    topics = log["topics"]
    emitter, guid = bytes(log["address"]), bytes(topics[1])
    if guid == _ZERO_GUID:
        if not _is_stargate(emitter, network):
            return None  # a bus ride rides a Stargate pool
    else:
        packet = sent.get(guid)
        if packet is None or not _oapp_emits(packet[0], emitter, network):
            return None
    sender = bytes(topics[2])[-20:]
    dst_eid, amount, _ = _words(log["data"], 3)
    # any token: locked in the emitter (a pool, an OFT adapter), else burnt
    # from the sender
    funding = _transfers(before, receiver=emitter, value=amount) or _transfers(
        before, sender=sender, receiver=_ZERO_ADDRESS, value=amount
    )
    if funding:
        asset = _asset(funding) or UNKNOWN
    elif emitter in _STARGATE_NATIVE_POOLS.get(network, frozenset()):
        asset = "native"
    else:
        return None  # nothing left the sender: no transfer
    return Bridge(
        fromAddress=_address(sender),
        fromAsset=asset,
        fromAmount=amount,
        fromNetwork=network,
        fromPayment=_leg(log, funding),
        # the recipient is only named on arrival; amountReceivedLD is in the
        # SOURCE token's decimals
        toAddress=UNKNOWN,
        toAsset=UNKNOWN,
        toAmount=0,
        toNetwork=_LZ_NETWORKS.get(dst_eid, UNKNOWN),
        toPayment=PENDING,
    )


def _received(
    network: str, log: Log, before: List[Log], delivered: _Packets
) -> Optional[Bridge]:
    topics = log["topics"]
    emitter, guid = bytes(log["address"]), bytes(topics[1])
    packet = delivered.get(guid)
    if packet is None or not _oapp_emits(packet[1], emitter, network):
        return None
    recipient = bytes(topics[2])[-20:]
    src_eid, amount = _words(log["data"], 2)
    payout = _transfers(before, receiver=recipient, value=amount)
    if payout:
        asset = _asset(payout) or UNKNOWN
    elif emitter in _STARGATE_NATIVE_POOLS.get(network, frozenset()):
        asset = "native"
    else:
        return None
    return Bridge(
        fromAddress=UNKNOWN,
        fromAsset=UNKNOWN,
        fromAmount=0,
        fromNetwork=_LZ_NETWORKS.get(src_eid, UNKNOWN),
        fromPayment=UNKNOWN,
        toAddress=_address(recipient),
        toAsset=asset,
        toAmount=amount,
        toNetwork=network,
        toPayment=_leg(log, payout),
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's LayerZero half bridges on network, in log order."""
    sent, delivered = _packets(network, logs)
    bridges = []
    for position, log in enumerate(logs):
        topics = log.get("topics") or ()
        if len(topics) != 3:
            continue
        topic0 = bytes(topics[0])
        if topic0 == _OFT_SENT:
            reader, packets = _sent, sent
        elif topic0 == _OFT_RECEIVED:
            reader, packets = _received, delivered
        else:
            continue
        try:
            bridge = reader(network, log, logs[:position], packets)
        except (IndexError, ValueError) as exc:
            logger.warning(
                "LayerZero event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
            continue
        if bridge is not None:
            bridges.append(bridge)
    return bridges
