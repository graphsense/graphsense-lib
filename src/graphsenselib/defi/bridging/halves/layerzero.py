"""LayerZero V2 OFT half bridges, Stargate V2's pools and USDT0 included
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
leaves later); it counts only from a Stargate pool. A bus delivery is one
OFTReceived per passenger, each carrying the bus's packet guid.

Legs, pinned as the external backend adapter pins them (services/bridges/
layerzero.py), so its pairs name these legs: a send's leg is the one Transfer of
amountSentLD in the OFT's token locked in the emitter, else burnt (a mint/burn
OFT); a receive's is the one Transfer of amountReceivedLD to the recipient; a
Stargate native pool moves the chain's coin, so its leg is the tx. Several
fitting Transfers: the leg is unknown, never a guess. A send's fromAddress is
set by half_bridges (delegated._paid_by): whoever paid its leg; a receive names
no sender. OFTSent names no recipient, and amountReceivedLD is in the source
token's decimals: the other side holds only the network the event names.

A half's transfer id is the message guid; a bus ride has none of its own (guid
0: the bus's message, sent later, carries it). Its app is Stargate when a
Stargate pool emitted, else USDT0 when the emitter or the packet's OApp on the
other chain is one of USDT0's OFTs; a generic OFT names none.

Contracts as the adapter's: the eth endpoint from recorded receipts, the
Stargate pools and TokenMessaging from Stargate's mainnet contract list,
USDT0's OFTs from USDT0's own deployment list (docs.usdt0.to, "Native
Deployments"): eth's is the lockbox (OFT Adapter) over Tether's USDT."""

from typing import Dict, FrozenSet, List, Optional, Tuple

from eth_hash.auto import keccak

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _pinned_leg,
    _transfers,
    _words,
)
from graphsenselib.defi.bridging.models import Bridge

PROTOCOL = "layerzero"
NATIVE = "native"


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
# The token a pool or OFT adapter holds, where it is not the OFT itself (a
# mint/burn OFT is its own token), as the adapter's: an adapter missing here is
# not pinned to a token.
_OFT_TOKENS = {
    "eth": {
        # Stargate USDC and USDT pools
        _hex("0xc026395860db2d07ee33e05fe50ed7bd583189c7"): _hex(
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        ),
        _hex("0x933597a323eb81cae705c5bc29985172fd5a3973"): _hex(
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
        ),
        # USDT0's lockbox over Tether's USDT
        _hex("0x6c96de32cea08842dcc4058c14d3aaad7fa41dee"): _hex(
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
        ),
    }
}
# (network, OFT): only to NAME a half's app, never to admit one
_USDT0_OFTS: FrozenSet[Tuple[str, bytes]] = frozenset(
    {
        ("eth", _hex("0x6c96de32cea08842dcc4058c14d3aaad7fa41dee")),
        ("arb", _hex("0x14e4a1b13bf7f943c8ff7c51fb60fa964a298d92")),
        ("pol", _hex("0x6ba10300f0dc58b7a1e4c0e41f5dabb7d7829e13")),
        ("opt", _hex("0xf03b4d9ac1d5d1e7c4cef54c2a313b9fe051a0ad")),
    }
)

# guid -> (sender OApp, receiver OApp)
_Packets = Dict[bytes, Tuple[bytes, bytes]]


def _sent_packet(data: bytes) -> Tuple[bytes, bytes, bytes]:
    """(guid, sender, receiver) from a PacketSent's payload header: version(1)
    nonce(8) srcEid(4) sender(32) dstEid(4) receiver(32) guid(32)."""
    payload = data[int.from_bytes(data[0:32], "big") + 32 :]
    return payload[81:113], payload[25:45], payload[61:81]


def _delivered_packet(data: bytes, network: str) -> Tuple[bytes, bytes, bytes]:
    """(guid, sender, receiver) of a PacketDelivered, the guid recomputed as
    keccak(nonce ‖ srcEid ‖ sender ‖ dstEid ‖ receiver)."""
    src_eid, _, nonce, _ = _words(data, 4)
    sender, receiver = data[32:64], data[96:128]
    guid = keccak(
        nonce.to_bytes(8, "big")
        + src_eid.to_bytes(4, "big")
        + sender
        + _NETWORK_EIDS[network].to_bytes(4, "big")
        + receiver
    )
    return guid, sender[-20:], receiver[-20:]


def _packets(network: str, logs: List[Log]) -> Tuple[_Packets, _Packets]:
    """The endpoint's sent and delivered packets among a tx's logs."""
    endpoint = _ENDPOINT_V2[network]
    sent: _Packets = {}
    delivered: _Packets = {}
    for log in logs:
        topics = log.get("topics") or ()
        if not topics or bytes(log["address"]) != endpoint:
            continue
        topic0, data = bytes(topics[0]), bytes(log["data"])
        if topic0 == _PACKET_SENT:
            guid, sender, receiver = _sent_packet(data)
            sent[guid] = (sender, receiver)
        elif topic0 == _PACKET_DELIVERED:
            guid, sender, receiver = _delivered_packet(data, network)
            delivered[guid] = (sender, receiver)
    return sent, delivered


def _is_stargate(emitter: bytes, network: str) -> bool:
    return emitter in _STARGATE_POOLS.get(network, frozenset())


def _emits(oapp: bytes, emitter: bytes, network: str) -> bool:
    """Whether the OFT events of this packet's OApp come from `emitter`."""
    return oapp == emitter or (
        _is_stargate(emitter, network)
        and oapp == _STARGATE_TOKEN_MESSAGING.get(network)
    )


def _app(
    emitter: bytes, network: str, other_oapp: Optional[bytes], other_network: str
) -> Optional[str]:
    """The app a half names, as the adapter's row from either end."""
    if _is_stargate(emitter, network):
        return "stargate"
    if (network, emitter) in _USDT0_OFTS or (
        other_network,
        other_oapp,
    ) in _USDT0_OFTS:
        return "usdt0"
    return None


def _fitting(
    before: List[Log], emitter: bytes, network: str, receiver: bytes, value: int
) -> List[Log]:
    """The Transfers of `value` to `receiver` in the OFT's token: the pool's or
    adapter's token, else the OFT itself (a mint/burn OFT), else any token (an
    adapter not in the table)."""
    token = _OFT_TOKENS.get(network, {}).get(emitter)
    if token is not None:
        return _transfers(before, token=token, receiver=receiver, value=value)
    return _transfers(
        before, token=emitter, receiver=receiver, value=value
    ) or _transfers(before, receiver=receiver, value=value)


def _leg(
    network: str, emitter: bytes, before: List[Log], receiver: bytes, value: int
) -> Optional[Tuple[str, int, List[Log]]]:
    """(asset, amount, Transfers) of an event's leg, or None when no Transfer
    moved its funds; a native pool's leg is the tx."""
    if emitter in _STARGATE_NATIVE_POOLS.get(network, frozenset()):
        return NATIVE, value, []
    fitting = _fitting(before, emitter, network, receiver, value)
    if not fitting:
        return None
    if len(fitting) > 1:  # several fit: never a guess
        return UNKNOWN, 0, fitting
    return _address(bytes(fitting[0]["address"])), value, fitting


def _sent(
    network: str, log: Log, before: List[Log], sent: _Packets
) -> Optional[Bridge]:
    topics = log["topics"]
    emitter, guid = bytes(log["address"]), bytes(topics[1])
    packet = None
    if guid == _ZERO_GUID:
        if not _is_stargate(emitter, network):
            return None  # a bus ride rides a Stargate pool
    else:
        packet = sent.get(guid)
        if packet is None or not _emits(packet[0], emitter, network):
            return None
    dst_eid, amount, _ = _words(log["data"], 3)
    # locked in the emitter (a pool, an OFT adapter), else burnt
    leg = _leg(network, emitter, before, emitter, amount) or _leg(
        network, emitter, before, _ZERO_ADDRESS, amount
    )
    if leg is None:
        return None
    asset, amount, funding = leg
    destination = _LZ_NETWORKS.get(dst_eid, UNKNOWN)
    return Bridge(
        fromAddress=UNKNOWN,  # set by half_bridges: whoever paid the leg
        fromAsset=asset,
        fromAmount=amount,
        fromNetwork=network,
        fromPayment=_pinned_leg(log, funding),
        toAddress=UNKNOWN,
        toAsset=UNKNOWN,
        toAmount=0,
        toNetwork=destination,
        toPayment=PENDING,
        protocol=PROTOCOL,
        protocolApp=_app(emitter, network, packet[1] if packet else None, destination),
        transferId=None if guid == _ZERO_GUID else _address(guid),
    )


def _received(
    network: str, log: Log, before: List[Log], delivered: _Packets
) -> Optional[Bridge]:
    topics = log["topics"]
    emitter, guid = bytes(log["address"]), bytes(topics[1])
    packet = delivered.get(guid)
    if packet is None or not _emits(packet[1], emitter, network):
        return None
    recipient = bytes(topics[2])[-20:]
    src_eid, amount = _words(log["data"], 2)
    leg = _leg(network, emitter, before, recipient, amount)
    if leg is None:
        return None
    asset, amount, payout = leg
    source = _LZ_NETWORKS.get(src_eid, UNKNOWN)
    return Bridge(
        fromAddress=UNKNOWN,
        fromAsset=UNKNOWN,
        fromAmount=0,
        fromNetwork=source,
        fromPayment=UNKNOWN,
        toAddress=_address(recipient),
        toAsset=asset,
        toAmount=amount,
        toNetwork=network,
        toPayment=_pinned_leg(log, payout),
        protocol=PROTOCOL,
        protocolApp=_app(emitter, network, packet[0], source),
        transferId=_address(guid),
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's LayerZero half bridges on network, in log order."""
    sent, delivered = _packets(network, logs)
    bridges = []
    for position, log in enumerate(logs):
        topics = log.get("topics") or ()
        topic0 = bytes(topics[0]) if topics else b""
        if topic0 == _OFT_SENT:
            bridge = _sent(network, log, logs[:position], sent)
        elif topic0 == _OFT_RECEIVED:
            bridge = _received(network, log, logs[:position], delivered)
        else:
            continue
        if bridge is not None:
            bridges.append(bridge)
    return bridges
