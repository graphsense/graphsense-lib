"""Across half bridges (defi/bridging/delegated.py): V2.0, V2.5 and V3 SpokePool
deposits and fills.

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

Every V2.5 and V3 event indexes the depositId second (topic 2): the half's
transfer id, in decimal, unique per ORIGIN chain only.

Both V3 eras share one word layout; the old fill carries ``bytes message``, so
its relayExecutionInfo sits at the offset in word 11. Before them, Across V2.5
(May 2023 to the V3 upgrade) used other events: the deposit names no output
token or amount, the fill no origin token and only the pre-fee amount, and it
pays BEFORE its event. The same SpokePool emits all three eras. Across V2.0
(May 2022 to April 2023) ran on its own SpokePools, whose events count only
from them: like V2.5, no output token on the deposit, paid = fillAmount x (1 -
appliedRelayerFeePct - realizedLpFeePct), paid BEFORE the event; the deposit's
depositId is topic 1 and its token topic 2, the fill's depositId and origin
chain are data words. A segment ends at the same SpokePool's next event.
"""

from typing import List, Optional

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
# Across V2.5 (May 2023 to the V3 upgrade), the same SpokePools:
# FundsDeposited(uint256,uint256,uint256,int64,uint32,uint32,address,address,
# address,bytes)
_V2_FUNDS_DEPOSITED = _hex(
    "0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6"
)
# FilledRelay(uint256,uint256,uint256,uint256,uint256,uint256,int64,int64,uint32,
# address,address,address,address,bytes,(address,bytes,int64,bool,int256))
_V2_FILLED_RELAY = _hex(
    "0x8ab9dc6c19fe88e69bc70221b339c84332752fdd49591b7c51e66bae3947b73c"
)
# Across V2.0 (May 2022 to April 2023), its own SpokePools:
# FundsDeposited(uint256,uint256,uint256,uint64,uint32,uint32,address,address,
# address), indexed: depositId, originToken, depositor
_V20_FUNDS_DEPOSITED = _hex(
    "0x4a4fc49abd237bfd7f4ac82d6c7a284c69daaea5154430cff04ad7482c6c4254"
)
# FilledRelay(uint256,uint256,uint256,uint256,uint256,uint256,uint64,uint64,
# uint64,uint32,address,address,address,address,bool), indexed: relayer, depositor
_V20_FILLED_RELAY = _hex(
    "0x56450a30040c51955338a4a9fbafcf94f7ca4b75f4cd83c2f5e29ef77fbe0a3a"
)
_DEPOSITS = (
    _FUNDS_DEPOSITED,
    _V3_FUNDS_DEPOSITED,
    _V2_FUNDS_DEPOSITED,
    _V20_FUNDS_DEPOSITED,
)
# today's SpokePool emits these; V2.0's emits _V20_EVENTS
_EVENTS = (
    _FUNDS_DEPOSITED,
    _V3_FUNDS_DEPOSITED,
    _V2_FUNDS_DEPOSITED,
    _FILLED_RELAY,
    _FILLED_V3_RELAY,
    _V2_FILLED_RELAY,
)
_V20_EVENTS = (_V20_FUNDS_DEPOSITED, _V20_FILLED_RELAY)
_PAY_FIRST = (_V2_FILLED_RELAY, _V20_FILLED_RELAY)  # fills paid before their event
_FEE_SCALE = 10**18  # V2 fee percentages are 1e18 fixed point

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
# Across V2.0's SpokePools (as in the adapter's table), the only emitter of its
# events
_V20_SPOKE_POOLS = {
    "eth": _hex("0x4d9079bb4165aeb4084c526a32695dcfd2f77381"),
    "opt": _hex("0xa420b2d1c0841415a695b81e5b867bcd07dff8c9"),
    "pol": _hex("0x69b5c72837769ef1e7c164abc6515dcff217f920"),
    "arb": _hex("0xb88690461ddbab6f04dfad7df66b7725942feb9c"),
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
    topic0 = bytes(log["topics"][0])
    if topic0 == _V20_FUNDS_DEPOSITED:
        return str(int.from_bytes(bytes(log["topics"][1]), "big"))
    if topic0 == _V20_FILLED_RELAY:
        return str(_words(log["data"], 10)[9])
    return str(int.from_bytes(bytes(log["topics"][2]), "big"))


def _spoke(network: str, log: Log) -> Optional[bytes]:
    """The SpokePool whose deposit or fill log is on network: V2.5 and V3
    events from today's SpokePool, V2.0 events from V2.0's; else None."""
    topics = log.get("topics")
    if not topics:
        return None
    address, topic0 = bytes(log["address"]), bytes(topics[0])
    if address == _SPOKE_POOLS.get(network) and topic0 in _EVENTS:
        return address
    if address == _V20_SPOKE_POOLS.get(network) and topic0 in _V20_EVENTS:
        return address
    return None


def is_event(network: str, log: Log) -> bool:
    """Whether log is an Across deposit or fill this reader reads on network."""
    return _spoke(network, log) is not None


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
    if bytes(log["topics"][0]) == _V20_FUNDS_DEPOSITED:
        # amount, originChainId, destinationChainId, relayerFeePct,
        # quoteTimestamp, recipient; the token is topic 2
        in_amount, _, chain, _, _, recipient = _words(log["data"], 6)
        destination = _ACROSS_NETWORKS.get(chain)
        in_token = int.from_bytes(bytes(log["topics"][2]), "big")
        out_token, out_amount = 0, 0
    elif bytes(log["topics"][0]) == _V2_FUNDS_DEPOSITED:
        # amount, originChainId, relayerFeePct, quoteTimestamp, originToken,
        # recipient: no output token or amount (the route picked them)
        in_amount, _, _, _, in_token, recipient = _words(log["data"], 6)
        out_token, out_amount = 0, 0
    else:
        (in_token, out_token, in_amount, out_amount, _, _, _, recipient) = _words(
            log["data"], 8
        )
    token = in_token.to_bytes(32, "big")[12:]
    funding = _transfers(before, token=token, receiver=spoke, value=in_amount)
    native = not funding and _wrapped(before, _WRAP, token, spoke, in_amount)
    # whoever paid into the SpokePool; the depositor for a bare-tx leg
    payer = bytes(funding[0]["topics"][1])[-20:] if len(funding) == 1 else None
    payer = payer or _evm(depositor)
    asset, amount = ("native" if native else _address(token)), in_amount
    if len(funding) > 1:  # several fit: never a guess
        asset, amount = UNKNOWN, 0
    return Bridge(
        fromAddress=_address(payer) if payer else UNKNOWN,
        fromAsset=asset,
        fromAmount=amount,
        fromNetwork=network,
        fromPayment=_pinned_leg(log, funding),
        toAddress=_declared(recipient, destination),
        toAsset=_declared(out_token, destination),
        toAmount=out_amount,
        toNetwork=destination or UNKNOWN,
        toPayment=PENDING,
        protocol=PROTOCOL,
        transferId=_deposit_id(log),
    )


def _v2_fill(log: Log) -> tuple:
    """(in token, out token, in amount, recipient, paid amount) of a
    V2.5 FilledRelay: no origin token; it gives the pre-fee fillAmount, so
    paid = fillAmount x (1 - relayer fee - LP fee), the fee of a speed-up
    (updatableRelayData, at the offset in word 11) included."""
    words = _words(log["data"], 12)
    info = words[11] // 32
    words = _words(log["data"], info + 3)
    fees = words[info + 2] + words[6]
    paid = words[2] * (_FEE_SCALE - fees) // _FEE_SCALE
    return 0, words[7], words[0], words[info], paid


def _v20_fill(log: Log) -> tuple:
    """(origin chain, in token, out token, in amount, recipient, paid amount) of
    a V2.0 FilledRelay: paid = fillAmount x (1 - appliedRelayerFeePct -
    realizedLpFeePct), only relayer and depositor indexed."""
    words = _words(log["data"], 12)
    paid = words[2] * (_FEE_SCALE - words[7] - words[8]) // _FEE_SCALE
    return words[4], 0, words[10], words[0], words[11], paid


def _fill(network: str, log: Log, around: List[Log], spoke: bytes) -> Bridge:
    """`around`: the logs the payout is among (after a V3 fill's event, before
    a V2 fill's)."""
    origin = _ACROSS_NETWORKS.get(int.from_bytes(bytes(log["topics"][1]), "big"))
    if bytes(log["topics"][0]) == _V20_FILLED_RELAY:
        chain, in_token, out_token, in_amount, recipient, amount = _v20_fill(log)
        origin = _ACROSS_NETWORKS.get(chain)
    elif bytes(log["topics"][0]) == _V2_FILLED_RELAY:
        in_token, out_token, in_amount, recipient, amount = _v2_fill(log)
    else:
        words = _words(log["data"], 15)
        in_token, out_token, in_amount = words[0], words[1], words[2]
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
        _transfers(around, token=token, receiver=payee, value=amount) if payee else []
    )
    native = not payout and _wrapped(around, _UNWRAP, token, spoke, amount)
    asset = "native" if native else _address(token)
    if len(payout) > 1:  # several fit: never a guess
        asset, amount = UNKNOWN, 0
    return Bridge(
        # the depositor is not the deposit's payer (a router may pay): unknown
        fromAddress=UNKNOWN,
        fromAsset=_declared(in_token, origin),
        fromAmount=in_amount,
        fromNetwork=origin or UNKNOWN,
        fromPayment=UNKNOWN,
        toAddress=_address(payee) if payee else UNKNOWN,
        toAsset=asset,
        toAmount=amount,
        toNetwork=network,
        toPayment=_pinned_leg(log, payout),
        protocol=PROTOCOL,
        transferId=_deposit_id(log),
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's Across half bridges on network, in log order. An event counts
    only from that network's SpokePool of its era."""
    spokes = [_spoke(network, log) for log in logs]
    bridges = []
    for position, spoke in enumerate(spokes):
        if spoke is None:
            continue
        log = logs[position]
        # the logs since / until the same SpokePool's previous / next event
        ours = [i for i, other in enumerate(spokes) if other == spoke]
        n = ours.index(position)
        since = ours[n - 1] + 1 if n else 0
        until = ours[n + 1] if n + 1 < len(ours) else len(logs)
        before, after = logs[since:position], logs[position + 1 : until]
        if bytes(log["topics"][0]) in _DEPOSITS:
            bridges.append(_deposit(network, log, before, spoke))
        elif bytes(log["topics"][0]) in _PAY_FIRST:  # pays before its event
            bridges.append(_fill(network, log, before, spoke))
        else:
            bridges.append(_fill(network, log, after, spoke))
    return bridges
