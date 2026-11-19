"""deBridge DLN half bridges (defi/bridging/delegated.py): orders on DlnSource
and their fills on DlnDestination.

A CreatedOrder sends: the order names the chain, token, amount and receiver of
the other leg, which a taker fills later (destination "pending"). A
FulfilledOrder receives: the order names the chain, token, amount and maker of
the source, but not which tx paid it (source "unknown"). FulfilledOrder has two
signatures: an upgrade added ``uint256 actualFulfillAmount`` after the orderId,
what the taker states it paid; the old one pays the order's takeAmount.

Legs, pinned as the external backend adapter pins them
(services/bridges/debridge.py), so its pairs name these legs: an ERC20 order's
leg is the one Transfer of giveToken into DlnSource before its event (DlnSource
takes its fee in between, so not pinned on value); a fill's leg is the one
Transfer of takeToken of its amount to its receiver before its event (one tx
fills several orders). A token the order names as the zero address or as empty
bytes is the native coin: that leg is the tx, of giveAmount + percentFee + the
affiliate fee + nativeFixFee for an order. Several fitting Transfers: the leg
is unknown, never a guess. A send's fromAddress is set by half_bridges
(delegated._paid_by): whoever paid its leg. A member that is not a 20-byte
address (a Solana order) is "unknown", as is a chain deBridge numbers but the
conversions do not name.

Order (both events carry it at the offset in data word 0; orderId is data word
1): makerOrderNonce, makerSrc (bytes), giveChainId, giveTokenAddress (bytes),
giveAmount, takeChainId, takeTokenAddress (bytes), takeAmount, receiverDst
(bytes), ...; bytes members are offsets from the struct's start. CreatedOrder
continues with bytes affiliateFee, uint256 nativeFixFee, uint256 percentFee.

A half's transfer id is the orderId (0x hex), the same in both events.
"""

from typing import List, NamedTuple, Optional

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _pinned_leg,
    _transfers,
)
from graphsenselib.defi.bridging.models import Bridge

PROTOCOL = "debridge"
NATIVE = "native"


def _hex(s: str) -> bytes:
    return bytes.fromhex(s.removeprefix("0x"))


_CREATED_ORDER = _hex(
    "0xfc8703fd57380f9dd234a89dce51333782d49c5902f307b02f03e014d18fe471"
)
# FulfilledOrder(Order, bytes32 orderId, address sender, address unlockAuthority)
_FULFILLED_ORDER = _hex(
    "0xd281ee92bab1446041582480d2c0a9dc91f855386bb27ea295faac1e992f7fe4"
)
# FulfilledOrder(Order, bytes32 orderId, uint256 actualFulfillAmount,
# address sender, address unlockAuthority)
_FULFILLED_ORDER_V2 = _hex(
    "0xc164aca37b9805a1c9027b6f32260a069723a82926f6e9ece4926e4dd3ea8ecf"
)

# One address on every EVM chain deBridge serves (as in the adapter's table)
_DLN_SOURCE = _hex("0xef4fb24ad0916217251f553c0596f8edc630eb66")
_DLN_DESTINATION = _hex("0xe7351fd770a37282b91d153ee690b63579d6dd7f")

# deBridge's chain ids of the networks the conversions name: the EVM chain id,
# except where deBridge minted its own (gnosis)
_DLN_NETWORKS = {
    1: "eth",
    10: "opt",
    56: "bnb",
    137: "pol",
    4663: "robinhood",
    8453: "base",
    42161: "arb",
    43114: "avax",
    100000002: "gnosis",
}


class _Order(NamedTuple):
    maker: Optional[str]
    give_chain: int
    give_token: Optional[str]  # NATIVE, a contract, or None
    give_amount: int
    take_chain: int
    take_token: Optional[str]
    take_amount: int
    receiver: Optional[str]


def _word(data: bytes, index: int) -> int:
    return int.from_bytes(data[32 * index : 32 * (index + 1)], "big")


def _bytes(data: bytes, offset: int) -> bytes:
    """The ABI `bytes` value at byte offset `offset` of data."""
    return data[offset + 32 : offset + 32 + _word(data, offset // 32)]


def _evm(raw: bytes) -> Optional[str]:
    return _address(raw) if len(raw) == 20 else None


def _token(raw: bytes) -> Optional[str]:
    return NATIVE if not raw or raw == bytes(20) else _evm(raw)


def _order(data: bytes) -> _Order:
    start = _word(data, 0)
    head = start // 32

    def member(position: int) -> bytes:
        return _bytes(data, start + _word(data, head + position))

    return _Order(
        maker=_evm(member(1)),
        give_chain=_word(data, head + 2),
        give_token=_token(member(3)),
        give_amount=_word(data, head + 4),
        take_chain=_word(data, head + 5),
        take_token=_token(member(6)),
        take_amount=_word(data, head + 7),
        receiver=_evm(member(8)),
    )


def _declared(value: Optional[str], network: Optional[str]) -> str:
    """What an event says about the other leg, on a chain the conversions name."""
    return (value if network else None) or UNKNOWN


def _created(network: str, log: Log, before: List[Log]) -> Optional[Bridge]:
    data = bytes(log["data"])
    order = _order(data)
    destination = _DLN_NETWORKS.get(order.take_chain)
    if order.give_token == NATIVE:
        affiliate = _bytes(data, _word(data, 2))
        fee = int.from_bytes(affiliate[20:], "big")
        amount = order.give_amount + _word(data, 4) + fee + _word(data, 3)
        funding, asset = [], NATIVE
    else:
        token = _hex(order.give_token)
        funding = _transfers(before, token=token, receiver=_DLN_SOURCE)
        if not funding:
            return None  # no Transfer moved this order's funds in this tx
        asset = order.give_token
        amount = int.from_bytes(bytes(funding[0]["data"]), "big")
        if len(funding) > 1:  # several fit: never a guess
            asset, amount = UNKNOWN, 0
    return Bridge(
        fromAddress=UNKNOWN,  # set by half_bridges: whoever paid the leg
        fromAsset=asset,
        fromAmount=amount,
        fromNetwork=network,
        fromPayment=_pinned_leg(log, funding),
        toAddress=_declared(order.receiver, destination),
        toAsset=_declared(order.take_token, destination),
        toAmount=order.take_amount,
        toNetwork=destination or UNKNOWN,
        toPayment=PENDING,
        protocol=PROTOCOL,
        transferId="0x" + data[32:64].hex(),
    )


def _fulfilled(network: str, log: Log, before: List[Log]) -> Optional[Bridge]:
    data = bytes(log["data"])
    order = _order(data)
    if bytes(log["topics"][0]) == _FULFILLED_ORDER_V2:
        amount = _word(data, 2)  # actualFulfillAmount
    else:
        amount = order.take_amount
    asset, payout = order.take_token, []
    if asset != NATIVE:
        payout = _transfers(
            before, token=_hex(asset), receiver=_hex(order.receiver), value=amount
        )
        if not payout:
            return None  # no Transfer paid this order in this tx
        if len(payout) > 1:  # several fit: never a guess
            asset, amount = UNKNOWN, 0
    source = _DLN_NETWORKS.get(order.give_chain)
    return Bridge(
        fromAddress=_declared(order.maker, source),
        fromAsset=_declared(order.give_token, source),
        fromAmount=order.give_amount,
        fromNetwork=source or UNKNOWN,
        fromPayment=UNKNOWN,
        toAddress=order.receiver,
        toAsset=asset,
        toAmount=amount,
        toNetwork=network,
        toPayment=_pinned_leg(log, payout),
        protocol=PROTOCOL,
        transferId="0x" + data[32:64].hex(),
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's deBridge half bridges on network, in log order (logs sorted by
    log index). An event counts only from its own DLN contract."""
    bridges = []
    for position, log in enumerate(logs):
        topics = log.get("topics")
        if not topics:
            continue
        topic0, emitter = bytes(topics[0]), bytes(log["address"])
        if topic0 == _CREATED_ORDER and emitter == _DLN_SOURCE:
            bridge = _created(network, log, logs[:position])
        elif (
            topic0 in (_FULFILLED_ORDER, _FULFILLED_ORDER_V2)
            and emitter == _DLN_DESTINATION
        ):
            bridge = _fulfilled(network, log, logs[:position])
        else:
            continue
        if bridge is not None:
            bridges.append(bridge)
    return bridges
