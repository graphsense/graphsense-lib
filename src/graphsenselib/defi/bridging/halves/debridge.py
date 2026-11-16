"""deBridge DLN half bridges (defi/bridging/delegated.py).

A CreatedOrder on DlnSource sends: the order names the chain, token, amount
and receiver of the other leg, which a taker fills later (destination
"pending"). A FulfilledOrder on DlnDestination receives: the order names the
chain, token, amount and maker of the source, but not which tx paid it
(source "unknown"). FulfilledOrder has two signatures: an upgrade added
``uint256 actualFulfillAmount`` after the orderId, which the taker states it
paid; the old one pays the order's takeAmount. One tx can fill several
orders, so each fill's payout is the Transfer to its receiver of its amount
since the previous fill.

A token the order names as the zero address (or as empty bytes) is the
native coin: that leg moves no Transfer, so it is the tx. An ERC20 order's
leg is the Transfer into DlnSource since the previous order, and its
fromAddress is that Transfer's sender (who paid into DlnSource, maybe a
forwarder), not the maker. For a native order the logs do not say who paid,
so it is the maker. A token, maker or receiver that is not a 20-byte address
(a Solana order) is "unknown".

Order (the struct both events carry, at the offset in data word 0; orderId
is data word 1): makerOrderNonce, makerSrc (bytes), giveChainId,
giveTokenAddress (bytes), giveAmount, takeChainId, takeTokenAddress (bytes),
takeAmount, receiverDst (bytes), ...; bytes members are offsets from the
struct's start. CreatedOrder continues with bytes affiliateFee, uint256
nativeFixFee, uint256 percentFee, ...: a tx pays DlnSource giveAmount +
percentFee + the affiliate fee, and nativeFixFee in the native coin on top.
"""

import logging
from typing import Dict, List, NamedTuple, Optional, Tuple

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _leg,
)
from graphsenselib.defi.bridging.models import Bridge

logger = logging.getLogger(__name__)

NATIVE = "native"

_TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
_CREATED_ORDER = bytes.fromhex(
    "fc8703fd57380f9dd234a89dce51333782d49c5902f307b02f03e014d18fe471"
)
# FulfilledOrder(Order, bytes32 orderId, address sender, address unlockAuthority)
_FULFILLED_ORDER = bytes.fromhex(
    "d281ee92bab1446041582480d2c0a9dc91f855386bb27ea295faac1e992f7fe4"
)
# FulfilledOrder(Order, bytes32 orderId, uint256 actualFulfillAmount,
# address sender, address unlockAuthority)
_FULFILLED_ORDER_V2 = bytes.fromhex(
    "c164aca37b9805a1c9027b6f32260a069723a82926f6e9ece4926e4dd3ea8ecf"
)

# network -> (DlnSource, DlnDestination). One address on every EVM chain
# deBridge serves; the entries are those seen on recorded logs (eth: both,
# bnb: DlnSource).
_DLN_SOURCE = bytes.fromhex("ef4fb24ad0916217251f553c0596f8edc630eb66")
_DLN_DESTINATION = bytes.fromhex("e7351fd770a37282b91d153ee690b63579d6dd7f")
_CONTRACTS: Dict[str, Tuple[bytes, bytes]] = {
    "eth": (_DLN_SOURCE, _DLN_DESTINATION),
    "bnb": (_DLN_SOURCE, _DLN_DESTINATION),
}

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

# affiliateFee: abi.encodePacked(address beneficiary, uint256 amount)
_AFFILIATE_FEE_BYTES = 20 + 32


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
    word = data[32 * index : 32 * (index + 1)]
    if len(word) != 32:
        raise ValueError(f"{len(data)} bytes of data, word {index} expected")
    return int.from_bytes(word, "big")


def _bytes(data: bytes, offset: int) -> bytes:
    """The ABI `bytes` value at byte offset `offset` of data."""
    length = _word(data, offset // 32)
    value = data[offset + 32 : offset + 32 + length]
    if len(value) != length:
        raise ValueError("bytes value overruns the data")
    return value


def _evm_address(raw: bytes) -> Optional[str]:
    return _address(raw) if len(raw) == 20 else None


def _token(raw: bytes) -> Optional[str]:
    if len(raw) == 0 or raw == bytes(20):
        return NATIVE
    return _evm_address(raw)


def _order(data: bytes) -> _Order:
    start = _word(data, 0)
    head = start // 32

    def member(position: int) -> bytes:
        return _bytes(data, start + _word(data, head + position))

    return _Order(
        maker=_evm_address(member(1)),
        give_chain=_word(data, head + 2),
        give_token=_token(member(3)),
        give_amount=_word(data, head + 4),
        take_chain=_word(data, head + 5),
        take_token=_token(member(6)),
        take_amount=_word(data, head + 7),
        receiver=_evm_address(member(8)),
    )


def _transfers_of(
    logs: List[Log], token: str, receiver: bytes, value: Optional[int] = None
) -> List[Log]:
    """ERC20 Transfers of token to receiver (of value, if given)."""
    return [
        log
        for log in logs
        if _address(bytes(log["address"])) == token
        and len(log.get("topics") or ()) == 3
        and bytes(log["topics"][0]) == _TRANSFER
        and bytes(log["topics"][2])[-20:] == receiver
        and (value is None or int.from_bytes(bytes(log["data"]), "big") == value)
    ]


def _created(network: str, log: Log, before: List[Log]) -> Optional[Bridge]:
    data = bytes(log["data"])
    order = _order(data)
    if order.give_token is None:
        return None
    destination = _DLN_NETWORKS.get(order.take_chain)
    if order.give_token == NATIVE:
        affiliate = _bytes(data, _word(data, 2))
        affiliate_fee = (
            int.from_bytes(affiliate[20:], "big")
            if len(affiliate) == _AFFILIATE_FEE_BYTES
            else 0
        )
        payer = order.maker or UNKNOWN
        amount = order.give_amount + _word(data, 4) + affiliate_fee + _word(data, 3)
        payment = _leg(log, [])
    else:
        funding = _transfers_of(before, order.give_token, _DLN_SOURCE)
        if not funding:
            return None  # nothing paid into DlnSource: not this tx's order
        payment = _leg(log, funding)
        if len(funding) == 1:
            payer = _address(bytes(funding[0]["topics"][1])[-20:])
            amount = int.from_bytes(bytes(funding[0]["data"]), "big")
        else:
            payer = order.maker or UNKNOWN
            amount = sum(int.from_bytes(bytes(t["data"]), "big") for t in funding)
    return Bridge(
        fromAddress=payer,
        fromAsset=order.give_token,
        fromAmount=amount,
        fromNetwork=network,
        fromPayment=payment,
        toAddress=(order.receiver if destination else None) or UNKNOWN,
        toAsset=(order.take_token if destination else None) or UNKNOWN,
        toAmount=order.take_amount,
        toNetwork=destination or UNKNOWN,
        toPayment=PENDING,
    )


def _fulfilled(network: str, log: Log, before: List[Log]) -> Optional[Bridge]:
    data = bytes(log["data"])
    order = _order(data)
    if order.receiver is None or order.take_token is None:
        return None
    if bytes(log["topics"][0]) == _FULFILLED_ORDER_V2:
        amount = _word(data, 2)  # actualFulfillAmount
    else:
        amount = order.take_amount
    if order.take_token == NATIVE:
        payment = _leg(log, [])
    else:
        receiver = bytes.fromhex(order.receiver.removeprefix("0x"))
        payout = _transfers_of(before, order.take_token, receiver, amount)
        if not payout:
            return None
        payment = _leg(log, payout)
    source = _DLN_NETWORKS.get(order.give_chain)
    return Bridge(
        fromAddress=(order.maker if source else None) or UNKNOWN,
        fromAsset=(order.give_token if source else None) or UNKNOWN,
        fromAmount=order.give_amount,
        fromNetwork=source or UNKNOWN,
        fromPayment=UNKNOWN,
        toAddress=order.receiver,
        toAsset=order.take_token,
        toAmount=amount,
        toNetwork=network,
        toPayment=payment,
    )


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's deBridge half bridges on network, in log order."""
    contracts = _CONTRACTS.get(network)
    if contracts is None:
        return []
    source, destination = contracts
    logs = sorted(logs, key=lambda log: log["log_index"])
    bridges = []
    since = {"created": 0, "fulfilled": 0}  # position after the previous event
    for position, log in enumerate(logs):
        topics = log.get("topics")
        if not topics:
            continue
        topic0, emitter = bytes(topics[0]), bytes(log["address"])
        if topic0 == _CREATED_ORDER and emitter == source:
            kind, read_event = "created", _created
        elif (
            topic0 in (_FULFILLED_ORDER, _FULFILLED_ORDER_V2) and emitter == destination
        ):
            kind, read_event = "fulfilled", _fulfilled
        else:
            continue
        try:
            bridge = read_event(network, log, logs[since[kind] : position])
        except (IndexError, ValueError) as exc:
            # an event that does not decode is no half bridge
            logger.warning(
                "deBridge event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
            bridge = None
        since[kind] = position + 1
        if bridge is not None:
            bridges.append(bridge)
    return bridges
