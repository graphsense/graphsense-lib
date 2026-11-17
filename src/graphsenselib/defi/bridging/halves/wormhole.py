"""Wormhole Portal Token Bridge half bridges (defi/bridging/delegated.py).

A send is the Core contract's LogMessagePublished whose indexed sender is the
network's Token Bridge, carrying a payload of type 1 (transfer) or 3 (transfer
with payload): type(1) amount(32) token(32) tokenChain(2) to(32) toChain(2).
Any other Core sender is generic Wormhole messaging, not a token transfer. The
payload amount is normalised to 8 decimals and the bridge moves only the
de-dusted amount, so the funding Transfer (into the Token Bridge, since the
previous send) is exactly amount * 10**k for the token's k extra decimals. With
none, the bridge wrapped the native coin itself (WETH Deposit to the bridge).

A redeem is the Token Bridge's TransferRedeemed(emitterChain, emitterAddress,
sequence), emitted before the bridge pays out: it mints the wrapped token
(Transfer from 0x0), releases the locked one (Transfer from itself), or unwraps
WETH (a Withdrawal by the bridge; the coin is sent on, which no log shows). A
Transfer to 0x0 is a burn, never a payout. The payout is read among the logs
after the redeem and before the next bridge event of any family the half
readers know (another Wormhole message or redeem, a CCTP, LayerZero, Across,
deBridge or Relay event: delegated.py's is_bridge_event): the first payout
there names its kind (the token and who sent it, or a Withdrawal by the
bridge), and the payout is the LAST one of that kind, since a legacy relayer
fee (type 1, fee > 0) is paid to msg.sender first in the same way. A payout of
another kind, such as a CCTP mint of the token the bridge released, is not the
redeem's.

The pair key, (emitterChain, emitterAddress, sequence), is the external
backend's to use; a half bridge carries what its own event says, and names
that key as its transfer id, the VAA id Wormholescan finds a message by:
`<emitterChain>/<emitterAddress, 64 hex, no 0x>/<sequence>`. A send's emitter
is its own network's Token Bridge and its sequence the LogMessagePublished's
first data word; a redeem's are TransferRedeemed's indexed three."""

import logging
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _leg,
)
from graphsenselib.defi.bridging.models import Bridge

logger = logging.getLogger(__name__)

_LOG_MESSAGE_PUBLISHED = bytes.fromhex(
    "6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2"
)
_TRANSFER_REDEEMED = bytes.fromhex(
    "caf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169"
)
_TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
_DEPOSIT = bytes.fromhex(
    "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
)
_WITHDRAWAL = bytes.fromhex(
    "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
)
_ZERO_ADDRESS = bytes(20)
_TRANSFER_TYPES = (1, 3)
_MAX_EXTRA_DECIMALS = 10
PROTOCOL = "wormhole"


class _Deployment(NamedTuple):
    chain_id: int  # Wormhole's, not the EVM chain id
    core: bytes
    token_bridge: bytes


def _deployment(chain_id: int, core: str, token_bridge: str) -> _Deployment:
    return _Deployment(chain_id, bytes.fromhex(core), bytes.fromhex(token_bridge))


# Mainnet contracts, wormhole.com/docs/reference/contract-addresses/; chain ids
# from the Wormhole repo's sdk/vaa/structs.go. eth, bnb and base (Core and Token
# Bridge) and the arb Token Bridge are also on recorded mainnet txs.
_DEPLOYMENTS: Dict[str, _Deployment] = {
    "eth": _deployment(
        2,
        "98f3c9e6e3face36baad05fe09d375ef1464288b",
        "3ee18b2214aff97000d974cf647e7c347e8fa585",
    ),
    "bnb": _deployment(
        4,
        "98f3c9e6e3face36baad05fe09d375ef1464288b",
        "b6f6d86a8f9879a9c87f643768d9efc38c1da6e7",
    ),
    "pol": _deployment(
        5,
        "7a4b5a56256163f07b2c80a7ca55abe66c4ec4d7",
        "5a58505a96d1dbf8df91cb21b54419fc36e93fde",
    ),
    "avax": _deployment(
        6,
        "54a8e5f9c4cba08f9943965859f6c34eaf03e26c",
        "0e082f06ff657d94310cb8ce8b0d9a04541d8052",
    ),
    "arb": _deployment(
        23,
        "a5f208e072434bc67592e4c49c1b991ba79bca46",
        "0b2402144bb366a632d14b83f244d2e0e21bd39c",
    ),
    "opt": _deployment(
        24,
        "ee91c335eab126df5fdb3797ea9d6ad93aec9722",
        "1d68124e65fafc907325e3edbf8c4d84499daa8b",
    ),
    "base": _deployment(
        30,
        "bebdb6c8ddc678ffa9f8748f85c815c556dd8ac6",
        "8d2de8d2f73f1f4cab472ac9a881c9b123c79627",
    ),
}
_CHAIN_NETWORKS = {d.chain_id: network for network, d in _DEPLOYMENTS.items()}


def _topics(log: Log) -> List[bytes]:
    return [bytes(t) for t in (log.get("topics") or ())]


def _is_send(log: Log, deployment: _Deployment) -> bool:
    topics = _topics(log)
    return (
        bytes(log["address"]) == deployment.core
        and len(topics) == 2
        and topics[0] == _LOG_MESSAGE_PUBLISHED
        and topics[1][-20:] == deployment.token_bridge
        and topics[1][:12] == bytes(12)
    )


def _is_redeem(log: Log, deployment: _Deployment) -> bool:
    topics = _topics(log)
    return (
        bytes(log["address"]) == deployment.token_bridge
        and len(topics) == 4
        and topics[0] == _TRANSFER_REDEEMED
    )


def is_event(network: str, log: Log) -> bool:
    """Whether log is a Wormhole event on network that bounds a payout: any
    Core message (generic messaging included) or a Token Bridge redeem."""
    deployment = _DEPLOYMENTS.get(network)
    if deployment is None:
        return False
    topics = _topics(log)
    return (
        bytes(log["address"]) == deployment.core
        and topics[:1] == [_LOG_MESSAGE_PUBLISHED]
    ) or _is_redeem(log, deployment)


class _Message(NamedTuple):
    amount: int
    token: bytes
    to: bytes
    to_chain: int


def _message(data: bytes) -> Optional[_Message]:
    data = bytes(data)
    if len(data) < 128:
        return None
    offset = int.from_bytes(data[64:96], "big")
    size = int.from_bytes(data[offset : offset + 32], "big")
    payload = data[offset + 32 : offset + 32 + size]
    if len(payload) < 101 or payload[0] not in _TRANSFER_TYPES:
        return None
    return _Message(
        amount=int.from_bytes(payload[1:33], "big"),
        token=payload[33:65],
        to=payload[67:99],
        to_chain=int.from_bytes(payload[99:101], "big"),
    )


def _scaled(value: int, amount: int) -> bool:
    return amount > 0 and any(
        value == amount * 10**k for k in range(_MAX_EXTRA_DECIMALS + 1)
    )


def _value(log: Log) -> int:
    return int.from_bytes(bytes(log["data"]), "big")


def _vaa_id(emitter_chain: int, emitter: bytes, sequence: int) -> str:
    return f"{emitter_chain}/{emitter.rjust(32, bytes(1)).hex()}/{sequence}"


def _send(
    network: str, log: Log, before: List[Log], deployment: _Deployment
) -> Optional[Bridge]:
    message = _message(log["data"])
    if message is None:
        return None
    tb = deployment.token_bridge
    payer, asset, amount, funding = UNKNOWN, UNKNOWN, 0, []
    for candidate in before:
        topics = _topics(candidate)
        if (
            len(topics) == 3
            and topics[0] == _TRANSFER
            and topics[2][-20:] == tb
            and _scaled(_value(candidate), message.amount)
        ):
            payer = _address(topics[1][-20:])
            asset = _address(bytes(candidate["address"]))
            amount, funding = _value(candidate), [candidate]
            break
    else:
        for candidate in before:
            topics = _topics(candidate)
            if (
                len(topics) == 2
                and topics[0] == _DEPOSIT
                and message.token[:12] == bytes(12)
                and bytes(candidate["address"]) == message.token[12:]
                and topics[1][-20:] == tb
                and _scaled(_value(candidate), message.amount)
            ):
                # the payer is the tx's sender, which the logs do not name
                asset, amount = "native", _value(candidate)
                break
    destination = _CHAIN_NETWORKS.get(message.to_chain)
    evm_recipient = destination is not None and message.to[:12] == bytes(12)
    return Bridge(
        fromAddress=payer,
        fromAsset=asset,
        fromAmount=amount,
        fromNetwork=network,
        fromPayment=_leg(log, funding),
        toAddress=_address(message.to[12:]) if evm_recipient else UNKNOWN,
        # the destination's wrapped token, in its decimals: known when redeemed
        toAsset=UNKNOWN,
        toAmount=0,
        toNetwork=destination or UNKNOWN,
        toPayment=PENDING,
        protocol=PROTOCOL,
        # LogMessagePublished(sender, uint64 sequence, ...): data word 0
        transferId=_vaa_id(
            deployment.chain_id,
            tb,
            int.from_bytes(bytes(log["data"])[:32], "big"),
        ),
    )


def _payout_kind(log: Log, tb: bytes) -> Optional[Tuple[bytes, bytes]]:
    """(asset, how) if a redeem by the Token Bridge tb could have paid log out:
    a Transfer minted (from 0x0) or released (from tb) to anyone but 0x0, how
    being its sender, or a WETH Withdrawal by tb, how being the Withdrawal
    topic; else None."""
    topics = _topics(log)
    if (
        len(topics) == 3
        and topics[0] == _TRANSFER
        and topics[1][-20:] in (_ZERO_ADDRESS, tb)
        and topics[2][-20:] != _ZERO_ADDRESS
    ):
        return bytes(log["address"]), topics[1][-20:]
    if len(topics) == 2 and topics[0] == _WITHDRAWAL and topics[1][-20:] == tb:
        return bytes(log["address"]), _WITHDRAWAL
    return None


def _redeem(
    network: str, log: Log, after: List[Log], deployment: _Deployment
) -> Bridge:
    """after: the logs up to the next bridge event of any family."""
    tb = deployment.token_bridge
    emitter_chain = int.from_bytes(_topics(log)[1], "big")
    emitter, sequence = _topics(log)[2], int.from_bytes(_topics(log)[3], "big")
    kind, paid = None, None
    for candidate in after:
        candidate_kind = _payout_kind(candidate, tb)
        if candidate_kind is not None and kind in (None, candidate_kind):
            # the last one: a relayer fee is paid first, the same way
            kind, paid = candidate_kind, candidate
    recipient, asset, amount, payout = UNKNOWN, UNKNOWN, 0, []
    if paid is not None and _topics(paid)[0] == _TRANSFER:
        recipient = _address(_topics(paid)[2][-20:])
        asset, amount, payout = _address(bytes(paid["address"])), _value(paid), [paid]
    elif paid is not None:
        # the coin's recipient is in no log, and no Transfer moved it
        asset, amount = "native", _value(paid)
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
        toPayment=_leg(log, payout),
        protocol=PROTOCOL,
        transferId=_vaa_id(emitter_chain, emitter, sequence),
    )


def read(
    network: str,
    logs: List[Log],
    is_bridge_event: Callable[[str, Log], bool] = is_event,
) -> List[Bridge]:
    """A tx's Wormhole half bridges on network, in log order. A redeem's payout
    is read up to the next log that is_bridge_event names, or Wormhole's own
    next event (delegated.py passes the events of every bridge it reads)."""
    deployment = _DEPLOYMENTS.get(network)
    if deployment is None:
        return []
    bridges: List[Bridge] = []
    sends = [i for i, log in enumerate(logs) if _is_send(log, deployment)]
    redeems = [i for i, log in enumerate(logs) if _is_redeem(log, deployment)]
    fences = [
        i
        for i, log in enumerate(logs)
        if is_event(network, log) or is_bridge_event(network, log)
    ]
    for position, log in enumerate(logs):
        try:
            if position in sends:
                since = max((i for i in sends if i < position), default=-1) + 1
                bridge = _send(network, log, logs[since:position], deployment)
            elif position in redeems:
                until = min((i for i in fences if i > position), default=len(logs))
                bridge = _redeem(network, log, logs[position + 1 : until], deployment)
            else:
                continue
        except (IndexError, ValueError) as exc:
            logger.warning(
                "Wormhole event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
            continue
        if bridge is not None:
            bridges.append(bridge)
    return bridges
