"""Bridges graphsense-lib finds only half of.

graphsense-lib reads these bridges on one chain only: it decodes the leg in
the tx at hand, but cannot find the transfer's leg on the other chain. Each
such event is a half bridge: a Bridge whose own leg is complete, whose other
leg holds what the event says about it, and whose other asset transfer is
"pending" (not made yet, as far as this tx tells) or "unknown" (made, but not
named here), like THORChain's partial bridge (thorchain.py). An external
backend may pair it with the other leg (web/middleware/external_backends.py,
rule 6); without one the half bridge is the answer. A tx without such an
event costs nothing: the check reads no more than the tx's logs, which the
conversions are read from anyway.

An event counts only from its protocol's own contract on that network, by
topic0 AND emitter: any contract can emit an event with the same signature.

Circle CCTP: each version's TokenMessenger emits DepositForBurn on the burning
chain and MintAndWithdraw on the minting chain, where the MessageTransmitter's
MessageReceived names the chain the USDC was burned on. A burn's leg is the
Transfer that funded it (depositor to TokenMinter), a mint's the Transfer that
paid the recipient, each found among the logs since the previous burn or
mint; when not exactly one Transfer fits, the leg is the tx itself.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from graphsenselib.defi.bridging.models import Bridge
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)

logger = logging.getLogger(__name__)

CCTP = "cctp"
PENDING = "pending"
UNKNOWN = "unknown"

_TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
_ZERO_ADDRESS = bytes(20)

# Circle's domains of the networks the conversions name, all of them EVM
# (developers.circle.com/cctp/cctp-supported-blockchains)
_CCTP_NETWORKS = {
    0: "eth",
    1: "avax",
    2: "opt",
    3: "arb",
    6: "base",
    7: "pol",
    17: "bnb",
}


@dataclass(frozen=True)
class _CctpVersion:
    """One CCTP version's eth contracts and event signatures."""

    token_messenger: bytes
    message_transmitter: bytes
    token_minter: bytes
    deposit_for_burn: bytes
    mint_and_withdraw: bytes
    message_received: bytes
    # DepositForBurn's indexed burnToken; the depositor is the topic after it
    burn_token_topic: int


def _hex(s: str) -> bytes:
    return bytes.fromhex(s.removeprefix("0x"))


_CCTP_V1 = _CctpVersion(
    token_messenger=_hex("0xbd3fa81b58ba92a82136038b25adec7066af3155"),
    message_transmitter=_hex("0x0a992d191deec32afe36203ad87d7d289a738f81"),
    token_minter=_hex("0xc4922d64a24675e16e1586e3e3aa56c06fabe907"),
    # DepositForBurn(uint64,address,uint256,address,bytes32,uint32,bytes32,bytes32)
    deposit_for_burn=_hex(
        "0x2fa9ca894982930190727e75500a97d8dc500233a5065e0f3126c48fbe0343c0"
    ),
    # MintAndWithdraw(address,uint256,address)
    mint_and_withdraw=_hex(
        "0x1b2a7ff080b8cb6ff436ce0372e399692bbfb6d4ae5766fd8d58a7b8cc6142e6"
    ),
    # MessageReceived(address,uint32,uint64,bytes32,bytes)
    message_received=_hex(
        "0x58200b4c34ae05ee816d710053fff3fb75af4395915d3d2a771b24aa10e3cc5d"
    ),
    burn_token_topic=2,
)
_CCTP_V2 = _CctpVersion(
    token_messenger=_hex("0x28b5a0e9c621a5badaa536219b3a228c8168cf5d"),
    message_transmitter=_hex("0x81d40f21f12a8f0e3252bccb954d722d4c464b64"),
    token_minter=_hex("0xfd78ee919681417d192449715b2594ab58f5d002"),
    # DepositForBurn(address,uint256,address,bytes32,uint32,bytes32,bytes32,
    # uint256,uint32,bytes)
    deposit_for_burn=_hex(
        "0x0c8c1cbdc5190613ebd485511d4e2812cfa45eecb79d845893331fedad5130a5"
    ),
    # MintAndWithdraw(address,uint256,address,uint256)
    mint_and_withdraw=_hex(
        "0x50c55e915134d457debfa58eb6f4342956f8b0616d51a89a3659360178e1ab63"
    ),
    # MessageReceived(address,uint32,bytes32,bytes32,uint32,bytes)
    message_received=_hex(
        "0xff48c13eda96b1cceacc6b9edeedc9e9db9d6226afbc30146b720c19d3addb1c"
    ),
    burn_token_topic=1,
)

Log = Dict[str, Any]


def half_bridges(
    network: str, raw_logs: Sequence[Log], included_bridges: Sequence[str]
) -> Tuple[Bridge, ...]:
    """The half bridges among a tx's raw logs (rows of the log table), of the
    bridges in included_bridges."""
    readers = _HALF_BRIDGE_READERS.get(network, {})
    wanted = [read for bridge, read in readers.items() if bridge in included_bridges]
    if not wanted:
        return ()
    logs = sorted(raw_logs, key=lambda log: log["log_index"])
    return tuple(bridge for read in wanted for bridge in read(logs))


def _cctp_half_bridges(logs: List[Log]) -> List[Bridge]:
    """A tx's CCTP burns and mints on eth, in log order."""
    bridges = []
    since = 0  # the position after the previous burn or mint
    for position, log in enumerate(logs):
        topics = log.get("topics")
        key = (bytes(topics[0]), bytes(log["address"])) if topics else None
        event = _CCTP_EVENTS.get(key)
        if event is None:
            continue
        read, version = event
        try:
            bridges.append(
                read(log, logs[since:position], logs[position + 1 :], version)
            )
        except (IndexError, ValueError) as exc:
            logger.warning(
                "CCTP event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
        since = position + 1
    return bridges


def _burn(
    log: Log, before: List[Log], after: List[Log], version: _CctpVersion
) -> Bridge:
    """A DepositForBurn: eth sends, the mint is still to come."""
    topics = log["topics"]
    token = bytes(topics[version.burn_token_topic])[-20:]
    depositor = bytes(topics[version.burn_token_topic + 1])[-20:]
    amount, recipient, domain = _words(log["data"], 3)
    network = _CCTP_NETWORKS.get(domain)
    funding = _transfers(before, token, depositor, version.token_minter, amount)
    return Bridge(
        fromAddress=_address(depositor),
        fromAsset=_address(token),
        fromAmount=amount,
        fromNetwork="eth",
        fromPayment=_leg(log, funding),
        # a named network is EVM: its recipient is the word's low 20 bytes
        toAddress=_address(recipient.to_bytes(32, "big")[-20:]) if network else UNKNOWN,
        toAsset=UNKNOWN,
        toAmount=0,  # less Circle's fee, known when minted
        toNetwork=network or UNKNOWN,
        toPayment=PENDING,
    )


def _mint(
    log: Log, before: List[Log], after: List[Log], version: _CctpVersion
) -> Bridge:
    """A MintAndWithdraw: eth receives what an unknown burn sent."""
    topics = log["topics"]
    recipient = bytes(topics[1])[-20:]
    token = bytes(topics[2])[-20:]
    (amount,) = _words(log["data"], 1)
    received = _first(after, version.message_received, version.message_transmitter)
    domain = _words(received["data"], 1)[0] if received is not None else None
    payout = _transfers(before, token, _ZERO_ADDRESS, recipient, amount)
    return Bridge(
        fromAddress=UNKNOWN,
        fromAsset=UNKNOWN,
        fromAmount=0,
        fromNetwork=_CCTP_NETWORKS.get(domain, UNKNOWN),
        fromPayment=UNKNOWN,
        toAddress=_address(recipient),
        toAsset=_address(token),
        toAmount=amount,
        toNetwork="eth",
        toPayment=_leg(log, payout),
    )


def _words(data: bytes, count: int) -> List[int]:
    """The first `count` 32-byte words of an event's data."""
    data = bytes(data)
    if len(data) < 32 * count:
        raise ValueError(f"{len(data)} bytes of data, {count} words expected")
    return [int.from_bytes(data[32 * i : 32 * (i + 1)], "big") for i in range(count)]


def _transfers(
    logs: List[Log], token: bytes, sender: bytes, receiver: bytes, value: int
) -> List[Log]:
    """The ERC20 Transfers of `value` of `token` from sender to receiver."""
    return [
        log
        for log in logs
        if bytes(log["address"]) == token
        and len(log.get("topics") or ()) == 3
        and bytes(log["topics"][0]) == _TRANSFER
        and bytes(log["topics"][1])[-20:] == sender
        and bytes(log["topics"][2])[-20:] == receiver
        and int.from_bytes(bytes(log["data"]), "big") == value
    ]


def _first(logs: List[Log], topic0: bytes, emitter: bytes) -> Optional[Log]:
    return next(
        (
            log
            for log in logs
            if log.get("topics")
            and bytes(log["topics"][0]) == topic0
            and bytes(log["address"]) == emitter
        ),
        None,
    )


def _leg(event: Log, transfers: List[Log]) -> str:
    """The asset transfer of an event's leg: its one Transfer, or the tx."""
    tx_hash = bytes(event["tx_hash"]).hex()
    if len(transfers) != 1:
        return tx_hash
    return SubTransactionIdentifier(
        tx_hash=tx_hash,
        tx_type=SubTransactionType.ERC20,
        sub_index=transfers[0]["log_index"],
    ).to_string()


def _address(raw: bytes) -> str:
    return "0x" + raw.hex()


# (topic0, emitter) -> the reader of that CCTP event, and its version
_CCTP_EVENTS = {
    **{
        (v.deposit_for_burn, v.token_messenger): (_burn, v)
        for v in (_CCTP_V1, _CCTP_V2)
    },
    **{
        (v.mint_and_withdraw, v.token_messenger): (_mint, v)
        for v in (_CCTP_V1, _CCTP_V2)
    },
}

# network -> bridge -> its half bridges among a tx's logs in log order.
# Another bridge that an external backend pairs is one more entry here.
_HALF_BRIDGE_READERS: Dict[str, Dict[str, Callable[[List[Log]], List[Bridge]]]] = {
    "eth": {CCTP: _cctp_half_bridges},
}

# the included_bridges names of the bridges an external backend pairs
DELEGATED_BRIDGES = tuple(
    sorted({bridge for readers in _HALF_BRIDGE_READERS.values() for bridge in readers})
)
