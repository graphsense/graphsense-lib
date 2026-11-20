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
mint. When none fits a DepositForBurn, the event is not a burn: V1's
replaceDepositForBurn re-emits it for an earlier burn, with its amount and
depositor, and moves nothing. When several fit, the leg is the tx itself.
"""

import logging
import sys
from dataclasses import dataclass
from functools import partial
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _first,
    _leg,
    _transfers,
    _words,
)
from graphsenselib.defi.bridging.halves import across, debridge, layerzero, wormhole
from graphsenselib.defi.bridging.models import Bridge
from graphsenselib.utils.transactions import SubTransactionIdentifier

logger = logging.getLogger(__name__)

CCTP = "cctp"
ACROSS = "across"
DEBRIDGE = "debridge"
LAYERZERO = "layerzero"
WORMHOLE = "wormhole"
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
    # 1 or 2, for the log
    number: int


@dataclass(frozen=True)
class _Skipped:
    """A CCTP event that is no half bridge: the reason code the log carries
    (``unfunded_burn``, ``no_minter_burn``) and what it says in words."""

    reason: str
    what: str


def _hex(s: str) -> bytes:
    return bytes.fromhex(s.removeprefix("0x"))


_CCTP_V1 = _CctpVersion(
    number=1,
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
    number=2,
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


def half_bridges(
    network: str,
    raw_logs: Sequence[Log],
    included_bridges: Sequence[str],
    tx_sender: Optional[str] = None,
) -> Tuple[Bridge, ...]:
    """The half bridges among a tx's raw logs (rows of the log table), of the
    bridges in included_bridges. A half this tx sends names whoever paid its
    leg (_paid_by); tx_sender is the tx's from_address (0x hex)."""
    readers = _HALF_BRIDGE_READERS.get(network, {})
    wanted = [read for bridge, read in readers.items() if bridge in included_bridges]
    if not wanted:
        return ()
    logs = sorted(raw_logs, key=lambda log: log["log_index"])
    return tuple(
        _paid_by(bridge, network, logs, tx_sender)
        for read in wanted
        for bridge in read(logs)
    )


def _paid_by(
    bridge: Bridge, network: str, logs: List[Log], tx_sender: Optional[str]
) -> Bridge:
    """A half this tx sends with fromAddress whoever paid its leg (user decision
    2026-11-18, as the adapter): the sender of its fromPayment Transfer, the
    tx's sender for a bare-tx leg, unknown for an unknown leg. Pathfinder reads
    Input Value off that leg for fromAddress."""
    if bridge.fromNetwork != network:
        return bridge  # the other chain sent it: its own event names the sender
    if bridge.fromPayment == UNKNOWN:
        payer = UNKNOWN
    else:
        leg = SubTransactionIdentifier.from_string(bridge.fromPayment)
        if leg.sub_index is None:
            payer = tx_sender or UNKNOWN
        else:
            paid = next(log for log in logs if log["log_index"] == leg.sub_index)
            payer = "0x" + bytes(paid["topics"][1])[-20:].hex()
    return bridge.model_copy(update={"fromAddress": payer})


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
            bridge = read(log, logs[since:position], logs[position + 1 :], version)
            if isinstance(bridge, _Skipped):
                _log_skipped(log, bridge, version)
            else:
                bridges.append(bridge)
        except (IndexError, ValueError) as exc:
            logger.warning(
                "CCTP event %s of tx %s does not decode: %r",
                log["log_index"],
                bytes(log["tx_hash"]).hex(),
                exc,
            )
        since = position + 1
    return bridges


def _log_skipped(log: Log, skipped: "_Skipped", version: _CctpVersion) -> None:
    """At INFO, with a logfmt tail: the gs-rest side of "did a burn get
    skipped" is answered from the log alone. A diagnostic: a failure in
    writing it is contained, so it never changes the answer."""
    try:
        tx_hash = "0x" + bytes(log["tx_hash"]).hex()
        logger.info(
            "CCTP event %s of tx %s: DepositForBurn %s, not a burn"
            " | event=cctp_half_skipped reason=%s tx=%s log_index=%s"
            " version=%s",
            log["log_index"],
            tx_hash,
            skipped.what,
            skipped.reason,
            tx_hash,
            log["log_index"],
            version.number,
        )
    except Exception as exc:
        if logging.raiseExceptions:
            try:
                sys.stderr.write(f"--- log line (cctp_half_skipped) dropped: {exc!r}\n")
            except Exception:
                pass


def _burn(
    log: Log, before: List[Log], after: List[Log], version: _CctpVersion
) -> Union[Bridge, _Skipped]:
    """A DepositForBurn: eth sends, the mint is still to come. Skipped when
    no Transfer funded it, or the TokenMinter burned nothing: a real burn
    always moves the amount from the depositor to the TokenMinter in the same
    call, but V1's replaceDepositForBurn re-emits the event of an earlier
    burn (same amount, nonce and depositor, maybe a new recipient) without
    moving funds, so it is no bridge. V2 has no replace, but the rule costs
    nothing there."""
    topics = log["topics"]
    token = bytes(topics[version.burn_token_topic])[-20:]
    depositor = bytes(topics[version.burn_token_topic + 1])[-20:]
    amount, recipient, domain = _words(log["data"], 3)
    network = _CCTP_NETWORKS.get(domain)
    funding = _transfers(before, token, depositor, version.token_minter, amount)
    # USDC sent to the TokenMinter in the same tx is not a burn: the minter
    # must also have burned the amount (Transfer to the zero address)
    burned = _transfers(before, token, version.token_minter, bytes(20), amount)
    if not funding:
        return _Skipped("unfunded_burn", "without its funding Transfer")
    if not burned:
        return _Skipped("no_minter_burn", "without the TokenMinter's burn")
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
    "eth": {
        CCTP: _cctp_half_bridges,
        ACROSS: partial(across.read, "eth"),
        DEBRIDGE: partial(debridge.read, "eth"),
        LAYERZERO: partial(layerzero.read, "eth"),
        WORMHOLE: partial(wormhole.read, "eth"),
    },
}

# the included_bridges names of the bridges an external backend pairs
DELEGATED_BRIDGES = tuple(
    sorted({bridge for readers in _HALF_BRIDGE_READERS.values() for bridge in readers})
)
